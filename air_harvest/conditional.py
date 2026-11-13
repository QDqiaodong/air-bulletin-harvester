"""Conditional collection cache for loopback bulletins.

This module adds an opt-in conditional acquisition protocol on top of the
existing append-only collector database.  It is intentionally separate from the
legacy :func:`air_harvest.store.ingest` path:

* the raw caller URL is only used to address a caller-controlled loopback
  fixture; cache identity is the SHA256 of a canonical URL and the raw string is
  never stored, hashed or turned into a path;
* a network request never holds a SQLite write lock.  The request-start
  generation and source digest are captured first, the response is read and
  parsed without a transaction, and only then does a short ``BEGIN IMMEDIATE``
  transaction compare-and-swap the state;
* every accepted ``200`` stores the exact original bytes (BLOB), keeps the
  first accepted source label/row count, and advances the per-URL generation
  by one in that same transaction;
* a ``304`` must answer a conditional request whose cached content, source and
  observations still agree; it never writes anything;
* ``status`` and ``export`` are offline read-only operations that refuse to
  create a missing database and never fabricate bytes for sources stored by the
  older, content-less collector version.

Everything here uses the Python standard library and SQLite.  All data are
synthetic demonstration records; no health or regulatory judgment is made.
"""
from __future__ import annotations

import hashlib
import os
import sqlite3
import tempfile
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path

from .parser import parse_bulletin
from .source import (
    InvalidResponse, StaleResponse, canonical_url, fetch_response, url_identity,
)
from .store import SCHEMA_STATEMENTS, apply_batch

FORMAT_ID = "air_harvest.conditional/v1"
STATUS_FORMAT_ID = "air_harvest.status/v1"
SCHEMA_VERSION = 1

BASE_TABLES = {"sources", "observations"}
NEW_TABLES = {"source_content", "fetch_state"}
ALLOWED_TABLES = BASE_TABLES | NEW_TABLES

BASE_SHAPES = {
    "sources": [
        ("digest", "TEXT", 0, 1),
        ("label", "TEXT", 1, 0),
        ("row_count", "INTEGER", 1, 0),
    ],
    "observations": [
        ("station", "TEXT", 1, 1),
        ("observed_at", "TEXT", 1, 2),
        ("pollutant", "TEXT", 1, 3),
        ("value_ug_m3", "TEXT", 0, 0),
        ("qualifier", "TEXT", 1, 0),
        ("source_digest", "TEXT", 1, 0),
    ],
    "source_content": [
        ("digest", "TEXT", 0, 1),
        ("byte_length", "INTEGER", 1, 0),
        ("raw_bytes", "BLOB", 1, 0),
    ],
    "fetch_state": [
        ("url_key", "TEXT", 0, 1),
        ("url", "TEXT", 1, 0),
        ("source_digest", "TEXT", 1, 0),
        ("generation", "INTEGER", 1, 0),
        ("validator_kind", "TEXT", 0, 0),
        ("validator_value", "TEXT", 0, 0),
        ("last_http_status", "INTEGER", 1, 0),
        ("last_row_count", "INTEGER", 1, 0),
        ("last_inserted", "INTEGER", 1, 0),
        ("last_duplicates", "INTEGER", 1, 0),
    ],
}

NEW_SCHEMA_STATEMENTS = [
    "CREATE TABLE IF NOT EXISTS source_content ("
    " digest TEXT PRIMARY KEY REFERENCES sources(digest),"
    " byte_length INTEGER NOT NULL, raw_bytes BLOB NOT NULL)",
    "CREATE TABLE IF NOT EXISTS fetch_state ("
    " url_key TEXT PRIMARY KEY, url TEXT NOT NULL,"
    " source_digest TEXT NOT NULL REFERENCES sources(digest),"
    " generation INTEGER NOT NULL,"
    " validator_kind TEXT, validator_value TEXT,"
    " last_http_status INTEGER NOT NULL,"
    " last_row_count INTEGER NOT NULL,"
    " last_inserted INTEGER NOT NULL,"
    " last_duplicates INTEGER NOT NULL)",
]


class CollectionError(ValueError):
    """A conditional cache operation is invalid."""


class UnknownSchema(CollectionError):
    """The SQLite file is not a known collector/cache database."""


class CacheCorruption(CollectionError):
    """Stored bytes no longer agree with their digest, source or observations."""


class ExportError(CollectionError):
    """An original-bytes export cannot be performed safely."""


@dataclass(frozen=True)
class RequestStart:
    generation: int
    source_digest: str | None
    validator_kind: str | None
    validator_value: str | None


# --------------------------------------------------------------------------- #
# Connections and structural verification
# --------------------------------------------------------------------------- #
def _ro_uri(database):
    return Path(database).resolve().as_uri() + "?mode=ro"


def open_readonly(database):
    """Open an existing database read-only; never create or modify it."""
    path = Path(database)
    if not path.is_file():
        raise FileNotFoundError(f"database does not exist: {database}")
    connection = sqlite3.connect(_ro_uri(path), uri=True)
    connection.execute("PRAGMA foreign_keys=ON")
    return connection


def _table_names(connection):
    return {row[0] for row in connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table' "
        "AND name NOT LIKE 'sqlite_%'")}


def _check_objects(connection):
    objects = connection.execute(
        "SELECT type, name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' "
        "AND type IN ('table', 'view', 'trigger')").fetchall()
    unknown = sorted(name for kind, name in objects
                     if kind != "table" or name not in ALLOWED_TABLES)
    if unknown:
        raise UnknownSchema(
            "unknown database structure (unexpected object(s)): " + ", ".join(unknown))


def _shape(connection, table):
    return [(name, declared, notnull, pk) for name, declared, notnull, pk
            in connection.execute(
                "SELECT name, type, \"notnull\", pk FROM pragma_table_info(?)",
                (table,))]


def _expected_shape(table):
    return list(BASE_SHAPES[table])


def _verify_table_shape(connection, table):
    actual = _shape(connection, table)
    if actual != _expected_shape(table):
        raise UnknownSchema(f"{table} table has an unexpected shape: {actual}")


def _verify_foreign_keys(connection):
    expected = {
        "observations": ("sources", "source_digest", "digest"),
        "source_content": ("sources", "digest", "digest"),
        "fetch_state": ("sources", "source_digest", "digest"),
    }
    for table, reference in expected.items():
        if table not in _table_names(connection):
            continue
        rows = connection.execute(
            "SELECT \"table\", \"from\", \"to\" FROM pragma_foreign_key_list(?)",
            (table,)).fetchall()
        if not any(tuple(row) == reference for row in rows):
            raise UnknownSchema(
                f"{table} is missing the required foreign key to "
                f"{reference[0]}({reference[2]})")


def _inspect_schema(connection):
    """Validate known structure and return ``(tables, base_present)``."""
    _check_objects(connection)
    tables = _table_names(connection)
    if tables & BASE_TABLES and not BASE_TABLES <= tables:
        raise UnknownSchema("database has only part of the collector schema")
    new_present = tables & NEW_TABLES
    if new_present and new_present != NEW_TABLES:
        raise UnknownSchema("database has only part of the conditional-cache schema")
    for table in sorted(tables):
        _verify_table_shape(connection, table)
    _verify_foreign_keys(connection)
    return tables, BASE_TABLES <= tables


def ensure_schema_in_transaction(connection):
    """Create/verify the cache schema inside an open write transaction."""
    tables, base_present = _inspect_schema(connection)
    if tables and not base_present:
        raise UnknownSchema("database has no collector tables")
    if not base_present:
        for statement in SCHEMA_STATEMENTS:
            connection.execute(statement)
    for statement in NEW_SCHEMA_STATEMENTS:
        connection.execute(statement)
    _inspect_schema(connection)


# --------------------------------------------------------------------------- #
# Validator helpers
# --------------------------------------------------------------------------- #
def conditional_headers(start):
    """Return the request headers that make a 304 legal for ``start``."""
    if start is None or start.generation < 1:
        return {}
    if start.validator_kind == "etag" and start.validator_value is not None:
        return {"If-None-Match": start.validator_value}
    if start.validator_kind == "last_modified" and start.validator_value is not None:
        return {"If-Modified-Since": start.validator_value}
    return {}


def _stored_validator(start):
    if start.validator_kind in ("etag", "last_modified") and start.validator_value:
        return start.validator_value
    return None


def _valid_etag(value):
    if not isinstance(value, str):
        return False
    opaque = value[2:] if value.startswith("W/") else value
    return (len(opaque) >= 2 and opaque[0] == '"' and opaque[-1] == '"')


def _selected_validator(response):
    """ETag wins over Last-Modified; either absent clears the stored value.

    The source layer validates header syntax while parsing, so any value that
    reaches here is already a legal strong or weak entity tag.
    """
    etag = response.validators().etag
    if etag is not None:
        if not _valid_etag(etag):  # defensive: transports may synthesize headers
            raise InvalidResponse(f"malformed ETag response header: {etag!r}")
        return "etag", etag
    last_modified = response.validators().last_modified
    if last_modified is not None:
        return "last_modified", last_modified
    return None, None


def _verify_304_validators(start, response):
    """A 304 validator, if present, must equal the single stored validator.

    Only one validator is ever sent with the conditional request, so any
    returned ETag or Last-Modified that differs proves the response does not
    describe the cached representation.  A 304 that returns neither still
    proves nothing new; the cache agreement check below carries the load.
    """
    stored = _stored_validator(start)
    if stored is None:
        raise InvalidResponse("a 304 response requires a stored validator")
    for value in (response.validators().etag,
                  response.validators().last_modified):
        if value is not None and value != stored:
            raise InvalidResponse(
                "304 validator does not match the stored validator")


# --------------------------------------------------------------------------- #
# Cache verification
# --------------------------------------------------------------------------- #
def _source_row(connection, digest):
    return connection.execute(
        "SELECT digest, label, row_count FROM sources WHERE digest=?",
        (digest,)).fetchone()


def _content_row(connection, digest):
    if "source_content" not in _table_names(connection):
        return None
    return connection.execute(
        "SELECT byte_length, raw_bytes FROM source_content WHERE digest=?",
        (digest,)).fetchone()


def _same_observation(parsed, stored):
    station, instant, pollutant, value, qualifier = parsed
    stored_station, stored_instant, stored_pollutant, stored_value, stored_qualifier = stored
    if (station, instant, pollutant, qualifier) != (
            stored_station, stored_instant, stored_pollutant, stored_qualifier):
        return False
    if value is None or stored_value is None:
        return value is None and stored_value is None
    try:
        return Decimal(value) == Decimal(stored_value)
    except InvalidOperation:
        return False


def verify_source_cache(connection, digest):
    """Validate digest, source, parsed rows and observations for one source.

    Returns ``(source_row, content_bytes)``.  A source whose original bytes
    were never stored (legacy collector) is an error; it is never recreated
    from observations.
    """
    source = _source_row(connection, digest)
    if source is None:
        raise CacheCorruption(f"source {digest} does not exist")
    content = _content_row(connection, digest)
    if content is None:
        raise CacheCorruption(
            f"original bytes for source {digest} were not stored (legacy database); "
            "they cannot be reconstructed")
    byte_length, raw = content
    raw = bytes(raw)
    if len(raw) != byte_length:
        raise CacheCorruption(
            f"stored byte length {byte_length} does not match {len(raw)} bytes")
    if hashlib.sha256(raw).hexdigest() != digest:
        raise CacheCorruption(f"stored bytes do not hash to source digest {digest}")
    try:
        rows = parse_bulletin(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise CacheCorruption(
            f"stored bytes for source {digest} do not parse: {exc}") from exc
    if source[2] != len(rows):
        raise CacheCorruption(
            f"source {digest} row_count {source[2]} does not match {len(rows)} parsed rows")
    parsed_keys = {(station, instant, pollutant): (value, qualifier)
                   for station, instant, pollutant, value, qualifier in rows}
    owned = {(row[0], row[1], row[2]) for row in connection.execute(
        "SELECT station, observed_at, pollutant FROM observations "
        "WHERE source_digest=?", (digest,))}
    stored = connection.execute(
        "SELECT station, observed_at, pollutant, value_ug_m3, qualifier "
        "FROM observations").fetchall()
    for entry in stored:
        key = (entry[0], entry[1], entry[2])
        if key in parsed_keys:
            value, qualifier = parsed_keys[key]
            if not _same_observation((key[0], key[1], key[2], value, qualifier),
                                     entry):
                raise CacheCorruption(
                    f"observation {key} disagrees with the stored source bytes")
        elif key in owned:
            raise CacheCorruption(
                f"observation {key} attributed to source {digest} is absent "
                "from the stored bytes")
    for key, (value, qualifier) in parsed_keys.items():
        match = connection.execute(
            "SELECT station, observed_at, pollutant, value_ug_m3, qualifier "
            "FROM observations WHERE station=? AND observed_at=? AND pollutant=?",
            key).fetchone()
        if match is None or not _same_observation(
                (key[0], key[1], key[2], value, qualifier), match):
            raise CacheCorruption(
                f"parsed row {key} from source {digest} is missing or altered "
                "in observations")
    return source, raw


def verify_state_cache(connection, state_row):
    """Validate one ``fetch_state`` row and its retained cache."""
    url_key, url, digest, generation = state_row[:4]
    if not url_key or not url or not digest:
        raise CacheCorruption("fetch_state row has empty identity fields")
    if not isinstance(generation, int) or generation < 1:
        raise CacheCorruption(f"fetch_state {url_key} has an illegal generation")
    source, raw = verify_source_cache(connection, digest)
    return source, raw


# --------------------------------------------------------------------------- #
# Fetch
# --------------------------------------------------------------------------- #
def _read_start(connection, url_key):
    row = connection.execute(
        "SELECT generation, source_digest, validator_kind, validator_value "
        "FROM fetch_state WHERE url_key=?", (url_key,)).fetchone()
    if row is None:
        return RequestStart(0, None, None, None)
    return RequestStart(*row)


def _read_start_for_database(database, url_key):
    path = Path(database)
    if not path.is_file():
        return RequestStart(0, None, None, None)
    connection = open_readonly(path)
    try:
        tables, base_present = _inspect_schema(connection)
        if not base_present or "fetch_state" not in tables:
            return RequestStart(0, None, None, None)
        return _read_start(connection, url_key)
    finally:
        connection.close()


def _decode_and_parse(body):
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise InvalidResponse("response body is not valid UTF-8") from exc
    try:
        return parse_bulletin(text)
    except ValueError as exc:
        raise InvalidResponse(f"response body violates the bulletin contract: {exc}") from exc


def _start_insert(connection, canonical, url_key, start, digest, validators,
                  rows, inserted):
    generation = start.generation + 1
    fields = (digest, generation, validators[0], validators[1], 200, len(rows),
              inserted, len(rows) - inserted)
    if start.generation == 0:
        connection.execute(
            "INSERT INTO fetch_state (source_digest, generation, "
            "validator_kind, validator_value, last_http_status, "
            "last_row_count, last_inserted, last_duplicates, url, url_key) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (*fields, canonical, url_key))
    else:
        cursor = connection.execute(
            "UPDATE fetch_state SET source_digest=?, generation=?, "
            "validator_kind=?, validator_value=?, last_http_status=?, "
            "last_row_count=?, last_inserted=?, last_duplicates=? "
            "WHERE url_key=? AND generation=?",
            (*fields, url_key, start.generation))
        if cursor.rowcount != 1:
            raise StaleResponse("stale_response: conditional cache update lost")
    return generation


def _commit_200(database, canonical, url_key, start, body, rows, label, validators):
    Path(database).parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(database, isolation_level=None)
    try:
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("BEGIN IMMEDIATE")
        ensure_schema_in_transaction(connection)
        current = _read_start(connection, url_key)
        if (current.generation, current.source_digest) != (
                start.generation, start.source_digest):
            raise StaleResponse(
                "stale_response: cache state advanced while the response was "
                "in flight; the response was not applied")
        digest, inserted = apply_batch(connection, body, rows, label)
        existing = _content_row(connection, digest)
        if existing is None:
            connection.execute(
                "INSERT INTO source_content VALUES (?, ?, ?)",
                (digest, len(body), sqlite3.Binary(body)))
        elif int(existing[0]) != len(body) or bytes(existing[1]) != body:
            raise CacheCorruption(
                f"source {digest} already has different stored bytes")
        generation = _start_insert(connection, canonical, url_key, start,
                                   digest, validators, rows, inserted)
        total = connection.execute(
            "SELECT COUNT(*) FROM observations").fetchone()[0]
        connection.execute("COMMIT")
    except BaseException:
        try:
            connection.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise
    finally:
        connection.close()
    return digest, generation, inserted, total


def _verify_304_state(database, url_key, start):
    """Confirm the cached source, rows and observations all still agree."""
    connection = open_readonly(database)
    try:
        connection.execute("BEGIN")
        connection.execute("SELECT 1")
        current = _read_start(connection, url_key)
        if (current.generation, current.source_digest) != (
                start.generation, start.source_digest):
            raise StaleResponse(
                "stale_response: cache state advanced while the 304 was in flight")
        state = connection.execute(
            "SELECT url_key, url, source_digest, generation, last_http_status, "
            "last_row_count, last_inserted, last_duplicates "
            "FROM fetch_state WHERE url_key=?", (url_key,)).fetchone()
        if state is None or state[3] != current.generation:
            raise StaleResponse(
                "stale_response: cached state changed during verification")
        source, _raw = verify_state_cache(connection, state)
        total = connection.execute(
            "SELECT COUNT(*) FROM observations").fetchone()[0]
        return {
            "result": "unchanged",
            "url": state[1],
            "url_sha256": url_key,
            "http_status": 304,
            "generation": current.generation,
            "source_sha256": current.source_digest,
            "rows": source[2],
            "inserted": 0,
            "duplicates": 0,
            "total": total,
        }
    finally:
        connection.close()


def _unchanged(database, url_key, start, response):
    if not conditional_headers(start):
        raise InvalidResponse("a 304 response requires a conditional request")
    if (start.generation < 1 or start.source_digest is None
            or not _stored_validator(start)):
        raise InvalidResponse(
            "a 304 response requires an existing cached validator")
    if response.body:
        raise InvalidResponse("a 304 response must not carry a body")
    _verify_304_validators(start, response)
    return _verify_304_state(database, url_key, start)


def fetch_url(database, url, label=None, *, transport=None, barriers=None):
    """Conditionally acquire ``url`` into ``database``.

    ``transport`` defaults to :func:`air_harvest.source.fetch_response`; it is
    called as ``transport(url, headers)`` and must return a
    :class:`air_harvest.source.Response`.  ``barriers`` is a test seam mapping
    hook names to callables: ``after_state_read``, ``after_network`` and
    ``after_commit``.
    """
    barriers = barriers or {}
    canonical = canonical_url(url)
    url_key = url_identity(url)
    start = _read_start_for_database(database, url_key)
    hook = barriers.get("after_state_read")
    if hook is not None:
        hook()
    headers = conditional_headers(start)
    transport = transport or fetch_response
    response = transport(url, headers)
    hook = barriers.get("after_network")
    if hook is not None:
        hook()
    if response.status == 304:
        return _unchanged(database, url_key, start, response)
    if response.status != 200:
        raise InvalidResponse(
            f"unexpected HTTP status {response.status}; only 200 and 304 are accepted")
    rows = _decode_and_parse(response.body)
    validators = _selected_validator(response)
    if label is not None and (not isinstance(label, str) or not label):
        raise CollectionError("source label must be a non-empty string")
    default_label = label if label is not None else canonical
    digest, generation, inserted, total = _commit_200(
        database, canonical, url_key, start, response.body, rows,
        default_label, validators)
    hook = barriers.get("after_commit")
    if hook is not None:
        hook()
    return {
        "result": "accepted",
        "url": canonical,
        "url_sha256": url_key,
        "http_status": 200,
        "generation": generation,
        "source_sha256": digest,
        "rows": len(rows),
        "inserted": inserted,
        "duplicates": len(rows) - inserted,
        "total": total,
    }


# --------------------------------------------------------------------------- #
# Status
# --------------------------------------------------------------------------- #
def status_report(database):
    """Return the deterministic, offline status document for ``database``."""
    connection = open_readonly(database)
    try:
        connection.execute("BEGIN")
        connection.execute("SELECT 1")
        tables, base_present = _inspect_schema(connection)
        if not base_present:
            raise UnknownSchema("database does not contain collector tables")
        source_count = connection.execute(
            "SELECT COUNT(*) FROM sources").fetchone()[0]
        observation_count = connection.execute(
            "SELECT COUNT(*) FROM observations").fetchone()[0]
        urls = []
        totals = {"rows": 0, "inserted": 0, "duplicates": 0}
        if "fetch_state" in tables:
            states = connection.execute(
                "SELECT url_key, url, source_digest, generation, "
                "last_http_status, last_row_count, last_inserted, "
                "last_duplicates FROM fetch_state ORDER BY url_key").fetchall()
            for state in states:
                url_key, url, digest, generation, status, row_count, inserted, duplicates = state
                source, raw = verify_state_cache(connection, state)
                if not isinstance(generation, int) or generation < 1:
                    raise CacheCorruption(
                        f"fetch_state {url_key} has an illegal generation")
                if status != 200:
                    raise CacheCorruption(
                        f"fetch_state {url_key} has an unsupported last HTTP status")
                urls.append({
                    "url": url,
                    "url_sha256": url_key,
                    "http_status": status,
                    "generation": generation,
                    "source_sha256": digest,
                    "source_label": source[1],
                    "row_count": source[2],
                    "content_bytes": len(raw),
                    "last_fetch": {
                        "rows": row_count,
                        "inserted": inserted,
                        "duplicates": duplicates,
                    },
                })
                totals["rows"] += row_count
                totals["inserted"] += inserted
                totals["duplicates"] += duplicates
        if "source_content" in tables:
            archived = connection.execute(
                "SELECT COUNT(*) FROM source_content").fetchone()[0]
        else:
            archived = 0
        return {
            "schema_version": SCHEMA_VERSION,
            "report": STATUS_FORMAT_ID,
            "urls": urls,
            "totals": {
                "url_count": len(urls),
                "source_count": source_count,
                "observation_count": observation_count,
                "archived_source_count": archived,
                "unarchived_source_count": source_count - archived,
                **totals,
            },
        }
    finally:
        connection.close()


# --------------------------------------------------------------------------- #
# Export
# --------------------------------------------------------------------------- #
def _guard_output(output, database):
    target = Path(output)
    db_path = Path(database)
    if target.is_symlink():
        raise ExportError(f"output {output}: symbolic-link outputs are not accepted")
    try:
        db_stat = db_path.stat()
    except OSError:
        raise FileNotFoundError(f"database does not exist: {database}")
    if target.exists():
        if not target.is_file():
            raise ExportError(f"output {output}: not a regular file")
        stat_result = target.stat()
        if (stat_result.st_dev, stat_result.st_ino) == (db_stat.st_dev, db_stat.st_ino):
            raise ExportError(f"output {output}: must not overwrite the database")
    elif target.resolve(strict=False) == db_path.resolve(strict=False):
        raise ExportError(f"output {output}: must not overwrite the database")
    parent = target.parent
    if not parent.is_dir():
        raise ExportError(f"output directory does not exist: {parent}")


def _atomic_write_bytes(path, data):
    target = Path(path)
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{target.name}.", suffix=".tmp", dir=target.parent)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, target)
    except BaseException:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise


def export_original(database, output, *, url=None, digest=None):
    """Atomically export the exact stored original bytes of one source.

    Exactly one of ``url`` or ``digest`` selects the entry.  The cache is
    verified before any output is touched; on failure an existing output stays
    byte-for-byte intact and the temporary file is removed.
    """
    if (url is None) == (digest is None):
        raise ExportError("select exactly one of url or digest")
    _guard_output(output, database)
    connection = open_readonly(database)
    try:
        connection.execute("BEGIN")
        connection.execute("SELECT 1")
        tables, base_present = _inspect_schema(connection)
        if not base_present:
            raise UnknownSchema("database does not contain collector tables")
        if url is not None:
            url_key = url_identity(url)
            if "fetch_state" not in tables:
                raise CacheCorruption("no conditional cache exists in this database")
            state = connection.execute(
                "SELECT url_key, url, source_digest, generation, "
                "last_http_status, last_row_count, last_inserted, last_duplicates "
                "FROM fetch_state WHERE url_key=?", (url_key,)).fetchone()
            if state is None:
                raise CacheCorruption(f"no cached source for URL {canonical_url(url)}")
            source, raw = verify_state_cache(connection, state)
            selected = state[2]
        else:
            source, raw = verify_source_cache(connection, digest)
            selected = digest
    finally:
        connection.close()
    _atomic_write_bytes(output, raw)
    return {
        "source_sha256": selected,
        "source_label": source[1],
        "row_count": source[2],
        "byte_length": len(raw),
    }
