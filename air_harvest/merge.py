"""Independent multi-shard merge warehouse for synthetic air bulletins.

The merge reads several existing collector SQLite databases (``shards``) from
independent read-only snapshots, reconciles their observations on the business
key ``station + UTC instant + pollutant``, requires explicit human-supplied
resolutions for every conflicting key, and writes one self-contained SQLite
warehouse that keeps every witness (including losing candidates) and the full
logical snapshot of every input database.

Only the Python standard library and SQLite are used.  Concentrations are
decimal strings compared with :class:`decimal.Decimal`; no ``float`` is ever
produced.  Everything here is original synthetic demo data; the module emits no
health or regulatory judgment.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path, PurePosixPath

FORMAT_ID = "air_harvest.merge/v1"
SNAPSHOT_FORMAT_ID = "air_harvest.merge.shard_snapshot/v1"
SCHEMA_VERSION = 1
POLLUTANTS = ("PM2.5", "PM10", "NO2")
QUALIFIERS = ("measured", "ND")
MAX_VALUE = Decimal("1e9")
MIN_NONZERO = Decimal("1e-12")


class MergeError(ValueError):
    """A manifest, shard snapshot, reconciliation or write step is invalid."""


# --------------------------------------------------------------------------- #
# Small typed records
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class BusinessKey:
    station: str
    observed_at_utc: str
    pollutant: str

    def label(self):
        return f"{self.station}|{self.observed_at_utc}|{self.pollutant}"


@dataclass
class SourceRow:
    digest: str
    label: str
    row_count: int


@dataclass
class ObservationRow:
    station: str
    observed_at: str
    pollutant: str
    value_ug_m3: str | None
    qualifier: str
    source_digest: str


@dataclass
class ShardSnapshot:
    shard_id: str
    rel_path: str
    sources: list[SourceRow]
    observations: list[ObservationRow]
    canonical_json: str
    snapshot_sha256: str


@dataclass
class Witness:
    shard_id: str
    source_digest: str
    raw_observed_at: str
    ordinal: int


@dataclass
class Candidate:
    qualifier: str
    value_ug_m3: str | None  # canonical decimal text, None for ND
    witnesses: list[Witness] = field(default_factory=list)


@dataclass
class KeyFacts:
    key: BusinessKey
    candidates: dict[tuple[str, str | None], Candidate] = field(default_factory=dict)


# --------------------------------------------------------------------------- #
# Exact-value helpers (never float)
# --------------------------------------------------------------------------- #
def canonical_utc_instant(text):
    """Normalize an aware timestamp text to a canonical UTC ``...Z`` string."""
    if not isinstance(text, str) or not text:
        raise MergeError("observed_at must be a non-empty timestamp string")
    try:
        instant = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise MergeError(f"unparseable timestamp {text!r}: {exc}") from exc
    if instant.tzinfo is None:
        raise MergeError(f"timestamp {text!r} is missing a timezone offset")
    instant = instant.astimezone(timezone.utc)
    if instant.microsecond:
        return instant.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"
    return instant.strftime("%Y-%m-%dT%H:%M:%SZ")


def canonical_decimal(text):
    """Return the canonical finite decimal text for a measured concentration."""
    if not isinstance(text, str) or not text:
        raise MergeError("measured concentration must be a decimal string")
    try:
        value = Decimal(text)
    except InvalidOperation as exc:
        raise MergeError(f"invalid decimal concentration {text!r}") from exc
    if not value.is_finite():
        raise MergeError(f"concentration {text!r} is not finite")
    if value < 0:
        raise MergeError(f"concentration {text!r} is negative")
    if value != 0 and value < MIN_NONZERO:
        raise MergeError(f"nonzero concentration {text!r} is below 1e-12")
    if value > MAX_VALUE:
        raise MergeError(f"concentration {text!r} exceeds the 1e9 ug/m3 bound")
    # normalize() collapses 25 / 25.0 / 25.000 to one identity; format('f')
    # avoids scientific notation in the stored canonical text.
    return format(value.normalize(), "f")


def candidate_identity(qualifier, value_text):
    if qualifier == "ND":
        if value_text is not None:
            raise MergeError("ND candidate must carry a null concentration")
        return ("ND", None)
    if value_text is None:
        raise MergeError("measured candidate must carry a decimal concentration")
    return ("measured", canonical_decimal(value_text))


# --------------------------------------------------------------------------- #
# Manifest validation
# --------------------------------------------------------------------------- #
def _is_int(value):
    return isinstance(value, int) and not isinstance(value, bool)


def _exact_object(node, path, allowed):
    if not isinstance(node, dict):
        raise MergeError(f"{path}: expected an object")
    extra = sorted(set(node) - set(allowed))
    if extra:
        raise MergeError(f"{path}: unknown key(s): {', '.join(extra)}")
    missing = sorted(set(allowed) - set(node))
    if missing:
        raise MergeError(f"{path}: missing key(s): {', '.join(missing)}")


def load_manifest(manifest_bytes):
    """Validate manifest bytes and return the manifest dictionary."""
    try:
        manifest = json.loads(manifest_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MergeError(f"manifest: must be valid UTF-8 JSON ({exc})") from exc
    _exact_object(manifest, "manifest",
                  ("schema_version", "shards", "resolutions"))

    version = manifest["schema_version"]
    if not _is_int(version):
        raise MergeError("manifest.schema_version: must be an integer (booleans are not accepted)")
    if version != SCHEMA_VERSION:
        raise MergeError(f"manifest.schema_version: only {SCHEMA_VERSION} is supported")

    shards = manifest["shards"]
    if not isinstance(shards, list) or not shards:
        raise MergeError("manifest.shards: must be a non-empty array")
    seen_ids = set()
    for index, entry in enumerate(shards):
        path = f"manifest.shards[{index}]"
        _exact_object(entry, path, ("shard_id", "path"))
        shard_id = entry["shard_id"]
        if not isinstance(shard_id, str) or not shard_id:
            raise MergeError(f"{path}.shard_id: must be a non-empty string")
        if shard_id != shard_id.strip():
            raise MergeError(f"{path}.shard_id: must not have leading/trailing whitespace")
        if shard_id in seen_ids:
            raise MergeError(f"{path}.shard_id: duplicate shard_id {shard_id!r} (case-sensitive)")
        seen_ids.add(shard_id)
        rel_path = entry["path"]
        if not isinstance(rel_path, str) or not rel_path:
            raise MergeError(f"{path}.path: must be a non-empty relative path string")

    resolutions = manifest["resolutions"]
    if not isinstance(resolutions, list):
        raise MergeError("manifest.resolutions: must be an array")
    for index, entry in enumerate(resolutions):
        path = f"manifest.resolutions[{index}]"
        _exact_object(entry, path, ("station", "observed_at", "pollutant",
                                    "selected_shard", "expected", "reason"))
        for field_name in ("station", "observed_at", "pollutant",
                           "selected_shard", "reason"):
            if not isinstance(entry[field_name], str) or not entry[field_name]:
                raise MergeError(f"{path}.{field_name}: must be a non-empty string")
        reason = entry["reason"]
        if reason != reason.strip():
            raise MergeError(f"{path}.reason: must not have leading/trailing whitespace")
        expected = entry["expected"]
        if not isinstance(expected, list) or not expected:
            raise MergeError(f"{path}.expected: must be a non-empty array")
        expected_shards = set()
        for e_index, item in enumerate(expected):
            epath = f"{path}.expected[{e_index}]"
            _exact_object(item, epath, ("shard_id", "qualifier", "value_ug_m3"))
            if not isinstance(item["shard_id"], str) or not item["shard_id"]:
                raise MergeError(f"{epath}.shard_id: must be a non-empty string")
            if item["shard_id"] in expected_shards:
                raise MergeError(f"{epath}.shard_id: duplicate {item['shard_id']!r} in expected")
            expected_shards.add(item["shard_id"])
            if item["qualifier"] not in QUALIFIERS:
                raise MergeError(f"{epath}.qualifier: must be one of {', '.join(QUALIFIERS)}")
            value = item["value_ug_m3"]
            if value is None:
                if item["qualifier"] != "ND":
                    raise MergeError(f"{epath}: null concentration is only valid for ND")
            else:
                if item["qualifier"] != "measured":
                    raise MergeError(f"{epath}: ND concentration must be null")
                canonical_decimal(value)  # raises with located path context
    return manifest


# --------------------------------------------------------------------------- #
# Filesystem path safety
# --------------------------------------------------------------------------- #
def _validate_relative_path(rel_path, base_dir, path):
    if rel_path.startswith(("/", "\\")) or "\\" in rel_path or "\x00" in rel_path:
        raise MergeError(f"{path}: must be a relative POSIX path, got {rel_path!r}")
    posix = PurePosixPath(rel_path)
    if posix.is_absolute() or any(part == "\x00" for part in posix.parts):
        raise MergeError(f"{path}: absolute paths are not accepted: {rel_path!r}")
    if any(len(part) >= 2 and part[1] == ":" for part in posix.parts):
        raise MergeError(f"{path}: drive-letter paths are not accepted: {rel_path!r}")
    candidate = base_dir / rel_path
    base_resolved = base_dir.resolve()
    resolved = candidate.resolve(strict=False)
    # A literal ".." that stays inside the manifest directory (e.g.
    # "nested/../a.sqlite") is an ordinary relative path; only references that
    # resolve outside the directory are treated as traversal escapes.
    if not resolved.is_relative_to(base_resolved):
        raise MergeError(f"{path}: {rel_path!r} escapes the manifest directory")
    # Reject symlinks at every path component, including the final file.
    walked = base_resolved
    for part in posix.parts:
        walked = walked / part
        if walked.is_symlink():
            raise MergeError(f"{path}: symbolic links are not accepted: {rel_path!r}")
    if not candidate.exists():
        raise MergeError(f"{path}: shard database does not exist: {rel_path}")
    if not candidate.is_file():
        raise MergeError(f"{path}: shard path is not a regular file: {rel_path}")
    return candidate


def resolve_shard_files(manifest, manifest_path):
    """Return ``[(shard_id, rel_path, absolute_path)]`` with safety checks."""
    base_dir = Path(manifest_path).resolve().parent
    resolved = []
    seen_inodes = set()
    manifest_stat = Path(manifest_path).stat()
    seen_inodes.add((manifest_stat.st_dev, manifest_stat.st_ino))
    for index, entry in enumerate(manifest["shards"]):
        path = f"manifest.shards[{index}].path"
        absolute = _validate_relative_path(entry["path"], base_dir, path)
        stat_result = absolute.stat()
        identity = (stat_result.st_dev, stat_result.st_ino)
        if identity in seen_inodes:
            raise MergeError(
                f"{path}: {entry['path']!r} is the same physical file as an earlier "
                "shard or the manifest (hard-link duplicates are rejected)")
        seen_inodes.add(identity)
        resolved.append((entry["shard_id"], entry["path"], absolute))
    return resolved


def guard_output_path(output_path, protected_paths):
    """Reject an output that aliases the manifest or any input shard."""
    output = Path(output_path)
    protected_inodes = set()
    for protected in protected_paths:
        target = Path(protected)
        if target.exists():
            stat_result = target.stat()
            protected_inodes.add((stat_result.st_dev, stat_result.st_ino))
    if output.exists():
        if output.is_symlink():
            raise MergeError(f"output {output}: symbolic-link outputs are not accepted")
        stat_result = output.stat()
        if (stat_result.st_dev, stat_result.st_ino) in protected_inodes:
            raise MergeError(
                f"output {output}: must not overwrite the manifest or an input shard "
                "(hard-link alias detected)")
    parent = output.parent
    if not parent.is_dir():
        raise MergeError(f"output directory does not exist: {parent}")


# --------------------------------------------------------------------------- #
# Input schema verification and single-transaction snapshots
# --------------------------------------------------------------------------- #
def _table_columns(connection, table):
    return connection.execute(
        "SELECT name, type, \"notnull\", pk FROM pragma_table_info(?)",
        (table,)).fetchall()


def verify_shard_schema(connection):
    tables = {row[0] for row in connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    if "sources" not in tables or "observations" not in tables:
        raise MergeError("shard must contain both sources and observations tables")

    def shape(table, expected):
        # Primary-key columns are implicitly NOT NULL even though SQLite's
        # pragma reports notnull=0 for them, so compare (name, type, pk) and
        # require an explicit NOT NULL for non-PK columns.
        columns = _table_columns(connection, table)
        actual = [(name, col_type, pk, 1 if pk else notnull)
                  for name, col_type, notnull, pk in columns]
        if actual != expected:
            raise MergeError(f"{table} table has an unexpected shape: {columns}")

    shape("sources", [
        ("digest", "TEXT", 1, 1),
        ("label", "TEXT", 0, 1),
        ("row_count", "INTEGER", 0, 1),
    ])
    shape("observations", [
        ("station", "TEXT", 1, 1),
        ("observed_at", "TEXT", 2, 1),
        ("pollutant", "TEXT", 3, 1),
        ("value_ug_m3", "TEXT", 0, 0),
        ("qualifier", "TEXT", 0, 1),
        ("source_digest", "TEXT", 0, 1),
    ])

    foreign_keys = connection.execute(
        "SELECT \"table\", \"from\", \"to\", on_update, on_delete FROM pragma_foreign_key_list('observations')"
    ).fetchall()
    if not any(row[0] == "sources" and row[1] == "source_digest" and row[2] == "digest"
               for row in foreign_keys):
        raise MergeError("observations.source_digest must reference sources(digest)")


def _read_shard_rows(connection, shard_id):
    sources = [SourceRow(*row) for row in connection.execute(
        "SELECT digest, label, row_count FROM sources "
        "ORDER BY digest, label, row_count")]
    observations = [ObservationRow(*row) for row in connection.execute(
        "SELECT station, observed_at, pollutant, value_ug_m3, qualifier, source_digest "
        "FROM observations ORDER BY station, observed_at, pollutant")]

    source_digests = {row.digest for row in sources}
    if len(source_digests) != len(sources):
        raise MergeError(f"shard {shard_id!r}: duplicate source digest")
    for source in sources:
        if not source.digest or not isinstance(source.label, str):
            raise MergeError(f"shard {shard_id!r}: invalid sources row {source}")
        if not _is_int(source.row_count) or source.row_count < 0:
            raise MergeError(f"shard {shard_id!r}: row_count must be a nonnegative integer")

    seen_keys = set()
    for ordinal, obs in enumerate(observations):
        location = f"shard {shard_id!r} observation {obs.station}|{obs.observed_at}|{obs.pollutant}"
        if not obs.station or not obs.observed_at or not obs.source_digest:
            raise MergeError(f"{location}: empty key or source_digest")
        if obs.pollutant not in POLLUTANTS:
            raise MergeError(f"{location}: pollutant {obs.pollutant!r} is not legal")
        if obs.qualifier not in QUALIFIERS:
            raise MergeError(f"{location}: qualifier {obs.qualifier!r} is not legal")
        if obs.source_digest not in source_digests:
            raise MergeError(f"{location}: dangling source_digest {obs.source_digest!r}")
        if obs.qualifier == "ND":
            if obs.value_ug_m3 is not None:
                raise MergeError(f"{location}: ND must have a NULL concentration")
        else:
            if obs.value_ug_m3 is None:
                raise MergeError(f"{location}: measured observation needs a concentration")
            try:
                canonical_decimal(obs.value_ug_m3)
            except MergeError as exc:
                raise MergeError(f"{location}: {exc}") from exc
        instant = canonical_utc_instant(obs.observed_at)
        key = BusinessKey(obs.station, instant, obs.pollutant)
        if key in seen_keys:
            raise MergeError(
                f"{location}: duplicate canonical business key {key.label()} "
                "inside one shard (distinct timestamp spellings of one UTC "
                "instant are not accepted as separate facts)")
        seen_keys.add(key)
        obs.ordinal = ordinal
    return sources, observations


def canonical_snapshot_text(shard_id, sources, observations):
    payload = {
        "format": SNAPSHOT_FORMAT_ID,
        "shard_id": shard_id,
        "sources": [
            {"digest": s.digest, "label": s.label, "row_count": s.row_count}
            for s in sources],
        "observations": [
            {"station": o.station, "observed_at": o.observed_at,
             "pollutant": o.pollutant, "value_ug_m3": o.value_ug_m3,
             "qualifier": o.qualifier, "source_digest": o.source_digest}
            for o in observations],
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"))


def capture_snapshot(shard_id, rel_path, absolute_path, barriers=None):
    """Capture all sources and observations from one pinned read snapshot."""
    uri = Path(absolute_path).resolve().as_uri() + "?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    try:
        # Pin one deferred read transaction immediately; every SELECT below
        # shares this snapshot even if a collector commits concurrently.
        connection.execute("BEGIN")
        connection.execute("SELECT 1")
        verify_shard_schema(connection)
        sources, observations = _read_shard_rows(connection, shard_id)
        if barriers is not None and "after_observations" in barriers:
            # Test seam: a controlled concurrent writer may commit here. The
            # pinned snapshot must remain unchanged for the following reads.
            barriers["after_observations"]()
        # Re-read sources through the same snapshot after the barrier.
        sources = [SourceRow(*row) for row in connection.execute(
            "SELECT digest, label, row_count FROM sources "
            "ORDER BY digest, label, row_count")]
        source_digests = {row.digest for row in sources}
        for obs in observations:
            if obs.source_digest not in source_digests:
                raise MergeError(
                    f"shard {shard_id!r}: dangling source_digest {obs.source_digest!r}")
    finally:
        connection.close()

    canonical = canonical_snapshot_text(shard_id, sources, observations)
    return ShardSnapshot(
        shard_id=shard_id, rel_path=rel_path, sources=sources,
        observations=observations, canonical_json=canonical,
        snapshot_sha256=hashlib.sha256(canonical.encode("utf-8")).hexdigest())


# --------------------------------------------------------------------------- #
# Reconciliation
# --------------------------------------------------------------------------- #
def build_model(snapshots):
    """Aggregate snapshots into business keys, candidates and witnesses."""
    model: dict[BusinessKey, KeyFacts] = {}
    witness_count = 0
    for snapshot in snapshots:
        for obs in snapshot.observations:
            instant = canonical_utc_instant(obs.observed_at)
            key = BusinessKey(obs.station, instant, obs.pollutant)
            facts = model.setdefault(key, KeyFacts(key=key))
            identity = candidate_identity(obs.qualifier, obs.value_ug_m3)
            candidate = facts.candidates.get(identity)
            if candidate is None:
                candidate = Candidate(qualifier=identity[0], value_ug_m3=identity[1])
                facts.candidates[identity] = candidate
            witness = Witness(
                shard_id=snapshot.shard_id, source_digest=obs.source_digest,
                raw_observed_at=obs.observed_at, ordinal=obs.ordinal)
            if any(w.shard_id == snapshot.shard_id for w in candidate.witnesses):
                raise MergeError(
                    f"shard {snapshot.shard_id!r} witnesses key {key.label()} twice")
            candidate.witnesses.append(witness)
            witness_count += 1

    for facts in model.values():
        shard_witness = {}
        for candidate in facts.candidates.values():
            for witness in candidate.witnesses:
                if witness.shard_id in shard_witness:
                    raise MergeError(
                        f"key {facts.key.label()}: shard {witness.shard_id!r} has "
                        "more than one observation for the business key")
                shard_witness[witness.shard_id] = candidate
    return model, witness_count


def conflict_keys(model):
    return [key for key, facts in model.items() if len(facts.candidates) > 1]


def validate_resolutions(manifest, model, shard_ids):
    """Validate every manifest resolution against the captured snapshot model."""
    shard_id_set = set(shard_ids)
    validated = {}
    ordered = sorted(
        enumerate(manifest["resolutions"]),
        key=lambda pair: canonical_utc_instant(pair[1]["observed_at"]))
    seen_resolution_keys = set()
    for index, entry in ordered:
        path = f"manifest.resolutions[{index}]"
        if entry["selected_shard"] not in shard_id_set:
            raise MergeError(
                f"{path}.selected_shard: unknown shard {entry['selected_shard']!r}")
        if entry["pollutant"] not in POLLUTANTS:
            raise MergeError(f"{path}.pollutant: {entry['pollutant']!r} is not legal")
        key = BusinessKey(entry["station"],
                          canonical_utc_instant(entry["observed_at"]),
                          entry["pollutant"])
        if key in seen_resolution_keys:
            raise MergeError(f"{path}: duplicate resolution for key {key.label()}")
        seen_resolution_keys.add(key)
        if key not in model:
            raise MergeError(f"{path}: key {key.label()} does not exist in any shard")
        facts = model[key]
        actual = {}
        for identity, candidate in facts.candidates.items():
            for witness in candidate.witnesses:
                actual[witness.shard_id] = identity
        if entry["selected_shard"] not in actual:
            raise MergeError(
                f"{path}.selected_shard: shard {entry['selected_shard']!r} does not "
                f"witness key {key.label()}")
        if len(facts.candidates) < 2:
            raise MergeError(
                f"{path}: key {key.label()} has no conflicting candidates and "
                "must not carry a resolution")

        expected = {}
        for item in entry["expected"]:
            try:
                identity = candidate_identity(item["qualifier"], item["value_ug_m3"])
            except MergeError as exc:
                raise MergeError(f"{path}.expected: {exc}") from exc
            expected[item["shard_id"]] = identity
        if set(expected) != set(actual):
            missing = sorted(set(actual) - set(expected))
            extra = sorted(set(expected) - set(actual))
            raise MergeError(
                f"{path}: expected witness set for {key.label()} does not match the "
                f"captured snapshot (missing shards {missing}, extra shards {extra})")
        for shard_id, identity in actual.items():
            if expected[shard_id] != identity:
                raise MergeError(
                    f"{path}: expected candidate for shard {shard_id!r} on key "
                    f"{key.label()} is {expected[shard_id]}, snapshot holds {identity}")
        validated[key] = entry
    unresolved = sorted((key.label() for key in conflict_keys(model)
                         if key not in validated))
    if unresolved:
        raise MergeError(
            "conflicting keys without an explicit resolution: " + ", ".join(unresolved))
    return validated


# --------------------------------------------------------------------------- #
# Output warehouse schema
# --------------------------------------------------------------------------- #
OUTPUT_SCHEMA = """
CREATE TABLE merge_meta (
    meta_key TEXT PRIMARY KEY,
    meta_value TEXT NOT NULL
);
CREATE TABLE manifest_bytes (
    manifest_sha256 TEXT PRIMARY KEY,
    raw_bytes BLOB NOT NULL
);
CREATE TABLE shards (
    shard_id TEXT PRIMARY KEY,
    rel_path TEXT NOT NULL,
    manifest_ordinal INTEGER NOT NULL,
    snapshot_sha256 TEXT NOT NULL,
    source_count INTEGER NOT NULL,
    observation_count INTEGER NOT NULL
);
CREATE TABLE shard_snapshots (
    shard_id TEXT PRIMARY KEY REFERENCES shards(shard_id),
    snapshot_format TEXT NOT NULL,
    canonical_json TEXT NOT NULL,
    snapshot_sha256 TEXT NOT NULL
);
CREATE TABLE sources (
    shard_id TEXT NOT NULL REFERENCES shards(shard_id),
    digest TEXT NOT NULL,
    label TEXT NOT NULL,
    row_count INTEGER NOT NULL,
    PRIMARY KEY (shard_id, digest)
);
CREATE TABLE business_keys (
    key_id INTEGER PRIMARY KEY,
    station TEXT NOT NULL,
    observed_at_utc TEXT NOT NULL,
    pollutant TEXT NOT NULL,
    UNIQUE (station, observed_at_utc, pollutant)
);
CREATE TABLE candidates (
    candidate_id INTEGER PRIMARY KEY,
    key_id INTEGER NOT NULL REFERENCES business_keys(key_id),
    qualifier TEXT NOT NULL,
    value_ug_m3 TEXT,
    UNIQUE (key_id, qualifier, value_ug_m3)
);
CREATE TABLE witnesses (
    witness_id INTEGER PRIMARY KEY,
    shard_id TEXT NOT NULL REFERENCES shards(shard_id),
    key_id INTEGER NOT NULL REFERENCES business_keys(key_id),
    candidate_id INTEGER NOT NULL REFERENCES candidates(candidate_id),
    source_digest TEXT NOT NULL,
    raw_observed_at TEXT NOT NULL,
    ordinal_in_shard INTEGER NOT NULL,
    UNIQUE (shard_id, key_id),
    FOREIGN KEY (shard_id, source_digest) REFERENCES sources(shard_id, digest)
);
CREATE TABLE resolutions (
    resolution_id INTEGER PRIMARY KEY,
    key_id INTEGER NOT NULL UNIQUE REFERENCES business_keys(key_id),
    selected_shard_id TEXT NOT NULL REFERENCES shards(shard_id),
    reason TEXT NOT NULL
);
CREATE TABLE resolution_expected (
    resolution_id INTEGER NOT NULL REFERENCES resolutions(resolution_id),
    shard_id TEXT NOT NULL REFERENCES shards(shard_id),
    qualifier TEXT NOT NULL,
    value_ug_m3 TEXT,
    PRIMARY KEY (resolution_id, shard_id)
);
CREATE TABLE final_facts (
    key_id INTEGER PRIMARY KEY REFERENCES business_keys(key_id),
    candidate_id INTEGER NOT NULL REFERENCES candidates(candidate_id),
    resolution_id INTEGER REFERENCES resolutions(resolution_id),
    is_conflict INTEGER NOT NULL CHECK (is_conflict IN (0, 1))
);
CREATE INDEX idx_candidates_key ON candidates(key_id);
CREATE INDEX idx_witnesses_key ON witnesses(key_id);
CREATE INDEX idx_witnesses_candidate ON witnesses(candidate_id);
CREATE INDEX idx_witnesses_shard ON witnesses(shard_id);
CREATE INDEX idx_sources_digest ON sources(digest);
CREATE VIEW v_final_fact_trace AS
SELECT k.key_id, k.station, k.observed_at_utc, k.pollutant,
       c.qualifier AS final_qualifier, c.value_ug_m3 AS final_value_ug_m3,
       ff.is_conflict, ff.resolution_id,
       w.shard_id, w.source_digest, w.raw_observed_at,
       CASE WHEN w.candidate_id = ff.candidate_id THEN 'supporter'
            ELSE 'loser' END AS witness_role
FROM final_facts ff
JOIN business_keys k ON k.key_id = ff.key_id
JOIN candidates c ON c.candidate_id = ff.candidate_id
JOIN witnesses w ON w.key_id = ff.key_id;
CREATE VIEW v_conflicts AS
SELECT k.key_id, k.station, k.observed_at_utc, k.pollutant,
       (SELECT count(*) FROM candidates c2 WHERE c2.key_id = k.key_id) AS candidate_count,
       r.resolution_id, r.selected_shard_id, r.reason
FROM business_keys k
JOIN final_facts ff ON ff.key_id = k.key_id AND ff.is_conflict = 1
LEFT JOIN resolutions r ON r.key_id = k.key_id;
"""


def _open_temp_database(output_path):
    target = Path(output_path)
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{target.name}.", suffix=".tmp", dir=target.parent)
    os.close(descriptor)
    return temporary


def write_merged_database(output_path, manifest_bytes, manifest_sha, snapshots,
                          model, validated_resolutions):
    """Build the warehouse in a private temp file, then atomically replace output."""
    temporary = _open_temp_database(output_path)
    try:
        connection = sqlite3.connect(temporary)
        try:
            connection.execute("PRAGMA foreign_keys=ON")
            connection.executescript(OUTPUT_SCHEMA)
            with connection:  # one writer transaction for the full load
                connection.execute(
                    "INSERT INTO merge_meta(meta_key, meta_value) VALUES (?, ?)",
                    ("format", FORMAT_ID))
                connection.execute(
                    "INSERT INTO merge_meta(meta_key, meta_value) VALUES (?, ?)",
                    ("schema_version", str(SCHEMA_VERSION)))
                connection.execute(
                    "INSERT INTO merge_meta(meta_key, meta_value) VALUES (?, ?)",
                    ("manifest_sha256", manifest_sha))
                connection.execute(
                    "INSERT INTO merge_meta(meta_key, meta_value) VALUES (?, ?)",
                    ("manifest_label", "manifest.json"))
                connection.execute(
                    "INSERT INTO manifest_bytes VALUES (?, ?)",
                    (manifest_sha, sqlite3.Binary(manifest_bytes)))

                ordinal_by_shard = {s.shard_id: index
                                    for index, s in enumerate(snapshots)}
                for snapshot in snapshots:
                    connection.execute(
                        "INSERT INTO shards VALUES (?, ?, ?, ?, ?, ?)",
                        (snapshot.shard_id, snapshot.rel_path,
                         ordinal_by_shard[snapshot.shard_id],
                         snapshot.snapshot_sha256, len(snapshot.sources),
                         len(snapshot.observations)))
                    connection.execute(
                        "INSERT INTO shard_snapshots VALUES (?, ?, ?, ?)",
                        (snapshot.shard_id, SNAPSHOT_FORMAT_ID,
                         snapshot.canonical_json, snapshot.snapshot_sha256))
                    for source in snapshot.sources:
                        connection.execute(
                            "INSERT INTO sources VALUES (?, ?, ?, ?)",
                            (snapshot.shard_id, source.digest, source.label,
                             source.row_count))

                sorted_keys = sorted(model, key=lambda k: (k.station, k.observed_at_utc,
                                                           k.pollutant))
                key_ids = {key: index for index, key in enumerate(sorted_keys, start=1)}
                for key_id, key in enumerate(sorted_keys, start=1):
                    connection.execute(
                        "INSERT INTO business_keys VALUES (?, ?, ?, ?)",
                        (key_id, key.station, key.observed_at_utc, key.pollutant))
                candidate_ids = {}
                for key in sorted_keys:
                    facts = model[key]
                    ordered_candidates = sorted(
                        facts.candidates.values(),
                        key=lambda c: (c.qualifier, c.value_ug_m3 or ""))
                    for candidate in ordered_candidates:
                        cursor = connection.execute(
                            "INSERT INTO candidates(key_id, qualifier, value_ug_m3) "
                            "VALUES (?, ?, ?)",
                            (key_ids[key], candidate.qualifier, candidate.value_ug_m3))
                        candidate_ids[(key, candidate.qualifier,
                                       candidate.value_ug_m3)] = cursor.lastrowid

                witness_id = 0
                for key in sorted_keys:
                    facts = model[key]
                    for candidate in sorted(facts.candidates.values(),
                                            key=lambda c: (c.qualifier,
                                                           c.value_ug_m3 or "")):
                        for witness in sorted(candidate.witnesses,
                                              key=lambda w: w.shard_id):
                            witness_id += 1
                            connection.execute(
                                "INSERT INTO witnesses VALUES (?, ?, ?, ?, ?, ?, ?)",
                                (witness_id, witness.shard_id, key_ids[key],
                                 candidate_ids[(key, candidate.qualifier,
                                                candidate.value_ug_m3)],
                                 witness.source_digest, witness.raw_observed_at,
                                 witness.ordinal))

                resolution_ids = {}
                for resolution_index, key in enumerate(
                        sorted(validated_resolutions), start=1):
                    entry = validated_resolutions[key]
                    cursor = connection.execute(
                        "INSERT INTO resolutions(key_id, selected_shard_id, reason) "
                        "VALUES (?, ?, ?)",
                        (key_ids[key], entry["selected_shard"], entry["reason"]))
                    resolution_id = cursor.lastrowid
                    resolution_ids[key] = resolution_id
                    for item in sorted(entry["expected"],
                                       key=lambda item: item["shard_id"]):
                        identity = candidate_identity(item["qualifier"],
                                                      item["value_ug_m3"])
                        connection.execute(
                            "INSERT INTO resolution_expected VALUES (?, ?, ?, ?)",
                            (resolution_id, item["shard_id"], identity[0], identity[1]))

                for key in sorted_keys:
                    facts = model[key]
                    is_conflict = len(facts.candidates) > 1
                    if is_conflict:
                        entry = validated_resolutions[key]
                        selected_identity = next(
                            identity for identity, candidate in facts.candidates.items()
                            if any(w.shard_id == entry["selected_shard"]
                                   for w in candidate.witnesses))
                        resolution_id = resolution_ids[key]
                    else:
                        selected_identity = next(iter(facts.candidates))
                        resolution_id = None
                    connection.execute(
                        "INSERT INTO final_facts VALUES (?, ?, ?, ?)",
                        (key_ids[key], candidate_ids[(key, *selected_identity)],
                         resolution_id, 1 if is_conflict else 0))

            foreign_key_violations = connection.execute(
                "PRAGMA foreign_key_check").fetchall()
            if foreign_key_violations:
                raise MergeError(
                    f"output warehouse failed foreign key checks: {foreign_key_violations}")
            integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
            if integrity != "ok":
                raise MergeError(f"output warehouse integrity check failed: {integrity}")
        finally:
            connection.close()
        os.replace(temporary, output_path)
    except BaseException:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #
def run_merge(manifest_path, output_path, barriers=None):
    """Validate, snapshot, reconcile and publish one merge warehouse."""
    manifest_path = Path(manifest_path)
    manifest_bytes = manifest_path.read_bytes()
    manifest_sha = hashlib.sha256(manifest_bytes).hexdigest()
    manifest = load_manifest(manifest_bytes)
    shard_files = resolve_shard_files(manifest, manifest_path)
    guard_output_path(output_path, [manifest_path,
                                    *(absolute for _, _, absolute in shard_files)])

    snapshots = [
        capture_snapshot(shard_id, rel_path, absolute, barriers=barriers)
        for shard_id, rel_path, absolute in shard_files
    ]
    model, witness_count = build_model(snapshots)
    validated = validate_resolutions(
        manifest, model, [shard_id for shard_id, _, _ in shard_files])

    write_merged_database(output_path, manifest_bytes, manifest_sha, snapshots,
                          model, validated)

    conflicts = conflict_keys(model)
    distinct_candidates = sum(len(facts.candidates) for facts in model.values())
    return {
        "format": FORMAT_ID,
        "schema_version": SCHEMA_VERSION,
        "shard_count": len(snapshots),
        "input_witness_count": witness_count,
        "business_key_count": len(model),
        "distinct_candidate_count": distinct_candidates,
        "conflict_key_count": len(conflicts),
        "resolved_key_count": len(validated),
        "final_fact_count": len(model),
        "manifest_sha256": manifest_sha,
        "snapshot_sha256": {s.shard_id: s.snapshot_sha256 for s in snapshots},
    }


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Merge independent collector SQLite shards into one warehouse")
    parser.add_argument("--manifest", required=True, type=Path,
                        help="UTF-8 JSON merge manifest")
    parser.add_argument("--output", required=True, type=Path,
                        help="new self-contained SQLite warehouse path")
    args = parser.parse_args(argv)
    try:
        summary = run_merge(args.manifest, args.output)
    except (MergeError, FileNotFoundError, OSError, sqlite3.Error) as exc:
        parser.exit(2, f"merge failed: {exc}\n")
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
