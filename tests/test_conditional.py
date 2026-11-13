"""End-to-end tests for the conditional collection cache.

Most tests run a real loopback HTTP server on a temporary port; a few use a
fake transport or the real CLI.  They cover validator handling, cache
verification, compare-and-swap concurrency (including cold start), rollback,
status/export write safety and the legacy interfaces.  All HTML is synthetic.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
import unittest.mock
from html import escape
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from air_harvest.conditional import (
    CacheCorruption, ExportError, StaleResponse, UnknownSchema, export_original,
    fetch_url, status_report,
)
from air_harvest.source import Response, canonical_url, url_identity
from air_harvest.store import ingest

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "data" / "bulletin.html"
HEADERS = ("station", "observed_at", "pollutant", "value", "unit", "qualifier")
TABLES = ("sources", "observations", "source_content", "fetch_state")


def bulletin(rows):
    head = ('<table id="observations"><tr>' +
            "".join(f"<th>{h}</th>" for h in HEADERS) + "</tr>")
    body = "".join(
        "<tr>" + "".join(f"<td>{escape(str(cell))}</td>" for cell in row) + "</tr>"
        for row in rows)
    return ("<!doctype html><meta charset=\"utf-8\">" + head + body +
            "</table>").encode("utf-8")


def rows_v1():
    return [
        ("River & Park", "2026-01-01T08:00:00+08:00", "PM2.5", "25",
         "ug/m3", "measured"),
        ("Hill", "2026-01-01T00:00:00Z", "NO2", "", "ug/m3", "ND"),
        ("Hill", "2026-01-01T01:00:00Z", "PM10", "0", "ug/m3", "measured"),
    ]


def rows_v2():
    # Adds one new key and repeats two existing equivalent keys; the collector
    # never corrects an existing fact, so no value may change.
    return [
        ("Hill", "2026-01-01T00:00:00Z", "NO2", "", "ug/m3", "ND"),
        ("Hill", "2026-01-01T02:00:00Z", "PM10", "5", "ug/m3", "measured"),
        ("River & Park", "2026-01-01T00:00:00Z", "PM2.5", "25",
         "ug/m3", "measured"),
    ]


class Server:
    """A tiny loopback fixture whose response can change between calls."""

    def __init__(self):
        self.body = bulletin(rows_v1())
        self.etag = '"v1"'
        self.last_modified = None
        self.status = 200
        self.requests = []
        self.override = None
        server = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_GET(self):
                server.requests.append({
                    "path": self.path,
                    "headers": {k.lower(): v for k, v in self.headers.items()},
                })
                try:
                    if server.override is not None:
                        server.override(self)
                        return
                    if self.path.split("?")[0] != "/bulletin":
                        self.send_error(404)
                        return
                    self.send_response(server.status)
                    if server.etag is not None:
                        self.send_header("ETag", server.etag)
                    if server.last_modified is not None:
                        self.send_header("Last-Modified", server.last_modified)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header("Content-Length", str(len(server.body)))
                    self.end_headers()
                    if server.status != 304:
                        self.wfile.write(server.body)
                except (BrokenPipeError, ConnectionResetError):
                    pass

            def log_message(self, *args):
                return

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.httpd.daemon_threads = True
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    @property
    def url(self):
        return f"http://127.0.0.1:{self.httpd.server_port}/bulletin"

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join()


class ServerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.dir = Path(self.temp.name)
        self.db = self.dir / "air.sqlite"
        self.server = Server()
        self.addCleanup(self.server.close)

    def _fetch(self, url=None, **kwargs):
        return fetch_url(self.db, url or self.server.url, **kwargs)

    def connection(self):
        connection = sqlite3.connect(self.db)
        connection.row_factory = sqlite3.Row
        self.addCleanup(connection.close)
        return connection

    def snapshot(self):
        connection = sqlite3.connect(self.db)
        try:
            return {table: connection.execute(
                "SELECT * FROM " + table).fetchall() for table in TABLES}
        finally:
            connection.close()

    # ------------------------------------------------------------------ #
    # URL isolation and transport contract
    # ------------------------------------------------------------------ #
    def test_01_url_isolation_and_canonical_identity(self):
        for raw in ("https://127.0.0.1/x", "http://example.com/x",
                    "http://user:secret@127.0.0.1/x", "http://127.0.0.1/x#frag",
                    "ftp://127.0.0.1/x", "not a url", "http://127.0.0.1:99999/x"):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                fetch_url(self.db, raw)
        self.assertFalse(self.db.exists())

        # Different spellings of one logical URL share a cache entry, even
        # though the raw strings differ.  A fake transport avoids a DNS or
        # port dependency while still driving the real cache path.
        self.server.body = bulletin(rows_v1())

        def transport(url, headers):
            return Response(200, (("ETag", '"same"'),), bulletin(rows_v1()))

        first = fetch_url(self.db, "HTTP://LocalHost/bulletin", transport=transport)
        second = fetch_url(self.db, "http://localhost:80/bulletin",
                           transport=transport)
        self.assertEqual(first["url"], "http://localhost:80/bulletin")
        self.assertEqual(second["url"], "http://localhost:80/bulletin")
        self.assertEqual(first["url_sha256"], second["url_sha256"])
        self.assertEqual(second["generation"], 2)
        # The raw caller string is never stored as content, path or label.
        raw_text = self.db.read_bytes().decode("latin-1")
        self.assertNotIn("HTTP://LocalHost", raw_text)
        # Port and path are part of cache identity.
        self.assertNotEqual(
            url_identity("http://127.0.0.1:9/a"),
            url_identity("http://127.0.0.1:9/b"))
        self.assertEqual(
            url_identity("http://LOCALHOST:80/x"), url_identity("http://localhost/x"))

    def test_02_non_200_and_unconditional_304_are_rejected(self):
        self.server.status = 404
        with self.assertRaisesRegex(ValueError, "unexpected HTTP status"):
            self._fetch()
        self.assertFalse(self.db.exists())

        self.server.status = 302
        with self.assertRaisesRegex(ValueError, "unexpected HTTP status"):
            self._fetch()
        self.assertFalse(self.db.exists())

        # A 304 answering an unconditional request is never accepted.
        self.server.status = 304
        self.server.body = b""
        with self.assertRaisesRegex(ValueError, "304"):
            self._fetch()
        self.assertFalse(self.db.exists())

        # A 304 with a body is rejected even after a cached 200 exists.
        self.server.status = 200
        self.server.body = bulletin(rows_v1())
        fetch_url(self.db, self.server.url)
        self.server.status = 304
        self.server.body = b"unexpected"
        with self.assertRaisesRegex(ValueError, "304"):
            self._fetch()
        self.assertEqual(self.connection().execute(
            "SELECT generation FROM fetch_state").fetchone()[0], 1)

        # A 200 with a non-UTF-8 charset declaration is rejected.
        self.server.status = 200
        self.server.body = bulletin(rows_v1())

        def latin(request):
            request.send_response(200)
            request.send_header("Content-Type", "text/html; charset=iso-8859-1")
            request.send_header("Content-Length", str(len(self.server.body)))
            request.end_headers()
            request.wfile.write(self.server.body)

        self.server.override = latin
        with self.assertRaisesRegex(ValueError, "charset"):
            self._fetch()
        self.server.override = None

    def test_03_redirect_and_body_limit(self):
        self.server.status = 301
        with self.assertRaisesRegex(ValueError, "unexpected HTTP status"):
            self._fetch()
        self.assertFalse(self.db.exists())

        self.server.status = 200
        self.server.body = b"x" * (2 * 1024 * 1024 + 1)
        with self.assertRaisesRegex(ValueError, "limit"):
            self._fetch()
        self.assertFalse(self.db.exists())

    def test_04_short_read_bad_encoding_and_length_mismatch(self):
        self.server.status = 200
        self.server.body = b"\xff\xfeinvalid utf-8"
        with self.assertRaisesRegex(ValueError, "UTF-8"):
            self._fetch()
        self.assertFalse(self.db.exists())

        body = bulletin(rows_v1())
        self.server.body = body

        def short(request):
            request.close_connection = True
            request.send_response(200)
            request.send_header("ETag", '"short"')
            request.send_header("Content-Type", "text/html; charset=utf-8")
            request.send_header("Content-Length", str(len(body) + 10))
            request.end_headers()
            request.wfile.write(body)
            request.wfile.flush()
            self.server.httpd.shutdown_request(request.connection)

        self.server.override = short
        with self.assertRaisesRegex(ValueError, "short read"):
            self._fetch()
        self.assertFalse(self.db.exists())

        # A declared Content-Length over the bound fails before any read.
        def huge(request):
            request.close_connection = True
            request.send_response(200)
            request.send_header("Content-Length", str(2 * 1024 * 1024 + 1))
            request.end_headers()

        self.server.override = huge
        with self.assertRaisesRegex(ValueError, "limit"):
            self._fetch()
        self.assertFalse(self.db.exists())
        self.server.override = None

    # ------------------------------------------------------------------ #
    # 200 acceptance, provenance, generation and exact bytes
    # ------------------------------------------------------------------ #
    def test_05_acceptance_semantics_and_original_bytes(self):
        body = bulletin(rows_v1())
        self.server.body = body
        result = fetch_url(self.db, self.server.url, label="fixture-v1")
        self.assertEqual(result["result"], "accepted")
        self.assertEqual((result["rows"], result["inserted"],
                          result["duplicates"], result["generation"]),
                         (3, 3, 0, 1))
        self.assertEqual(result["source_sha256"], hashlib.sha256(body).hexdigest())

        db = self.connection()
        source = db.execute("SELECT * FROM sources").fetchone()
        self.assertEqual((source["digest"], source["label"], source["row_count"]),
                         (result["source_sha256"], "fixture-v1", 3))
        content = db.execute("SELECT * FROM source_content").fetchone()
        self.assertEqual((content["byte_length"], bytes(content["raw_bytes"])),
                         (len(body), body))
        state = db.execute("SELECT * FROM fetch_state").fetchone()
        self.assertEqual((state["url_key"], state["generation"],
                          state["validator_kind"], state["validator_value"]),
                         (result["url_sha256"], 1, "etag", '"v1"'))
        values = dict(db.execute(
            "SELECT station||'|'||pollutant, value_ug_m3 FROM observations")
            .fetchall())
        self.assertEqual(values["River & Park|PM2.5"], "25")
        self.assertIsNone(values["Hill|NO2"])
        self.assertEqual(values["Hill|PM10"], "0")

        # Replay of identical content: new generation, first source preserved.
        second = fetch_url(self.db, self.server.url, label="fixture-again")
        self.assertEqual(second["generation"], 2)
        self.assertEqual((second["inserted"], second["duplicates"]), (0, 3))
        db = self.connection()
        self.assertEqual(db.execute(
            "SELECT label FROM sources").fetchall()[0]["label"], "fixture-v1")

    def test_05b_changed_content_bumps_generation_and_keeps_history(self):
        self.server.body = bulletin(rows_v1())
        fetch_url(self.db, self.server.url)
        self.server.body = bulletin(rows_v2())
        self.server.etag = '"v2"'
        result = fetch_url(self.db, self.server.url)
        self.assertEqual(result["generation"], 2)
        self.assertEqual((result["inserted"], result["duplicates"]), (1, 2))
        db = self.connection()
        self.assertEqual(db.execute(
            "SELECT COUNT(*) FROM source_content").fetchone()[0], 2)
        state = db.execute("SELECT * FROM fetch_state").fetchone()
        self.assertEqual(state["validator_value"], '"v2"')
        # First accepted source still owns the shared pre-existing keys.
        pm10 = db.execute(
            "SELECT source_digest, value_ug_m3 FROM observations "
            "WHERE station='Hill' AND pollutant='PM10'").fetchone()
        self.assertEqual(pm10["value_ug_m3"], "0")

    def test_06_validator_priority_and_clearing(self):
        self.server.last_modified = "Wed, 01 Jan 2026 00:00:00 GMT"
        fetch_url(self.db, self.server.url)
        self.assertEqual(
            self.connection().execute(
                "SELECT validator_kind, validator_value FROM fetch_state")
            .fetchone()[:], ("etag", '"v1"'))

        # Next 200 advertises only Last-Modified, so ETag is cleared.
        self.server.etag = None
        self.server.last_modified = "Fri, 02 Jan 2026 00:00:00 GMT"
        fetch_url(self.db, self.server.url)
        self.assertEqual(
            self.connection().execute(
                "SELECT validator_kind, validator_value FROM fetch_state")
            .fetchone()[:],
            ("last_modified", "Fri, 02 Jan 2026 00:00:00 GMT"))

        # No validators at all clears both fields.
        self.server.last_modified = None
        fetch_url(self.db, self.server.url)
        self.assertEqual(
            self.connection().execute(
                "SELECT validator_kind, validator_value FROM fetch_state")
            .fetchone()[:], (None, None))
        # Without a stored validator no conditional request is possible.
        self.server.status = 304
        self.server.body = b""
        with self.assertRaisesRegex(ValueError, "304"):
            fetch_url(self.db, self.server.url)

        # A weak ETag is retained verbatim (including the W/ prefix).
        self.server.status = 200
        self.server.body = bulletin(rows_v1())
        self.server.etag = 'W/"weak-1"'
        fetch_url(self.db, self.server.url)
        state = self.connection().execute("SELECT * FROM fetch_state").fetchone()
        self.assertEqual((state["validator_kind"], state["validator_value"]),
                         ("etag", 'W/"weak-1"'))
        self.server.status = 304
        self.server.body = b""
        result = fetch_url(self.db, self.server.url)
        self.assertEqual(result["result"], "unchanged")
        self.assertEqual(
            self.server.requests[-1]["headers"].get("if-none-match"), 'W/"weak-1"')

    # ------------------------------------------------------------------ #
    # 304 handling
    # ------------------------------------------------------------------ #
    def test_07_304_returns_unchanged_without_writing(self):
        self.server.body = bulletin(rows_v1())
        fetch_url(self.db, self.server.url)
        before = self.snapshot()
        self.server.status = 304
        self.server.body = b""
        result = fetch_url(self.db, self.server.url)
        self.assertEqual((result["result"], result["http_status"],
                          result["generation"], result["duplicates"]),
                         ("unchanged", 304, 1, 0))
        self.assertEqual(self.server.requests[-1]["headers"].get("if-none-match"),
                         '"v1"')
        self.assertEqual(before, self.snapshot())

    def test_08_304_requires_agreement_with_cached_cache(self):
        self.server.body = bulletin(rows_v1())
        fetch_url(self.db, self.server.url)
        with sqlite3.connect(self.db) as writer:
            writer.execute(
                "UPDATE observations SET value_ug_m3='26.000' "
                "WHERE station='River & Park' AND pollutant='PM2.5'")
        self.server.status = 304
        self.server.body = b""
        with self.assertRaises(CacheCorruption):
            fetch_url(self.db, self.server.url)
        self.assertEqual(self.snapshot(), self.snapshot())  # still open
        with sqlite3.connect(self.db) as rest:
            self.assertEqual(rest.execute(
                "SELECT value_ug_m3 FROM observations WHERE station='River & Park' "
                "AND pollutant='PM2.5'").fetchone()[0], "26.000")
            rest.execute(
                "UPDATE observations SET value_ug_m3='25.000' "
                "WHERE station='River & Park' AND pollutant='PM2.5'")
            rest.execute("UPDATE source_content SET raw_bytes=?",
                         (sqlite3.Binary(b"bad"),))
        with self.assertRaises(CacheCorruption):
            fetch_url(self.db, self.server.url)

        # A 304 whose validator disagrees with the stored one is rejected.
        with sqlite3.connect(self.db) as writer:
            writer.execute("UPDATE source_content SET raw_bytes=?",
                           (sqlite3.Binary(bulletin(rows_v1())),))
        self.server.etag = '"different"'
        with self.assertRaisesRegex(ValueError, "304 validator"):
            fetch_url(self.db, self.server.url)
        self.assertEqual(
            self.connection().execute(
                "SELECT generation FROM fetch_state").fetchone()[0], 1)

    # ------------------------------------------------------------------ #
    # Cache verification / legacy databases
    # ------------------------------------------------------------------ #
    def test_09_legacy_database_has_no_original_bytes_and_is_refused(self):
        ingest(FIXTURE.read_bytes(), self.db, "legacy")
        payload = status_report(self.db)
        self.assertEqual(payload["urls"], [])
        self.assertEqual(payload["totals"]["url_count"], 0)
        self.assertEqual(payload["totals"]["source_count"], 1)
        self.assertEqual(payload["totals"]["archived_source_count"], 0)
        self.assertEqual(payload["totals"]["unarchived_source_count"], 1)
        digest = sqlite3.connect(self.db).execute(
            "SELECT digest FROM sources").fetchone()[0]
        output = self.dir / "out.html"
        with self.assertRaisesRegex(CacheCorruption, "cannot be reconstructed"):
            export_original(self.db, output, digest=digest)
        self.assertFalse(output.exists())
        # Fetch may add the cache schema but never fabricates old bytes.
        self.server.body = bulletin(rows_v1())
        result = fetch_url(self.db, self.server.url)
        self.assertEqual(result["result"], "accepted")
        payload = status_report(self.db)
        self.assertEqual(payload["totals"]["source_count"], 2)
        self.assertEqual(payload["totals"]["archived_source_count"], 1)
        self.assertEqual(payload["totals"]["unarchived_source_count"], 1)

    def test_10_unknown_schema_is_rejected(self):
        with sqlite3.connect(self.db) as db:
            db.execute("CREATE TABLE mystery (x TEXT)")
        with self.assertRaises(UnknownSchema):
            status_report(self.db)
        with self.assertRaises(UnknownSchema):
            export_original(self.db, self.dir / "out.html", digest="0" * 64)
        with self.assertRaises(UnknownSchema):
            fetch_url(self.db, self.server.url)
        # A wrong-shape sources table is likewise unknown, not silently used.
        other = self.dir / "shape.sqlite"
        with sqlite3.connect(other) as db:
            db.execute("CREATE TABLE sources (digest TEXT PRIMARY KEY, label TEXT)")
            db.execute("CREATE TABLE observations (x TEXT)")
        with self.assertRaises(UnknownSchema):
            status_report(other)

    # ------------------------------------------------------------------ #
    # Concurrency, CAS and rollback
    # ------------------------------------------------------------------ #
    def test_11_cold_start_also_uses_compare_and_swap(self):
        self.server.body = bulletin(rows_v1())

        def concurrent():
            fetch_url(self.db, self.server.url, label="winner")

        with self.assertRaisesRegex(StaleResponse, "stale_response"):
            fetch_url(self.db, self.server.url,
                      barriers={"after_state_read": concurrent})
        db = self.connection()
        self.assertEqual(db.execute(
            "SELECT generation FROM fetch_state").fetchone()[0], 1)
        self.assertEqual(db.execute(
            "SELECT label FROM sources").fetchall()[0]["label"], "winner")
        self.assertEqual(db.execute(
            "SELECT COUNT(*) FROM observations").fetchone()[0], 3)

    def test_12_late_200_reports_stale_response_without_retry(self):
        self.server.body = bulletin(rows_v1())
        fetch_url(self.db, self.server.url)  # generation 1

        calls = []

        def transport(url, headers):
            calls.append(headers)
            return Response(200, (("ETag", '"v-late"'),),
                            bulletin(rows_v1()))

        def concurrent():
            fetch_url(self.db, self.server.url, label="concurrent",
                      transport=transport)

        with self.assertRaisesRegex(StaleResponse, "stale_response"):
            fetch_url(self.db, self.server.url, transport=transport,
                      barriers={"after_network": concurrent})
        db = self.connection()
        self.assertEqual(db.execute(
            "SELECT COUNT(*) FROM observations").fetchone()[0], 3)
        self.assertEqual(db.execute(
            "SELECT generation FROM fetch_state").fetchone()[0], 2)
        self.assertEqual(db.execute(
            "SELECT COUNT(*) FROM sources").fetchone()[0], 1)
        # Each fetch made exactly one request; the stale one did not retry.
        self.assertEqual(len(calls), 2)
        self.assertEqual(
            db.execute("SELECT validator_value FROM fetch_state").fetchone()[0],
            '"v-late"')

    def test_13_late_304_is_also_stale(self):
        self.server.body = bulletin(rows_v1())
        fetch_url(self.db, self.server.url)
        self.server.status = 304
        self.server.body = b""

        def concurrent_200():
            self.server.status = 200
            self.server.body = bulletin(rows_v1())
            fetch_url(self.db, self.server.url, label="racing")

        with self.assertRaisesRegex(StaleResponse, "stale_response"):
            fetch_url(self.db, self.server.url,
                      barriers={"after_network": concurrent_200})
        self.assertEqual(
            self.connection().execute(
                "SELECT generation FROM fetch_state").fetchone()[0], 2)

    def test_14_failure_before_commit_rolls_back_everything(self):
        self.server.body = bulletin(rows_v1())

        def explode(*args, **kwargs):
            raise ValueError("simulated commit-phase failure")

        with unittest.mock.patch(
                "air_harvest.conditional.apply_batch", side_effect=explode):
            with self.assertRaisesRegex(ValueError, "simulated"):
                fetch_url(self.db, self.server.url, label="doomed")
        # The write transaction also rolled back its schema creation.
        connection = sqlite3.connect(self.db)
        try:
            tables = {row[0] for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
        finally:
            connection.close()
        self.assertFalse({"sources", "observations", "source_content",
                          "fetch_state"} & tables)
        result = fetch_url(self.db, self.server.url)
        self.assertEqual((result["generation"], result["inserted"]), (1, 3))

    def test_15_network_phase_holds_no_write_lock(self):
        self.server.body = bulletin(rows_v1())
        fetch_url(self.db, self.server.url)

        writes = []

        def concurrent_writer():
            connection = sqlite3.connect(self.db, timeout=0)
            try:
                connection.execute(
                    "INSERT INTO sources VALUES ('concurrent', 'note', 0)")
                connection.commit()
                writes.append("ok")
            finally:
                connection.close()

        result = fetch_url(self.db, self.server.url, barriers={
            "after_network": concurrent_writer})
        self.assertEqual(result["generation"], 2)
        self.assertEqual(writes, ["ok"])

    def test_16_conflict_rolls_back_whole_200_and_keeps_generation(self):
        self.server.body = bulletin(rows_v1())
        fetch_url(self.db, self.server.url, label="first")
        before = self.snapshot()
        # A later response conflicts with a stored fact, so the full batch —
        # including bytes, source row and generation — must roll back.
        self.server.body = bulletin(rows_v1()[:1] + [
            ("Hill", "2026-01-01T01:00:00Z", "PM10", "9", "ug/m3", "measured")])
        self.server.etag = '"v2"'
        with self.assertRaisesRegex(ValueError, "conflicting"):
            fetch_url(self.db, self.server.url, label="changed")
        self.assertEqual(before, self.snapshot())
        state = self.connection().execute("SELECT * FROM fetch_state").fetchone()
        self.assertEqual((state["generation"], state["validator_value"]),
                         (1, '"v1"'))
        # A corrected response for the same cache generation still applies.
        self.server.body = bulletin(rows_v2())
        result = fetch_url(self.db, self.server.url)
        self.assertEqual(result["generation"], 2)
        self.assertEqual((result["inserted"], result["duplicates"]), (1, 2))

    # ------------------------------------------------------------------ #
    # Status and export
    # ------------------------------------------------------------------ #
    def test_17_status_is_offline_read_only_and_deterministic(self):
        missing = self.dir / "missing.sqlite"
        with self.assertRaises(FileNotFoundError):
            status_report(missing)
        self.assertFalse(missing.exists())

        self.server.body = bulletin(rows_v1())
        fetch_url(self.db, self.server.url)
        payload = status_report(self.db)
        self.assertEqual(payload["totals"]["url_count"], 1)
        self.assertEqual(payload["totals"]["observation_count"], 3)
        entry = payload["urls"][0]
        self.assertEqual((entry["http_status"], entry["generation"],
                          entry["row_count"], entry["content_bytes"]),
                         (200, 1, 3, len(bulletin(rows_v1()))))
        text = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        self.assertNotIn(str(self.dir), text)
        self.assertEqual(text, json.dumps(status_report(self.db), sort_keys=True))

        # A second URL is counted separately.
        self.server.body = bulletin(rows_v2())
        self.server.etag = '"v2"'
        other = self.server.url + "?station=hill"
        fetch_url(self.db, other)
        payload = status_report(self.db)
        self.assertEqual(payload["totals"]["url_count"], 2)
        by_url = {entry["url"]: entry for entry in payload["urls"]}
        self.assertEqual(by_url[self.server.url]["content_bytes"],
                         len(bulletin(rows_v1())))
        self.assertEqual(by_url[other]["content_bytes"], len(bulletin(rows_v2())))

        # status verifies the cache; tampered bytes fail it without writes.
        with sqlite3.connect(self.db) as writer:
            writer.execute("UPDATE source_content SET raw_bytes=?",
                           (sqlite3.Binary(b"tampered"),))
        before = _logical_dump(self.db)
        with self.assertRaises(CacheCorruption):
            status_report(self.db)
        self.assertEqual(_logical_dump(self.db), before)

    def test_18_export_is_atomic_and_refuses_dangerous_targets(self):
        self.server.body = bulletin(rows_v1())
        fetch_url(self.db, self.server.url, label="fixture")
        digest = sqlite3.connect(self.db).execute(
            "SELECT digest FROM sources").fetchone()[0]
        target = self.dir / "original.html"
        result = export_original(self.db, target, url=self.server.url)
        self.assertEqual(target.read_bytes(), bulletin(rows_v1()))
        self.assertEqual(result["byte_length"], len(bulletin(rows_v1())))

        with self.assertRaises(ExportError):
            export_original(self.db, self.db, url=self.server.url)
        link = self.dir / "alias.html"
        link.symlink_to(self.db)
        with self.assertRaises(ExportError):
            export_original(self.db, link, url=self.server.url)
        link.unlink()

        # Unknown selection fails without touching an existing export.
        target.write_bytes(b"previous-good")
        with self.assertRaises(CacheCorruption):
            export_original(self.db, target, url=self.server.url + "?missing")
        self.assertEqual(target.read_bytes(), b"previous-good")
        self.assertEqual(
            [p.name for p in self.dir.iterdir() if p.name.startswith(".original")],
            [])
        self.assertEqual(
            export_original(self.db, target, digest=digest)["byte_length"],
            len(bulletin(rows_v1())))

    def test_18b_receipt_failure_after_commit_does_not_undo_the_commit(self):
        self.server.body = bulletin(rows_v1())

        def fail_receipt():
            raise RuntimeError("stdout/receipt unavailable")

        with self.assertRaisesRegex(RuntimeError, "receipt"):
            fetch_url(self.db, self.server.url,
                      barriers={"after_commit": fail_receipt})
        # The transaction had already committed, so the data stays published.
        db = self.connection()
        self.assertEqual(db.execute(
            "SELECT generation FROM fetch_state").fetchone()[0], 1)
        self.assertEqual(db.execute(
            "SELECT COUNT(*) FROM observations").fetchone()[0], 3)
        # A normal retry sees the cache and gets a 304-style unchanged result.
        self.server.status = 304
        self.server.body = b""
        result = fetch_url(self.db, self.server.url)
        self.assertEqual((result["result"], result["generation"]),
                         ("unchanged", 1))

    def test_18c_export_failure_keeps_previous_output_and_cleans_temp(self):
        self.server.body = bulletin(rows_v1())
        fetch_url(self.db, self.server.url)
        target = self.dir / "original.html"
        target.write_bytes(b"previous-good")
        with unittest.mock.patch(
                "air_harvest.conditional.os.replace",
                side_effect=OSError("replace failed")):
            with self.assertRaisesRegex(OSError, "replace failed"):
                export_original(self.db, target, url=self.server.url)
        self.assertEqual(target.read_bytes(), b"previous-good")
        self.assertEqual(
            [p for p in self.dir.iterdir() if p.name.startswith(".original")],
            [])

    def test_18d_fetch_creates_missing_parent_directory_only_on_commit(self):
        nested = self.dir / "nested" / "deeper" / "air.sqlite"
        self.server.status = 500
        with self.assertRaisesRegex(ValueError, "unexpected HTTP status"):
            fetch_url(nested, self.server.url)
        self.assertFalse(nested.parent.exists())
        self.server.status = 200
        self.server.body = bulletin(rows_v1())
        result = fetch_url(nested, self.server.url)
        self.assertEqual(result["result"], "accepted")
        self.assertTrue(nested.is_file())

    # ------------------------------------------------------------------ #
    # CLI parity
    # ------------------------------------------------------------------ #
    def test_19_cli_end_to_end(self):
        self.server.body = bulletin(rows_v1())

        def run(*args):
            return subprocess.run(
                [sys.executable, "-m", "air_harvest", *args],
                cwd=ROOT, capture_output=True, text=True)

        first = run("fetch", "--url", self.server.url, "--db", str(self.db),
                    "--label", "cli")
        self.assertEqual(first.returncode, 0, first.stderr)
        payload = json.loads(first.stdout)
        self.assertEqual((payload["result"], payload["generation"],
                          payload["inserted"]), ("accepted", 1, 3))

        self.server.status = 304
        self.server.body = b""
        second = run("fetch", "--url", self.server.url, "--db", str(self.db))
        self.assertEqual(second.returncode, 0, second.stderr)
        unchanged = json.loads(second.stdout)
        self.assertEqual((unchanged["result"], unchanged["generation"],
                          unchanged["duplicates"]), ("unchanged", 1, 0))

        status = run("status", "--db", str(self.db))
        self.assertEqual(status.returncode, 0, status.stderr)
        self.assertEqual(json.loads(status.stdout)["totals"]["url_count"], 1)

        output = self.dir / "cli-original.html"
        export = run("export", "--db", str(self.db), "--output", str(output),
                     "--url", self.server.url)
        self.assertEqual(export.returncode, 0, export.stderr)
        self.assertEqual(output.read_bytes(), bulletin(rows_v1()))

        missing = run("status", "--db", str(self.dir / "absent.sqlite"))
        self.assertNotEqual(missing.returncode, 0)
        self.assertFalse((self.dir / "absent.sqlite").exists())

        bad = run("export", "--db", str(self.db), "--output", str(output),
                  "--digest", "0" * 64)
        self.assertNotEqual(bad.returncode, 0)
        self.assertEqual(output.read_bytes(), bulletin(rows_v1()))

        other_db = self.dir / "will-not-exist.sqlite"
        self.server.status = 500
        failed = run("fetch", "--url", self.server.url, "--db", str(other_db))
        self.assertNotEqual(failed.returncode, 0)
        self.assertFalse(other_db.exists())

        # The original one-shot CLI contract still works unchanged.
        legacy = run("--input", str(FIXTURE), "--db", str(self.dir / "legacy.sqlite"))
        self.assertEqual(legacy.returncode, 0, legacy.stderr)
        self.assertEqual(json.loads(legacy.stdout)["inserted"], 3)

    # ------------------------------------------------------------------ #
    # Legacy interfaces remain intact
    # ------------------------------------------------------------------ #
    def test_20_legacy_ingest_and_daily_untouched(self):
        legacy_db = self.dir / "legacy.sqlite"
        first = ingest(FIXTURE.read_bytes(), legacy_db, "fixture")
        second = ingest(FIXTURE.read_bytes(), legacy_db, "fixture")
        self.assertEqual((first["inserted"], first["duplicates"]), (3, 1))
        self.assertEqual((second["inserted"], second["duplicates"]), (0, 4))

        self.server.body = bulletin(rows_v1())
        fetch_url(self.db, self.server.url)
        plan = self.dir / "plan.json"
        plan.write_text(json.dumps({
            "schema_version": 1, "min_valid_percent": 75,
            "stations": [{"station": "Hill", "timezone": "UTC",
                          "pollutants": ["NO2"]}]}))
        report = self.dir / "daily.json"
        result = subprocess.run(
            [sys.executable, "-m", "air_harvest.daily", "--db", str(self.db),
             "--plan", str(plan), "--start-date", "2026-01-01",
             "--end-date", "2026-01-01", "--output", str(report)],
            cwd=ROOT, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(report.read_bytes())
        self.assertEqual(payload["schema_version"], 1)
        self.assertEqual(payload["stations"][0]["pollutants"][0]["days"][0]
                         ["nd_only_hours"], 1)


def _logical_dump(database):
    connection = sqlite3.connect(database)
    try:
        return {table: connection.execute(
            "SELECT * FROM " + table).fetchall() for table in TABLES}
    finally:
        connection.close()


class TransportContractTests(unittest.TestCase):
    """Validator parsing and transport seams without a live server."""

    def test_21_response_validator_parsing(self):
        weak = Response(200, (("ETag", 'W/"abc"'),), b"x")
        self.assertEqual(weak.validators().etag, 'W/"abc"')
        bare = Response(200, (("ETag", '"a-b_c"'),), b"x")
        self.assertEqual(bare.validators().etag, '"a-b_c"')
        for value in ('abc', '"a b"', '"a"b"', '"line\nbreak"', 'W/abc'):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "ETag"):
                    Response(200, (("ETag", value),), b"x").validators()
        conflicting = Response(200, (("ETag", '"a"'), ("ETag", '"b"')), b"x")
        with self.assertRaisesRegex(ValueError, "conflicting"):
            conflicting.validators()

    def test_22_last_modified_is_canonicalized(self):
        response = Response(200, (
            ("Last-Modified", "Wed, 01 Jan 2026 05:00:00 +0500"),), b"x")
        self.assertEqual(response.validators().last_modified,
                         "Thu, 01 Jan 2026 00:00:00 GMT")
        for value in ("not a date", "Wed, 01 Jan 2026 05:00:00"):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "Last-Modified"):
                    Response(200, (("Last-Modified", value),), b"x").validators()

    def test_23_fake_transport_unchanged_and_stale(self):
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / "air.sqlite"
            body = bulletin(rows_v1())
            calls = []

            def transport(url, headers):
                calls.append(headers)
                return Response(200, (("ETag", '"e1"'),), body)

            fetch_url(db, "http://127.0.0.1:8080/b", transport=transport)
            self.assertEqual(calls, [{}])
            fetch_url(db, "http://127.0.0.1:8080/b", transport=transport)
            self.assertEqual(calls[1], {"If-None-Match": '"e1"'})

            def stale(url, headers):
                raise AssertionError("not used")

            with self.assertRaisesRegex(StaleResponse, "stale_response"):
                fetch_url(db, "http://127.0.0.1:8080/b", transport=transport,
                          barriers={"after_network": lambda: fetch_url(
                              db, "http://127.0.0.1:8080/b", transport=transport)})

    def test_24_canonical_url_is_stable_and_never_the_raw_string(self):
        cases = {
            "http://LOCALHOST:80/bulletin": "http://localhost:80/bulletin",
            "http://127.0.0.1/bulletin?x=1": "http://127.0.0.1:80/bulletin?x=1",
            "http://[::1]:99/a": "http://[::1]:99/a",
        }
        for raw, expected in cases.items():
            self.assertEqual(canonical_url(raw), expected)
        self.assertEqual(len(url_identity("http://127.0.0.1/x")), 64)


if __name__ == "__main__":
    unittest.main()
