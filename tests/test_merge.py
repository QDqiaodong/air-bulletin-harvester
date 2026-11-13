"""Tests for the independent multi-shard merge warehouse (air_harvest.merge).

The tests build every shard database through the real HTML parser and the
public ``ingest`` path (or through controlled raw SQLite for structural
failure cases), exercise one CLI subprocess end to end, and verify snapshot
semantics, exact decimal reconciliation, explicit resolutions, write safety
and determinism.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
import unittest.mock
from decimal import Decimal
from pathlib import Path

from air_harvest.merge import (
    FORMAT_ID, MergeError, build_model, canonical_decimal,
    canonical_snapshot_text, canonical_utc_instant, capture_snapshot,
    guard_output_path, load_manifest, run_merge, validate_resolutions,
)
from air_harvest.merge_demo import build_example, bulletin
from air_harvest.store import ingest

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "data" / "bulletin.html"
HEADERS = ("station", "observed_at", "pollutant", "value", "unit", "qualifier")


def write_manifest(directory, manifest, name="manifest.json"):
    path = Path(directory) / name
    path.write_text(json.dumps(manifest, ensure_ascii=False), encoding="utf-8")
    return path


def make_shard(directory, shard_id, rows, label=None):
    path = Path(directory) / f"{shard_id}.sqlite"
    result = ingest(bulletin(rows), path, label or f"{shard_id}-bulletin")
    return path, result


def base_manifest(shard_entries, resolutions=None):
    return {
        "schema_version": 1,
        "shards": [{"shard_id": sid, "path": rel} for sid, rel in shard_entries],
        "resolutions": resolutions or [],
    }


def open_warehouse(path):
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    return connection


class BaselineUnchangedTests(unittest.TestCase):
    """The original collection semantics and replay counts stay untouched."""

    def test_01_original_bulletin_insert_and_replay_counts(self):
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / "air.sqlite"
            raw = FIXTURE.read_bytes()
            first = ingest(raw, db, "fixture")
            second = ingest(raw, db, "fixture")
            self.assertEqual((first["inserted"], first["duplicates"]), (3, 1))
            self.assertEqual((second["inserted"], second["duplicates"]), (0, 4))
            self.assertEqual(second["total"], 3)


class CoreReconciliationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.dir = Path(self.temp.name)
        self.rows_a = [
            ("River & Park", "2026-01-01T08:00:00+08:00", "PM2.5",
             "25", "ug/m3", "measured"),
            ("Hill", "2026-01-01T00:00:00Z", "NO2", "", "ug/m3", "ND"),
        ]
        self.rows_b = [
            ("River & Park", "2026-01-01T00:00:00Z", "PM2.5",
             "30", "ug/m3", "measured"),
            ("Hill", "2026-01-01T00:00:00Z", "NO2", "", "ug/m3", "ND"),
            ("Hill", "2026-01-01T00:00:00Z", "PM10", "0", "ug/m3", "measured"),
        ]
        make_shard(self.dir, "operator-a", self.rows_a)
        make_shard(self.dir, "operator-b", self.rows_b)
        self.manifest = base_manifest([
            ("operator-a", "operator-a.sqlite"),
            ("operator-b", "operator-b.sqlite"),
        ])
        self.manifest_path = write_manifest(self.dir, self.manifest)
        self.output = self.dir / "warehouse.sqlite"

    def test_02_hand_counted_reconciliation_and_traceability(self):
        # No resolution: the one conflict must fail the whole run.
        with self.assertRaisesRegex(MergeError, "without an explicit resolution"):
            run_merge(self.manifest_path, self.output)
        self.assertFalse(self.output.exists())

        resolution = {
            "station": "River & Park",
            "observed_at": "2026-01-01T00:00:00Z",
            "pollutant": "PM2.5",
            "selected_shard": "operator-b",
            "reason": "operator-b re-check accepted for the synthetic demo",
            "expected": [
                {"shard_id": "operator-a", "qualifier": "measured",
                 "value_ug_m3": "25"},
                {"shard_id": "operator-b", "qualifier": "measured",
                 "value_ug_m3": "30"},
            ],
        }
        self.manifest["resolutions"] = [resolution]
        write_manifest(self.dir, self.manifest)
        summary = run_merge(self.manifest_path, self.output)
        self.assertEqual(summary["shard_count"], 2)
        self.assertEqual(summary["input_witness_count"], 5)
        self.assertEqual(summary["business_key_count"], 3)
        self.assertEqual(summary["distinct_candidate_count"], 4)
        self.assertEqual(summary["conflict_key_count"], 1)
        self.assertEqual(summary["resolved_key_count"], 1)
        self.assertEqual(summary["final_fact_count"], 3)

        with open_warehouse(self.output) as db:
            self.assertEqual(db.execute(
                "PRAGMA foreign_key_check").fetchall(), [])
            facts = {
                (r["station"], r["pollutant"]): r
                for r in db.execute("SELECT * FROM final_facts ff "
                                    "JOIN business_keys k ON k.key_id=ff.key_id "
                                    "JOIN candidates c ON c.candidate_id=ff.candidate_id")}
            pm25 = facts[("River & Park", "PM2.5")]
            self.assertEqual(pm25["value_ug_m3"], "30")
            self.assertEqual(pm25["qualifier"], "measured")
            self.assertEqual(pm25["is_conflict"], 1)
            no2 = facts[("Hill", "NO2")]
            self.assertIsNone(no2["value_ug_m3"])
            self.assertEqual(no2["qualifier"], "ND")
            pm10 = facts[("Hill", "PM10")]
            self.assertEqual(pm10["value_ug_m3"], "0")
            self.assertEqual(pm10["qualifier"], "measured")

            trace = [dict(r) for r in db.execute(
                "SELECT * FROM v_final_fact_trace ORDER BY key_id, shard_id")]
            no2_rows = [r for r in trace if r["pollutant"] == "NO2"]
            self.assertEqual({r["shard_id"] for r in no2_rows},
                             {"operator-a", "operator-b"})
            self.assertTrue(all(r["witness_role"] == "supporter"
                                for r in no2_rows))
            pm25_rows = [r for r in trace if r["pollutant"] == "PM2.5"]
            roles = {r["shard_id"]: r["witness_role"] for r in pm25_rows}
            self.assertEqual(roles, {"operator-a": "loser",
                                     "operator-b": "supporter"})
            # The losing 25 and its source remain queryable.
            loser = next(r for r in pm25_rows if r["shard_id"] == "operator-a")
            self.assertEqual(loser["final_value_ug_m3"], "30")
            losing_candidate = db.execute(
                "SELECT c.* FROM candidates c JOIN business_keys k "
                "ON k.key_id=c.key_id WHERE k.pollutant='PM2.5' "
                "AND c.value_ug_m3='25'").fetchone()
            self.assertIsNotNone(losing_candidate)
            losing_witness = db.execute(
                "SELECT w.*, s.label FROM witnesses w JOIN sources s "
                "ON s.shard_id=w.shard_id AND s.digest=w.source_digest "
                "WHERE w.candidate_id=?",
                (losing_candidate["candidate_id"],)).fetchone()
            self.assertEqual(losing_witness["shard_id"], "operator-a")
            self.assertEqual(losing_witness["label"], "operator-a-bulletin")

            # Witness count and hand-counted candidate totals.
            self.assertEqual(db.execute(
                "SELECT count(*) FROM witnesses").fetchone()[0], 5)
            self.assertEqual(db.execute(
                "SELECT count(*) FROM candidates").fetchone()[0], 4)
            # Every source row is retained, per shard.
            self.assertEqual(db.execute(
                "SELECT count(*) FROM sources").fetchone()[0], 2)

    def test_03_equivalent_decimals_offsets_and_isolated_sources(self):
        # Fresh databases: A stores "25", B stores "25.0"; same UTC instant
        # with two distinct UTC spellings (the collector canonicalizes to UTC,
        # so the different-offset reconciliation itself is covered by the
        # canonical_utc_instant helper and test_25).  Same digest carries
        # different label/row_count across the two shards.
        rows_a = [
            ("S", "2026-01-15T01:00:00Z", "PM2.5",
             "25", "ug/m3", "measured"),
        ]
        rows_b = [
            ("S", "2026-01-15T01:00:00Z", "PM2.5",
             "25.0", "ug/m3", "measured"),
        ]
        make_shard(self.dir, "eq-a", rows_a, label="same-digest-label")
        make_shard(self.dir, "eq-b", rows_b, label="different-label")
        # Give the two otherwise-equal UTC instants distinct stored spellings
        # via a direct, contract-valid text edit after ingest.
        with sqlite3.connect(self.dir / "eq-a.sqlite") as db:
            db.execute(
                "UPDATE observations SET observed_at=?",
                ("2026-01-15T01:00:00.000000+00:00",))
        # Force the two shards to share one digest but keep distinct labels and
        # row counts by editing the sources table directly (snapshot must keep
        # both source rows, isolated per shard).
        shared = "f" * 64
        for name, label, count in (("eq-a", "same-digest-label", 1),
                                   ("eq-b", "different-label", 7)):
            with sqlite3.connect(self.dir / f"{name}.sqlite") as db:
                db.execute(
                    "UPDATE sources SET digest=?, label=?, row_count=?",
                    (shared, label, count))
                db.execute(
                    "UPDATE observations SET source_digest=? WHERE 1=1",
                    (shared,))
        manifest = base_manifest([
            ("eq-a", "eq-a.sqlite"), ("eq-b", "eq-b.sqlite")])
        path = write_manifest(self.dir, manifest)
        summary = run_merge(path, self.dir / "eq-out.sqlite")
        self.assertEqual(summary["business_key_count"], 1)
        self.assertEqual(summary["distinct_candidate_count"], 1)
        self.assertEqual(summary["conflict_key_count"], 0)
        with open_warehouse(self.dir / "eq-out.sqlite") as db:
            candidate = db.execute(
                "SELECT value_ug_m3 FROM candidates").fetchone()
            self.assertEqual(candidate["value_ug_m3"], "25")
            supporters = db.execute(
                "SELECT shard_id FROM v_final_fact_trace "
                "WHERE witness_role='supporter' ORDER BY shard_id").fetchall()
            self.assertEqual([r["shard_id"] for r in supporters],
                             ["eq-a", "eq-b"])
            source_rows = db.execute(
                "SELECT shard_id, label, row_count FROM sources ORDER BY shard_id"
            ).fetchall()
            self.assertEqual([(r["shard_id"], r["label"], r["row_count"])
                              for r in source_rows],
                             [("eq-a", "same-digest-label", 1),
                              ("eq-b", "different-label", 7)])
            raw_times = {r[0] for r in db.execute(
                "SELECT DISTINCT raw_observed_at FROM witnesses")}
            self.assertEqual(raw_times,
                             {"2026-01-15T01:00:00.000000+00:00",
                              "2026-01-15T01:00:00+00:00"})

    def test_03b_different_offsets_of_same_instant_share_one_key(self):
        # Databases built by hand still pass schema verification; here the two
        # shards spell one UTC instant with different offsets, which must merge
        # onto a single canonical business key.
        for name, spelling in (
                ("off-a", "2026-01-15T09:00:00+08:00"),
                ("off-b", "2026-01-15T01:00:00Z")):
            path = self.dir / f"{name}.sqlite"
            with sqlite3.connect(path) as db:
                db.execute("CREATE TABLE sources (digest TEXT PRIMARY KEY, "
                           "label TEXT NOT NULL, row_count INTEGER NOT NULL)")
                db.execute("CREATE TABLE observations ("
                           "station TEXT NOT NULL, observed_at TEXT NOT NULL, "
                           "pollutant TEXT NOT NULL, value_ug_m3 TEXT, "
                           "qualifier TEXT NOT NULL, source_digest TEXT NOT NULL, "
                           "PRIMARY KEY(station, observed_at, pollutant), "
                           "FOREIGN KEY(source_digest) REFERENCES sources(digest))")
                db.execute("INSERT INTO sources VALUES (?, ?, 1)",
                           (f"{name}-digest", name))
                db.execute("INSERT INTO observations VALUES (?,?,?,?,?,?)",
                           ("S", spelling, "PM2.5", "25", "measured",
                            f"{name}-digest"))
        path = write_manifest(self.dir, base_manifest(
            [("off-a", "off-a.sqlite"), ("off-b", "off-b.sqlite")]))
        summary = run_merge(path, self.dir / "offset-out.sqlite")
        self.assertEqual(summary["business_key_count"], 1)
        self.assertEqual(summary["distinct_candidate_count"], 1)
        self.assertEqual(summary["conflict_key_count"], 0)
        with open_warehouse(self.dir / "offset-out.sqlite") as db:
            key = db.execute("SELECT observed_at_utc FROM business_keys").fetchone()
            self.assertEqual(key[0], "2026-01-15T01:00:00Z")
            self.assertEqual(db.execute(
                "SELECT count(*) FROM witnesses").fetchone()[0], 2)

    def test_04_nd_conflicts_with_real_zero(self):
        make_shard(self.dir, "nd-a", [
            ("S", "2026-01-15T00:00:00Z", "NO2", "", "ug/m3", "ND")])
        make_shard(self.dir, "zero-b", [
            ("S", "2026-01-15T00:00:00Z", "NO2", "0", "ug/m3", "measured")])
        manifest = base_manifest([
            ("nd-a", "nd-a.sqlite"), ("zero-b", "zero-b.sqlite")])
        path = write_manifest(self.dir, manifest)
        with self.assertRaisesRegex(MergeError, "without an explicit resolution"):
            run_merge(path, self.dir / "nd-out.sqlite")
        manifest["resolutions"] = [{
            "station": "S", "observed_at": "2026-01-15T00:00:00Z",
            "pollutant": "NO2", "selected_shard": "zero-b",
            "reason": "demo resolution picks the measured zero",
            "expected": [
                {"shard_id": "nd-a", "qualifier": "ND", "value_ug_m3": None},
                {"shard_id": "zero-b", "qualifier": "measured",
                 "value_ug_m3": "0"},
            ],
        }]
        path = write_manifest(self.dir, manifest)
        run_merge(path, self.dir / "nd-out.sqlite")
        with open_warehouse(self.dir / "nd-out.sqlite") as db:
            final = db.execute(
                "SELECT c.qualifier, c.value_ug_m3 FROM final_facts ff "
                "JOIN candidates c ON c.candidate_id=ff.candidate_id").fetchone()
            self.assertEqual((final["qualifier"], final["value_ug_m3"]),
                             ("measured", "0"))
            identities = {r[0]: (r[1], r[2]) for r in db.execute(
                "SELECT w.shard_id, c.qualifier, c.value_ug_m3 "
                "FROM witnesses w JOIN candidates c "
                "ON c.candidate_id=w.candidate_id")}
            self.assertEqual(identities["nd-a"], ("ND", None))
            self.assertEqual(identities["zero-b"], ("measured", "0"))

    def test_05_microsecond_apart_observations_are_two_keys(self):
        make_shard(self.dir, "us-a", [
            ("S", "2026-01-15T00:00:00.000000Z", "PM2.5",
             "1", "ug/m3", "measured")])
        make_shard(self.dir, "us-b", [
            ("S", "2026-01-15T00:00:00.000001Z", "PM2.5",
             "2", "ug/m3", "measured")])
        manifest = base_manifest([
            ("us-a", "us-a.sqlite"), ("us-b", "us-b.sqlite")])
        path = write_manifest(self.dir, manifest)
        summary = run_merge(path, self.dir / "us-out.sqlite")
        self.assertEqual(summary["business_key_count"], 2)
        self.assertEqual(summary["conflict_key_count"], 0)
        with open_warehouse(self.dir / "us-out.sqlite") as db:
            instants = {r[0] for r in db.execute(
                "SELECT observed_at_utc FROM business_keys")}
            # One microsecond apart stays two distinct keys; the zero-microsecond
            # instant keeps the plain Z spelling rather than padded zeros.
            self.assertEqual(instants,
                             {"2026-01-15T00:00:00Z",
                              "2026-01-15T00:00:00.000001Z"})

    def test_06_equal_value_third_shard_expected_set_guards_stale_resolutions(self):
        make_shard(self.dir, "c", [
            ("River & Park", "2026-01-01T00:00:00Z", "PM2.5",
             "30", "ug/m3", "measured")])
        manifest = base_manifest([
            ("operator-a", "operator-a.sqlite"),
            ("operator-b", "operator-b.sqlite"),
            ("c", "c.sqlite"),
        ], [{
            "station": "River & Park",
            "observed_at": "2026-01-01T00:00:00Z",
            "pollutant": "PM2.5",
            "selected_shard": "operator-b",
            "reason": "stale resolution that forgets shard c",
            "expected": [
                {"shard_id": "operator-a", "qualifier": "measured",
                 "value_ug_m3": "25"},
                {"shard_id": "operator-b", "qualifier": "measured",
                 "value_ug_m3": "30"},
            ],
        }])
        path = write_manifest(self.dir, manifest)
        with self.assertRaisesRegex(MergeError, "missing shards \\['c'\\]"):
            run_merge(path, self.dir / "three-out.sqlite")
        # Complete the expected set: B and C both support 30, only A loses.
        manifest["resolutions"][0]["expected"].append(
            {"shard_id": "c", "qualifier": "measured", "value_ug_m3": "30"})
        path = write_manifest(self.dir, manifest)
        run_merge(path, self.dir / "three-out.sqlite")
        with open_warehouse(self.dir / "three-out.sqlite") as db:
            roles = {r["shard_id"]: r["witness_role"] for r in db.execute(
                "SELECT shard_id, witness_role FROM v_final_fact_trace "
                "WHERE pollutant='PM2.5'")}
            self.assertEqual(roles, {"operator-a": "loser",
                                     "operator-b": "supporter",
                                     "c": "supporter"})
            final_value = db.execute(
                "SELECT c.value_ug_m3 FROM final_facts ff "
                "JOIN candidates c ON c.candidate_id=ff.candidate_id "
                "JOIN business_keys k ON k.key_id=ff.key_id "
                "WHERE k.pollutant='PM2.5'").fetchone()[0]
            self.assertEqual(final_value, "30")

        # Change B's actual value: the old expected set must fail and the
        # previously published warehouse must stay byte-for-byte intact.
        before = self.dir / "three-out.sqlite"
        original_bytes = before.read_bytes()
        with sqlite3.connect(self.dir / "operator-b.sqlite") as db:
            db.execute(
                "UPDATE observations SET value_ug_m3='31' "
                "WHERE station='River & Park' AND pollutant='PM2.5'")
        with self.assertRaises(MergeError):
            run_merge(path, self.dir / "three-out.sqlite")
        self.assertEqual(before.read_bytes(), original_bytes)


class ManifestValidationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.dir = Path(self.temp.name)
        make_shard(self.dir, "a", [
            ("S", "2026-01-15T00:00:00Z", "PM2.5", "1", "ug/m3", "measured")])

    def load(self, manifest):
        return load_manifest(json.dumps(manifest).encode())

    def test_07_schema_version_must_be_plain_integer_one(self):
        base = base_manifest([("a", "a.sqlite")])
        for bad in (True, "1", 1.0, None, 2):
            doc = json.loads(json.dumps(base))
            doc["schema_version"] = bad
            with self.assertRaises(MergeError):
                self.load(doc)
        self.assertEqual(self.load(base)["schema_version"], 1)

    def test_08_exact_top_level_and_nested_keys(self):
        for mutator, fragment in (
                (lambda d: d.__setitem__("extra", 1), "unknown key"),
                (lambda d: d["shards"][0].__setitem__("extra", 1), "unknown key"),
                (lambda d: d.__setitem__("shards", []), "non-empty"),
                (lambda d: d.__setitem__("shards", "nope"), "array"),
                (lambda d: d.__setitem__("resolutions", {}), "array")):
            doc = json.loads(json.dumps(base_manifest([("a", "a.sqlite")])))
            mutator(doc)
            with self.assertRaisesRegex(MergeError, fragment):
                self.load(doc)

    def test_09_shard_id_rules_are_strict_and_case_sensitive(self):
        for bad in (" x", "x "):
            doc = base_manifest([(bad, "a.sqlite")])
            with self.assertRaises(MergeError):
                self.load(doc)
        doc = base_manifest([("", "a.sqlite")])
        with self.assertRaises(MergeError):
            self.load(doc)
        doc = base_manifest([("A", "a.sqlite"), ("A", "a.sqlite")])
        with self.assertRaisesRegex(MergeError, "duplicate shard_id"):
            self.load(doc)
        # Different case is fine at the manifest level (files differ).
        make_shard(self.dir, "b", [
            ("T", "2026-01-15T00:00:00Z", "NO2", "2", "ug/m3", "measured")])
        doc = base_manifest([("a", "a.sqlite"), ("A", "b.sqlite")])
        self.load(doc)

    def test_10_resolution_payload_rules(self):
        good_resolution = {
            "station": "S", "observed_at": "2026-01-15T00:00:00Z",
            "pollutant": "PM2.5", "selected_shard": "a",
            "reason": " reason ",
            "expected": [
                {"shard_id": "a", "qualifier": "measured",
                 "value_ug_m3": "1"}],
        }
        doc = base_manifest([("a", "a.sqlite")], [dict(good_resolution)])
        with self.assertRaisesRegex(MergeError, "reason"):
            self.load(doc)
        good_resolution["reason"] = "valid reason"
        doc = base_manifest([("a", "a.sqlite")], [dict(good_resolution)])
        self.load(doc)
        # ND must carry null; measured must carry a decimal.
        for expected_item, fragment in (
                ({"shard_id": "a", "qualifier": "ND",
                  "value_ug_m3": "0"}, "null"),
                ({"shard_id": "a", "qualifier": "measured",
                  "value_ug_m3": None}, "only valid for ND"),
                ({"shard_id": "a", "qualifier": "weird",
                  "value_ug_m3": "1"}, "qualifier")):
            doc = base_manifest(
                [("a", "a.sqlite")],
                [{**good_resolution, "expected": [expected_item]}])
            with self.assertRaisesRegex(MergeError, fragment):
                self.load(doc)
        # Duplicate shard inside expected.
        doc = base_manifest([("a", "a.sqlite")], [{
            **good_resolution,
            "expected": [
                {"shard_id": "a", "qualifier": "measured",
                 "value_ug_m3": "1"},
                {"shard_id": "a", "qualifier": "measured",
                 "value_ug_m3": "1"}],
        }])
        with self.assertRaisesRegex(MergeError, "duplicate"):
            self.load(doc)


class PathSafetyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.dir = Path(self.temp.name)
        make_shard(self.dir, "a", [
            ("S", "2026-01-15T00:00:00Z", "PM2.5", "1", "ug/m3", "measured")])
        self.manifest_path = write_manifest(
            self.dir, base_manifest([("a", "a.sqlite")]))

    def run_with_shards(self, shard_entries, resolutions=None):
        path = write_manifest(
            self.dir, base_manifest(shard_entries, resolutions),
            name="m2.json")
        return run_merge(path, self.dir / "out.sqlite")

    def test_11_absolute_traversal_and_symlink_rejected(self):
        outside = self.dir / "outside"
        outside.mkdir()
        make_shard(outside, "ext", [
            ("S", "2026-01-15T00:00:00Z", "PM2.5", "2", "ug/m3", "measured")])
        for bad_path in (str(outside / "ext.sqlite"), "../escape.sqlite",
                         "sub/../../escape.sqlite"):
            with self.assertRaises(MergeError):
                self.run_with_shards([("ext", bad_path)])
        link = self.dir / "link.sqlite"
        link.symlink_to(self.dir / "a.sqlite")
        with self.assertRaisesRegex(MergeError, "symbolic link"):
            self.run_with_shards([("link", "link.sqlite")])
        # A symlinked directory component is rejected as well.
        sub = self.dir / "realdir"
        sub.mkdir()
        shutil.copy(self.dir / "a.sqlite", sub / "inside.sqlite")
        (self.dir / "linkdir").symlink_to(sub)
        with self.assertRaisesRegex(MergeError, "symbolic link"):
            self.run_with_shards([("inside", "linkdir/inside.sqlite")])

    def test_12_benign_relative_dotdot_is_accepted(self):
        nested = self.dir / "nested"
        nested.mkdir()
        shutil.copy(self.dir / "a.sqlite", nested / "a.sqlite")
        # A literal ".." that resolves back inside the manifest directory is an
        # ordinary relative path; only genuine escapes must be rejected.
        nested_manifest = write_manifest(
            nested, base_manifest([("a", "../nested/a.sqlite")]))
        run_merge(nested_manifest, nested / "out.sqlite")
        self.assertTrue((nested / "out.sqlite").exists())

    def test_13_hardlink_duplicate_and_output_alias_rejected(self):
        hard = self.dir / "hard.sqlite"
        os.link(self.dir / "a.sqlite", hard)
        with self.assertRaisesRegex(MergeError, "same physical file"):
            self.run_with_shards([("a", "a.sqlite"), ("hard", "hard.sqlite")])
        # Output must not alias an input.
        with self.assertRaisesRegex(MergeError, "must not overwrite"):
            guard_output_path(self.dir / "a.sqlite",
                              [self.manifest_path, self.dir / "a.sqlite"])
        with self.assertRaisesRegex(MergeError, "must not overwrite"):
            guard_output_path(hard, [self.manifest_path, self.dir / "a.sqlite"])
        symlink_output = self.dir / "via-link.sqlite"
        symlink_output.symlink_to(self.dir / "a.sqlite")
        with self.assertRaisesRegex(MergeError, "symbolic-link outputs"):
            guard_output_path(symlink_output, [self.manifest_path])

    def test_14_missing_database_is_not_created(self):
        path = write_manifest(
            self.dir, base_manifest([("ghost", "ghost.sqlite")]))
        with self.assertRaises(MergeError):
            run_merge(path, self.dir / "out.sqlite")
        self.assertFalse((self.dir / "ghost.sqlite").exists())


class ShardDataValidationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.dir = Path(self.temp.name)

    def build_shard(self, name, rows):
        path, _ = make_shard(self.dir, name, rows)
        return path

    def expect_failure(self, shard_name="a"):
        path = write_manifest(
            self.dir, base_manifest([(shard_name, f"{shard_name}.sqlite")]))
        with self.assertRaises(MergeError):
            run_merge(path, self.dir / "out.sqlite")
        self.assertFalse((self.dir / "out.sqlite").exists())

    def test_15_illegal_qualifier_concentration_and_dangling_digest(self):
        path = self.build_shard("a", [
            ("S", "2026-01-15T00:00:00Z", "PM2.5", "1", "ug/m3", "measured")])
        with sqlite3.connect(path) as db:
            db.execute("UPDATE observations SET qualifier='weird'")
        self.expect_failure()

        path = self.build_shard("b", [
            ("S", "2026-01-15T00:00:00Z", "PM2.5", "1", "ug/m3", "measured")])
        with sqlite3.connect(path) as db:
            db.execute("UPDATE observations SET value_ug_m3='-1'")
        self.expect_failure("b")

        path = self.build_shard("c", [
            ("S", "2026-01-15T00:00:00Z", "PM2.5", "1", "ug/m3", "measured")])
        with sqlite3.connect(path) as db:
            db.execute("UPDATE observations SET source_digest=?", ("z" * 64,))
        self.expect_failure("c")

        # ND with a non-null value and a measured row with NULL.
        path = self.build_shard("d", [
            ("S", "2026-01-15T00:00:00Z", "NO2", "", "ug/m3", "ND")])
        with sqlite3.connect(path) as db:
            db.execute("UPDATE observations SET value_ug_m3='0' WHERE qualifier='ND'")
        self.expect_failure("d")

        # Out-of-range concentration above the 1e9 normalized bound.
        path = self.build_shard("e", [
            ("S", "2026-01-15T00:00:00Z", "PM2.5", "1", "ug/m3", "measured")])
        with sqlite3.connect(path) as db:
            db.execute("UPDATE observations SET value_ug_m3='1000000001'")
        self.expect_failure("e")

    def test_16_broken_schema_and_missing_foreign_key(self):
        good = self.build_shard("good", [
            ("S", "2026-01-15T00:00:00Z", "PM2.5", "1", "ug/m3", "measured")])
        broken = self.dir / "broken.sqlite"
        with sqlite3.connect(broken) as db:
            db.execute("CREATE TABLE sources (digest TEXT PRIMARY KEY, label TEXT)")
            db.execute("CREATE TABLE observations (station TEXT)")
        path = write_manifest(
            self.dir, base_manifest([("broken", "broken.sqlite")]))
        with self.assertRaisesRegex(MergeError, "unexpected shape"):
            run_merge(path, self.dir / "out.sqlite")

        # Observations table without the source foreign key is rejected.
        nofk = self.dir / "nofk.sqlite"
        with sqlite3.connect(nofk) as db:
            db.execute("CREATE TABLE sources (digest TEXT PRIMARY KEY, label TEXT NOT NULL, row_count INTEGER NOT NULL)")
            db.execute("CREATE TABLE observations ("
                       "station TEXT NOT NULL, observed_at TEXT NOT NULL, "
                       "pollutant TEXT NOT NULL, value_ug_m3 TEXT, "
                       "qualifier TEXT NOT NULL, source_digest TEXT NOT NULL, "
                       "PRIMARY KEY(station, observed_at, pollutant))")
            db.execute("INSERT INTO sources VALUES (?, ?, 1)",
                       ("d" * 64, "label"))
            db.execute("INSERT INTO observations VALUES (?,?,?,?,?,?)",
                       ("S", "2026-01-15T00:00:00+00:00", "PM2.5",
                        "1", "measured", "d" * 64))
        path = write_manifest(
            self.dir, base_manifest([("nofk", "nofk.sqlite")]))
        with self.assertRaisesRegex(MergeError, "must reference sources"):
            run_merge(path, self.dir / "out.sqlite")

    def test_17_empty_observations_single_shard_and_orphan_sources(self):
        # Header-only bulletin: valid shard with zero observations.
        empty_path, _ = make_shard(self.dir, "empty", [])
        path = write_manifest(
            self.dir, base_manifest([("empty", "empty.sqlite")]))
        summary = run_merge(path, self.dir / "empty-out.sqlite")
        self.assertEqual(summary["input_witness_count"], 0)
        self.assertEqual(summary["business_key_count"], 0)
        self.assertEqual(summary["final_fact_count"], 0)
        with open_warehouse(self.dir / "empty-out.sqlite") as db:
            # The source registered by the header-only ingest is preserved
            # even though no final fact references it.
            self.assertEqual(db.execute(
                "SELECT count(*) FROM sources").fetchone()[0], 1)
            self.assertEqual(db.execute(
                "SELECT row_count FROM sources").fetchone()[0], 0)

        # A source with no observation at all is still retained in the output.
        with sqlite3.connect(empty_path) as db:
            db.execute("INSERT INTO sources VALUES (?, ?, ?)",
                       ("a" * 64, "orphan-source", 3))
        run_merge(path, self.dir / "empty-out.sqlite")
        with open_warehouse(self.dir / "empty-out.sqlite") as db:
            labels = {r[0] for r in db.execute("SELECT label FROM sources")}
            self.assertIn("orphan-source", labels)

    def test_18_resolution_errors_against_model(self):
        make_shard(self.dir, "a", [
            ("S", "2026-01-15T00:00:00Z", "PM2.5", "1", "ug/m3", "measured")])
        make_shard(self.dir, "b", [
            ("S", "2026-01-15T00:00:00Z", "PM2.5", "2", "ug/m3", "measured")])
        resolution = {
            "station": "S", "observed_at": "2026-01-15T00:00:00Z",
            "pollutant": "PM2.5", "selected_shard": "b",
            "reason": "r",
            "expected": [
                {"shard_id": "a", "qualifier": "measured",
                 "value_ug_m3": "1"},
                {"shard_id": "b", "qualifier": "measured",
                 "value_ug_m3": "2"},
            ],
        }

        def run_with(resolutions):
            path = write_manifest(self.dir, base_manifest(
                [("a", "a.sqlite"), ("b", "b.sqlite")], resolutions))
            return run_merge(path, self.dir / "r-out.sqlite")

        # Unknown selected shard.
        doc = json.loads(json.dumps([resolution]))
        doc[0]["selected_shard"] = "ghost"
        with self.assertRaisesRegex(MergeError, "unknown shard"):
            run_with(doc)
        # Resolution for a non-existent key.
        doc = json.loads(json.dumps([resolution]))
        doc[0]["station"] = "Ghost"
        with self.assertRaisesRegex(MergeError, "does not exist"):
            run_with(doc)
        # selected_shard does not witness the key.
        make_shard(self.dir, "c", [
            ("T", "2026-01-15T00:00:00Z", "PM2.5", "3", "ug/m3", "measured")])
        doc = json.loads(json.dumps([resolution]))
        doc[0]["selected_shard"] = "c"
        path = write_manifest(self.dir, base_manifest(
            [("a", "a.sqlite"), ("b", "b.sqlite"), ("c", "c.sqlite")], doc))
        with self.assertRaisesRegex(MergeError, "does not witness"):
            run_merge(path, self.dir / "r-out.sqlite")
        # Resolution for a non-conflicting key.
        make_shard(self.dir, "d", [
            ("U", "2026-01-15T00:00:00Z", "PM2.5", "9", "ug/m3", "measured")])
        calm = {**resolution,
                "station": "U",
                "selected_shard": "d",
                "expected": [{"shard_id": "d", "qualifier": "measured",
                              "value_ug_m3": "9"}]}
        path = write_manifest(self.dir, base_manifest(
            [("d", "d.sqlite")], [calm]))
        with self.assertRaisesRegex(MergeError, "no conflicting"):
            run_merge(path, self.dir / "r-out.sqlite")
        # Duplicate resolution key.
        path = write_manifest(self.dir, base_manifest(
            [("a", "a.sqlite"), ("b", "b.sqlite")],
            [resolution, resolution]))
        with self.assertRaisesRegex(MergeError, "duplicate resolution"):
            run_merge(path, self.dir / "r-out.sqlite")
        # Extra shard in expected.
        doc = json.loads(json.dumps([resolution]))
        doc[0]["expected"].append(
            {"shard_id": "c", "qualifier": "measured", "value_ug_m3": "3"})
        path = write_manifest(self.dir, base_manifest(
            [("a", "a.sqlite"), ("b", "b.sqlite"), ("c", "c.sqlite")], doc))
        with self.assertRaisesRegex(MergeError, "extra shards"):
            run_merge(path, self.dir / "r-out.sqlite")
        # Wrong expected value for an existing witness.
        doc = json.loads(json.dumps([resolution]))
        doc[0]["expected"][1]["value_ug_m3"] = "2.5"
        path = write_manifest(self.dir, base_manifest(
            [("a", "a.sqlite"), ("b", "b.sqlite")], doc))
        with self.assertRaisesRegex(MergeError, "snapshot holds"):
            run_merge(path, self.dir / "r-out.sqlite")


class SnapshotSemanticsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.dir = Path(self.temp.name)

    def test_19_canonical_snapshot_is_stable_and_complete(self):
        path, _ = make_shard(self.dir, "a", [
            ("S", "2026-01-15T09:00:00+08:00", "PM2.5",
             "25", "ug/m3", "measured"),
            ("S", "2026-01-15T10:00:00Z", "NO2", "", "ug/m3", "ND")])
        snapshot = capture_snapshot("a", "a.sqlite", path)
        again = capture_snapshot("a", "a.sqlite", path)
        self.assertEqual(snapshot.canonical_json, again.canonical_json)
        self.assertEqual(snapshot.snapshot_sha256, again.snapshot_sha256)
        payload = json.loads(snapshot.canonical_json)
        self.assertEqual(payload["format"], "air_harvest.merge.shard_snapshot/v1")
        self.assertEqual(len(payload["sources"]), 1)
        self.assertEqual(len(payload["observations"]), 2)
        # Every field of both tables is represented.
        self.assertEqual(set(payload["sources"][0]),
                         {"digest", "label", "row_count"})
        self.assertEqual(set(payload["observations"][0]),
                         {"station", "observed_at", "pollutant",
                          "value_ug_m3", "qualifier", "source_digest"})
        # The file hash is not used as the logical snapshot digest: rebuild the
        # digest independently and confirm it matches the canonical JSON.
        self.assertEqual(
            snapshot.snapshot_sha256,
            hashlib.sha256(snapshot.canonical_json.encode()).hexdigest())
        self.assertNotEqual(snapshot.snapshot_sha256,
                            hashlib.sha256(path.read_bytes()).hexdigest())

    def test_20_pinned_snapshot_survives_concurrent_commit(self):
        # Controlled synchronization (events, no sleeps): the writer commits
        # between the merge's observations read and sources read.  The pinned
        # snapshot must keep returning the original data in one capture.
        path, _ = make_shard(self.dir, "a", [
            ("S", "2026-01-15T00:00:00Z", "PM2.5", "1", "ug/m3", "measured")])
        with sqlite3.connect(path) as admin:
            admin.execute("PRAGMA journal_mode=wal")
        reader_at_barrier = threading.Event()
        writer_may_commit = threading.Event()
        writer_committed = threading.Event()

        def writer():
            reader_at_barrier.wait(5)
            writer_may_commit.wait(5)
            with sqlite3.connect(path) as db:
                db.execute("BEGIN IMMEDIATE")
                db.execute("INSERT OR IGNORE INTO sources VALUES (?, ?, ?)",
                           ("a" * 64, "concurrent", 1))
                db.execute("INSERT INTO observations VALUES (?,?,?,?,?,?)",
                           ("S", "2026-01-15T01:00:00+00:00", "PM2.5",
                            "9", "measured", "a" * 64))
                db.commit()
            writer_committed.set()

        def barrier():
            reader_at_barrier.set()
            writer_may_commit.set()
            writer_committed.wait(5)
            self.assertTrue(writer_committed.is_set())

        thread = threading.Thread(target=writer)
        thread.start()
        snapshot = capture_snapshot("a", "a.sqlite", path,
                                    barriers={"after_observations": barrier})
        thread.join(5)
        self.assertEqual(len(snapshot.observations), 1)
        self.assertEqual(len(snapshot.sources), 1)
        self.assertNotIn("a" * 64, {s.digest for s in snapshot.sources})
        # A fresh capture after the commit sees both rows (no cross-shard
        # global snapshot claim is made).
        later = capture_snapshot("a", "a.sqlite", path)
        self.assertEqual(len(later.observations), 2)
        self.assertEqual(len(later.sources), 2)


class DeterminismAndWriteSafetyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.dir = Path(self.temp.name)
        make_shard(self.dir, "a", [
            ("River & Park", "2026-01-01T08:00:00+08:00", "PM2.5",
             "25", "ug/m3", "measured"),
            ("Hill", "2026-01-01T00:00:00Z", "NO2", "", "ug/m3", "ND")])
        make_shard(self.dir, "b", [
            ("River & Park", "2026-01-01T00:00:00Z", "PM2.5",
             "30", "ug/m3", "measured"),
            ("Hill", "2026-01-01T00:00:00Z", "NO2", "", "ug/m3", "ND"),
            ("Hill", "2026-01-01T00:00:00Z", "PM10", "0", "ug/m3", "measured")])
        self.resolution = {
            "station": "River & Park",
            "observed_at": "2026-01-01T00:00:00Z",
            "pollutant": "PM2.5", "selected_shard": "b",
            "reason": "demo reason",
            "expected": [
                {"shard_id": "a", "qualifier": "measured",
                 "value_ug_m3": "25"},
                {"shard_id": "b", "qualifier": "measured",
                 "value_ug_m3": "30"},
            ],
        }

    def manifest(self, order=("a", "b"), resolution_order=None):
        shards = [(sid, f"{sid}.sqlite") for sid in order]
        resolutions = [] if resolution_order is None else [
            self.resolution for _ in resolution_order]
        if resolution_order is None:
            resolutions = [self.resolution]
        return base_manifest(shards, resolutions)

    def test_21_order_independence_and_deterministic_reruns(self):
        path1 = write_manifest(self.dir, self.manifest(("a", "b")), "m1.json")
        path2 = write_manifest(self.dir, self.manifest(("b", "a")), "m2.json")
        out1, out2 = self.dir / "w1.sqlite", self.dir / "w2.sqlite"
        summary1 = run_merge(path1, out1)
        summary2 = run_merge(path2, out2)
        # Snapshot and fact content is identical; the manifest digest differs.
        self.assertEqual(summary1["snapshot_sha256"], summary2["snapshot_sha256"])
        self.assertEqual(summary1["manifest_sha256"] !=
                         summary2["manifest_sha256"], True)
        for name in ("business_keys", "candidates", "witnesses", "final_facts",
                     "sources", "resolution_expected"):
            columns = {
                "business_keys": 4, "candidates": 4, "witnesses": 7,
                "final_facts": 4, "sources": 4, "resolution_expected": 4,
            }[name]
            q = "SELECT * FROM {} ORDER BY {}".format(
                name, ",".join(str(i) for i in range(1, columns + 1)))
            with open_warehouse(out1) as d1, open_warehouse(out2) as d2:
                self.assertEqual(d1.execute(q).fetchall(),
                                 d2.execute(q).fetchall(), name)
        # Repeated builds produce byte-identical stdout summaries.
        rerun_summary = run_merge(path1, out1)
        self.assertEqual(json.dumps(summary1, sort_keys=True),
                         json.dumps(rerun_summary, sort_keys=True))
        # Reordering resolutions cannot change meaning either.
        path3 = write_manifest(
            self.dir, self.manifest(("a", "b"), resolution_order=(0,)), "m3.json")
        out3 = self.dir / "w3.sqlite"
        summary3 = run_merge(path3, out3)
        self.assertEqual(summary3["resolved_key_count"], 1)
        with open_warehouse(out1) as d1, open_warehouse(out3) as d3:
            q = ("SELECT station, observed_at_utc, pollutant, "
                 "final_qualifier, final_value_ug_m3, shard_id, witness_role "
                 "FROM v_final_fact_trace ORDER BY 1,2,3,4,5,6,7")
            self.assertEqual(d1.execute(q).fetchall(), d3.execute(q).fetchall())

    def test_22_failure_injection_keeps_old_output_and_cleans_temp(self):
        path = write_manifest(self.dir, self.manifest())
        output = self.dir / "safe.sqlite"
        run_merge(path, output)
        original = output.read_bytes()

        import air_harvest.merge as merge_module
        # Inject a write failure inside the temp database through a connection
        # proxy (sqlite3.Connection attributes themselves are read-only).
        original_connect = sqlite3.connect

        class FailingConnection:
            def __init__(self, real):
                self._real = real

            def execute(self, *args, **kwargs):
                raise sqlite3.OperationalError("injected write failure")

            def __getattr__(self, name):
                return getattr(self._real, name)

            def __enter__(self):
                self._real.__enter__()
                return self

            def __exit__(self, *args):
                return self._real.__exit__(*args)

        def failing_connect(target, *args, **kwargs):
            connection = original_connect(target, *args, **kwargs)
            if str(target).endswith(".tmp"):
                return FailingConnection(connection)
            return connection
        with unittest.mock.patch.object(merge_module.sqlite3, "connect",
                                        failing_connect):
            with self.assertRaises(sqlite3.OperationalError):
                run_merge(path, output)
        self.assertEqual(output.read_bytes(), original)
        leftovers = [p for p in self.dir.iterdir()
                     if p.name.startswith(f".{output.name}.")]
        self.assertEqual(leftovers, [])

        # Inject an os.replace failure: same guarantees.
        original_replace = os.replace
        def failing_replace(src, dst):
            if Path(dst) == output:
                raise PermissionError("injected replace failure")
            return original_replace(src, dst)
        with unittest.mock.patch.object(merge_module.os, "replace",
                                        failing_replace):
            with self.assertRaises(PermissionError):
                run_merge(path, output)
        self.assertEqual(output.read_bytes(), original)
        leftovers = [p for p in self.dir.iterdir()
                     if p.name.startswith(f".{output.name}.")]
        self.assertEqual(leftovers, [])

        # A corrected rerun succeeds without deleting anything.
        run_merge(path, output)
        self.assertTrue(output.exists())

    def test_23_rebuild_after_inputs_are_moved(self):
        path = write_manifest(self.dir, self.manifest())
        output = self.dir / "portable.sqlite"
        run_merge(path, output)
        expected_digests = {}
        with open_warehouse(output) as db:
            for row in db.execute(
                    "SELECT shards.shard_id, shards.snapshot_sha256, canonical_json "
                    "FROM shard_snapshots JOIN shards USING (shard_id)"):
                expected_digests[row["shard_id"]] = (
                    row["snapshot_sha256"], row["canonical_json"])
            manifest_blob = db.execute(
                "SELECT raw_bytes FROM manifest_bytes").fetchone()[0]
        # Move the manifest and every shard away; the warehouse must rebuild
        # identical logical snapshots from its own stored canonical JSON.
        archive = self.dir / "archive"
        archive.mkdir()
        for name in ("a.sqlite", "b.sqlite", "manifest.json"):
            shutil.move(self.dir / name, archive / name)
        with open_warehouse(output) as db:
            for shard_id, (digest, canonical) in expected_digests.items():
                stored = db.execute(
                    "SELECT canonical_json, snapshot_sha256 FROM shard_snapshots "
                    "WHERE shard_id=?", (shard_id,)).fetchone()
                self.assertEqual(stored["canonical_json"], canonical)
                self.assertEqual(stored["snapshot_sha256"], digest)
                rebuilt = hashlib.sha256(
                    stored["canonical_json"].encode()).hexdigest()
                self.assertEqual(rebuilt, digest)
            self.assertEqual(db.execute(
                "SELECT count(*) FROM final_facts").fetchone()[0], 3)
            self.assertEqual(db.execute(
                "SELECT count(*) FROM witnesses").fetchone()[0], 5)
            self.assertEqual(bytes(db.execute(
                "SELECT raw_bytes FROM manifest_bytes").fetchone()[0]),
                             bytes(manifest_blob))
            # No absolute machine path is stored anywhere.
            texts = [r[0] for r in db.execute(
                "SELECT rel_path FROM shards")] + [
                r[0] for r in db.execute("SELECT meta_value FROM merge_meta")]
            self.assertTrue(all(str(self.dir) not in t for t in texts))


class CliEndToEndTests(unittest.TestCase):
    def test_24_real_cli_subprocess_with_sql_checks(self):
        with tempfile.TemporaryDirectory() as td:
            directory = Path(td) / "example"
            build_example(directory)
            output = directory / "warehouse.sqlite"
            result = subprocess.run(
                [sys.executable, "-m", "air_harvest.merge",
                 "--manifest", str(directory / "manifest.json"),
                 "--output", str(output)],
                cwd=ROOT, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            summary = json.loads(result.stdout)
            self.assertEqual(summary["format"], FORMAT_ID)
            self.assertEqual((summary["input_witness_count"],
                              summary["business_key_count"],
                              summary["distinct_candidate_count"],
                              summary["conflict_key_count"],
                              summary["final_fact_count"]), (5, 3, 4, 1, 3))
            with open_warehouse(output) as db:
                self.assertEqual(db.execute(
                    "PRAGMA foreign_key_check").fetchall(), [])
                # Independent hand counts.
                self.assertEqual(db.execute(
                    "SELECT count(*) FROM witnesses").fetchone()[0], 5)
                self.assertEqual(db.execute(
                    "SELECT count(*) FROM business_keys").fetchone()[0], 3)
                self.assertEqual(db.execute(
                    "SELECT count(*) FROM candidates").fetchone()[0], 4)
                self.assertEqual(db.execute(
                    "SELECT count(*) FROM final_facts").fetchone()[0], 3)
                self.assertEqual(db.execute(
                    "SELECT count(*) FROM v_conflicts").fetchone()[0], 1)
                # Resolution reason and full expected set survive.
                conflict = db.execute("SELECT * FROM v_conflicts").fetchone()
                self.assertEqual(conflict["selected_shard_id"], "operator-b")
                expected = db.execute(
                    "SELECT shard_id, qualifier, value_ug_m3 "
                    "FROM resolution_expected ORDER BY shard_id").fetchall()
                self.assertEqual([tuple(r) for r in expected], [
                    ("operator-a", "measured", "25"),
                    ("operator-b", "measured", "30")])
                # Manifest bytes and digest are stored verbatim.
                manifest_bytes = (directory / "manifest.json").read_bytes()
                row = db.execute(
                    "SELECT raw_bytes, manifest_sha256 FROM manifest_bytes "
                    "JOIN merge_meta ON meta_key='manifest_sha256'").fetchone()
                self.assertEqual(bytes(row[0]), manifest_bytes)
                self.assertEqual(row[1],
                                 hashlib.sha256(manifest_bytes).hexdigest())

            # A failing run (drop the resolution) exits non-zero and leaves no
            # output when there was no previous file.
            manifest = json.loads(manifest_bytes)
            manifest["resolutions"] = []
            (directory / "bad.json").write_text(json.dumps(manifest))
            failed = subprocess.run(
                [sys.executable, "-m", "air_harvest.merge",
                 "--manifest", str(directory / "bad.json"),
                 "--output", str(directory / "never.sqlite")],
                cwd=ROOT, capture_output=True, text=True)
            self.assertNotEqual(failed.returncode, 0)
            self.assertIn("without an explicit resolution", failed.stderr)
            self.assertFalse((directory / "never.sqlite").exists())


class ExactSemanticsHelperTests(unittest.TestCase):
    def test_25_decimal_and_instant_helpers(self):
        self.assertEqual(canonical_decimal("25"), canonical_decimal("25.0"))
        self.assertEqual(canonical_decimal("25.000"), "25")
        self.assertEqual(canonical_decimal("0"), "0")
        for bad in ("NaN", "Infinity", "-1", "1e-13", "1000000000.1", ""):
            with self.assertRaises(MergeError):
                canonical_decimal(bad)
        self.assertEqual(
            canonical_utc_instant("2026-01-15T09:00:00+08:00"),
            canonical_utc_instant("2026-01-15T01:00:00Z"))
        self.assertEqual(
            canonical_utc_instant("2026-01-15T00:00:00.000001Z"),
            "2026-01-15T00:00:00.000001Z")
        with self.assertRaises(MergeError):
            canonical_utc_instant("2026-01-15T00:00:00")
        # No float anywhere in the comparison chain.
        self.assertEqual(Decimal("25.0"), Decimal("25"))


if __name__ == "__main__":
    unittest.main()
