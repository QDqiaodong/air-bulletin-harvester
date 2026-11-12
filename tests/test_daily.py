"""Tests for the independent daily coverage report (air_harvest.daily)."""
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from datetime import date, datetime, timedelta, timezone, tzinfo
from fractions import Fraction
from html import escape
from pathlib import Path
from zoneinfo import ZoneInfo

from air_harvest.daily import (
    PlanError, atomic_write, build_report, daterange, expected_slot_starts,
    format_six, load_plan, open_readonly_connection, parse_cli_date,
    read_snapshot, slot_start_for_instant,
)
from air_harvest.store import ingest

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "data" / "bulletin.html"
NY = ZoneInfo("America/New_York")
UTC = timezone.utc
HEADERS = ("station", "observed_at", "pollutant", "value", "unit", "qualifier")


def bulletin(rows):
    head = ("<table id=\"observations\"><tr>" +
            "".join(f"<th>{h}</th>" for h in HEADERS) + "</tr>")
    body = "".join(
        "<tr>" + "".join(f"<td>{escape(str(cell))}</td>" for cell in row) + "</tr>"
        for row in rows)
    return ("<!doctype html><meta charset=\"utf-8\">" + head + body +
            "</table>").encode("utf-8")


def plan_bytes(stations, threshold=75):
    return json.dumps({
        "schema_version": 1,
        "min_valid_percent": threshold,
        "stations": stations,
    }, ensure_ascii=False).encode("utf-8")


def plan_station(station, zone, pollutants):
    return {"station": station, "timezone": zone, "pollutants": pollutants}


def ny_slot_starts(day):
    return expected_slot_starts(day, NY)


def day_block(report, station, pollutant, day_text):
    station_block = next(s for s in report["stations"] if s["station"] == station)
    pollutant_block = next(p for p in station_block["pollutants"]
                           if p["pollutant"] == pollutant)
    return next(d for d in pollutant_block["days"] if d["date"] == day_text)


class GapZone(tzinfo):
    """A zone whose wall clocks jump forward 24h30m at UTC 2026-02-01T05:00.

    Before the jump the fixed offset is -05:00; from that UTC instant onward
    it is +19:30.  The UTC hour starts from 2026-02-01T05:00 through
    2026-02-02T04:00 then convert to local datetimes dated 2026-02-02, so
    local 2026-02-01 has zero expected slots and exercises the
    no_expected_hours branch.  Both offsets are legal fixed tzinfo offsets
    (strictly inside +/-24h).
    """

    BEFORE = timedelta(hours=-5)
    AFTER = timedelta(hours=19, minutes=30)
    JUMP_UTC = datetime(2026, 2, 1, 5, 0)

    def _offset(self, dt):
        if dt is None:
            return self.BEFORE
        # astimezone() probes the target offset with the UTC wall-clock time.
        return self.AFTER if dt.replace(tzinfo=None) >= self.JUMP_UTC else self.BEFORE

    def utcoffset(self, dt):
        return self._offset(dt)

    def tzname(self, dt):
        return "Synthetic/Gap"

    def dst(self, dt):
        return timedelta(0)


class DailyReportTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = Path(self.temp.name) / "air.sqlite"
        self.plan_path = Path(self.temp.name) / "plan.json"
        self.output = Path(self.temp.name) / "report.json"

    # ------------------------------------------------------------------ #
    # Existing collection behaviour stays intact
    # ------------------------------------------------------------------ #
    def test_01_baseline_ingest_facts_are_unchanged(self):
        raw = FIXTURE.read_bytes()
        first = ingest(raw, self.db, "fixture")
        second = ingest(raw, self.db, "fixture")
        self.assertEqual((first["inserted"], first["duplicates"], first["total"]),
                         (3, 1, 3))
        self.assertEqual((second["inserted"], second["duplicates"], second["total"]),
                         (0, 4, 3))
        with sqlite3.connect(self.db) as db:
            self.assertEqual(db.execute(
                "SELECT count(*) FROM sources").fetchone()[0], 1)
            digest = db.execute("SELECT digest FROM sources").fetchone()[0]
            first_sources = {row[0] for row in db.execute(
                "SELECT DISTINCT source_digest FROM observations")}
            self.assertEqual(first_sources, {digest})
            values = dict(db.execute(
                "SELECT station||'|'||pollutant, value_ug_m3 FROM observations")
                .fetchall())
            self.assertEqual(values["River & Park|PM2.5"], "25.000")
            self.assertIsNone(values["Hill|NO2"])
            self.assertEqual(values["Hill|PM10"], "0")
        # Building a report does not mutate any collection table.
        self.plan_path.write_bytes(plan_bytes([
            plan_station("River & Park", "America/New_York", ["PM2.5", "PM10"]),
            plan_station("Hill", "America/New_York", ["NO2"]),
        ]))
        result = subprocess.run(
            [sys.executable, "-m", "air_harvest.daily", "--db", str(self.db),
             "--plan", str(self.plan_path), "--start-date", "2026-01-01",
             "--end-date", "2026-01-01", "--output", str(self.output)],
            cwd=ROOT, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        with sqlite3.connect(self.db) as db:
            self.assertEqual(db.execute(
                "SELECT count(*) FROM observations").fetchone()[0], 3)
            self.assertEqual(db.execute(
                "SELECT count(*) FROM sources").fetchone()[0], 1)

    # ------------------------------------------------------------------ #
    # Ordinary 24-hour day: hourly equal weighting, thresholds, ND
    # ------------------------------------------------------------------ #
    def test_02_hour_equal_weighted_mean_and_threshold(self):
        rows = []
        # Hour 00 UTC: three measured values 0, 30, 60 inside one slot.
        for minute, value in ((0, 0), (15, 30), (45, 60)):
            rows.append(("S", f"2026-01-15T00:{minute:02d}:00Z", "PM2.5",
                         value, "ug/m3", "measured"))
        # Seventeen further hours, each a single measured 50.
        for hour in range(1, 18):
            rows.append(("S", f"2026-01-15T{hour:02d}:00:00Z", "PM2.5",
                         50, "ug/m3", "measured"))
        ingest(bulletin(rows), self.db, "dense-day")

        for threshold, should_publish in ((75, True), (76, False)):
            plan, zones = load_plan(plan_bytes(
                [plan_station("S", "UTC", ["PM2.5"])], threshold))
            connection = open_readonly_connection(self.db)
            try:
                data, sources = read_snapshot(connection, ["S"], {"PM2.5"})
                report = build_report(b"{}", plan, zones,
                                      date(2026, 1, 15), date(2026, 1, 15),
                                      data, sources)
            finally:
                connection.close()
            day = day_block(report, "S", "PM2.5", "2026-01-15")
            self.assertEqual(day["expected_hours"], 24)
            self.assertEqual(day["numeric_hours"], 18)
            self.assertEqual(day["missing_hours"], 6)
            self.assertEqual(day["nd_only_hours"], 0)
            self.assertEqual(day["received_hours"], 18)
            self.assertEqual(day["descriptive_mean"], "48.888889")
            # The raw 20-value average would be 47.000000 and must not appear.
            self.assertNotIn("47.000000", json.dumps(day))
            self.assertEqual(day["numeric_coverage"], {
                "numerator": 18, "denominator": 24,
                "min_valid_percent": threshold,
                "threshold_met": should_publish})
            if should_publish:
                self.assertEqual(day["status"], "published")
                self.assertEqual(day["published_mean"], "48.888889")
            else:
                self.assertEqual(day["status"], "insufficient_coverage")
                self.assertIsNone(day["published_mean"])
            dense_slot = day["slots"][0]
            self.assertEqual(dense_slot["state"], "numeric")
            self.assertEqual(dense_slot["measured_count"], 3)
            self.assertEqual(dense_slot["hour_mean"], "30.000000")

    def test_03_nd_only_hour_and_nd_inside_numeric_hour(self):
        rows = []
        for minute, value in ((0, 0), (15, 30), (45, 60)):
            rows.append(("S", f"2026-01-15T00:{minute:02d}:00Z", "PM2.5",
                         value, "ug/m3", "measured"))
        # ND inside the dense numeric hour: must not become zero or reweight.
        rows.append(("S", "2026-01-15T00:30:00Z", "PM2.5", "", "ug/m3", "ND"))
        for hour in range(1, 18):
            rows.append(("S", f"2026-01-15T{hour:02d}:00:00Z", "PM2.5",
                         50, "ug/m3", "measured"))
        # Hour 18 is ND-only.
        rows.append(("S", "2026-01-15T18:00:00Z", "PM2.5", "", "ug/m3", "ND"))
        ingest(bulletin(rows), self.db, "nd-day")

        plan, zones = load_plan(plan_bytes([plan_station("S", "UTC", ["PM2.5"])]))
        connection = open_readonly_connection(self.db)
        try:
            data, sources = read_snapshot(connection, ["S"], {"PM2.5"})
            report = build_report(b"{}", plan, zones,
                                  date(2026, 1, 15), date(2026, 1, 15),
                                  data, sources)
        finally:
            connection.close()
        day = day_block(report, "S", "PM2.5", "2026-01-15")
        self.assertEqual(day["numeric_hours"], 18)
        self.assertEqual(day["nd_only_hours"], 1)
        self.assertEqual(day["received_hours"], 19)
        self.assertEqual(day["missing_hours"], 5)
        self.assertEqual(day["descriptive_mean"], "48.888889")
        dense = day["slots"][0]
        self.assertEqual(dense["hour_mean"], "30.000000")
        self.assertEqual(dense["nd_count"], 1)
        self.assertEqual(dense["measured_count"], 3)
        nd_obs = next(o for o in dense["observations"] if o["qualifier"] == "ND")
        self.assertIsNone(nd_obs["concentration_ug_m3"])
        nd_only = day["slots"][18]
        self.assertEqual(nd_only["state"], "nd_only")
        self.assertIsNone(nd_only["hour_mean"])
        self.assertEqual(len(nd_only["observations"]), 1)

    # ------------------------------------------------------------------ #
    # DST: 23-hour spring day, 25-hour fall day with two distinct 01:00
    # ------------------------------------------------------------------ #
    def test_04_spring_forward_23_hour_day(self):
        day = date(2026, 3, 8)
        slots = ny_slot_starts(day)
        self.assertEqual(len(slots), 23)
        rows = []
        for slot in slots[:18]:
            rows.append(("S", slot.strftime("%Y-%m-%dT%H:%M:%SZ"), "PM2.5",
                         40, "ug/m3", "measured"))
        ingest(bulletin(rows), self.db, "spring")
        # 18/23 = 78.26%: 78% passes, 79% fails (75% passes, 76% also passes).
        for threshold, passes in ((75, True), (76, True), (78, True), (79, False)):
            plan, zones = load_plan(plan_bytes(
                [plan_station("S", "America/New_York", ["PM2.5"])], threshold))
            connection = open_readonly_connection(self.db)
            try:
                data, _ = read_snapshot(connection, ["S"], {"PM2.5"})
                report = build_report(b"{}", plan, zones, day, day, data, [])
            finally:
                connection.close()
            block = day_block(report, "S", "PM2.5", "2026-03-08")
            self.assertEqual(block["expected_hours"], 23)
            self.assertEqual(block["numeric_hours"], 18)
            self.assertEqual(block["missing_hours"], 5)
            self.assertEqual(block["numeric_coverage"]["numerator"], 18)
            self.assertEqual(block["numeric_coverage"]["denominator"], 23)
            self.assertEqual(block["numeric_coverage"]["threshold_met"], passes)
            self.assertEqual(block["status"],
                             "published" if passes else "insufficient_coverage")

        # Seventeen numeric hours (17/23 = 73.9%) fails the 75% threshold.
        db17 = Path(self.temp.name) / "spring17.sqlite"
        ingest(bulletin(rows[:17]), db17, "spring17")
        plan, zones = load_plan(plan_bytes(
            [plan_station("S", "America/New_York", ["PM2.5"])], 75))
        connection = open_readonly_connection(db17)
        try:
            data, _ = read_snapshot(connection, ["S"], {"PM2.5"})
            report = build_report(b"{}", plan, zones, day, day, data, [])
        finally:
            connection.close()
        block17 = day_block(report, "S", "PM2.5", "2026-03-08")
        self.assertEqual(block17["numeric_hours"], 17)
        self.assertEqual(block17["status"], "insufficient_coverage")
        self.assertIsNone(block17["published_mean"])

    def test_05_fall_back_25_hour_day_two_local_ones(self):
        day = date(2026, 11, 1)
        slots = ny_slot_starts(day)
        self.assertEqual(len(slots), 25)
        local_ones = [s for s in slots if s.astimezone(NY).hour == 1]
        self.assertEqual(len(local_ones), 2)
        first_one, second_one = local_ones
        rows = []
        # 17 generic measured slots, excluding the two local-01 slots.
        generic = [s for s in slots if s not in local_ones][:17]
        for slot in generic:
            rows.append(("S", slot.strftime("%Y-%m-%dT%H:%M:%SZ"), "PM2.5",
                         20, "ug/m3", "measured"))
        # First local 01 (-04:00) numeric, second local 01 (-05:00) ND-only.
        rows.append(("S", first_one.strftime("%Y-%m-%dT%H:%M:%SZ"), "PM2.5",
                     22, "ug/m3", "measured"))
        rows.append(("S", second_one.strftime("%Y-%m-%dT%H:%M:%SZ"), "PM2.5",
                     "", "ug/m3", "ND"))
        ingest(bulletin(rows), self.db, "fall")

        plan, zones = load_plan(plan_bytes(
            [plan_station("S", "America/New_York", ["PM2.5"])], 75))
        connection = open_readonly_connection(self.db)
        try:
            data, _ = read_snapshot(connection, ["S"], {"PM2.5"})
            report = build_report(b"{}", plan, zones, day, day, data, [])
        finally:
            connection.close()
        block = day_block(report, "S", "PM2.5", "2026-11-01")
        self.assertEqual(block["expected_hours"], 25)
        self.assertEqual(block["numeric_hours"], 18)
        self.assertEqual(block["nd_only_hours"], 1)
        self.assertEqual(block["received_hours"], 19)
        self.assertEqual(block["missing_hours"], 6)
        # 18/25 = 72%: fails both 75 and 76.
        self.assertEqual(block["status"], "insufficient_coverage")
        self.assertIsNone(block["published_mean"])
        self.assertIsNotNone(block["descriptive_mean"])
        one_blocks = [slot for slot in block["slots"]
                      if slot["local_start"][11:16] == "01:00"]
        self.assertEqual(len(one_blocks), 2)
        self.assertEqual({slot["local_start"] for slot in one_blocks},
                         {"2026-11-01T01:00:00-04:00",
                          "2026-11-01T01:00:00-05:00"})
        states = {slot["local_start"]: slot["state"] for slot in one_blocks}
        self.assertEqual(states["2026-11-01T01:00:00-04:00"], "numeric")
        self.assertEqual(states["2026-11-01T01:00:00-05:00"], "nd_only")
        # Each slot carries explicit UTC bounds.
        for slot in block["slots"]:
            start = datetime.strptime(slot["slot_start_utc"],
                                      "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
            end = datetime.strptime(slot["slot_end_utc"],
                                    "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
            self.assertEqual(end - start, timedelta(hours=1))

        # Nineteen numeric hours (19/25 = 76%) passes the 75% threshold:
        # turn the ND-only second local 01 into a measured observation.
        rows19 = rows[:-1] + [
            ("S", second_one.strftime("%Y-%m-%dT%H:%M:%SZ"), "PM2.5",
             24, "ug/m3", "measured")]
        db19 = Path(self.temp.name) / "fall19.sqlite"
        ingest(bulletin(rows19), db19, "fall19")
        plan19, zones19 = load_plan(plan_bytes(
            [plan_station("S", "America/New_York", ["PM2.5"])], 75))
        connection = open_readonly_connection(db19)
        try:
            data19, _ = read_snapshot(connection, ["S"], {"PM2.5"})
            report19 = build_report(b"{}", plan19, zones19, day, day, data19, [])
        finally:
            connection.close()
        block19 = day_block(report19, "S", "PM2.5", "2026-11-01")
        self.assertEqual(block19["expected_hours"], 25)
        self.assertEqual(block19["numeric_hours"], 19)
        self.assertEqual(block19["nd_only_hours"], 0)
        self.assertEqual(block19["received_hours"], 19)
        self.assertEqual(block19["missing_hours"], 6)
        self.assertEqual(block19["status"], "published")
        self.assertEqual(block19["published_mean"], block19["descriptive_mean"])

    # ------------------------------------------------------------------ #
    # Boundaries, offsets, equivalent units, gap days, empty plan targets
    # ------------------------------------------------------------------ #
    def test_06_boundary_instant_offsets_and_equivalent_units(self):
        # Observation exactly on 01:00Z enters the [01,02) slot, not [00,01).
        rows = [
            ("S", "2026-01-15T01:00:00+00:00", "PM2.5", 10, "ug/m3", "measured"),
            # Same UTC instant expressed with a different input offset.
            ("S", "2026-01-15T09:00:00+08:00", "PM2.5", 10, "ug/m3", "measured"),
            # Same business fact with an equivalent unit.
            ("S", "2026-01-15T01:00:00Z", "PM2.5", 0.01, "mg/m3", "measured"),
            # 00:59:59Z stays in the preceding slot.
            ("S", "2026-01-15T00:59:59Z", "PM2.5", 7, "ug/m3", "measured"),
        ]
        ingest(bulletin(rows), self.db, "boundary")
        plan, zones = load_plan(plan_bytes([plan_station("S", "UTC", ["PM2.5"])]))
        connection = open_readonly_connection(self.db)
        try:
            data, _ = read_snapshot(connection, ["S"], {"PM2.5"})
            report = build_report(b"{}", plan, zones,
                                  date(2026, 1, 15), date(2026, 1, 15), data, [])
        finally:
            connection.close()
        day = day_block(report, "S", "PM2.5", "2026-01-15")
        self.assertEqual(day["slots"][0]["state"], "numeric")
        self.assertEqual(day["slots"][0]["hour_mean"], "7.000000")
        self.assertEqual(day["slots"][0]["measured_count"], 1)
        target = day["slots"][1]
        self.assertEqual(target["slot_start_utc"], "2026-01-15T01:00:00Z")
        # Duplicates collapse to one accepted business fact: one observation,
        # and the equivalent-unit value is normalized once.
        self.assertEqual(target["measured_count"], 1)
        self.assertEqual(len(target["observations"]), 1)
        self.assertEqual(target["hour_mean"], "10.000000")
        self.assertEqual(target["observations"][0]["observed_at"],
                         "2026-01-15T01:00:00Z")

    def test_07_local_date_attribution_across_midnight(self):
        # 19:00Z on 2026-01-14 is local 14:00 in New York; 05:00Z on the 15th
        # is local 00:00 of the 15th.
        rows = [
            ("S", "2026-01-14T19:00:00Z", "PM2.5", 1, "ug/m3", "measured"),
            ("S", "2026-01-15T05:00:00Z", "PM2.5", 2, "ug/m3", "measured"),
        ]
        ingest(bulletin(rows), self.db, "midnight")
        plan, zones = load_plan(plan_bytes(
            [plan_station("S", "America/New_York", ["PM2.5"])]))
        connection = open_readonly_connection(self.db)
        try:
            data, _ = read_snapshot(connection, ["S"], {"PM2.5"})
            report = build_report(b"{}", plan, zones,
                                  date(2026, 1, 14), date(2026, 1, 15), data, [])
        finally:
            connection.close()
        d14 = day_block(report, "S", "PM2.5", "2026-01-14")
        d15 = day_block(report, "S", "PM2.5", "2026-01-15")
        self.assertEqual(d14["numeric_hours"], 1)
        d14_slot = next(slot for slot in d14["slots"]
                        if slot["slot_start_utc"] == "2026-01-14T19:00:00Z")
        self.assertEqual(d14_slot["state"], "numeric")
        self.assertEqual(d14_slot["local_start"],
                         "2026-01-14T14:00:00-05:00")
        self.assertEqual(d15["slots"][0]["slot_start_utc"],
                         "2026-01-15T05:00:00Z")
        self.assertEqual(d15["slots"][0]["state"], "numeric")
        self.assertEqual(d15["slots"][0]["local_start"],
                         "2026-01-15T00:00:00-05:00")

    def test_08_no_expected_hours_day_is_retained_without_division(self):
        zone = GapZone()
        self.assertEqual(expected_slot_starts(date(2026, 1, 31), zone) != [], True)
        self.assertEqual(expected_slot_starts(date(2026, 2, 1), zone), [])
        self.assertEqual(expected_slot_starts(date(2026, 2, 2), zone) != [], True)
        rows = [("S", "2026-02-01T12:00:00Z", "PM2.5", 9, "ug/m3", "measured")]
        ingest(bulletin(rows), self.db, "gap")
        plan_dict = {
            "schema_version": 1, "min_valid_percent": 75,
            "stations": [{"station": "S", "timezone": "Synthetic/Gap",
                          "pollutants": ["PM2.5"]}],
        }
        zones = {"S": zone}
        connection = open_readonly_connection(self.db)
        try:
            data, _ = read_snapshot(connection, ["S"], {"PM2.5"})
            report = build_report(json.dumps(plan_dict).encode(), plan_dict, zones,
                                  date(2026, 1, 31), date(2026, 2, 2), data, [])
        finally:
            connection.close()
        blocks = {d["date"]: d for d in
                  report["stations"][0]["pollutants"][0]["days"]}
        gap = blocks["2026-02-01"]
        self.assertEqual(gap["status"], "no_expected_hours")
        self.assertEqual(gap["expected_hours"], 0)
        self.assertEqual(gap["numeric_hours"], 0)
        self.assertIsNone(gap["published_mean"])
        self.assertIsNone(gap["descriptive_mean"])
        self.assertEqual(gap["slots"], [])
        self.assertEqual(gap["numeric_coverage"]["denominator"], 0)
        self.assertFalse(gap["numeric_coverage"]["threshold_met"])
        # The observation on that UTC day lands in another local date; it is
        # never force-attached to the gap day or used for division.
        self.assertTrue(blocks["2026-02-02"]["expected_hours"] > 0)

    def test_09_all_nd_day_and_completely_missing_day(self):
        rows = [
            ("S", "2026-01-15T00:00:00Z", "PM2.5", "", "ug/m3", "ND"),
            ("S", "2026-01-15T12:00:00Z", "PM2.5", "", "ug/m3", "ND"),
        ]
        ingest(bulletin(rows), self.db, "nd-only")
        plan, zones = load_plan(plan_bytes([plan_station("S", "UTC", ["PM2.5"])]))
        connection = open_readonly_connection(self.db)
        try:
            data, _ = read_snapshot(connection, ["S"], {"PM2.5"})
            report = build_report(b"{}", plan, zones,
                                  date(2026, 1, 15), date(2026, 1, 16), data, [])
        finally:
            connection.close()
        nd_day = day_block(report, "S", "PM2.5", "2026-01-15")
        empty_day = day_block(report, "S", "PM2.5", "2026-01-16")
        self.assertEqual(nd_day["expected_hours"], 24)
        self.assertEqual(nd_day["nd_only_hours"], 2)
        self.assertEqual(nd_day["numeric_hours"], 0)
        self.assertEqual(nd_day["received_hours"], 2)
        self.assertIsNone(nd_day["descriptive_mean"])
        self.assertEqual(nd_day["status"], "insufficient_coverage")
        self.assertEqual(empty_day["missing_hours"], 24)
        self.assertEqual(empty_day["status"], "insufficient_coverage")
        self.assertIsNone(empty_day["published_mean"])

    def test_10_unknown_station_and_pollutant_still_reported(self):
        ingest(bulletin([("S", "2026-01-15T00:00:00Z", "PM2.5",
                          5, "ug/m3", "measured")]), self.db, "one")
        plan, zones = load_plan(plan_bytes([
            plan_station("Ghost Station", "UTC", ["NO2"]),
            plan_station("S", "UTC", ["PM10"]),
        ]))
        connection = open_readonly_connection(self.db)
        try:
            data, sources = read_snapshot(
                connection, ["Ghost Station", "S"], {"NO2", "PM10"})
            report = build_report(b"{}", plan, zones,
                                  date(2026, 1, 15), date(2026, 1, 15),
                                  data, sources)
        finally:
            connection.close()
        self.assertEqual(len(report["stations"]), 2)
        for station_block in report["stations"]:
            block = station_block["pollutants"][0]["days"][0]
            self.assertEqual(block["expected_hours"], 24)
            self.assertEqual(block["missing_hours"], 24)
            self.assertEqual(block["status"], "insufficient_coverage")
        self.assertEqual(report["sources"], [])

    # ------------------------------------------------------------------ #
    # Plan validation
    # ------------------------------------------------------------------ #
    def test_11_plan_validation_errors_are_located(self):
        base = {"schema_version": 1, "min_valid_percent": 75,
                "stations": [{"station": "S", "timezone": "UTC",
                               "pollutants": ["PM2.5"]}]}

        def reject(mutator, fragment):
            doc = json.loads(json.dumps(base))
            mutator(doc)
            with self.assertRaises(PlanError) as ctx:
                load_plan(json.dumps(doc).encode())
            self.assertIn(fragment, str(ctx.exception))

        reject(lambda d: d.__setitem__("schema_version", 2), "schema_version")
        reject(lambda d: d.__setitem__("schema_version", True), "schema_version")
        reject(lambda d: d.__setitem__("schema_version", "1"), "schema_version")
        reject(lambda d: d.__setitem__("min_valid_percent", 0), "min_valid_percent")
        reject(lambda d: d.__setitem__("min_valid_percent", 101), "min_valid_percent")
        reject(lambda d: d.__setitem__("min_valid_percent", True),
               "min_valid_percent")
        reject(lambda d: d.__setitem__("stations", []), "stations")
        reject(lambda d: d["stations"].append(dict(d["stations"][0])),
               "duplicate station")
        reject(lambda d: d["stations"][0].__setitem__("station", ""),
               ".station")
        reject(lambda d: d["stations"][0].__setitem__("timezone", "Mars/Olympus"),
               "timezone")
        reject(lambda d: d["stations"][0].__setitem__("pollutants", []),
               "pollutants")
        reject(lambda d: d["stations"][0]["pollutants"].append("PM2.5"),
               "duplicate pollutant")
        reject(lambda d: d["stations"][0]["pollutants"].__setitem__(0, "O3"),
               "pollutants[0]")
        reject(lambda d: d.__setitem__("extra", 1), "unknown key")
        reject(lambda d: d["stations"][0].__setitem__("timezone", 12),
               "timezone")
        with self.assertRaises(PlanError):
            load_plan(b"not json")
        with self.assertRaises(PlanError):
            load_plan("[]".encode())
        with self.assertRaises(PlanError):
            parse_cli_date("2026-13-40", "start-date")
        with self.assertRaises(PlanError):
            parse_cli_date("15-01-2026", "end-date")

    def test_12_valid_plan_accepts_only_integer_one(self):
        plan, zones = load_plan(plan_bytes([
            plan_station("S", "UTC", ["PM2.5", "PM10", "NO2"])]))
        self.assertEqual(plan["schema_version"], 1)
        self.assertIn("S", zones)

    # ------------------------------------------------------------------ #
    # Read-only snapshot semantics
    # ------------------------------------------------------------------ #
    def test_13_missing_database_is_not_created_and_file_is_ro(self):
        missing = Path(self.temp.name) / "nope" / "air.sqlite"
        with self.assertRaises(FileNotFoundError):
            open_readonly_connection(missing)
        self.assertFalse(missing.exists())
        ingest(bulletin([("S", "2026-01-15T00:00:00Z", "PM2.5",
                          5, "ug/m3", "measured")]), self.db, "ro")
        connection = open_readonly_connection(self.db)
        try:
            with self.assertRaises(sqlite3.OperationalError):
                connection.execute(
                    "INSERT INTO observations VALUES ('x','2026-01-01T00:00:00+00:00','NO2',NULL,'ND','z')")
        finally:
            connection.close()

    def test_14_report_uses_one_snapshot_while_writer_commits(self):
        for journal_mode in ("delete", "wal"):
            with self.subTest(journal_mode=journal_mode):
                if self.db.exists():
                    self.db.unlink()
                for suffix in ("-wal", "-shm"):
                    sidecar = Path(str(self.db) + suffix)
                    if sidecar.exists():
                        sidecar.unlink()
                # Seed one source and one observation fresh for each mode.
                ingest(bulletin([("S", "2026-01-15T00:00:00Z", "PM2.5",
                                  5, "ug/m3", "measured")]), self.db, "seed")
                with sqlite3.connect(self.db) as admin:
                    admin.execute(f"PRAGMA journal_mode={journal_mode}")
                snapshot_opened = threading.Event()
                reader_released = threading.Event()
                writer_committed = threading.Event()
                write_error = []

                def writer():
                    # Wait until the reader has opened and pinned its snapshot.
                    snapshot_opened.wait(5)
                    if journal_mode != "wal":
                        # Rollback-journal mode blocks writers while a read
                        # transaction is active: wait for the reader to close.
                        reader_released.wait(5)
                    try:
                        with sqlite3.connect(self.db) as writer_db:
                            writer_db.execute("BEGIN IMMEDIATE")
                            digest = "a" * 64
                            writer_db.execute(
                                "INSERT OR IGNORE INTO sources VALUES (?, ?, ?)",
                                (digest, "concurrent", 1))
                            writer_db.execute(
                                "INSERT INTO observations VALUES (?,?,?,?,?,?)",
                                ("S", "2026-01-15T01:00:00+00:00", "PM2.5",
                                 "9", "measured", digest))
                            writer_db.commit()
                    except sqlite3.OperationalError as exc:  # pragma: no cover
                        write_error.append(str(exc))
                    writer_committed.set()

                thread = threading.Thread(target=writer)
                thread.start()
                connection = open_readonly_connection(self.db)
                try:
                    rows, sources = read_snapshot(connection, ["S"], {"PM2.5"})
                    rows_again, sources_again = read_snapshot(
                        connection, ["S"], {"PM2.5"})
                    snapshot_opened.set()
                    if journal_mode == "wal":
                        writer_committed.wait(5)
                        # The writer committed, but this open read transaction
                        # must still see its original frozen snapshot.
                        rows_third, sources_third = read_snapshot(
                            connection, ["S"], {"PM2.5"})
                    else:
                        rows_third, sources_third = rows_again, sources_again
                finally:
                    connection.close()
                    reader_released.set()
                    writer_committed.wait(5)
                    thread.join(5)
                self.assertEqual(write_error, [])
                self.assertEqual(len(rows), 1)
                self.assertEqual(len(rows_again), 1)
                self.assertEqual(len(rows_third), 1)
                self.assertEqual(rows, rows_again)
                self.assertEqual(rows, rows_third)
                self.assertEqual(len(sources), 1)
                self.assertEqual(sources, sources_again)
                self.assertEqual(sources, sources_third)
                # A fresh connection observes the committed data.
                fresh = open_readonly_connection(self.db)
                try:
                    new_rows, new_sources = read_snapshot(
                        fresh, ["S"], {"PM2.5"})
                finally:
                    fresh.close()
                self.assertEqual(len(new_rows), 2)
                self.assertEqual(len(new_sources), 2)

    # ------------------------------------------------------------------ #
    # Atomic output, byte stability, provenance links
    # ------------------------------------------------------------------ #
    def test_15_atomic_write_failure_keeps_existing_file(self):
        self.output.write_bytes(b"previous report")
        with self.assertRaises(OSError):
            atomic_write(Path(self.temp.name) / "missing-dir" / "r.json", b"new")
        self.assertEqual(self.output.read_bytes(), b"previous report")
        # Simulate a replacement failure against a non-writable directory.
        locked_dir = Path(self.temp.name) / "locked"
        locked_dir.mkdir()
        target = locked_dir / "report.json"
        target.write_bytes(b"old")
        os.chmod(locked_dir, 0o500)
        try:
            with self.assertRaises(PermissionError):
                atomic_write(target, b"new")
            self.assertEqual(target.read_bytes(), b"old")
        finally:
            os.chmod(locked_dir, 0o700)

    def test_16_byte_identical_reruns_and_source_links(self):
        raw_a = bulletin([
            ("S", "2026-01-15T00:00:00Z", "PM2.5", 5, "ug/m3", "measured"),
            ("S", "2026-01-15T00:30:00Z", "PM2.5", "", "ug/m3", "ND"),
        ])
        raw_b = bulletin([
            ("T", "2026-01-15T00:00:00Z", "NO2", 8, "ug/m3", "measured")])
        summary_a = ingest(raw_a, self.db, "bulletin-a")
        summary_b = ingest(raw_b, self.db, "bulletin-b")
        self.plan_path.write_bytes(plan_bytes([
            plan_station("S", "UTC", ["PM2.5"]),
            plan_station("T", "UTC", ["NO2"]),
        ]))
        payloads = []
        for _ in range(2):
            result = subprocess.run(
                [sys.executable, "-m", "air_harvest.daily", "--db", str(self.db),
                 "--plan", str(self.plan_path), "--start-date", "2026-01-15",
                 "--end-date", "2026-01-15", "--output", str(self.output)],
                cwd=ROOT, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            payloads.append(self.output.read_bytes())
        self.assertEqual(payloads[0], payloads[1])
        report = json.loads(payloads[0])
        self.assertEqual(report["schema_version"], 1)
        self.assertEqual(report["plan_sha256"],
                         hashlib.sha256(self.plan_path.read_bytes()).hexdigest())
        self.assertEqual(report["request"], {
            "start_date": "2026-01-15", "end_date": "2026-01-15",
            "min_valid_percent": 75})
        # Every referenced digest resolves to a source from the same snapshot.
        digests = {s["digest"] for s in report["sources"]}
        self.assertEqual(digests, {summary_a["source_sha256"],
                                   summary_b["source_sha256"]})
        referenced = set()
        for station_block in report["stations"]:
            for pollutant_block in station_block["pollutants"]:
                for day in pollutant_block["days"]:
                    for slot in day["slots"]:
                        for observation in slot["observations"]:
                            referenced.add(observation["source_digest"])
        self.assertTrue(referenced)
        self.assertEqual(referenced, digests)
        labels = {s["digest"]: s["label"] for s in report["sources"]}
        self.assertEqual(labels[summary_a["source_sha256"]], "bulletin-a")
        # ND observation is present with a saved concentration of null.
        s_day = day_block(report, "S", "PM2.5", "2026-01-15")
        nd = [o for o in s_day["slots"][0]["observations"]
              if o["qualifier"] == "ND"][0]
        self.assertIsNone(nd["concentration_ug_m3"])
        # No machine-specific absolute path or timestamp field leaks in.
        text = payloads[0].decode()
        self.assertNotIn(str(self.temp), text)
        self.assertNotIn("generated_at", text)

    # ------------------------------------------------------------------ #
    # End-to-end CLI through the real HTML parser and ingest chain
    # ------------------------------------------------------------------ #
    def test_17_cli_end_to_end_and_failure_recovery(self):
        # Build the database only through the public ingest path.
        raw = bulletin([
            ("S", "2026-01-15T00:00:00Z", "PM2.5", 0, "ug/m3", "measured"),
            ("S", "2026-01-15T00:15:00Z", "PM2.5", 30, "ug/m3", "measured"),
            ("S", "2026-01-15T00:45:00Z", "PM2.5", 60, "ug/m3", "measured"),
        ])
        ingest(raw, self.db, "e2e")
        self.plan_path.write_bytes(plan_bytes(
            [plan_station("S", "UTC", ["PM2.5"])], 75))

        def run_cli(db, plan, start, end, output):
            return subprocess.run(
                [sys.executable, "-m", "air_harvest.daily", "--db", str(db),
                 "--plan", str(plan), "--start-date", start, "--end-date", end,
                 "--output", str(output)],
                cwd=ROOT, capture_output=True, text=True)

        ok = run_cli(self.db, self.plan_path, "2026-01-15", "2026-01-15",
                     self.output)
        self.assertEqual(ok.returncode, 0, ok.stderr)
        first_bytes = self.output.read_bytes()
        report = json.loads(first_bytes)
        block = day_block(report, "S", "PM2.5", "2026-01-15")
        self.assertEqual(block["slots"][0]["hour_mean"], "30.000000")

        # Missing database: non-zero exit, no file created, old report intact.
        missing = Path(self.temp.name) / "absent.sqlite"
        gone = run_cli(missing, self.plan_path, "2026-01-15", "2026-01-15",
                       Path(self.temp.name) / "other.json")
        self.assertNotEqual(gone.returncode, 0)
        self.assertIn("database does not exist", gone.stderr)
        self.assertFalse(missing.exists())
        self.assertEqual(self.output.read_bytes(), first_bytes)

        # Invalid plan: non-zero exit, old report byte-identical.
        bad_plan = Path(self.temp.name) / "bad.json"
        bad_plan.write_text(json.dumps({"schema_version": 7,
                                        "min_valid_percent": 75, "stations": []}))
        bad = run_cli(self.db, bad_plan, "2026-01-15", "2026-01-15",
                      Path(self.temp.name) / "other2.json")
        self.assertNotEqual(bad.returncode, 0)
        self.assertIn("schema_version", bad.stderr)
        self.assertEqual(self.output.read_bytes(), first_bytes)

        # Inverted range: non-zero exit, old report intact.
        inverted = run_cli(self.db, self.plan_path, "2026-01-16",
                           "2026-01-15", Path(self.temp.name) / "other3.json")
        self.assertNotEqual(inverted.returncode, 0)
        self.assertIn("before", inverted.stderr)
        self.assertEqual(self.output.read_bytes(), first_bytes)

        # Fixed inputs rerun byte-identically without deleting anything.
        rerun = run_cli(self.db, self.plan_path, "2026-01-15", "2026-01-15",
                        self.output)
        self.assertEqual(rerun.returncode, 0, rerun.stderr)
        self.assertEqual(self.output.read_bytes(), first_bytes)

    def test_18_exact_arithmetic_never_uses_float(self):
        # A value whose average cannot be represented exactly in binary float.
        values = ["0.1", "0.2"]
        mean = sum((Fraction(v) for v in values), Fraction(0)) / len(values)
        self.assertEqual(format_six(mean), "0.150000")
        # ROUND_HALF_UP on an exact half at the seventh decimal:
        # 0.0000125 -> 0.000013 (binary float would round to 0.000012).
        tie = Fraction(125, 10000000)
        self.assertEqual(format_six(tie), "0.000013")

    def test_19_slot_flooring_rule(self):
        instant = datetime(2026, 1, 15, 7, 59, 59, tzinfo=UTC)
        self.assertEqual(slot_start_for_instant(instant),
                         datetime(2026, 1, 15, 7, tzinfo=UTC))
        boundary = datetime(2026, 1, 15, 8, 0, 0, tzinfo=UTC)
        self.assertEqual(slot_start_for_instant(boundary), boundary)
        offset = datetime.fromisoformat("2026-01-15T03:30:00-04:30")
        self.assertEqual(slot_start_for_instant(offset),
                         datetime(2026, 1, 15, 8, tzinfo=UTC))
        self.assertEqual(len(list(daterange(date(2026, 1, 31),
                                            date(2026, 2, 2)))), 3)


if __name__ == "__main__":
    unittest.main()
