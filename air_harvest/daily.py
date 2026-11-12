"""Read-only daily coverage and descriptive means for stored observations.

The report is built from a single SQLite read snapshot and uses exact rational
arithmetic (``fractions.Fraction``); no ``float`` value is ever produced while
aggregating concentrations.  See ``docs/daily-report.md`` for the full
statistical contract, timezone requirements and failure-recovery behaviour.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import tempfile
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP, localcontext
from fractions import Fraction
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

POLLUTANTS = ("PM2.5", "PM10", "NO2")
SCHEMA_VERSION = 1
SIX_PLACES = Decimal("0.000001")
UTC = timezone.utc


class PlanError(ValueError):
    """A plan document or CLI date argument is invalid; message locates the item."""


# --------------------------------------------------------------------------- #
# Plan validation
# --------------------------------------------------------------------------- #
def _is_int(value):
    """True only for genuine ints (``bool`` is rejected)."""
    return isinstance(value, int) and not isinstance(value, bool)


def _require_object(node, path):
    if not isinstance(node, dict):
        raise PlanError(f"{path}: expected an object")
    return node


def load_plan(plan_bytes):
    """Validate the UTF-8 JSON plan and return ``(plan_dict, zones)``.

    ``zones`` maps station name to a loaded ``ZoneInfo``.  Errors raise
    :class:`PlanError` whose message names the offending configuration path.
    """
    try:
        plan = json.loads(plan_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PlanError(f"plan: must be valid UTF-8 JSON ({exc})") from exc
    root = _require_object(plan, "plan")

    unknown = sorted(set(root) - {"schema_version", "min_valid_percent", "stations"})
    if unknown:
        raise PlanError(f"plan: unknown key(s): {', '.join(unknown)}")

    version = root.get("schema_version")
    if not _is_int(version):
        raise PlanError("plan.schema_version: must be an integer")
    if version != SCHEMA_VERSION:
        raise PlanError(f"plan.schema_version: only {SCHEMA_VERSION} is supported")

    threshold = root.get("min_valid_percent")
    if not _is_int(threshold):
        raise PlanError("plan.min_valid_percent: must be an integer (booleans are not accepted)")
    if not 1 <= threshold <= 100:
        raise PlanError("plan.min_valid_percent: must be between 1 and 100 inclusive")

    stations = root.get("stations")
    if not isinstance(stations, list) or not stations:
        raise PlanError("plan.stations: must be a non-empty array")

    zones = {}
    result_stations = []
    for index, entry in enumerate(stations):
        path = f"plan.stations[{index}]"
        node = _require_object(entry, path)
        extra = sorted(set(node) - {"station", "timezone", "pollutants"})
        if extra:
            raise PlanError(f"{path}: unknown key(s): {', '.join(extra)}")

        station = node.get("station")
        if not isinstance(station, str) or not station:
            raise PlanError(f"{path}.station: must be a non-empty string")
        if station in zones:
            raise PlanError(f"{path}.station: duplicate station {station!r}")

        tz_name = node.get("timezone")
        if not isinstance(tz_name, str) or not tz_name:
            raise PlanError(f"{path}.timezone: must be a non-empty IANA timezone name")
        try:
            zone = ZoneInfo(tz_name)
        except ZoneInfoNotFoundError as exc:
            raise PlanError(f"{path}.timezone: unknown or unavailable timezone {tz_name!r}") from exc

        pollutants = node.get("pollutants")
        if not isinstance(pollutants, list) or not pollutants:
            raise PlanError(f"{path}.pollutants: must be a non-empty array")
        seen_pollutants = set()
        for p_index, pollutant in enumerate(pollutants):
            ppath = f"{path}.pollutants[{p_index}]"
            if not isinstance(pollutant, str) or not pollutant:
                raise PlanError(f"{ppath}: must be a non-empty string")
            if pollutant not in POLLUTANTS:
                raise PlanError(f"{ppath}: pollutant must be one of {', '.join(POLLUTANTS)}")
            if pollutant in seen_pollutants:
                raise PlanError(f"{ppath}: duplicate pollutant {pollutant!r}")
            seen_pollutants.add(pollutant)

        zones[station] = zone
        result_stations.append({"station": station, "timezone": tz_name,
                                "pollutants": list(pollutants)})

    return {"schema_version": version, "min_valid_percent": threshold,
            "stations": result_stations}, zones


def parse_cli_date(value, field):
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d").date()
    except (TypeError, ValueError) as exc:
        raise PlanError(f"--{field}: expected YYYY-MM-DD calendar date, got {value!r}") from exc
    return parsed


def daterange(start, end):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


# --------------------------------------------------------------------------- #
# Hour-slot geometry (UTC hour slots attributed to local dates)
# --------------------------------------------------------------------------- #
def expected_slot_starts(day, zone):
    """UTC hour-aligned slot starts whose local date equals ``day``.

    The slot is ``[h, h+1h)`` in UTC.  Its local date comes from converting
    the slot start ``h`` into ``zone``.  The result is ordered by UTC time and
    may contain 23, 24, 25 or even zero entries depending on timezone history.
    """
    midnight_utc = datetime(day.year, day.month, day.day, tzinfo=zone).astimezone(UTC)
    # Floor to the preceding UTC hour; for sub-hour offsets this slot starts
    # just before local midnight and is skipped when its local date differs.
    hour = midnight_utc.replace(minute=0, second=0, microsecond=0)
    slots = []
    while True:
        local = hour.astimezone(zone)
        if local.date() > day:
            break
        if local.date() == day:
            slots.append(hour)
        hour += timedelta(hours=1)
    return slots


def slot_start_for_instant(instant):
    """Floor an aware UTC instant to its containing UTC hour slot start."""
    utc_instant = instant.astimezone(UTC)
    return utc_instant.replace(minute=0, second=0, microsecond=0)


# --------------------------------------------------------------------------- #
# Exact arithmetic helpers
# --------------------------------------------------------------------------- #
def format_six(rational):
    """Render an exact rational as a six-decimal string using ROUND_HALF_UP."""
    with localcontext() as context:
        context.prec = 50
        decimal_value = Decimal(rational.numerator) / Decimal(rational.denominator)
    return str(decimal_value.quantize(SIX_PLACES, rounding=ROUND_HALF_UP))


def _utc_text(instant):
    return instant.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


# --------------------------------------------------------------------------- #
# Snapshot reading
# --------------------------------------------------------------------------- #
def open_readonly_connection(database):
    """Open an existing SQLite file read-only; never create or modify it.

    An explicit deferred read transaction starts immediately so every SELECT
    shares one database snapshot even if a collector commits concurrently.
    """
    path = Path(database)
    if not path.is_file():
        raise FileNotFoundError(f"database does not exist: {path}")
    connection = sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True)
    # A deferred transaction only acquires its read snapshot on the first
    # statement, so run a control SELECT immediately to pin it.  The snapshot
    # is then held until close even if a collector commits concurrently; the
    # read-only URI already forbids any mutation.
    connection.execute("BEGIN")
    connection.execute("SELECT 1")
    return connection


def read_snapshot(connection, stations, pollutants):
    """Return observations and source rows from the open read transaction."""
    station_list = sorted(stations)
    pollutant_list = sorted(pollutants)
    observation_sql = (
        "SELECT o.station, o.observed_at, o.pollutant, o.value_ug_m3, "
        "o.qualifier, o.source_digest, s.label, s.row_count "
        "FROM observations o JOIN sources s ON s.digest = o.source_digest "
        "WHERE o.station IN ({}) AND o.pollutant IN ({}) "
        "ORDER BY o.station, o.observed_at, o.pollutant"
    ).format(",".join("?" * len(station_list)), ",".join("?" * len(pollutant_list)))
    observations = connection.execute(
        observation_sql, [*station_list, *pollutant_list]).fetchall()
    digests = sorted({row[5] for row in observations})
    source_rows = []
    if digests:
        source_rows = connection.execute(
            "SELECT digest, label, row_count FROM sources WHERE digest IN ({}) "
            "ORDER BY digest".format(",".join("?" * len(digests))), digests).fetchall()
    return observations, source_rows


# --------------------------------------------------------------------------- #
# Report building
# --------------------------------------------------------------------------- #
def build_report(plan_bytes, plan, zones, start_day, end_day, rows, source_rows):
    """Aggregate snapshot rows into the documented report structure."""
    grouped = {}
    for station, observed_at, pollutant, value, qualifier, digest, _label, _count in rows:
        instant = datetime.fromisoformat(observed_at)
        slot = slot_start_for_instant(instant)
        grouped.setdefault((station, pollutant, slot), []).append(
            {"observed_at": observed_at, "concentration_ug_m3": value,
             "qualifier": qualifier, "source_digest": digest})

    station_blocks = []
    for entry in plan["stations"]:
        station = entry["station"]
        zone = zones[station]
        pollutant_blocks = []
        for pollutant in entry["pollutants"]:
            day_blocks = []
            for day in daterange(start_day, end_day):
                slots = expected_slot_starts(day, zone)
                slot_blocks = []
                numeric_hours = nd_only_hours = missing_hours = 0
                hour_means = []
                for slot in slots:
                    observations = sorted(
                        grouped.get((station, pollutant, slot), []),
                        key=lambda item: (
                            item["observed_at"], item["qualifier"],
                            item["concentration_ug_m3"] is None,
                            item["concentration_ug_m3"] or "",
                            item["source_digest"]))
                    measured = [item for item in observations
                                if item["qualifier"] == "measured"]
                    nd_count = len(observations) - len(measured)
                    local_start = slot.astimezone(zone)
                    block = {
                        "slot_start_utc": _utc_text(slot),
                        "slot_end_utc": _utc_text(slot + timedelta(hours=1)),
                        "local_start": local_start.isoformat(),
                        "state": "missing",
                        "measured_count": len(measured),
                        "nd_count": nd_count,
                        "hour_mean": None,
                        "observations": [
                            {"station": station,
                             "observed_at": _utc_text(datetime.fromisoformat(item["observed_at"])),
                             "pollutant": pollutant,
                             "concentration_ug_m3": item["concentration_ug_m3"],
                             "qualifier": item["qualifier"],
                             "source_digest": item["source_digest"]}
                            for item in observations],
                    }
                    if measured:
                        block["state"] = "numeric"
                        total = sum((Fraction(item["concentration_ug_m3"])
                                     for item in measured), Fraction(0))
                        mean = total / len(measured)
                        block["hour_mean"] = format_six(mean)
                        hour_means.append(mean)
                        numeric_hours += 1
                    elif observations:
                        block["state"] = "nd_only"
                        nd_only_hours += 1
                    else:
                        missing_hours += 1
                    slot_blocks.append(block)

                expected_hours = len(slots)
                received_hours = numeric_hours + nd_only_hours
                descriptive_mean = None
                if hour_means:
                    day_mean = sum(hour_means, Fraction(0)) / len(hour_means)
                    descriptive_mean = format_six(day_mean)
                if expected_hours == 0:
                    status = "no_expected_hours"
                    published_mean = None
                elif numeric_hours * 100 >= expected_hours * plan["min_valid_percent"]:
                    status = "published"
                    published_mean = descriptive_mean
                else:
                    status = "insufficient_coverage"
                    published_mean = None
                day_blocks.append({
                    "date": day.isoformat(),
                    "status": status,
                    "expected_hours": expected_hours,
                    "numeric_hours": numeric_hours,
                    "nd_only_hours": nd_only_hours,
                    "missing_hours": missing_hours,
                    "received_hours": received_hours,
                    "numeric_coverage": {
                        "numerator": numeric_hours,
                        "denominator": expected_hours,
                        "min_valid_percent": plan["min_valid_percent"],
                        "threshold_met": status == "published",
                    },
                    "descriptive_mean": descriptive_mean,
                    "published_mean": published_mean,
                    "slots": slot_blocks,
                })
            pollutant_blocks.append({"pollutant": pollutant, "days": day_blocks})
        station_blocks.append({
            "station": station,
            "timezone": entry["timezone"],
            "pollutants": pollutant_blocks,
        })

    sources = [{"digest": digest, "label": label, "row_count": row_count}
               for digest, label, row_count in source_rows]
    return {
        "schema_version": SCHEMA_VERSION,
        "report": "air_harvest.daily/v1",
        "plan_sha256": hashlib.sha256(plan_bytes).hexdigest(),
        "request": {
            "start_date": start_day.isoformat(),
            "end_date": end_day.isoformat(),
            "min_valid_percent": plan["min_valid_percent"],
        },
        "stations": station_blocks,
        "sources": sources,
    }


def render_report(payload):
    return (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=False) + "\n").encode("utf-8")


def atomic_write(path, data):
    """Write ``data`` next to ``path`` then atomically replace it."""
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


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def build_from_files(db_path, plan_path, start_text, end_text):
    plan_bytes = Path(plan_path).read_bytes()  # missing plan fails before any DB work
    plan, zones = load_plan(plan_bytes)
    start_day = parse_cli_date(start_text, "start-date")
    end_day = parse_cli_date(end_text, "end-date")
    if end_day < start_day:
        raise PlanError(
            f"--end-date {end_text!r} is before --start-date {start_text!r}")
    connection = open_readonly_connection(db_path)
    try:
        rows, source_rows = read_snapshot(
            connection,
            [entry["station"] for entry in plan["stations"]],
            {pollutant for entry in plan["stations"]
             for pollutant in entry["pollutants"]})
        return build_report(plan_bytes, plan, zones, start_day, end_day,
                            rows, source_rows)
    finally:
        connection.close()


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Build a read-only daily coverage report from stored observations")
    parser.add_argument("--db", required=True, type=Path, help="existing SQLite database")
    parser.add_argument("--plan", required=True, type=Path, help="UTF-8 JSON sampling plan")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        payload = build_from_files(args.db, args.plan, args.start_date, args.end_date)
        data = render_report(payload)
        atomic_write(args.output, data)
    except (PlanError, FileNotFoundError, sqlite3.Error, OSError) as exc:
        parser.exit(2, f"daily report failed: {exc}\n")
    print(f"wrote {args.output} ({len(data)} bytes)")


if __name__ == "__main__":
    main()
