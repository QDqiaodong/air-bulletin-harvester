"""Generate the synthetic example shard databases and merge manifest.

Everything is produced through the public collection path: HTML bulletins are
parsed by :func:`air_harvest.parser.parse_bulletin` and stored by
:func:`air_harvest.store.ingest`, so the generated shard databases have exactly
the schema and semantics of real collector output.  The data are original
synthetic demonstrations; no health or regulatory judgment is attached.

Outputs (all under ``--dir``, created fresh)::

    shards/operator-a.sqlite
    shards/operator-b.sqlite
    manifest.json

``manifest.json`` references the shards by paths relative to its own directory
and contains the one explicit resolution required by the example.
"""
from __future__ import annotations

import argparse
import json
import shutil
from html import escape
from pathlib import Path

from .store import ingest

HEADERS = ("station", "observed_at", "pollutant", "value", "unit", "qualifier")


def bulletin(rows):
    head = ('<table id="observations"><tr>' +
            "".join(f"<th>{h}</th>" for h in HEADERS) + "</tr>")
    body = "".join(
        "<tr>" + "".join(f"<td>{escape(str(cell))}</td>" for cell in row) + "</tr>"
        for row in rows)
    return ("<!doctype html><meta charset=\"utf-8\"><title>Synthetic merge "
            "bulletin</title>" + head + body + "</table>").encode("utf-8")


def shard_a_html():
    # River & Park PM2.5 = 25 at the canonical UTC instant, Hill NO2 = ND.
    return bulletin([
        ("River & Park", "2026-01-01T08:00:00+08:00", "PM2.5",
         "25", "ug/m3", "measured"),
        ("Hill", "2026-01-01T00:00:00Z", "NO2", "", "ug/m3", "ND"),
    ])


def shard_b_html():
    # Same two business keys, but PM2.5 conflicts at 30; ND is an equal witness.
    # Hill PM10 = 0 (a real measured zero, distinct from ND) only exists in B.
    return bulletin([
        ("River & Park", "2026-01-01T00:00:00Z", "PM2.5",
         "30", "ug/m3", "measured"),
        ("Hill", "2026-01-01T00:00:00Z", "NO2", "", "ug/m3", "ND"),
        ("Hill", "2026-01-01T00:00:00Z", "PM10", "0", "ug/m3", "measured"),
    ])


def example_manifest():
    return {
        "schema_version": 1,
        "shards": [
            {"shard_id": "operator-a", "path": "shards/operator-a.sqlite"},
            {"shard_id": "operator-b", "path": "shards/operator-b.sqlite"},
        ],
        "resolutions": [
            {
                "station": "River & Park",
                "observed_at": "2026-01-01T00:00:00Z",
                "pollutant": "PM2.5",
                "selected_shard": "operator-b",
                "reason": "operator-b re-checked the sample against the chain of custody and its 30 ug/m3 reading is accepted for the demo",
                "expected": [
                    {"shard_id": "operator-a", "qualifier": "measured",
                     "value_ug_m3": "25"},
                    {"shard_id": "operator-b", "qualifier": "measured",
                     "value_ug_m3": "30"},
                ],
            }
        ],
    }


def build_example(directory):
    directory = Path(directory)
    if directory.exists():
        shutil.rmtree(directory)
    shard_dir = directory / "shards"
    shard_dir.mkdir(parents=True)
    db_a = shard_dir / "operator-a.sqlite"
    db_b = shard_dir / "operator-b.sqlite"
    result_a = ingest(shard_a_html(), db_a, "operator-a-bulletin")
    result_b = ingest(shard_b_html(), db_b, "operator-b-bulletin")
    manifest = example_manifest()
    manifest_path = directory / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return {
        "directory": str(directory),
        "operator_a": result_a,
        "operator_b": result_b,
        "manifest": str(manifest_path),
    }


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Generate synthetic merge example shards and manifest")
    parser.add_argument("--dir", required=True, type=Path,
                        help="fresh output directory (removed first if it exists)")
    args = parser.parse_args(argv)
    summary = build_example(args.dir)
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
