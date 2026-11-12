"""Transactional append-only observation storage with source content hashes."""
import hashlib
import sqlite3
from decimal import Decimal
from pathlib import Path
from .parser import parse_bulletin

SCHEMA = """
CREATE TABLE IF NOT EXISTS sources (
 digest TEXT PRIMARY KEY, label TEXT NOT NULL, row_count INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS observations (
 station TEXT NOT NULL, observed_at TEXT NOT NULL, pollutant TEXT NOT NULL,
 value_ug_m3 TEXT, qualifier TEXT NOT NULL,
 source_digest TEXT NOT NULL REFERENCES sources(digest),
 PRIMARY KEY(station, observed_at, pollutant));
"""


def ingest(raw, database, label):
    rows = parse_bulletin(raw.decode("utf-8"))
    digest = hashlib.sha256(raw).hexdigest()
    Path(database).parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(database) as db:
        db.execute("PRAGMA foreign_keys=ON")
        db.executescript(SCHEMA)
        db.execute("INSERT OR IGNORE INTO sources VALUES (?, ?, ?)", (digest, label, len(rows)))
        inserted = 0
        for station, instant, pollutant, value, qualifier in rows:
            key = (station, instant, pollutant)
            existing = db.execute("SELECT value_ug_m3,qualifier FROM observations WHERE station=? AND observed_at=? AND pollutant=?", key).fetchone()
            if existing:
                same_value = existing[0] == value or (existing[0] is not None and value is not None and Decimal(existing[0]) == Decimal(value))
                if not same_value or existing[1] != qualifier:
                    raise ValueError(f"conflicting observation: {key}")
                continue
            db.execute("INSERT INTO observations VALUES (?,?,?,?,?,?)", (*key, value, qualifier, digest))
            inserted += 1
        total = db.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
    return {"source_sha256": digest, "rows": len(rows), "inserted": inserted, "duplicates": len(rows)-inserted, "total": total}
