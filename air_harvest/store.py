"""Transactional append-only observation storage with source content hashes."""
import hashlib
import sqlite3
from decimal import Decimal
from pathlib import Path
from .parser import parse_bulletin

SCHEMA_STATEMENTS = [
    "CREATE TABLE IF NOT EXISTS sources ("
    " digest TEXT PRIMARY KEY, label TEXT NOT NULL, row_count INTEGER NOT NULL)",
    "CREATE TABLE IF NOT EXISTS observations ("
    " station TEXT NOT NULL, observed_at TEXT NOT NULL, pollutant TEXT NOT NULL,"
    " value_ug_m3 TEXT, qualifier TEXT NOT NULL,"
    " source_digest TEXT NOT NULL REFERENCES sources(digest),"
    " PRIMARY KEY(station, observed_at, pollutant))",
]
SCHEMA = "".join(statement + ";\n" for statement in SCHEMA_STATEMENTS)


def apply_batch(db, raw, rows, label):
    """Apply parsed rows to an open connection and return ``(digest, inserted)``.

    The caller owns the transaction and the schema.  Source registration is
    insert-or-ignore so the first accepted label and parsed row count win;
    duplicate content never rewrites that provenance.  A conflicting value or
    qualifier raises :class:`ValueError`, which rolls back whatever transaction
    encloses this call.
    """
    digest = hashlib.sha256(raw).hexdigest()
    db.execute("INSERT OR IGNORE INTO sources VALUES (?, ?, ?)",
               (digest, label, len(rows)))
    inserted = 0
    for station, instant, pollutant, value, qualifier in rows:
        key = (station, instant, pollutant)
        existing = db.execute(
            "SELECT value_ug_m3,qualifier FROM observations "
            "WHERE station=? AND observed_at=? AND pollutant=?", key).fetchone()
        if existing:
            same_value = existing[0] == value or (
                existing[0] is not None and value is not None
                and Decimal(existing[0]) == Decimal(value))
            if not same_value or existing[1] != qualifier:
                raise ValueError(f"conflicting observation: {key}")
            continue
        db.execute("INSERT INTO observations VALUES (?,?,?,?,?,?)",
                   (*key, value, qualifier, digest))
        inserted += 1
    return digest, inserted


def ingest(raw, database, label):
    rows = parse_bulletin(raw.decode("utf-8"))
    Path(database).parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(database) as db:
        db.execute("PRAGMA foreign_keys=ON")
        db.executescript(SCHEMA)
        digest, inserted = apply_batch(db, raw, rows, label)
        total = db.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
    return {"source_sha256": digest, "rows": len(rows), "inserted": inserted,
            "duplicates": len(rows) - inserted, "total": total}
