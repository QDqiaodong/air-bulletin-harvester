import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from air_harvest.parser import parse_bulletin
from air_harvest.source import acquire, MAX_BYTES
from air_harvest.store import ingest

FIXTURE = Path(__file__).resolve().parents[1] / "data/bulletin.html"


class CollectorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = Path(self.temp.name) / "air.sqlite"
        self.raw = FIXTURE.read_bytes()

    def test_unit_timezone_entities_and_nd(self):
        rows = parse_bulletin(self.raw.decode())
        self.assertEqual(rows[0][:4], ("River & Park", "2026-01-01T00:00:00+00:00", "PM2.5", "25.000"))
        self.assertIsNone(rows[2][3])
        self.assertEqual(rows[3][3], "0")

    def test_deduplication_and_replay(self):
        self.assertEqual(ingest(self.raw, self.db, "fixture")["inserted"], 3)
        self.assertEqual(ingest(self.raw, self.db, "fixture")["inserted"], 0)
        with sqlite3.connect(self.db) as db:
            self.assertEqual(db.execute("SELECT count(*) FROM sources").fetchone()[0], 1)
            self.assertEqual(db.execute("SELECT count(*) FROM observations WHERE value_ug_m3 IS NULL").fetchone()[0], 1)

    def test_conflict_rolls_back_batch_and_provenance(self):
        ingest(self.raw, self.db, "first")
        changed = self.raw.replace(b"River &amp; Park", b"New Station", 1).replace(b"<td>25</td>", b"<td>26</td>")
        with self.assertRaisesRegex(ValueError, "conflicting"):
            ingest(changed, self.db, "changed")
        with sqlite3.connect(self.db) as db:
            self.assertEqual(db.execute("SELECT count(*) FROM observations").fetchone()[0], 3)
            self.assertEqual(db.execute("SELECT count(*) FROM sources").fetchone()[0], 1)

    def test_missing_table_or_headers(self):
        for raw in ["<html></html>", self.raw.decode().replace("station</th>", "site</th>")]:
            with self.assertRaises(ValueError):
                parse_bulletin(raw)

    def test_invalid_values_and_missing_timezone(self):
        for replacement in ["NaN", "-1", "Infinity", "bad", "1e1000", "1e-1000"]:
            with self.subTest(value=replacement), self.assertRaises(ValueError):
                parse_bulletin(self.raw.decode().replace("0.025", replacement))
        with self.assertRaisesRegex(ValueError, "timezone"):
            parse_bulletin(self.raw.decode().replace("2026-01-01T08:00:00+08:00", "2026-01-01T08:00:00"))

    def test_nd_cannot_be_zero(self):
        with self.assertRaisesRegex(ValueError, "ND"):
            parse_bulletin(self.raw.decode().replace("<td></td>", "<td>0</td>"))

    def test_missing_and_oversized_source(self):
        with self.assertRaises(FileNotFoundError):
            acquire(Path(self.temp.name) / "absent")
        large = Path(self.temp.name) / "large.html"
        large.write_bytes(b"x" * (MAX_BYTES+1))
        with self.assertRaisesRegex(ValueError, "limit"):
            acquire(large)

    def test_no_external_endpoint_or_credentials(self):
        for url in ["https://example.com", "http://example.com", "http://user:secret@localhost/"]:
            with self.assertRaises(ValueError):
                acquire(url, http=True)

    def test_empty_valid_table(self):
        header = self.raw.decode().split("<tr><td>")[0] + "</table>"
        self.assertEqual(ingest(header.encode(), self.db, "empty")["total"], 0)


if __name__ == "__main__":
    unittest.main()
