"""Live loopback demonstration of the conditional collection cache.

The demo starts a temporary HTTP fixture on a loopback port, performs real
conditional GET requests against it, and prints the resulting JSON receipts.
It exercises the same public path as ``python -m air_harvest fetch``:

1. a first ``200`` accepts the bulletin (original bytes archived);
2. a second conditional request receives ``304`` and reports ``unchanged``;
3. a changed bulletin (new ETag and an added observation) is accepted again;
4. ``status`` and ``export`` are then read back offline.

``--clean`` removes the demo directory first.  All data are synthetic
demonstration records; no external endpoint is contacted.
"""
from __future__ import annotations

import argparse
import json
import shutil
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from .conditional import export_original, fetch_url, status_report
from .merge_demo import bulletin

FIRST_ROWS = [
    ("River & Park", "2026-01-01T00:00:00Z", "PM2.5", "25", "ug/m3", "measured"),
    ("Hill", "2026-01-01T00:00:00Z", "NO2", "", "ug/m3", "ND"),
    ("Hill", "2026-01-01T01:00:00Z", "PM10", "0", "ug/m3", "measured"),
]
SECOND_ROWS = [
    ("River & Park", "2026-01-01T00:00:00Z", "PM2.5", "25", "ug/m3", "measured"),
    ("Hill", "2026-01-01T00:00:00Z", "NO2", "", "ug/m3", "ND"),
    ("Hill", "2026-01-01T01:00:00Z", "PM10", "0", "ug/m3", "measured"),
    ("Hill", "2026-01-01T02:00:00Z", "NO2", "7", "ug/m3", "measured"),
]


class Fixture:
    def __init__(self):
        self.body = bulletin(FIRST_ROWS)
        self.etag = '"version-1"'
        self.hits = []
        fixture = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_GET(self):
                fixture.hits.append(self.headers.get("If-None-Match"))
                if self.headers.get("If-None-Match") == fixture.etag:
                    self.send_response(304)
                    self.send_header("ETag", fixture.etag)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                self.send_response(200)
                self.send_header("ETag", fixture.etag)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(fixture.body)))
                self.end_headers()
                self.wfile.write(fixture.body)

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


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Loopback demo of conditional fetch, status and export")
    parser.add_argument("--dir", type=Path, default=Path("run/conditional-demo"))
    parser.add_argument("--clean", action="store_true")
    args = parser.parse_args(argv)
    if args.clean and args.dir.exists():
        shutil.rmtree(args.dir)
    args.dir.mkdir(parents=True, exist_ok=True)
    database = args.dir / "air.sqlite"
    if database.exists():
        parser.exit(2, f"demo failed: {database} already exists; rerun with --clean\n")

    fixture = Fixture()
    try:
        def show(title, payload):
            print(f"# {title}")
            print(json.dumps(payload, ensure_ascii=False, sort_keys=True))

        show("first fetch (expect accepted, generation 1)",
             fetch_url(database, fixture.url, label="demo-bulletin"))
        show("second fetch (expect unchanged via 304)",
             fetch_url(database, fixture.url, label="demo-bulletin"))

        fixture.body = bulletin(SECOND_ROWS)
        fixture.etag = '"version-2"'
        show("changed bulletin (expect accepted, generation 2, one insert)",
             fetch_url(database, fixture.url, label="demo-bulletin"))

        show("status (offline)", status_report(database))
        exported = args.dir / "original.html"
        receipt = export_original(database, exported, url=fixture.url)
        show("export (exact original bytes)",
             {**receipt, "output": exported.name})
        print("# fixture received If-None-Match headers:",
              json.dumps(fixture.hits))
        print("# exported bytes match the last accepted bulletin:",
              exported.read_bytes() == fixture.body)
    finally:
        fixture.close()
    print(f"# demo database: {database}")
    return 0


if __name__ == "__main__":
    main()
