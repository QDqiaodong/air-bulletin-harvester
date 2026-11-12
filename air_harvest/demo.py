"""An actual HTTP acquisition with a short-lived loopback fixture server."""
import argparse
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from .source import acquire
from .store import ingest


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixture", type=Path, default=Path("data/bulletin.html"))
    parser.add_argument("--db", type=Path, default=Path("run/air.sqlite"))
    args = parser.parse_args()
    payload = args.fixture.read_bytes()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != "/bulletin":
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *args):
            return

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        try:
            raw = acquire(f"http://127.0.0.1:{server.server_port}/bulletin", http=True)
            print(json.dumps(ingest(raw, args.db, args.fixture.name), sort_keys=True))
        finally:
            server.shutdown()
            worker.join()


if __name__ == "__main__":
    main()
