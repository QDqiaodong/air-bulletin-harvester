import argparse
import json
from pathlib import Path
from .source import acquire
from .store import ingest


def main():
    parser = argparse.ArgumentParser(description="Collect a synthetic air-monitoring bulletin")
    sources = parser.add_mutually_exclusive_group(required=True)
    sources.add_argument("--input", type=Path)
    sources.add_argument("--url", help="local fixture HTTP endpoint only")
    parser.add_argument("--db", type=Path, default=Path("run/air.sqlite"))
    args = parser.parse_args()
    try:
        raw = acquire(str(args.input or args.url), http=bool(args.url))
        result = ingest(raw, args.db, Path(args.input).name if args.input else "local-http-bulletin")
    except (OSError, ValueError) as exc:
        parser.exit(2, f"collection failed: {exc}\n")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
