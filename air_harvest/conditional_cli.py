"""Command line entry points for the conditional collection cache.

``python -m air_harvest fetch|status|export ...`` prints one deterministic JSON
document to stdout.  Failures print a short located message to stderr and exit
with status 2; a failed command never publishes a partial receipt file and
never mutates an existing export.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

from .conditional import (
    CacheCorruption, CollectionError, ExportError, UnknownSchema, export_original,
    fetch_url, status_report,
)
from .source import AcquisitionError


def _parser():
    parser = argparse.ArgumentParser(
        prog="python -m air_harvest",
        description="Conditional loopback bulletin collection cache")
    commands = parser.add_subparsers(dest="command", required=True)

    fetch = commands.add_parser(
        "fetch", help="conditionally GET one loopback bulletin")
    fetch.add_argument("--url", required=True,
                       help="plain http loopback URL (127.0.0.1, localhost or ::1)")
    fetch.add_argument("--db", required=True, type=Path,
                       help="collector SQLite database (created on first accept)")
    fetch.add_argument("--label", default=None,
                       help="optional source label; defaults to the canonical URL")

    status = commands.add_parser(
        "status", help="offline read-only cache status and verification")
    status.add_argument("--db", required=True, type=Path)

    export = commands.add_parser(
        "export", help="atomically export exact stored original bytes")
    export.add_argument("--db", required=True, type=Path)
    export.add_argument("--output", required=True, type=Path)
    selector = export.add_mutually_exclusive_group(required=True)
    selector.add_argument("--url", help="loopback URL whose cached bytes to export")
    selector.add_argument("--digest", help="source SHA256 whose bytes to export")
    return parser


def main(argv=None):
    parser = _parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "fetch":
            payload = fetch_url(args.db, args.url, args.label)
        elif args.command == "status":
            payload = status_report(args.db)
        else:
            payload = export_original(
                args.db, args.output, url=args.url, digest=args.digest)
    except (CollectionError, AcquisitionError, UnknownSchema, CacheCorruption,
            ExportError, FileNotFoundError, sqlite3.Error, OSError) as exc:
        parser.exit(2, f"{args.command} failed: {exc}\n")
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    main()
