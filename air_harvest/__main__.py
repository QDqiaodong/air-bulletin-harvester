"""Module entry point.

``fetch``, ``status`` and ``export`` dispatch to the conditional collection
cache.  ``demo``, ``daily``, ``merge`` and ``merge-demo`` dispatch to their own
modules.  Every other invocation keeps the original one-shot CLI contract
(``--input``/``--url`` + ``--db``) byte-for-byte.
"""
import sys

from .conditional_cli import main as conditional_main

COMMANDS = {"fetch", "status", "export", "demo", "daily", "merge", "merge-demo"}


def legacy_main(argv=None):
    import argparse
    import json
    from pathlib import Path
    from .source import acquire
    from .store import ingest

    parser = argparse.ArgumentParser(
        description="Collect a synthetic air-monitoring bulletin")
    sources = parser.add_mutually_exclusive_group(required=True)
    sources.add_argument("--input", type=Path)
    sources.add_argument("--url", help="local fixture HTTP endpoint only")
    parser.add_argument("--db", type=Path, default=Path("run/air.sqlite"))
    args = parser.parse_args(argv)
    try:
        raw = acquire(str(args.input or args.url), http=bool(args.url))
        result = ingest(
            raw, args.db,
            Path(args.input).name if args.input else "local-http-bulletin")
    except (OSError, ValueError) as exc:
        parser.exit(2, f"collection failed: {exc}\n")
    print(json.dumps(result, sort_keys=True))


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] in COMMANDS:
        command = argv[0]
        if command in {"fetch", "status", "export"}:
            return conditional_main(argv)
        if command == "demo":
            from .demo import main as demo_main
            return demo_main(argv[1:])
        if command == "daily":
            from .daily import main as daily_main
            return daily_main(argv[1:])
        if command == "merge":
            from .merge import main as merge_main
            return merge_main(argv[1:])
        from .merge_demo import main as merge_demo_main
        return merge_demo_main(argv[1:])
    return legacy_main(argv)


if __name__ == "__main__":
    main()
