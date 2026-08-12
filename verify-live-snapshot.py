#!/usr/bin/env python3
"""Validate a consumer-built quote/halt/LULD/broker snapshot at its cutoff."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from live_snapshot_contract import LiveSnapshotError, validate_snapshot


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", required=True)
    args = parser.parse_args(argv)
    try:
        value = json.loads(Path(args.snapshot).read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise LiveSnapshotError("Live-snapshot root is not an object")
        result = validate_snapshot(value)
    except (OSError, json.JSONDecodeError, LiveSnapshotError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
