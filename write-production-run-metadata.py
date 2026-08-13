#!/usr/bin/env python3
"""Attach dispatcher timing and outcome metadata to a release manifest."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path


class RunMetadataError(RuntimeError):
    """Raised when production run metadata is incomplete or inconsistent."""


def parse_utc(value: str, label: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise RunMetadataError(f"Invalid {label}") from exc
    if parsed.tzinfo is None:
        raise RunMetadataError(f"{label} has no timezone")
    return parsed.astimezone(timezone.utc)


def format_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--trigger", required=True)
    parser.add_argument("--phase", default="")
    parser.add_argument("--expected-session", default="")
    parser.add_argument("--decision-cutoff", default="")
    parser.add_argument("--scheduled-time", default="")
    parser.add_argument("--actual-start", required=True)
    parser.add_argument("--attempt", required=True)
    parser.add_argument("--idempotency-key", required=True)
    parser.add_argument("--benchmark-outcome", required=True)
    parser.add_argument("--workflow-run-id", required=True)
    parser.add_argument("--producer-commit", required=True)
    args = parser.parse_args(argv)
    try:
        path = Path(args.manifest)
        manifest = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(manifest, dict):
            raise RunMetadataError("Manifest root is not an object")
        completed = datetime.now(timezone.utc)
        actual_start = parse_utc(args.actual_start, "actual start")
        scheduled = (
            parse_utc(args.scheduled_time, "scheduled time")
            if args.scheduled_time
            else None
        )
        expected = args.expected_session or str(
            (manifest.get("validation") or {}).get("expected_latest_xnys_session")
            or ""
        )
        deadline_miss_reason: str | None = None
        if scheduled is None:
            deadline_miss_reason = "NO_EXTERNAL_SCHEDULE_METADATA"
        elif actual_start > scheduled:
            deadline_miss_reason = (
                "STARTED_AFTER_SCHEDULE_BY_"
                f"{int((actual_start - scheduled).total_seconds())}_SECONDS"
            )
        manifest["production_run"] = {
            "schema_version": "1.0.0",
            "trigger": args.trigger,
            "phase": args.phase or None,
            "expected_session": expected,
            "decision_cutoff_utc": args.decision_cutoff or None,
            "scheduled_time_utc": format_utc(scheduled) if scheduled else None,
            "actual_start_time_utc": format_utc(actual_start),
            "completion_time_utc": format_utc(completed),
            "deadline_miss_reason": deadline_miss_reason,
            "attempt": args.attempt,
            "idempotency_key": args.idempotency_key,
            "benchmark_outcome": args.benchmark_outcome.upper(),
            "workflow_run_id": int(args.workflow_run_id),
            "producer_commit": args.producer_commit,
        }
        temporary = path.with_suffix(path.suffix + ".run.tmp")
        temporary.write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        os.replace(temporary, path)
    except (OSError, json.JSONDecodeError, RunMetadataError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(manifest["production_run"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
