#!/usr/bin/env python3
"""Build a frozen-denominator diagnostic for one broad-market ingest attempt."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

import duckdb


class CoverageDiagnosticError(RuntimeError):
    """Raised when an ingest attempt cannot be described deterministically."""


def load_json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CoverageDiagnosticError(f"Cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise CoverageDiagnosticError(f"{path} root is not an object")
    return value


def load_universe(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        rows = [
            {str(key): str(value or "") for key, value in row.items()}
            for row in csv.DictReader(handle)
            if str(row.get("universe_admission_status") or "").upper()
            in {"ADMITTED", "ADMITTED_ETF"}
        ]
    identities = [row.get("security_id", "") for row in rows]
    if not rows or any(not value for value in identities) or len(set(identities)) != len(rows):
        raise CoverageDiagnosticError("Eligible-universe denominator is invalid")
    return rows


def current_rows(
    path: Path, expected_session: str
) -> tuple[set[str], str | None, dict[str, str]]:
    if not path.is_file():
        return set(), None, {}
    connection = duckdb.connect()
    try:
        valid = {
            str(row[0])
            for row in connection.execute(
                """
                SELECT DISTINCT security_id FROM read_parquet(?)
                WHERE session_date = cast(? AS DATE)
                  AND open IS NOT NULL AND high IS NOT NULL AND low IS NOT NULL
                  AND close IS NOT NULL AND volume IS NOT NULL
                  AND isfinite(open) AND isfinite(high) AND isfinite(low)
                  AND isfinite(close) AND open > 0 AND high > 0 AND low > 0
                  AND close > 0 AND volume >= 0 AND low <= least(open, close)
                  AND high >= greatest(open, close) AND low <= high
                """,
                [str(path), expected_session],
            ).fetchall()
        }
        maximum = connection.execute(
            "SELECT max(session_date) FROM read_parquet(?)", [str(path)]
        ).fetchone()[0]
        latest_by_id = {
            str(row[0]): str(row[1])
            for row in connection.execute(
                "SELECT security_id, max(session_date) FROM read_parquet(?) GROUP BY security_id",
                [str(path)],
            ).fetchall()
        }
    finally:
        connection.close()
    return valid, str(maximum) if maximum is not None else None, latest_by_id


def build(manifest: Mapping[str, object], universe: list[dict[str, str]], prices: Path) -> dict[str, object]:
    validation = manifest.get("validation")
    source = manifest.get("source")
    if not isinstance(validation, dict) or not isinstance(source, dict):
        raise CoverageDiagnosticError("Attempt manifest lacks validation/source data")
    quality = source.get("quality") if isinstance(source.get("quality"), dict) else {}
    expected = str(validation.get("expected_latest_xnys_session") or "")
    if not expected:
        raise CoverageDiagnosticError("Attempt manifest lacks expected XNYS session")
    valid_ids, observed_maximum, latest_by_id = current_rows(prices, expected)
    eligible_ids = {row["security_id"] for row in universe}
    valid_ids &= eligible_ids

    invalid_by_id: dict[str, str] = {}
    for item in quality.get("unresolved_invalid_rows") or []:
        if isinstance(item, dict) and str(item.get("session_date") or "") == expected:
            invalid_by_id[str(item.get("security_id") or "")] = str(
                item.get("reason") or "INVALID_CURRENT_SESSION_ROW"
            )
    stale_by_id = {
        str(item.get("security_id") or ""): str(item.get("latest_session") or "")
        for item in quality.get("stale_source_symbols") or []
        if isinstance(item, dict)
    }
    chart = source.get("yahoo_chart_supplement")
    chart = chart if isinstance(chart, dict) else {}
    targeted = chart.get("targeted_current_session_retry")
    targeted = targeted if isinstance(targeted, dict) else {}
    provider_failures = {
        str(item.get("security_id") or "")
        for item in chart.get("failures") or []
        if isinstance(item, dict)
    } | {
        str(item.get("security_id") or "")
        for item in targeted.get("failures") or []
        if isinstance(item, dict)
    }
    quarantined_sessions = {
        str(item.get("session_date") or "")
        for item in quality.get("common_mode_candidate_failures") or []
        if isinstance(item, dict)
    }

    per_security: list[dict[str, object]] = []
    reason_counts: Counter[str] = Counter()
    exchange_counts: dict[str, Counter[str]] = {}
    type_counts: dict[str, Counter[str]] = {}
    category_counts: Counter[str] = Counter()
    for identity in sorted(universe, key=lambda value: value["security_id"]):
        security_id = identity["security_id"]
        if security_id in valid_ids:
            category = "valid"
            codes: list[str] = []
        elif security_id in invalid_by_id:
            category = "invalid"
            codes = [invalid_by_id[security_id]]
        elif expected in quarantined_sessions:
            category = "quarantined"
            codes = ["QUARANTINED_COMMON_MODE_SESSION"]
        elif security_id in stale_by_id or (
            security_id in latest_by_id and latest_by_id[security_id] < expected
        ):
            category = "stale"
            latest = stale_by_id.get(security_id) or latest_by_id.get(security_id)
            codes = [f"STALE_SOURCE_MAX_SESSION_{latest}"]
        elif security_id in provider_failures:
            category = "missing"
            codes = ["PROVIDER_RETRIES_EXHAUSTED"]
        else:
            category = "missing"
            codes = ["MISSING_CURRENT_SESSION_ROW"]
        category_counts[category] += 1
        reason_counts.update(codes)
        exchange = identity.get("exchange_mic") or "UNKNOWN"
        security_type = identity.get("security_type") or "UNKNOWN"
        exchange_counts.setdefault(exchange, Counter())[category] += 1
        type_counts.setdefault(security_type, Counter())[category] += 1
        per_security.append(
            {
                "security_id": security_id,
                "ticker": identity.get("ticker"),
                "exchange_mic": exchange,
                "security_type": security_type,
                "state": category.upper(),
                "rejection_codes": codes,
            }
        )

    denominator = len(universe)
    accounted = sum(category_counts.values())
    if accounted != denominator:
        raise CoverageDiagnosticError("Coverage categories do not reconcile")
    repair = quality.get("repair") if isinstance(quality.get("repair"), dict) else {}
    return {
        "schema_version": "1.0.0",
        "state": "READY" if manifest.get("status") == "READY" else "QUARANTINED",
        "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "frozen_eligible_universe": {
            "denominator": denominator,
            "identity_sha256": (manifest.get("universe") or {}).get("sha256")
            if isinstance(manifest.get("universe"), dict)
            else None,
        },
        "expected_maximum_session": expected,
        "observed_maximum_session": observed_maximum,
        "coverage": {
            "valid_current_session_rows": category_counts["valid"],
            "missing": category_counts["missing"],
            "stale": category_counts["stale"],
            "invalid": category_counts["invalid"],
            "quarantined": category_counts["quarantined"],
            "ratio": category_counts["valid"] / denominator,
            "required_ratio": 0.95,
        },
        "counts_by_exchange": {
            key: dict(sorted(value.items())) for key, value in sorted(exchange_counts.items())
        },
        "counts_by_security_type": {
            key: dict(sorted(value.items())) for key, value in sorted(type_counts.items())
        },
        "counts_by_failure_reason": dict(sorted(reason_counts.items())),
        "provider_requests": {
            "current_delta": chart,
            "targeted_invalid_row_retries": repair,
        },
        "per_security": per_security,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--universe", required=True)
    parser.add_argument("--prices", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args(argv)
    try:
        payload = build(
            load_json(Path(args.manifest)),
            load_universe(Path(args.universe)),
            Path(args.prices),
        )
        target = Path(args.out)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    except (CoverageDiagnosticError, OSError, duckdb.Error, ZeroDivisionError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(payload["coverage"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
