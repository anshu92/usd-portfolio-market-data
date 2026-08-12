#!/usr/bin/env python3
"""Attach a source-backed, validated VTI total-return lane to a release."""

from __future__ import annotations

import argparse
import copy
import csv
import hashlib
import json
import math
import os
import sys
import tempfile
import time as time_module
import urllib.parse
import urllib.request
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Mapping, Sequence

import duckdb

from reliability_contract import apply_dataset_groups


BENCHMARK_ID = "USD_V5_FUNDED_BENCHMARK_INPUT_V1"
MINIMUM_SESSIONS = 140
MINIMUM_WEEKS = 26
RAW_CLOSE_RELATIVE_TOLERANCE = 1e-4
DIVIDEND_RETURN_ABSOLUTE_TOLERANCE = 1e-4
SOURCE_NAME = "Yahoo Finance Chart API"
USER_AGENT = "usd-portfolio-market-data/1.0"

DISTRIBUTION_COLUMNS = (
    ("security_id", "VARCHAR"),
    ("ex_date", "DATE"),
    ("record_date", "DATE"),
    ("pay_date", "DATE"),
    ("cash_amount", "DOUBLE"),
    ("currency", "VARCHAR"),
    ("distribution_type", "VARCHAR"),
    ("distribution_id", "VARCHAR"),
    ("known_at_utc", "TIMESTAMP"),
    ("revision", "VARCHAR"),
    ("source_locator", "VARCHAR"),
    ("source_revision", "VARCHAR"),
    ("source_publication_date", "DATE"),
    ("source_retrieved_at_utc", "TIMESTAMP"),
)

RETURN_COLUMNS = (
    ("security_id", "VARCHAR"),
    ("benchmark_id", "VARCHAR"),
    ("session_date", "DATE"),
    ("price_return", "DOUBLE"),
    ("distribution_return", "DOUBLE"),
    ("total_return", "DOUBLE"),
    ("total_return_index", "DOUBLE"),
    ("certification_status", "VARCHAR"),
    ("distribution_lineage_sha256", "VARCHAR"),
    ("corporate_action_lineage_sha256", "VARCHAR"),
    ("known_at_utc", "TIMESTAMP"),
    ("revision", "VARCHAR"),
    ("source_locator", "VARCHAR"),
    ("source_revision", "VARCHAR"),
    ("source_retrieved_at_utc", "TIMESTAMP"),
)


class BenchmarkBuildError(RuntimeError):
    """Raised when the benchmark lane cannot be certified."""


def canonical_json(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str
    ).encode("utf-8")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def format_utc(value: datetime) -> str:
    normalized = value.astimezone(timezone.utc).replace(microsecond=0)
    return normalized.isoformat().replace("+00:00", "Z")


def parse_utc(value: str | None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise BenchmarkBuildError("--retrieved-at must include a timezone")
    return parsed.astimezone(timezone.utc)


def load_manifest(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise BenchmarkBuildError(f"Cannot read release manifest: {exc}") from exc
    if not isinstance(value, dict) or value.get("status") != "READY":
        raise BenchmarkBuildError("Release manifest is not READY")
    if value.get("schema_version") != "1.0.0":
        raise BenchmarkBuildError("Unsupported release manifest schema")
    return value


def release_records(
    manifest: Mapping[str, object],
) -> dict[str, dict[str, object]]:
    raw = manifest.get("release_files")
    if not isinstance(raw, list):
        raise BenchmarkBuildError("Release manifest lacks release_files")
    output: dict[str, dict[str, object]] = {}
    for record in raw:
        if not isinstance(record, dict):
            raise BenchmarkBuildError("release_files contains a non-object")
        filename = str(record.get("file") or "")
        if not filename or Path(filename).name != filename or filename in output:
            raise BenchmarkBuildError(f"Unsafe or duplicate release file: {filename!r}")
        output[filename] = dict(record)
    return output


def benchmark_security_id(universe_path: Path, ticker: str) -> str:
    matches: list[str] = []
    with universe_path.open(newline="", encoding="utf-8-sig") as handle:
        for row in csv.DictReader(handle):
            if str(row.get("ticker") or "").upper() != ticker.upper():
                continue
            if str(row.get("universe_admission_status") or "").upper() not in {
                "ADMITTED",
                "ADMITTED_ETF",
            }:
                continue
            matches.append(str(row.get("security_id") or ""))
    if len(matches) != 1 or not matches[0]:
        raise BenchmarkBuildError(
            f"Expected one admitted {ticker.upper()} identity, found {matches}"
        )
    return matches[0]


def yahoo_url(symbol: str, target_session: date) -> str:
    period_start = target_session - timedelta(days=800)
    period_end = target_session + timedelta(days=1)
    period1 = int(datetime.combine(period_start, time.min, timezone.utc).timestamp())
    period2 = int(datetime.combine(period_end, time.min, timezone.utc).timestamp())
    return (
        "https://query2.finance.yahoo.com/v8/finance/chart/"
        + urllib.parse.quote(symbol, safe="")
        + f"?period1={period1}&period2={period2}"
        + "&interval=1d&events=div%2Csplits&includeAdjustedClose=true"
    )


def load_source_payload(source_json: Path | None, url: str) -> bytes:
    if source_json is not None:
        return source_json.read_bytes()
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    failures: list[str] = []
    for attempt in range(3):
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                return response.read()
        except OSError as exc:
            failures.append(str(exc))
            if attempt < 2:
                time_module.sleep(2**attempt)
    raise BenchmarkBuildError(
        "Benchmark source request failed after three attempts: " + "; ".join(failures)
    )


def source_result(payload: bytes) -> Mapping[str, object]:
    try:
        body = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BenchmarkBuildError(f"Benchmark source is not valid JSON: {exc}") from exc
    error = (body.get("chart") or {}).get("error") if isinstance(body, dict) else None
    result = (
        ((body.get("chart") or {}).get("result") or [None])[0]
        if isinstance(body, dict)
        else None
    )
    if error or not isinstance(result, dict):
        raise BenchmarkBuildError(f"Benchmark source returned no result: {error}")
    return result


def event_date(event: Mapping[str, object]) -> date:
    try:
        return datetime.fromtimestamp(int(event["date"]), timezone.utc).date()
    except (KeyError, TypeError, ValueError, OSError) as exc:
        raise BenchmarkBuildError("Benchmark event has an invalid date") from exc


def source_observations(
    result: Mapping[str, object], target_session: date
) -> list[tuple[date, float, float]]:
    timestamps = result.get("timestamp") or []
    indicators = result.get("indicators") or {}
    quotes = indicators.get("quote") or []
    adjusted = indicators.get("adjclose") or []
    if not timestamps or not quotes or not adjusted:
        raise BenchmarkBuildError("Benchmark source lacks close or adjusted-close data")
    close_values = quotes[0].get("close") or []
    adjusted_values = adjusted[0].get("adjclose") or []
    if len(close_values) != len(timestamps) or len(adjusted_values) != len(timestamps):
        raise BenchmarkBuildError("Benchmark source arrays have inconsistent lengths")
    by_date: dict[date, tuple[float, float]] = {}
    for timestamp, close, adjusted_close in zip(
        timestamps, close_values, adjusted_values, strict=True
    ):
        if close is None or adjusted_close is None:
            continue
        session = datetime.fromtimestamp(int(timestamp), timezone.utc).date()
        if session > target_session:
            continue
        values = (float(close), float(adjusted_close))
        if not all(math.isfinite(value) and value > 0 for value in values):
            raise BenchmarkBuildError(f"Invalid benchmark prices for {session}")
        if session in by_date:
            raise BenchmarkBuildError(f"Duplicate benchmark source session: {session}")
        by_date[session] = values
    return [(session, *by_date[session]) for session in sorted(by_date)]


def source_dividends(
    result: Mapping[str, object],
    security_id: str,
    first_session: date,
    target_session: date,
    currency: str,
    retrieved_at: str,
    source_locator: str,
    source_revision: str,
) -> list[tuple[object, ...]]:
    events = (result.get("events") or {}).get("dividends") or {}
    output: list[tuple[object, ...]] = []
    seen: set[tuple[date, float]] = set()
    for raw_event_id, raw in sorted(events.items()):
        if not isinstance(raw, dict):
            raise BenchmarkBuildError("Benchmark dividend event is not an object")
        ex_date = event_date(raw)
        if not first_session <= ex_date <= target_session:
            continue
        try:
            amount = float(raw["amount"])
        except (KeyError, TypeError, ValueError) as exc:
            raise BenchmarkBuildError("Benchmark dividend lacks a cash amount") from exc
        if not math.isfinite(amount) or amount <= 0:
            raise BenchmarkBuildError(f"Invalid benchmark dividend for {ex_date}")
        key = (ex_date, amount)
        if key in seen:
            continue
        seen.add(key)
        identity = hashlib.sha256(
            canonical_json(
                {
                    "security_id": security_id,
                    "ex_date": ex_date,
                    "cash_amount": amount,
                    "source_event_id": str(raw_event_id),
                }
            )
        ).hexdigest()
        output.append(
            (
                security_id,
                ex_date,
                None,
                None,
                amount,
                currency,
                "CASH_DIVIDEND",
                f"yahoo:{identity}",
                retrieved_at,
                source_revision,
                source_locator,
                source_revision,
                None,
                retrieved_at,
            )
        )
    return output


def canonical_market(
    release_dir: Path, security_id: str
) -> tuple[list[tuple[date, float]], list[dict[str, object]]]:
    connection = duckdb.connect()
    try:
        prices = [
            (row[0], float(row[1]))
            for row in connection.execute(
                "SELECT session_date, close FROM read_parquet(?) "
                "WHERE security_id = ? ORDER BY session_date",
                [str(release_dir / "yahoo-ohlcv-320.parquet"), security_id],
            ).fetchall()
        ]
        splits = [
            {"event_date": row[0], "split_factor": str(row[1])}
            for row in connection.execute(
                "SELECT event_date, split_factor FROM read_parquet(?) "
                "WHERE security_id = ? ORDER BY event_date, split_factor",
                [str(release_dir / "yahoo-splits.parquet"), security_id],
            ).fetchall()
        ]
    finally:
        connection.close()
    if len(prices) < MINIMUM_SESSIONS:
        raise BenchmarkBuildError(
            f"Canonical benchmark has {len(prices)} sessions; requires {MINIMUM_SESSIONS}"
        )
    return prices, splits


def lineage_sha256(events: Sequence[Mapping[str, object]], session: date) -> str:
    eligible = [event for event in events if event["event_date"] <= session]
    return hashlib.sha256(canonical_json(eligible)).hexdigest()


def build_return_rows(
    observations: Sequence[tuple[date, float, float]],
    canonical: Sequence[tuple[date, float]],
    dividends: Sequence[tuple[object, ...]],
    splits: Sequence[dict[str, object]],
    security_id: str,
    retrieved_at: str,
    source_locator: str,
    source_revision: str,
) -> tuple[list[tuple[object, ...]], float, float]:
    source_by_date = {
        session: (close, adjusted) for session, close, adjusted in observations
    }
    canonical_by_date = dict(canonical)
    missing = sorted(set(canonical_by_date) - set(source_by_date))
    if missing:
        raise BenchmarkBuildError(
            f"Benchmark source misses {len(missing)} canonical sessions; first={missing[0]}"
        )
    maximum_close_error = max(
        abs(source_by_date[session][0] - close) / close for session, close in canonical
    )
    if maximum_close_error > RAW_CLOSE_RELATIVE_TOLERANCE:
        raise BenchmarkBuildError(
            "Benchmark source does not reconcile to canonical closes: "
            f"maximum relative error={maximum_close_error:.8f}"
        )

    dividend_events = [
        {
            "event_date": row[1],
            "distribution_id": row[7],
            "cash_amount": row[4],
        }
        for row in dividends
    ]
    dividend_amounts: dict[date, float] = {}
    for row in dividends:
        dividend_amounts[row[1]] = dividend_amounts.get(row[1], 0.0) + float(row[4])
    ordered_source_dates = [row[0] for row in observations]
    source_position = {
        session: index for index, session in enumerate(ordered_source_dates)
    }
    rows: list[tuple[object, ...]] = []
    total_return_index = 100.0
    maximum_dividend_error = 0.0
    for session, _canonical_close in canonical:
        index = source_position[session]
        if index == 0:
            raise BenchmarkBuildError(
                "Benchmark source lacks the prior session needed for return calculation"
            )
        _previous_date, previous_close, previous_adjusted = observations[index - 1]
        _date, close, adjusted = observations[index]
        price_return = close / previous_close - 1.0
        total_return = adjusted / previous_adjusted - 1.0
        if price_return <= -1 or total_return <= -1:
            raise BenchmarkBuildError(f"Invalid benchmark return for {session}")
        distribution_return = dividend_amounts.get(session, 0.0) / previous_close
        error = abs(total_return - (price_return + distribution_return))
        maximum_dividend_error = max(maximum_dividend_error, error)
        if error > DIVIDEND_RETURN_ABSOLUTE_TOLERANCE:
            raise BenchmarkBuildError(
                f"Dividend-adjustment reconciliation failed for {session}: "
                f"absolute error={error:.8f}"
            )
        total_return_index *= 1.0 + total_return
        rows.append(
            (
                security_id,
                BENCHMARK_ID,
                session,
                price_return,
                distribution_return,
                total_return,
                total_return_index,
                "CERTIFIED",
                lineage_sha256(dividend_events, session),
                lineage_sha256(splits, session),
                retrieved_at,
                source_revision,
                source_locator,
                source_revision,
                retrieved_at,
            )
        )
    return rows, maximum_close_error, maximum_dividend_error


def write_parquet(
    path: Path,
    columns: Sequence[tuple[str, str]],
    rows: Sequence[tuple[object, ...]],
    order_by: str,
) -> None:
    connection = duckdb.connect()
    try:
        definitions = ",".join(f'"{name}" {kind}' for name, kind in columns)
        connection.execute(f"CREATE TABLE output({definitions})")
        if rows:
            placeholders = ",".join("?" for _ in columns)
            connection.executemany(f"INSERT INTO output VALUES ({placeholders})", rows)
        connection.execute(
            f"COPY (SELECT * FROM output ORDER BY {order_by}) TO ? "
            "(FORMAT PARQUET, COMPRESSION ZSTD)",
            [str(path)],
        )
    finally:
        connection.close()


def file_record(
    path: Path, rows: int, columns: Sequence[tuple[str, str]]
) -> dict[str, object]:
    return {
        "file": path.name,
        "sha256": sha256_file(path),
        "bytes": path.stat().st_size,
        "rows": rows,
        "schema": [name for name, _ in columns],
    }


def prior_group_overrides(
    manifest: Mapping[str, object],
) -> dict[str, dict[str, object]]:
    ignored = {
        "group_id",
        "group_contract_version",
        "files",
        "group_sha256",
        "dependencies",
        "dependency_group_identities",
    }
    output: dict[str, dict[str, object]] = {}
    for raw in manifest.get("dataset_groups", []):
        if not isinstance(raw, dict) or not raw.get("group_id"):
            continue
        output[str(raw["group_id"])] = {
            key: copy.deepcopy(value)
            for key, value in raw.items()
            if key not in ignored
        }
    return output


def attach(args: argparse.Namespace) -> dict[str, object]:
    release_dir = Path(args.release_dir).resolve()
    manifest_path = release_dir / "manifest.json"
    manifest = load_manifest(manifest_path)
    records = release_records(manifest)
    for required in (
        "security-universe.csv",
        "yahoo-ohlcv-320.parquet",
        "yahoo-splits.parquet",
    ):
        path = release_dir / required
        record = records.get(required)
        if (
            record is None
            or not path.is_file()
            or sha256_file(path) != record.get("sha256")
        ):
            raise BenchmarkBuildError(f"Release asset identity mismatch: {required}")

    try:
        target_session = date.fromisoformat(
            str((manifest.get("aggregate") or {})["max_date"])
        )
        expected_session = date.fromisoformat(
            str((manifest.get("validation") or {})["expected_latest_xnys_session"])
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise BenchmarkBuildError(
            "Release lacks valid market session watermarks"
        ) from exc
    if target_session != expected_session:
        raise BenchmarkBuildError(
            f"Market release is not current: observed={target_session}, expected={expected_session}"
        )

    security_id = benchmark_security_id(
        release_dir / "security-universe.csv", args.benchmark_ticker
    )
    canonical, splits = canonical_market(release_dir, security_id)
    url = yahoo_url(args.benchmark_ticker.upper(), target_session)
    source_locator = args.source_locator or url
    if not source_locator.startswith("https://"):
        raise BenchmarkBuildError("Benchmark source locator must be HTTPS")
    payload = load_source_payload(
        Path(args.source_json).resolve() if args.source_json else None, url
    )
    revision = hashlib.sha256(payload).hexdigest()
    result = source_result(payload)
    meta = result.get("meta") or {}
    if str(meta.get("symbol") or "").upper() != args.benchmark_ticker.upper():
        raise BenchmarkBuildError("Benchmark source symbol does not match the request")
    currency = str(meta.get("currency") or "")
    if currency != "USD":
        raise BenchmarkBuildError(f"Benchmark source currency is {currency!r}, not USD")
    observations = source_observations(result, target_session)
    if not observations or observations[-1][0] != target_session:
        observed = observations[-1][0] if observations else None
        raise BenchmarkBuildError(
            f"Benchmark source is stale: observed={observed}, expected={target_session}"
        )
    retrieved_at = format_utc(parse_utc(args.retrieved_at))
    dividends = source_dividends(
        result,
        security_id,
        canonical[0][0],
        target_session,
        currency,
        retrieved_at,
        source_locator,
        revision,
    )
    if not dividends:
        raise BenchmarkBuildError(
            "Benchmark source has no cash distributions in the certification window"
        )
    returns, close_error, dividend_error = build_return_rows(
        observations,
        canonical,
        dividends,
        splits,
        security_id,
        retrieved_at,
        source_locator,
        revision,
    )
    weeks = len({session.isocalendar()[:2] for _, _, session, *_ in returns})
    if len(returns) < MINIMUM_SESSIONS or weeks < MINIMUM_WEEKS:
        raise BenchmarkBuildError(
            f"Benchmark history is insufficient: sessions={len(returns)}, weeks={weeks}"
        )

    with tempfile.TemporaryDirectory(dir=release_dir, prefix="benchmark-") as raw_temp:
        temporary = Path(raw_temp)
        distributions_path = temporary / "distributions.parquet"
        returns_path = temporary / "benchmark-total-returns.parquet"
        write_parquet(
            distributions_path,
            DISTRIBUTION_COLUMNS,
            dividends,
            "security_id, ex_date, distribution_id",
        )
        write_parquet(
            returns_path,
            RETURN_COLUMNS,
            returns,
            "security_id, session_date",
        )
        final_distributions = release_dir / distributions_path.name
        final_returns = release_dir / returns_path.name
        os.replace(distributions_path, final_distributions)
        os.replace(returns_path, final_returns)

    records[final_distributions.name] = file_record(
        final_distributions, len(dividends), DISTRIBUTION_COLUMNS
    )
    records[final_returns.name] = file_record(
        final_returns, len(returns), RETURN_COLUMNS
    )
    manifest["release_files"] = [records[name] for name in sorted(records)]
    datasets = manifest.get("datasets")
    if not isinstance(datasets, list):
        datasets = []
    datasets = [
        record
        for record in datasets
        if isinstance(record, dict)
        and record.get("path") not in {final_distributions.name, final_returns.name}
    ]
    for path, minimum, maximum in (
        (
            final_distributions,
            min(row[1] for row in dividends),
            max(row[1] for row in dividends),
        ),
        (final_returns, canonical[0][0], target_session),
    ):
        datasets.append(
            {
                "path": path.name,
                "source": SOURCE_NAME,
                "source_name": SOURCE_NAME,
                "source_revision": revision,
                "immutable_source_revision": revision,
                "source_retrieved_at_utc": retrieved_at,
                "source_retrieval_time": retrieved_at,
                "minimum_event_date": minimum.isoformat(),
                "maximum_event_date": maximum.isoformat(),
                "point_in_time_safe": True,
            }
        )
    manifest["datasets"] = datasets
    manifest["benchmark_total_returns"] = {
        "status": "CERTIFIED",
        "certification_method": (
            "YAHOO_ADJUSTED_CLOSE_RECONCILED_TO_CANONICAL_RAW_CLOSE_AND_CASH_EVENTS"
        ),
        "benchmark_id": BENCHMARK_ID,
        "security_id": security_id,
        "ticker": args.benchmark_ticker.upper(),
        "currency": currency,
        "minimum_session": canonical[0][0].isoformat(),
        "maximum_session": target_session.isoformat(),
        "expected_latest_xnys_session": expected_session.isoformat(),
        "sessions": len(returns),
        "weekly_observations": weeks,
        "distributions": len(dividends),
        "source_name": SOURCE_NAME,
        "source_locator": source_locator,
        "source_revision": revision,
        "source_retrieved_at_utc": retrieved_at,
        "maximum_raw_close_relative_error": close_error,
        "maximum_total_return_reconciliation_error": dividend_error,
        "thresholds": {
            "minimum_sessions": MINIMUM_SESSIONS,
            "minimum_weekly_observations": MINIMUM_WEEKS,
            "maximum_raw_close_relative_error": RAW_CLOSE_RELATIVE_TOLERANCE,
            "maximum_total_return_reconciliation_error": (
                DIVIDEND_RETURN_ABSOLUTE_TOLERANCE
            ),
        },
    }
    overrides = prior_group_overrides(manifest)
    overrides["total_returns"] = {
        "state": "READY_NEW",
        "mode": "FRESH_CERTIFIED_CANDIDATE",
        "freshness": {
            "clock": "XNYS_ELIGIBLE_SESSIONS",
            "expected": expected_session.isoformat(),
            "observed": target_session.isoformat(),
            "lag_eligible_sessions": 0,
            "source_retrieved_at_utc": retrieved_at,
            "state": "READY",
        },
        "validation_errors": [],
        "validation_warnings": [],
        "exclusions": [],
    }
    apply_dataset_groups(
        manifest,
        release_dir,
        group_overrides=overrides,
        candidate_group_failures=manifest.get("candidate_group_failures", []),
        candidate_attempt_failures=manifest.get("candidate_attempt_failures", []),
    )
    temporary_manifest = manifest_path.with_suffix(".json.benchmark.tmp")
    temporary_manifest.write_text(
        json.dumps(manifest, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary_manifest, manifest_path)
    return manifest["benchmark_total_returns"]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release-dir", required=True)
    parser.add_argument("--benchmark-ticker", default="VTI")
    parser.add_argument("--source-json")
    parser.add_argument("--source-locator")
    parser.add_argument("--retrieved-at")
    args = parser.parse_args(argv)
    optional_paths = tuple(
        Path(args.release_dir).resolve() / filename
        for filename in (
            "distributions.parquet",
            "benchmark-total-returns.parquet",
        )
    )
    existed_before = {path: path.exists() for path in optional_paths}
    try:
        result = attach(args)
    except (BenchmarkBuildError, OSError, ValueError, duckdb.Error) as exc:
        for path in optional_paths:
            if not existed_before[path] and path.exists():
                path.unlink()
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
