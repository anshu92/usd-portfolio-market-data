#!/usr/bin/env python3
"""Build an independent, all-or-nothing certified benchmark return lane."""

from __future__ import annotations

import argparse
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


REQUIRED_BENCHMARKS = ("VTI", "SPY", "BIL")
BENCHMARK_ROLES = {
    "VTI": "FUNDED_EQUITY_BENCHMARK",
    "SPY": "US_EQUITY_REFERENCE",
    "BIL": "CASH_REFERENCE",
}
BENCHMARK_ID = "USD_V5_FUNDED_BENCHMARK_INPUT_V2"
MINIMUM_SESSIONS = 140
MINIMUM_WEEKS = 26
MAXIMUM_SESSIONS = 320
RETURN_RECONCILIATION_TOLERANCE = 1e-4
SOURCE_NAME = "Yahoo Finance Chart API"
USER_AGENT = "usd-portfolio-market-data/2.0"

RETURN_FILENAME = "benchmark-total-returns.parquet"
DISTRIBUTION_FILENAME = "benchmark-distributions.parquet"
CERTIFICATION_FILENAME = "benchmark-certification.json"

RETURN_COLUMNS = (
    ("security_id", "VARCHAR"),
    ("ticker", "VARCHAR"),
    ("benchmark_id", "VARCHAR"),
    ("benchmark_role", "VARCHAR"),
    ("session_date", "DATE"),
    ("raw_close", "DOUBLE"),
    ("split_factor", "DOUBLE"),
    ("cash_distribution", "DOUBLE"),
    ("price_return", "DOUBLE"),
    ("distribution_return", "DOUBLE"),
    ("total_return", "DOUBLE"),
    ("total_return_index", "DOUBLE"),
    ("currency", "VARCHAR"),
    ("certification_status", "VARCHAR"),
    ("distribution_lineage_sha256", "VARCHAR"),
    ("corporate_action_lineage_sha256", "VARCHAR"),
    ("source_locator", "VARCHAR"),
    ("source_revision", "VARCHAR"),
    ("source_retrieved_at_utc", "TIMESTAMP"),
    ("known_at_utc", "TIMESTAMP"),
    ("certified_at_utc", "TIMESTAMP"),
)

DISTRIBUTION_COLUMNS = (
    ("security_id", "VARCHAR"),
    ("ticker", "VARCHAR"),
    ("ex_date", "DATE"),
    ("cash_amount", "DOUBLE"),
    ("currency", "VARCHAR"),
    ("distribution_type", "VARCHAR"),
    ("distribution_id", "VARCHAR"),
    ("source_locator", "VARCHAR"),
    ("source_revision", "VARCHAR"),
    ("source_retrieved_at_utc", "TIMESTAMP"),
    ("known_at_utc", "TIMESTAMP"),
    ("certified_at_utc", "TIMESTAMP"),
)


class BenchmarkBuildError(RuntimeError):
    """Raised when no canonical benchmark lane can be certified."""


def canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def format_utc(value: datetime) -> str:
    return (
        value.astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def parse_utc(value: str | None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise BenchmarkBuildError("Timestamp is not RFC3339") from exc
    if parsed.tzinfo is None:
        raise BenchmarkBuildError("Timestamp must include a timezone")
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


def release_records(manifest: Mapping[str, object]) -> dict[str, dict[str, object]]:
    raw = manifest.get("release_files")
    if not isinstance(raw, list):
        raise BenchmarkBuildError("Release manifest lacks release_files")
    output: dict[str, dict[str, object]] = {}
    for record in raw:
        if not isinstance(record, dict):
            raise BenchmarkBuildError("release_files contains a non-object")
        filename = str(record.get("file") or "")
        if not filename or Path(filename).name != filename or filename in output:
            raise BenchmarkBuildError(f"Unsafe release file identity: {filename!r}")
        output[filename] = dict(record)
    return output


def benchmark_identities(
    universe_path: Path, required: Sequence[str]
) -> dict[str, dict[str, str]]:
    required_set = {value.upper() for value in required}
    matches: dict[str, list[dict[str, str]]] = {value: [] for value in required_set}
    with universe_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            ticker = str(row.get("ticker") or "").upper()
            if ticker not in required_set:
                continue
            if str(row.get("universe_admission_status") or "").upper() not in {
                "ADMITTED",
                "ADMITTED_ETF",
            }:
                continue
            matches[ticker].append(
                {
                    "security_id": str(row.get("security_id") or ""),
                    "ticker": ticker,
                    "exchange_mic": str(row.get("exchange_mic") or ""),
                    "security_type": str(row.get("security_type") or ""),
                    "currency": str(row.get("currency") or "USD"),
                }
            )
    invalid = {ticker: rows for ticker, rows in matches.items() if len(rows) != 1}
    if invalid or any(not rows[0]["security_id"] for rows in matches.values()):
        raise BenchmarkBuildError(
            "Benchmark identity subset is incomplete or ambiguous: "
            + ", ".join(f"{ticker}={len(rows)}" for ticker, rows in sorted(invalid.items()))
        )
    output = {ticker: rows[0] for ticker, rows in matches.items()}
    if any(record["currency"] not in {"", "USD"} for record in output.values()):
        raise BenchmarkBuildError("Benchmark identity subset contains a non-USD security")
    return output


def yahoo_url(symbol: str, target_session: date) -> str:
    period_start = target_session - timedelta(days=900)
    period_end = target_session + timedelta(days=1)
    period1 = int(datetime.combine(period_start, time.min, timezone.utc).timestamp())
    period2 = int(datetime.combine(period_end, time.min, timezone.utc).timestamp())
    return (
        "https://query2.finance.yahoo.com/v8/finance/chart/"
        + urllib.parse.quote(symbol, safe="")
        + f"?period1={period1}&period2={period2}"
        + "&interval=1d&events=div%2Csplits&includeAdjustedClose=true"
    )


def source_json_path(directory: Path | None, ticker: str) -> Path | None:
    if directory is None:
        return None
    for name in (f"{ticker}.json", f"{ticker.lower()}.json"):
        path = directory / name
        if path.is_file():
            return path
    raise BenchmarkBuildError(f"Fixture source is missing for {ticker}")


def load_source_payload(path: Path | None, url: str) -> tuple[bytes, list[dict[str, object]]]:
    if path is not None:
        return path.read_bytes(), [{"attempt": 1, "result": "FIXTURE"}]
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    attempts: list[dict[str, object]] = []
    for attempt in range(1, 5):
        started = datetime.now(timezone.utc)
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                payload = response.read()
            attempts.append(
                {
                    "attempt": attempt,
                    "result": "SUCCESS",
                    "http_status": 200,
                    "started_at_utc": format_utc(started),
                    "completed_at_utc": format_utc(datetime.now(timezone.utc)),
                }
            )
            return payload, attempts
        except OSError as exc:
            attempts.append(
                {
                    "attempt": attempt,
                    "result": "ERROR",
                    "error": str(exc),
                    "started_at_utc": format_utc(started),
                    "completed_at_utc": format_utc(datetime.now(timezone.utc)),
                }
            )
            if attempt < 4:
                time_module.sleep(2 ** (attempt - 1))
    raise BenchmarkBuildError("Benchmark source failed after four targeted attempts")


def source_result(payload: bytes, ticker: str) -> Mapping[str, object]:
    try:
        body = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BenchmarkBuildError(f"{ticker} source is not valid JSON") from exc
    chart = body.get("chart") if isinstance(body, dict) else None
    result = ((chart or {}).get("result") or [None])[0] if isinstance(chart, dict) else None
    if (chart or {}).get("error") or not isinstance(result, dict):
        raise BenchmarkBuildError(f"{ticker} source returned no result")
    meta = result.get("meta") or {}
    if str(meta.get("symbol") or "").upper() != ticker:
        raise BenchmarkBuildError(f"{ticker} source symbol mismatch")
    if str(meta.get("currency") or "") != "USD":
        raise BenchmarkBuildError(f"{ticker} source currency is not USD")
    return result


def event_date(event: Mapping[str, object]) -> date:
    try:
        return datetime.fromtimestamp(int(event["date"]), timezone.utc).date()
    except (KeyError, TypeError, ValueError, OSError) as exc:
        raise BenchmarkBuildError("Corporate action has an invalid date") from exc


def observations(result: Mapping[str, object], target: date) -> list[dict[str, object]]:
    timestamps = result.get("timestamp") or []
    indicators = result.get("indicators") or {}
    quote = ((indicators.get("quote") or [{}])[0])
    adjusted = ((indicators.get("adjclose") or [{}])[0]).get("adjclose") or []
    fields = {name: quote.get(name) or [] for name in ("open", "high", "low", "close", "volume")}
    if not timestamps or len(adjusted) != len(timestamps) or any(
        len(values) != len(timestamps) for values in fields.values()
    ):
        raise BenchmarkBuildError("Benchmark source arrays are incomplete")
    output: list[dict[str, object]] = []
    seen: set[date] = set()
    for index, timestamp in enumerate(timestamps):
        session = datetime.fromtimestamp(int(timestamp), timezone.utc).date()
        if session > target:
            continue
        values = {name: fields[name][index] for name in fields}
        values["adjusted_close"] = adjusted[index]
        if any(values[name] is None for name in ("open", "high", "low", "close", "volume", "adjusted_close")):
            continue
        numeric = {name: float(value) for name, value in values.items()}
        if (
            any(not math.isfinite(value) for value in numeric.values())
            or min(numeric[name] for name in ("open", "high", "low", "close", "adjusted_close")) <= 0
            or numeric["volume"] < 0
            or numeric["high"] < max(numeric["open"], numeric["low"], numeric["close"])
            or numeric["low"] > min(numeric["open"], numeric["high"], numeric["close"])
            or session in seen
        ):
            raise BenchmarkBuildError(f"Invalid OHLCV observation for {session}")
        seen.add(session)
        output.append({"session_date": session, **numeric})
    output.sort(key=lambda row: row["session_date"])
    return output


def dividend_events(result: Mapping[str, object]) -> list[dict[str, object]]:
    raw_events = ((result.get("events") or {}).get("dividends") or {})
    output: list[dict[str, object]] = []
    for event_id, raw in sorted(raw_events.items()):
        if not isinstance(raw, dict):
            raise BenchmarkBuildError("Dividend event is not an object")
        amount = float(raw.get("amount") or 0)
        if not math.isfinite(amount) or amount <= 0:
            raise BenchmarkBuildError("Dividend event has an invalid cash amount")
        output.append(
            {"event_id": str(event_id), "event_date": event_date(raw), "cash_amount": amount}
        )
    return output


def split_events(result: Mapping[str, object]) -> list[dict[str, object]]:
    raw_events = ((result.get("events") or {}).get("splits") or {})
    output: list[dict[str, object]] = []
    for event_id, raw in sorted(raw_events.items()):
        if not isinstance(raw, dict):
            raise BenchmarkBuildError("Split event is not an object")
        numerator = float(raw.get("numerator") or 0)
        denominator = float(raw.get("denominator") or 0)
        factor = numerator / denominator if denominator else 0
        if not math.isfinite(factor) or factor <= 0:
            raise BenchmarkBuildError("Split event has an invalid factor")
        output.append(
            {"event_id": str(event_id), "event_date": event_date(raw), "split_factor": factor}
        )
    return output


def lineage(events: Sequence[Mapping[str, object]], session: date) -> str:
    eligible = [event for event in events if event["event_date"] <= session]
    return hashlib.sha256(canonical_json(eligible)).hexdigest()


def build_security_rows(
    *,
    ticker: str,
    identity: Mapping[str, str],
    result: Mapping[str, object],
    target_session: date,
    source_locator: str,
    source_revision: str,
    retrieved_at: str,
    certified_at: str,
) -> tuple[list[tuple[object, ...]], list[tuple[object, ...]], dict[str, object]]:
    all_observations = observations(result, target_session)
    if not all_observations or all_observations[-1]["session_date"] != target_session:
        observed = all_observations[-1]["session_date"] if all_observations else None
        raise BenchmarkBuildError(
            f"{ticker} current-session coverage failed: observed={observed}, expected={target_session}"
        )
    selected = all_observations[-MAXIMUM_SESSIONS:]
    if selected and all_observations.index(selected[0]) == 0:
        selected = selected[1:]
    if len(selected) < MINIMUM_SESSIONS:
        raise BenchmarkBuildError(
            f"{ticker} has {len(selected)} sessions; requires {MINIMUM_SESSIONS}"
        )
    first_position = all_observations.index(selected[0])
    if first_position == 0:
        raise BenchmarkBuildError(f"{ticker} lacks a prior session for returns")
    dividends = dividend_events(result)
    splits = split_events(result)
    selected_start = selected[0]["session_date"]
    cash_events = [
        event for event in dividends if selected_start <= event["event_date"] <= target_session
    ]
    split_window = [
        event for event in splits if selected_start <= event["event_date"] <= target_session
    ]
    selected_sessions = {row["session_date"] for row in selected}
    orphan_actions = [
        event
        for event in (*cash_events, *split_window)
        if event["event_date"] not in selected_sessions
    ]
    if orphan_actions:
        raise BenchmarkBuildError(
            f"{ticker} corporate action does not map to a completed XNYS session"
        )
    cash_by_date: dict[date, float] = {}
    for event in cash_events:
        event_session = event["event_date"]
        cash_by_date[event_session] = cash_by_date.get(event_session, 0.0) + float(
            event["cash_amount"]
        )
    split_by_date: dict[date, float] = {}
    for event in split_window:
        split_by_date[event["event_date"]] = float(event["split_factor"])

    distribution_rows: list[tuple[object, ...]] = []
    for event in cash_events:
        distribution_id = hashlib.sha256(
            canonical_json(
                {
                    "security_id": identity["security_id"],
                    "event_date": event["event_date"],
                    "cash_amount": event["cash_amount"],
                    "source_event_id": event["event_id"],
                }
            )
        ).hexdigest()
        distribution_rows.append(
            (
                identity["security_id"],
                ticker,
                event["event_date"],
                event["cash_amount"],
                "USD",
                "CASH_DISTRIBUTION",
                f"yahoo:{distribution_id}",
                source_locator,
                source_revision,
                retrieved_at,
                retrieved_at,
                certified_at,
            )
        )

    return_rows: list[tuple[object, ...]] = []
    total_return_index = 100.0
    maximum_error = 0.0
    for row in selected:
        position = all_observations.index(row)
        previous = all_observations[position - 1]
        session = row["session_date"]
        split_factor = split_by_date.get(session, 1.0)
        price_return = float(row["close"]) * split_factor / float(previous["close"]) - 1.0
        distribution_return = cash_by_date.get(session, 0.0) / float(previous["close"])
        total_return = float(row["adjusted_close"]) / float(previous["adjusted_close"]) - 1.0
        error = abs(total_return - price_return - distribution_return)
        maximum_error = max(maximum_error, error)
        if error > RETURN_RECONCILIATION_TOLERANCE:
            raise BenchmarkBuildError(
                f"{ticker} distribution/action reconciliation failed for {session}: {error:.8f}"
            )
        if price_return <= -1 or total_return <= -1:
            raise BenchmarkBuildError(f"{ticker} contains an invalid return for {session}")
        total_return_index *= 1.0 + total_return
        return_rows.append(
            (
                identity["security_id"],
                ticker,
                BENCHMARK_ID,
                BENCHMARK_ROLES[ticker],
                session,
                row["close"],
                split_factor,
                cash_by_date.get(session, 0.0),
                price_return,
                distribution_return,
                total_return,
                total_return_index,
                "USD",
                "CERTIFIED",
                lineage(cash_events, session),
                lineage(split_window, session),
                source_locator,
                source_revision,
                retrieved_at,
                retrieved_at,
                certified_at,
            )
        )
    weekly = len({row["session_date"].isocalendar()[:2] for row in selected})
    if weekly < MINIMUM_WEEKS:
        raise BenchmarkBuildError(
            f"{ticker} has {weekly} weekly observations; requires {MINIMUM_WEEKS}"
        )
    summary = {
        "security_id": identity["security_id"],
        "ticker": ticker,
        "benchmark_role": BENCHMARK_ROLES[ticker],
        "currency": "USD",
        "minimum_session": selected[0]["session_date"].isoformat(),
        "maximum_session": selected[-1]["session_date"].isoformat(),
        "sessions": len(selected),
        "weekly_observations": weekly,
        "cash_distributions": len(cash_events),
        "splits": len(split_window),
        "maximum_return_reconciliation_error": maximum_error,
        "source_locator": source_locator,
        "source_revision": source_revision,
        "source_retrieved_at_utc": retrieved_at,
    }
    return return_rows, distribution_rows, summary


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


def file_record(path: Path, rows: int, schema: Sequence[tuple[str, str]] | None = None) -> dict[str, object]:
    record: dict[str, object] = {
        "file": path.name,
        "sha256": sha256_file(path),
        "bytes": path.stat().st_size,
        "rows": rows,
    }
    if schema is not None:
        record["schema"] = [name for name, _ in schema]
    return record


def group_overrides(manifest: Mapping[str, object]) -> dict[str, dict[str, object]]:
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
        if isinstance(raw, dict) and raw.get("group_id"):
            output[str(raw["group_id"])] = {
                key: value for key, value in raw.items() if key not in ignored
            }
    return output


def attach(args: argparse.Namespace) -> dict[str, object]:
    release_dir = Path(args.release_dir).resolve()
    manifest_path = release_dir / "manifest.json"
    manifest = load_manifest(manifest_path)
    records = release_records(manifest)
    expected_text = str(
        args.expected_session
        or (manifest.get("validation") or {}).get("expected_latest_xnys_session")
        or ""
    )
    try:
        expected_session = date.fromisoformat(expected_text)
    except ValueError as exc:
        raise BenchmarkBuildError("Expected completed XNYS session is invalid") from exc
    required = tuple(
        value.strip().upper()
        for value in str(args.benchmark_tickers).split(",")
        if value.strip()
    )
    if required != REQUIRED_BENCHMARKS:
        raise BenchmarkBuildError(
            f"Required benchmark set must be {','.join(REQUIRED_BENCHMARKS)}"
        )
    identity_path = Path(args.identity_universe).resolve() if args.identity_universe else release_dir / "security-universe.csv"
    if not identity_path.is_file():
        raise BenchmarkBuildError("Current benchmark identity universe is missing")
    identities = benchmark_identities(identity_path, required)
    identity_observed_at = format_utc(parse_utc(args.identity_observed_at))
    retrieved_at = format_utc(parse_utc(args.retrieved_at))
    certified_at = format_utc(parse_utc(args.certified_at or args.retrieved_at))
    fixture_directory = Path(args.source_json_dir).resolve() if args.source_json_dir else None

    all_returns: list[tuple[object, ...]] = []
    all_distributions: list[tuple[object, ...]] = []
    summaries: list[dict[str, object]] = []
    provider_results: dict[str, object] = {}
    for ticker in required:
        url = yahoo_url(ticker, expected_session)
        payload, attempts = load_source_payload(
            source_json_path(fixture_directory, ticker), url
        )
        revision = hashlib.sha256(payload).hexdigest()
        result = source_result(payload, ticker)
        returns, distributions, summary = build_security_rows(
            ticker=ticker,
            identity=identities[ticker],
            result=result,
            target_session=expected_session,
            source_locator=url,
            source_revision=revision,
            retrieved_at=retrieved_at,
            certified_at=certified_at,
        )
        all_returns.extend(returns)
        all_distributions.extend(distributions)
        summaries.append(summary)
        provider_results[ticker] = {"attempts": attempts, "source_revision": revision}

    if {row["ticker"] for row in summaries} != set(REQUIRED_BENCHMARKS):
        raise BenchmarkBuildError("Benchmark security coverage is not 100%")
    identity_subset = [identities[ticker] for ticker in REQUIRED_BENCHMARKS]
    identity_subset_sha = hashlib.sha256(canonical_json(identity_subset)).hexdigest()
    certification: dict[str, object] = {
        "schema_version": "1.0.0",
        "contract_version": "2.0.0",
        "status": "CERTIFIED",
        "benchmark_id": BENCHMARK_ID,
        "required_tickers": list(REQUIRED_BENCHMARKS),
        "coverage": {"required": len(REQUIRED_BENCHMARKS), "valid": len(summaries), "ratio": 1.0},
        "expected_latest_xnys_session": expected_session.isoformat(),
        "observed_latest_xnys_session": expected_session.isoformat(),
        "identity_subset": identity_subset,
        "identity_subset_sha256": identity_subset_sha,
        "identity_universe_sha256": sha256_file(identity_path),
        "identity_observed_at_utc": identity_observed_at,
        "source_retrieved_at_utc": retrieved_at,
        "known_at_utc": retrieved_at,
        "certified_at_utc": certified_at,
        "securities": summaries,
        "provider_results": provider_results,
        "validation": {
            "minimum_sessions": MINIMUM_SESSIONS,
            "minimum_weekly_observations": MINIMUM_WEEKS,
            "required_current_session_coverage": 1.0,
            "maximum_return_reconciliation_error": RETURN_RECONCILIATION_TOLERANCE,
            "distribution_reconciliation": "PASS",
            "corporate_action_reconciliation": "PASS",
        },
        "idempotency_key": str(args.idempotency_key or ""),
    }

    with tempfile.TemporaryDirectory(dir=release_dir, prefix="benchmark-v2-") as raw_temp:
        temporary = Path(raw_temp)
        returns_path = temporary / RETURN_FILENAME
        distributions_path = temporary / DISTRIBUTION_FILENAME
        certification_path = temporary / CERTIFICATION_FILENAME
        write_parquet(
            returns_path,
            RETURN_COLUMNS,
            all_returns,
            "security_id, session_date",
        )
        write_parquet(
            distributions_path,
            DISTRIBUTION_COLUMNS,
            all_distributions,
            "security_id, ex_date, distribution_id",
        )
        certification_path.write_bytes(canonical_json(certification))
        final_paths = {
            RETURN_FILENAME: release_dir / RETURN_FILENAME,
            DISTRIBUTION_FILENAME: release_dir / DISTRIBUTION_FILENAME,
            CERTIFICATION_FILENAME: release_dir / CERTIFICATION_FILENAME,
        }
        for filename, target in final_paths.items():
            os.replace(temporary / filename, target)

    records[RETURN_FILENAME] = file_record(
        release_dir / RETURN_FILENAME, len(all_returns), RETURN_COLUMNS
    )
    records[DISTRIBUTION_FILENAME] = file_record(
        release_dir / DISTRIBUTION_FILENAME,
        len(all_distributions),
        DISTRIBUTION_COLUMNS,
    )
    records[CERTIFICATION_FILENAME] = file_record(
        release_dir / CERTIFICATION_FILENAME, 1
    )
    manifest["release_files"] = [records[name] for name in sorted(records)]

    datasets = [
        record
        for record in manifest.get("datasets", [])
        if isinstance(record, dict)
        and record.get("path")
        not in {RETURN_FILENAME, DISTRIBUTION_FILENAME, CERTIFICATION_FILENAME}
    ]
    combined_revision = hashlib.sha256(
        canonical_json(
            {summary["ticker"]: summary["source_revision"] for summary in summaries}
        )
    ).hexdigest()
    for path, minimum, maximum in (
        (
            release_dir / RETURN_FILENAME,
            min(str(summary["minimum_session"]) for summary in summaries),
            expected_session.isoformat(),
        ),
        (
            release_dir / DISTRIBUTION_FILENAME,
            min(row[2] for row in all_distributions).isoformat(),
            max(row[2] for row in all_distributions).isoformat(),
        ),
        (release_dir / CERTIFICATION_FILENAME, expected_session.isoformat(), expected_session.isoformat()),
    ):
        datasets.append(
            {
                "path": path.name,
                "source": SOURCE_NAME,
                "source_name": SOURCE_NAME,
                "source_revision": combined_revision,
                "immutable_source_revision": combined_revision,
                "source_retrieved_at_utc": retrieved_at,
                "source_retrieval_time": retrieved_at,
                "minimum_event_date": minimum,
                "maximum_event_date": maximum,
                "point_in_time_safe": True,
            }
        )
    manifest["datasets"] = datasets
    manifest["benchmark_certification"] = certification
    overrides = group_overrides(manifest)
    overrides["total_returns"] = {
        "state": "READY_NEW",
        "mode": "FRESH_CERTIFIED_BENCHMARK_V2",
        "freshness": {
            "clock": "XNYS_ELIGIBLE_SESSIONS",
            "expected": expected_session.isoformat(),
            "observed": expected_session.isoformat(),
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
    return certification


def write_quarantine(path: Path, args: argparse.Namespace, error: Exception) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": "1.0.0",
        "state": "QUARANTINED",
        "attempted_at_utc": format_utc(datetime.now(timezone.utc)),
        "expected_session": args.expected_session,
        "required_tickers": list(REQUIRED_BENCHMARKS),
        "idempotency_key": str(args.idempotency_key or ""),
        "diagnostics": [str(error)],
    }
    path.write_bytes(canonical_json(payload))


def remove_uncertified_lane(release_dir: Path, error: Exception) -> None:
    """Ensure a failed attempt cannot carry an older certified lane forward."""
    manifest_path = release_dir / "manifest.json"
    if not manifest_path.is_file():
        return
    manifest = load_manifest(manifest_path)
    if not any(
        (release_dir / filename).exists()
        for filename in (RETURN_FILENAME, DISTRIBUTION_FILENAME, CERTIFICATION_FILENAME)
    ) and "benchmark_certification" not in manifest:
        return
    for filename in (RETURN_FILENAME, DISTRIBUTION_FILENAME, CERTIFICATION_FILENAME):
        path = release_dir / filename
        if path.exists():
            path.unlink()
    manifest["release_files"] = [
        record
        for record in manifest.get("release_files", [])
        if isinstance(record, dict)
        and record.get("file")
        not in {RETURN_FILENAME, DISTRIBUTION_FILENAME, CERTIFICATION_FILENAME}
    ]
    manifest["datasets"] = [
        record
        for record in manifest.get("datasets", [])
        if isinstance(record, dict)
        and record.get("path")
        not in {RETURN_FILENAME, DISTRIBUTION_FILENAME, CERTIFICATION_FILENAME}
    ]
    manifest.pop("benchmark_certification", None)
    failures = [
        dict(value)
        for value in manifest.get("candidate_attempt_failures", [])
        if isinstance(value, dict) and value.get("group_id") != "total_returns"
    ]
    failures.append(
        {
            "group_id": "total_returns",
            "attempt_state": "QUARANTINED",
            "diagnostics": [str(error)],
            "released_state": "NOT_CONFIGURED",
            "attempted_at_utc": format_utc(datetime.now(timezone.utc)),
        }
    )
    apply_dataset_groups(
        manifest,
        release_dir,
        group_overrides=group_overrides(manifest),
        candidate_group_failures=manifest.get("candidate_group_failures", []),
        candidate_attempt_failures=failures,
    )
    temporary = manifest_path.with_suffix(".json.benchmark-failed.tmp")
    temporary.write_text(
        json.dumps(manifest, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, manifest_path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release-dir", required=True)
    parser.add_argument("--identity-universe")
    parser.add_argument("--identity-observed-at")
    parser.add_argument("--expected-session")
    parser.add_argument("--benchmark-tickers", default=",".join(REQUIRED_BENCHMARKS))
    parser.add_argument("--source-json-dir")
    parser.add_argument("--retrieved-at")
    parser.add_argument("--certified-at")
    parser.add_argument("--idempotency-key")
    parser.add_argument("--quarantine-out")
    args = parser.parse_args(argv)
    if args.identity_observed_at is None:
        args.identity_observed_at = args.retrieved_at
    try:
        result = attach(args)
    except (BenchmarkBuildError, OSError, ValueError, duckdb.Error) as exc:
        try:
            remove_uncertified_lane(Path(args.release_dir).resolve(), exc)
        except (BenchmarkBuildError, OSError, ValueError, duckdb.Error) as cleanup_error:
            print(
                f"warning: could not remove uncertified benchmark lane: {cleanup_error}",
                file=sys.stderr,
            )
        if args.quarantine_out:
            write_quarantine(Path(args.quarantine_out).resolve(), args, exc)
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
