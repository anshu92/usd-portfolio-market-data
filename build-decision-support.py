#!/usr/bin/env python3
"""Build the compact, read-only decision-support artifact from a pinned release."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import shutil
import sqlite3
import subprocess
import tempfile
from datetime import date, datetime, time as datetime_time, timedelta, timezone
from pathlib import Path
from typing import Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

import duckdb
import exchange_calendars as xcals

from decision_support_contract import (
    ACTIONABILITY_MATRIX_FILENAME,
    BENCHMARK_MINIMUM_SESSIONS,
    BENCHMARK_MINIMUM_WEEKLY_OBSERVATIONS,
    CAPABILITIES,
    CANDIDATE_FUNNEL_SCHEMA,
    CANDIDATE_FUNNEL_FILENAME,
    CONTRACT_VERSION,
    DATABASE_FILENAME,
    EVIDENCE_PACKETS_FILENAME,
    EXCHANGE_TIMEZONE,
    MANIFEST_FILENAME,
    MAX_COMPRESSED_DATABASE_BYTES,
    OPTIONAL_SOURCE_FILES,
    OPERATING_MODES,
    PHASES,
    PHASE_PACK_DIRECTORY,
    RECENT_MARKET_SESSIONS,
    REQUIRED_SOURCE_FILES,
    SCHEMA_VERSION,
    TASK_TIMEZONE,
    USABLE_GROUP_STATES,
    VALIDATOR_FILES,
    canonical_json,
    sha256_file,
)
from enrichment_contract import ANALYST_ESTIMATES_CONTRACT, CONTRACTS, DatasetContract


ADMITTED_STATES = {"ADMITTED", "ADMITTED_ETF"}
SQLITE_TYPE = {
    "BIGINT": "INTEGER",
    "BOOLEAN": "INTEGER",
    "DATE": "TEXT",
    "DOUBLE": "REAL",
    "TIMESTAMP": "TEXT",
    "VARCHAR": "TEXT",
}

FUTURE_TABLE_COLUMNS = {
    "distributions": (
        ("security_id", "TEXT"),
        ("ticker", "TEXT"),
        ("ex_date", "TEXT"),
        ("cash_amount", "REAL"),
        ("currency", "TEXT"),
        ("distribution_type", "TEXT"),
        ("distribution_id", "TEXT"),
        ("source_locator", "TEXT"),
        ("source_revision", "TEXT"),
        ("source_retrieved_at_utc", "TEXT"),
        ("known_at_utc", "TEXT"),
        ("certified_at_utc", "TEXT"),
    ),
    "benchmark_total_returns": (
        ("security_id", "TEXT"),
        ("ticker", "TEXT"),
        ("benchmark_id", "TEXT"),
        ("benchmark_role", "TEXT"),
        ("session_date", "TEXT"),
        ("raw_close", "REAL"),
        ("split_factor", "REAL"),
        ("cash_distribution", "REAL"),
        ("price_return", "REAL"),
        ("distribution_return", "REAL"),
        ("total_return", "REAL"),
        ("total_return_index", "REAL"),
        ("certification_status", "TEXT"),
        ("distribution_lineage_sha256", "TEXT"),
        ("corporate_action_lineage_sha256", "TEXT"),
        ("source_locator", "TEXT"),
        ("source_revision", "TEXT"),
        ("source_retrieved_at_utc", "TEXT"),
        ("known_at_utc", "TEXT"),
        ("certified_at_utc", "TEXT"),
    ),
}

EVIDENCE_COLUMNS = (
    ("evidence_id", "TEXT"),
    ("security_id", "TEXT"),
    ("evidence_kind", "TEXT"),
    ("known_at_utc", "TEXT"),
    ("revision", "TEXT"),
    ("source_event_at", "TEXT"),
    ("source_locator", "TEXT"),
    ("headline", "TEXT"),
    ("summary", "TEXT"),
)


class DecisionSupportBuildError(RuntimeError):
    """Raised when a compact artifact cannot be built without weakening provenance."""


def format_utc(value: datetime) -> str:
    if value.tzinfo is None:
        raise DecisionSupportBuildError("Timestamp has no timezone")
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_utc(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise DecisionSupportBuildError(f"Invalid UTC timestamp: {value!r}") from exc
    if parsed.tzinfo is None:
        raise DecisionSupportBuildError(f"Timestamp has no timezone: {value!r}")
    return parsed.astimezone(timezone.utc)


def load_manifest(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DecisionSupportBuildError(f"Cannot read source manifest: {exc}") from exc
    if not isinstance(value, dict) or value.get("status") != "READY":
        raise DecisionSupportBuildError("Source manifest is not READY")
    if value.get("schema_version") != "1.0.0":
        raise DecisionSupportBuildError("Unsupported source manifest schema")
    return value


def release_records(manifest: Mapping[str, object]) -> dict[str, Mapping[str, object]]:
    records = manifest.get("release_files")
    if not isinstance(records, list):
        raise DecisionSupportBuildError("Source manifest lacks release_files")
    output: dict[str, Mapping[str, object]] = {}
    for record in records:
        if not isinstance(record, dict) or not isinstance(record.get("file"), str):
            raise DecisionSupportBuildError("Invalid source release_files entry")
        filename = str(record["file"])
        if filename in output:
            raise DecisionSupportBuildError(
                f"Duplicate source release file: {filename}"
            )
        output[filename] = record
    return output


def validate_source_files(
    release_dir: Path,
    manifest: Mapping[str, object],
    overlay_records: Mapping[str, Mapping[str, object]] | None = None,
) -> list[dict[str, object]]:
    records = release_records(manifest)
    overlays = overlay_records or {}
    used: list[dict[str, object]] = []
    for filename in REQUIRED_SOURCE_FILES:
        path = release_dir / filename
        if filename == "manifest.json":
            if not path.is_file() or path.is_symlink():
                raise DecisionSupportBuildError(f"Missing source file: {filename}")
            used.append(
                {
                    "file": filename,
                    "bytes": path.stat().st_size,
                    "sha256": sha256_file(path),
                }
            )
            continue
        record = overlays.get(filename) or records.get(filename)
        if record is None:
            raise DecisionSupportBuildError(
                f"Source manifest does not declare required file: {filename}"
            )
        if not path.is_file() or path.is_symlink():
            raise DecisionSupportBuildError(f"Missing source file: {filename}")
        actual_sha = sha256_file(path)
        if actual_sha != record.get("sha256"):
            raise DecisionSupportBuildError(f"Source SHA-256 mismatch: {filename}")
        if path.stat().st_size != record.get("bytes"):
            raise DecisionSupportBuildError(f"Source byte-size mismatch: {filename}")
        used.append(
            {
                "file": filename,
                "bytes": path.stat().st_size,
                "sha256": actual_sha,
                "origin": "HOT_LANE_OVERLAY" if filename in overlays else "CANONICAL_RELEASE",
            }
        )
    for filename in OPTIONAL_SOURCE_FILES:
        path = release_dir / filename
        if not path.exists():
            continue
        record = overlays.get(filename) or records.get(filename)
        if record is None:
            raise DecisionSupportBuildError(
                f"Optional source file is not declared by its manifest: {filename}"
            )
        if not path.is_file() or path.is_symlink():
            raise DecisionSupportBuildError(f"Invalid optional source file: {filename}")
        actual_sha = sha256_file(path)
        if actual_sha != record.get("sha256") or path.stat().st_size != record.get(
            "bytes"
        ):
            raise DecisionSupportBuildError(
                f"Optional source identity mismatch: {filename}"
            )
        used.append(
            {
                "file": filename,
                "bytes": path.stat().st_size,
                "sha256": actual_sha,
                "origin": "HOT_LANE_OVERLAY" if filename in overlays else "CANONICAL_RELEASE",
            }
        )
    return sorted(used, key=lambda item: str(item["file"]))


def load_benchmark_overlay(
    release_dir: Path, canonical_manifest_sha256: str
) -> tuple[
    dict[str, Mapping[str, object]],
    Mapping[str, object] | None,
    dict[str, object] | None,
]:
    path = release_dir / "benchmark-overlay.json"
    if not path.exists():
        return {}, None, None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DecisionSupportBuildError(f"Cannot read benchmark overlay: {exc}") from exc
    if (
        not isinstance(value, dict)
        or value.get("schema_version") != SCHEMA_VERSION
        or value.get("lane_id") != "certified_benchmark"
        or value.get("canonical_manifest_sha256") != canonical_manifest_sha256
    ):
        raise DecisionSupportBuildError("Benchmark overlay identity is invalid")
    raw_records = value.get("release_files")
    expected = {
        "benchmark-distributions.parquet",
        "benchmark-total-returns.parquet",
        "benchmark-certification.json",
    }
    if not isinstance(raw_records, list):
        raise DecisionSupportBuildError("Benchmark overlay lacks release-file identities")
    records: dict[str, Mapping[str, object]] = {}
    for record in raw_records:
        if not isinstance(record, dict) or not isinstance(record.get("file"), str):
            raise DecisionSupportBuildError("Benchmark overlay release-file identity is invalid")
        filename = str(record["file"])
        if filename in records:
            raise DecisionSupportBuildError(f"Duplicate benchmark overlay file: {filename}")
        records[filename] = record
    if set(records) != expected:
        raise DecisionSupportBuildError("Benchmark overlay file set is incomplete")
    group = value.get("dataset_group")
    if not isinstance(group, dict) or group.get("group_id") != "total_returns":
        raise DecisionSupportBuildError("Benchmark overlay lacks total_returns group")
    overlay_record = {
        "lane_id": "certified_benchmark",
        "manifest": {
            "path": path.name,
            "bytes": path.stat().st_size,
            "sha256": sha256_file(path),
        },
        "canonical_manifest_sha256": canonical_manifest_sha256,
        "expected_session": value.get("expected_session"),
        "generated_at_utc": value.get("generated_at_utc"),
        "idempotency_key": value.get("idempotency_key"),
        "release_files": [dict(records[name]) for name in sorted(records)],
    }
    return records, group, overlay_record


def quoted(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def sqlite_columns(contract: DatasetContract) -> tuple[tuple[str, str], ...]:
    try:
        return tuple((name, SQLITE_TYPE[kind]) for name, kind in contract.columns)
    except KeyError as exc:
        raise DecisionSupportBuildError(
            f"Unsupported SQLite source type: {exc.args[0]}"
        ) from exc


def create_table(
    connection: sqlite3.Connection,
    name: str,
    columns: Sequence[tuple[str, str]],
    primary_key: Sequence[str] = (),
) -> None:
    definitions = [f"{quoted(column)} {kind}" for column, kind in columns]
    if primary_key:
        definitions.append(
            "PRIMARY KEY (" + ",".join(quoted(column) for column in primary_key) + ")"
        )
    connection.execute(f"CREATE TABLE {quoted(name)} ({','.join(definitions)})")


def normalize_sqlite_value(value: object) -> object:
    if isinstance(value, datetime):
        return format_utc(
            value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        )
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bool):
        return int(value)
    return value


def insert_rows(
    connection: sqlite3.Connection,
    table: str,
    columns: Sequence[str],
    rows: Iterable[Sequence[object]],
) -> int:
    placeholders = ",".join("?" for _ in columns)
    statement = (
        f"INSERT INTO {quoted(table)} "
        f"({','.join(quoted(column) for column in columns)}) VALUES ({placeholders})"
    )
    count = 0
    batch: list[tuple[object, ...]] = []
    for row in rows:
        batch.append(tuple(normalize_sqlite_value(value) for value in row))
        if len(batch) >= 10_000:
            connection.executemany(statement, batch)
            count += len(batch)
            batch.clear()
    if batch:
        connection.executemany(statement, batch)
        count += len(batch)
    return count


def copy_query(
    sqlite_connection: sqlite3.Connection,
    duck_connection: duckdb.DuckDBPyConnection,
    table: str,
    columns: Sequence[tuple[str, str]],
    query: str,
    parameters: Sequence[object] = (),
) -> int:
    create_table(sqlite_connection, table, columns)
    cursor = duck_connection.execute(query, list(parameters))

    def rows() -> Iterable[Sequence[object]]:
        while True:
            batch = cursor.fetchmany(10_000)
            if not batch:
                return
            yield from batch

    return insert_rows(
        sqlite_connection, table, [column for column, _ in columns], rows()
    )


def group_records(manifest: Mapping[str, object]) -> dict[str, Mapping[str, object]]:
    raw = manifest.get("dataset_groups")
    if not isinstance(raw, list):
        raise DecisionSupportBuildError("Source manifest lacks dataset_groups")
    output: dict[str, Mapping[str, object]] = {}
    for record in raw:
        if not isinstance(record, dict) or not isinstance(record.get("group_id"), str):
            raise DecisionSupportBuildError("Invalid dataset_groups entry")
        output[str(record["group_id"])] = record
    return output


def benchmark_lane_status(release_dir: Path) -> dict[str, object]:
    returns_path = release_dir / "benchmark-total-returns.parquet"
    distributions_path = release_dir / "distributions.parquet"
    if not returns_path.is_file() or not distributions_path.is_file():
        return {
            "state": "NOT_CONFIGURED",
            "reason": "Certified total-return and distribution lanes are not configured",
            "observed_at": None,
            "sessions": 0,
            "weekly_observations": 0,
        }
    required_returns = {
        name for name, _ in FUTURE_TABLE_COLUMNS["benchmark_total_returns"]
    }
    required_distributions = {name for name, _ in FUTURE_TABLE_COLUMNS["distributions"]}
    connection = duckdb.connect()
    try:
        returns_columns = {
            str(row[0])
            for row in connection.execute(
                "DESCRIBE SELECT * FROM read_parquet(?)", [str(returns_path)]
            ).fetchall()
        }
        distribution_columns = {
            str(row[0])
            for row in connection.execute(
                "DESCRIBE SELECT * FROM read_parquet(?)", [str(distributions_path)]
            ).fetchall()
        }
        missing = sorted(
            (required_returns - returns_columns)
            | (required_distributions - distribution_columns)
        )
        if missing:
            return {
                "state": "INVALID",
                "reason": f"Certified return inputs lack required columns: {missing}",
                "observed_at": None,
                "sessions": 0,
                "weekly_observations": 0,
            }
        stats = connection.execute(
            """
            SELECT count(DISTINCT session_date),
                   count(DISTINCT date_trunc('week', session_date)),
                   count(*) FILTER (WHERE certification_status <> 'CERTIFIED'
                                      OR certification_status IS NULL),
                   max(source_retrieved_at_utc),
                   count(*) - count(DISTINCT (security_id, session_date)),
                   count(*) FILTER (
                     WHERE benchmark_id IS NULL OR total_return IS NULL
                        OR total_return_index IS NULL
                        OR distribution_lineage_sha256 IS NULL
                        OR corporate_action_lineage_sha256 IS NULL
                        OR NOT regexp_full_match(distribution_lineage_sha256, '[0-9a-f]{64}')
                        OR NOT regexp_full_match(corporate_action_lineage_sha256, '[0-9a-f]{64}')
                        OR known_at_utc IS NULL OR revision IS NULL
                        OR source_locator IS NULL
                   ),
                   max(session_date)
            FROM read_parquet(?)
            """,
            [str(returns_path)],
        ).fetchone()
        distribution_invalid = connection.execute(
            """
            SELECT count(*) - count(DISTINCT distribution_id)
                 + count(*) FILTER (
                     WHERE security_id IS NULL OR ex_date IS NULL
                        OR cash_amount IS NULL OR currency IS NULL
                        OR distribution_type IS NULL OR distribution_id IS NULL
                        OR known_at_utc IS NULL OR revision IS NULL
                        OR source_locator IS NULL
                   )
            FROM read_parquet(?)
            """,
            [str(distributions_path)],
        ).fetchone()[0]
        sessions = int(stats[0] or 0)
        weeks = int(stats[1] or 0)
        invalid_rows = (
            int(stats[2] or 0)
            + int(stats[4] or 0)
            + int(stats[5] or 0)
            + int(distribution_invalid or 0)
        )
        source_manifest = load_manifest(release_dir / "manifest.json")
        expected_session = str(
            (source_manifest.get("validation") or {}).get(
                "expected_latest_xnys_session"
            )
            or ""
        )
        market_session = str(
            (source_manifest.get("aggregate") or {}).get("max_date") or ""
        )
        observed_session = normalize_sqlite_value(stats[6])
        if invalid_rows:
            state = "INVALID"
            reason = f"Certified total-return lane has {invalid_rows} invalid rows"
        elif observed_session != expected_session or observed_session != market_session:
            state = "STALE"
            reason = (
                "Certified total-return lane is not current with the source market "
                f"session: observed={observed_session}, expected={expected_session}"
            )
        elif sessions < BENCHMARK_MINIMUM_SESSIONS:
            state = "INSUFFICIENT_HISTORY"
            reason = (
                f"Certified total-return lane has {sessions} sessions; "
                f"requires {BENCHMARK_MINIMUM_SESSIONS}"
            )
        elif weeks < BENCHMARK_MINIMUM_WEEKLY_OBSERVATIONS:
            state = "INSUFFICIENT_HISTORY"
            reason = (
                f"Certified total-return lane has {weeks} weekly observations; "
                f"requires {BENCHMARK_MINIMUM_WEEKLY_OBSERVATIONS}"
            )
        else:
            state = "READY"
            reason = None
        return {
            "state": state,
            "reason": reason,
            "observed_at": normalize_sqlite_value(stats[3]),
            "sessions": sessions,
            "weekly_observations": weeks,
        }
    finally:
        connection.close()


def benchmark_lane_status(release_dir: Path) -> dict[str, object]:
    returns_path = release_dir / "benchmark-total-returns.parquet"
    distributions_path = release_dir / "benchmark-distributions.parquet"
    certification_path = release_dir / "benchmark-certification.json"
    if not all(path.is_file() for path in (returns_path, distributions_path, certification_path)):
        return {
            "state": "NOT_CONFIGURED",
            "identity_state": "NOT_CONFIGURED",
            "market_state": "NOT_CONFIGURED",
            "reason": "Certified VTI/SPY/BIL benchmark lane is not configured",
            "observed_at": None,
            "identity_observed_at": None,
            "sessions": 0,
            "weekly_observations": 0,
            "securities": 0,
            "coverage": 0.0,
            "maximum_session": None,
        }
    try:
        certification = json.loads(certification_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DecisionSupportBuildError(
            f"Cannot read benchmark certification: {exc}"
        ) from exc
    if not isinstance(certification, dict):
        raise DecisionSupportBuildError("Benchmark certification root is not an object")
    if (
        certification.get("status") != "CERTIFIED"
        or set(certification.get("required_tickers") or []) != {"VTI", "SPY", "BIL"}
        or certification.get("coverage")
        != {"required": 3, "valid": 3, "ratio": 1.0}
    ):
        return {
            "state": "INVALID",
            "identity_state": "INVALID",
            "market_state": "INVALID",
            "reason": "Benchmark certification is incomplete",
            "observed_at": certification.get("source_retrieved_at_utc"),
            "identity_observed_at": certification.get("identity_observed_at_utc"),
            "sessions": 0,
            "weekly_observations": 0,
            "securities": 0,
            "coverage": 0.0,
            "maximum_session": certification.get("observed_latest_xnys_session"),
        }
    connection = duckdb.connect()
    try:
        rows = connection.execute(
            """
            SELECT ticker, count(*),
                   count(DISTINCT date_trunc('week', session_date)),
                   max(session_date),
                   count(*) FILTER (WHERE certification_status <> 'CERTIFIED')
            FROM read_parquet(?) GROUP BY ticker
            """,
            [str(returns_path)],
        ).fetchall()
        invalid_distributions = int(
            connection.execute(
                """
                SELECT count(*) FILTER (
                  WHERE cash_amount <= 0 OR currency <> 'USD'
                     OR distribution_type <> 'CASH_DISTRIBUTION'
                     OR source_retrieved_at_utc IS NULL
                ) FROM read_parquet(?)
                """,
                [str(distributions_path)],
            ).fetchone()[0]
        )
    finally:
        connection.close()
    by_ticker = {str(row[0]): row for row in rows}
    expected = str(certification.get("expected_latest_xnys_session") or "")
    sessions = min((int(row[1]) for row in rows), default=0)
    weeks = min((int(row[2]) for row in rows), default=0)
    current = (
        set(by_ticker) == {"VTI", "SPY", "BIL"}
        and all(str(row[3]) == expected for row in rows)
    )
    valid = (
        current
        and all(int(row[4] or 0) == 0 for row in rows)
        and invalid_distributions == 0
        and sessions >= BENCHMARK_MINIMUM_SESSIONS
        and weeks >= BENCHMARK_MINIMUM_WEEKLY_OBSERVATIONS
    )
    return {
        "state": "READY" if valid else "INVALID",
        "identity_state": "READY",
        "market_state": "READY" if current else "STALE",
        "reason": None if valid else "Benchmark assets fail current-session/history validation",
        "observed_at": certification.get("source_retrieved_at_utc"),
        "identity_observed_at": certification.get("identity_observed_at_utc"),
        "sessions": sessions,
        "weekly_observations": weeks,
        "securities": len(by_ticker),
        "coverage": len(by_ticker) / 3,
        "maximum_session": certification.get("observed_latest_xnys_session"),
        "identity_subset_sha256": certification.get("identity_subset_sha256"),
    }


def source_watermarks(
    groups: Mapping[str, Mapping[str, object]],
    capabilities: Mapping[str, Mapping[str, object]],
) -> list[dict[str, object]]:
    output: list[dict[str, object]] = []
    for capability_id, capability in sorted(capabilities.items()):
        source_group = capability.get("source_group")
        group = groups.get(str(source_group)) if source_group else None
        freshness_value = group.get("freshness") if group else None
        freshness = freshness_value if isinstance(freshness_value, dict) else {}
        output.append(
            {
                "capability_id": capability_id,
                "source_group": source_group,
                "capability_state": capability.get("state"),
                "clock": freshness.get("clock"),
                "expected": freshness.get("expected"),
                "observed": freshness.get("observed")
                if freshness
                else capability.get("observed_at"),
                "source_retrieved_at_utc": capability.get("observed_at")
                if source_group == "total_returns"
                else freshness.get("source_retrieved_at_utc"),
                "freshness_state": freshness.get("state"),
                "source_release_tag": group.get("source_release_tag")
                if group
                else None,
                "group_sha256": group.get("group_sha256") if group else None,
            }
        )
    return output


def validator_identity(producer_commit: str) -> dict[str, object]:
    root = Path(__file__).resolve().parent
    files = [
        {"path": filename, "sha256": sha256_file(root / filename)}
        for filename in VALIDATOR_FILES
    ]
    validator_set_sha256 = hashlib.sha256(canonical_json(files)).hexdigest()
    return {
        "contract_version": CONTRACT_VERSION,
        "producer_commit": producer_commit,
        "files": files,
        "validator_set_sha256": validator_set_sha256,
    }


def source_session(manifest: Mapping[str, object]) -> date:
    certification = manifest.get("benchmark_certification")
    if isinstance(certification, dict) and certification.get("status") == "CERTIFIED":
        value = certification.get("observed_latest_xnys_session")
        try:
            return date.fromisoformat(str(value))
        except ValueError as exc:
            raise DecisionSupportBuildError(
                "Source benchmark certification has an invalid session"
            ) from exc
    aggregate = manifest.get("aggregate")
    value = aggregate.get("max_date") if isinstance(aggregate, dict) else None
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise DecisionSupportBuildError(
            "Source manifest lacks aggregate.max_date"
        ) from exc


def phase_windows(phase_id: str, valid_session: date) -> list[dict[str, object]]:
    calendar = xcals.get_calendar("XNYS")
    try:
        session = calendar.date_to_session(valid_session, direction="none")
    except ValueError as exc:
        raise DecisionSupportBuildError(
            f"Source max_date is not an XNYS session: {valid_session}"
        ) from exc
    next_session = calendar.next_session(session)
    close_utc = calendar.session_close(session).to_pydatetime()
    next_date = next_session.date()
    task_zone = ZoneInfo(TASK_TIMEZONE)

    def local(day: date, hour: int, minute: int) -> datetime:
        return datetime.combine(day, datetime_time(hour, minute), task_zone)

    def window(
        window_id: str, decision: datetime, not_before: datetime, expires: datetime
    ) -> dict[str, object]:
        return {
            "window_id": window_id,
            "phase_decision_cutoff_utc": format_utc(decision),
            "not_before_utc": format_utc(not_before),
            "expires_at_utc": format_utc(expires),
        }

    next_preopen = local(next_date, 8, 10)
    if phase_id == "pre_open":
        return [
            window(
                "PRE_OPEN",
                next_preopen,
                local(next_date, 8, 5),
                local(next_date, 9, 30),
            )
        ]
    if phase_id == "execution_research":
        return [
            window(
                "EXECUTION_RESEARCH",
                local(next_date, 9, 38),
                local(next_date, 9, 30),
                local(next_date, 9, 45),
            )
        ]
    if phase_id == "exception_monitoring":
        return [
            window(
                "OPEN_EXCEPTION",
                local(next_date, 9, 55),
                local(next_date, 9, 50),
                local(next_date, 10, 5),
            ),
            window(
                "CLOSE_EXCEPTION",
                local(next_date, 15, 25),
                local(next_date, 15, 20),
                local(next_date, 15, 35),
            ),
        ]
    if phase_id == "terminal_review":
        return [
            window(
                "TERMINAL_REVIEW",
                close_utc + timedelta(minutes=20),
                close_utc + timedelta(minutes=15),
                close_utc + timedelta(minutes=45),
            )
        ]
    if phase_id == "accounting":
        return [
            window(
                "ACCOUNTING",
                close_utc + timedelta(minutes=45),
                close_utc + timedelta(minutes=40),
                local(next_date, 7, 45),
            )
        ]

    days_to_saturday = (5 - valid_session.weekday()) % 7
    saturday = valid_session + timedelta(days=days_to_saturday)
    if saturday <= valid_session:
        saturday += timedelta(days=7)
    if phase_id == "saturday_replay":
        return [
            window(
                "SATURDAY_REPLAY",
                local(saturday, 8, 30),
                local(saturday, 8, 0),
                local(saturday + timedelta(days=1), 17, 15),
            )
        ]
    days_to_sunday = (6 - valid_session.weekday()) % 7
    sunday = valid_session + timedelta(days=days_to_sunday)
    if sunday <= valid_session:
        sunday += timedelta(days=7)
    if phase_id == "sunday":
        post_sunday_session = calendar.date_to_session(sunday, direction="next").date()
        return [
            window(
                "SUNDAY",
                local(sunday, 17, 30),
                local(sunday, 17, 15),
                local(post_sunday_session, 7, 45),
            )
        ]
    raise DecisionSupportBuildError(f"No timing contract for phase: {phase_id}")


def phase_valid_session(phase_id: str, data_session: date) -> str:
    calendar = xcals.get_calendar("XNYS")
    session = calendar.date_to_session(data_session, direction="none")
    if phase_id in {"pre_open", "execution_research", "exception_monitoring"}:
        return calendar.next_session(session).date().isoformat()
    if phase_id == "sunday":
        days_to_sunday = (6 - data_session.weekday()) % 7
        sunday = data_session + timedelta(days=days_to_sunday or 7)
        return calendar.date_to_session(sunday, direction="next").date().isoformat()
    return data_session.isoformat()


def capability_records(
    groups: Mapping[str, Mapping[str, object]],
    release_dir: Path,
    as_of: datetime | None = None,
) -> dict[str, dict[str, object]]:
    output: dict[str, dict[str, object]] = {}
    benchmark = benchmark_lane_status(release_dir)
    for capability_id, contract in CAPABILITIES.items():
        if contract.external:
            output[capability_id] = {
                "capability_id": capability_id,
                "state": "CONSUMER_REQUIRED",
                "reason": "Fetched by the consumer at the actual decision cutoff",
                "source_group": None,
                "observed_at": None,
                "maximum_age_seconds": None,
            }
            continue
        source_group = contract.source_group
        group = groups.get(str(source_group)) if source_group else None
        if capability_id in {
            "benchmark_identity",
            "benchmark_market_current",
            "certified_total_returns",
            "funded_benchmark_inputs",
        }:
            state_key = (
                "identity_state"
                if capability_id == "benchmark_identity"
                else "market_state"
                if capability_id == "benchmark_market_current"
                else "state"
            )
            state = str(benchmark[state_key])
            reason = benchmark["reason"]
            freshness_value = group.get("freshness") if group else None
            freshness = freshness_value if isinstance(freshness_value, dict) else {}
            group_state = str(group.get("state") or "") if group else ""
            if state == "READY" and group_state not in USABLE_GROUP_STATES:
                state = group_state or "NOT_CONFIGURED"
                reason = "Certified total-return source group is not usable"
            if state == "READY" and as_of is not None:
                observed = benchmark.get(
                    "identity_observed_at"
                    if capability_id == "benchmark_identity"
                    else "observed_at"
                )
                try:
                    observed_at = datetime.fromisoformat(
                        str(observed).replace("Z", "+00:00")
                    )
                except ValueError:
                    state = "UNKNOWN_FRESHNESS"
                    reason = "Certified total-return lane has no valid retrieval time"
                else:
                    if observed_at.tzinfo is None:
                        observed_at = observed_at.replace(tzinfo=timezone.utc)
                    age_seconds = (as_of - observed_at).total_seconds()
                    maximum_age = contract.maximum_age_seconds
                    if age_seconds < 0:
                        state = "INVALID"
                        reason = (
                            "Certified total-return retrieval time is in the future"
                        )
                    elif maximum_age is not None and age_seconds > maximum_age:
                        state = "STALE"
                        reason = f"Source age exceeds {maximum_age} seconds"
        elif capability_id == "candidate_funnel":
            state = "NOT_CONFIGURED"
            reason = "Approved selector/candidate-funnel producer is not configured"
            freshness = {}
        elif (
            capability_id == "point_in_time_expectations"
            and not (release_dir / ANALYST_ESTIMATES_CONTRACT.filename).is_file()
        ):
            state = "NOT_CONFIGURED"
            reason = "Point-in-time analyst expectations are not configured"
            freshness = {}
        elif capability_id in {
            "rapid_event_news",
            "macro_industry",
            "etf_exposure",
            "survivorship_history",
        }:
            state = "NOT_CONFIGURED"
            reason = f"{capability_id} lane is not configured"
            freshness = {}
        elif group is None:
            state = "NOT_CONFIGURED"
            reason = f"Source group {source_group} is not configured"
            freshness = {}
        else:
            group_state = str(group.get("state") or "")
            freshness_value = group.get("freshness")
            freshness = freshness_value if isinstance(freshness_value, dict) else {}
            state = (
                "READY"
                if group_state in USABLE_GROUP_STATES
                else group_state or "UNAVAILABLE"
            )
            reason = (
                None
                if state == "READY"
                else f"Source group state is {group_state or 'missing'}"
            )
            if state == "READY" and capability_id in {
                "historical_market",
                "broad_market_current",
            }:
                lag = freshness.get("lag_eligible_sessions")
                if not isinstance(lag, int) or lag != 0:
                    state = "STALE"
                    reason = "Market group is not current through its expected completed session"
            if state == "READY" and contract.maximum_age_seconds is not None:
                lag_hours = freshness.get("lag_hours")
                if not isinstance(lag_hours, (int, float)):
                    state = "UNKNOWN_FRESHNESS"
                    reason = "Source group has no measurable freshness lag"
                elif float(lag_hours) * 3600 > contract.maximum_age_seconds:
                    state = "STALE"
                    reason = (
                        f"Source age exceeds {contract.maximum_age_seconds} seconds"
                    )
        output[capability_id] = {
            "capability_id": capability_id,
            "state": state,
            "reason": reason,
            "source_group": source_group,
            "observed_at": freshness.get("observed"),
            "maximum_age_seconds": contract.maximum_age_seconds,
        }
        if capability_id in {
            "benchmark_identity",
            "benchmark_market_current",
            "certified_total_returns",
            "funded_benchmark_inputs",
        }:
            output[capability_id].update(
                {
                    "observed_at": benchmark[
                        "identity_observed_at"
                        if capability_id == "benchmark_identity"
                        else "observed_at"
                    ],
                    "sessions": benchmark["sessions"],
                    "weekly_observations": benchmark["weekly_observations"],
                    "securities": benchmark["securities"],
                    "coverage": benchmark["coverage"],
                    "expected_coverage": 1.0,
                    "maximum_session": benchmark["maximum_session"],
                    "identity_subset_sha256": benchmark.get(
                        "identity_subset_sha256"
                    ),
                    "minimum_sessions": BENCHMARK_MINIMUM_SESSIONS,
                    "minimum_weekly_observations": BENCHMARK_MINIMUM_WEEKLY_OBSERVATIONS,
                }
            )
    return output


def apply_target_session_gates(
    capabilities: dict[str, dict[str, object]],
    phase_id: str,
    reference_session: date,
    canonical_session: date,
    release_dir: Path,
) -> None:
    required = set(PHASES[phase_id].required_capabilities)
    if "broad_market_current" in required and reference_session != canonical_session:
        capabilities["broad_market_current"].update(
            {
                "state": "STALE",
                "reason": (
                    "Broad market does not cover the targeted XNYS session: "
                    f"observed={canonical_session.isoformat()}, "
                    f"required={reference_session.isoformat()}"
                ),
            }
        )
    if phase_id in {"accounting", "sunday"}:
        certification_path = release_dir / "benchmark-certification.json"
        try:
            certification = json.loads(certification_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            certification = {}
        observed = (
            certification.get("observed_latest_xnys_session")
            if isinstance(certification, dict)
            else None
        )
        if observed != reference_session.isoformat():
            for capability_id in (
                "benchmark_market_current",
                "certified_total_returns",
                "funded_benchmark_inputs",
            ):
                capabilities[capability_id].update(
                    {
                        "state": "STALE",
                        "reason": (
                            "Benchmark lane does not cover the targeted XNYS session: "
                            f"observed={observed}, required={reference_session.isoformat()}"
                        ),
                    }
                )
    if phase_id == "saturday_replay" and reference_session != canonical_session:
        capabilities["canonical_replay"].update(
            {
                "state": "STALE",
                "reason": (
                    "Replay history does not cover its targeted reference session: "
                    f"observed={canonical_session.isoformat()}, "
                    f"required={reference_session.isoformat()}"
                ),
            }
        )


def build_phase_pack(
    phase_id: str,
    capabilities: Mapping[str, Mapping[str, object]],
    source_tag: str,
    source_manifest_sha256: str,
    database_sha256: str,
    built_at: str,
    data_cutoff: str,
    valid_session: str,
    watermarks: Sequence[Mapping[str, object]],
    windows: Sequence[Mapping[str, object]],
    auxiliary_assets: Mapping[str, Mapping[str, object]],
) -> dict[str, object]:
    contract = PHASES[phase_id]
    required = [dict(capabilities[value]) for value in contract.required_capabilities]
    optional = [dict(capabilities[value]) for value in contract.optional_capabilities]
    external = [dict(capabilities[value]) for value in contract.external_capabilities]
    unavailable_required = [
        item["capability_id"] for item in required if item.get("state") != "READY"
    ]
    unavailable_optional = [
        item["capability_id"] for item in optional if item.get("state") != "READY"
    ]
    status = (
        "BLOCKED"
        if unavailable_required
        else "DEGRADED"
        if unavailable_optional
        else "READY"
    )
    operating_modes = ["ARTIFACT_VALID"]
    if phase_id in {"sunday", "accounting"} and all(
        capabilities[capability_id].get("state") == "READY"
        for capability_id in (
            "benchmark_identity",
            "benchmark_market_current",
            "certified_total_returns",
            "funded_benchmark_inputs",
        )
    ):
        operating_modes.append("BENCHMARK_ONLY_SAFE")
    if phase_id != "accounting" and not unavailable_required and not unavailable_optional:
        operating_modes.append("CHALLENGER_RESEARCH_READY")
    if external:
        operating_modes.append("LIVE_SNAPSHOT_REQUIRED")
    if unavailable_required or unavailable_optional:
        operating_modes.append("CHALLENGER_BLOCKED")
    operating_modes = [mode for mode in OPERATING_MODES if mode in operating_modes]
    rejection_codes = sorted(
        {
            f"CAPABILITY_{item['capability_id'].upper()}_{item['state']}"
            for item in required + optional
            if item.get("state") != "READY"
        }
    )
    first_window = dict(windows[0])
    return {
        "schema_version": SCHEMA_VERSION,
        "contract_version": CONTRACT_VERSION,
        "phase_id": phase_id,
        "status": status,
        "operating_modes": operating_modes,
        "decision_mode": operating_modes[-1],
        "rejection_codes": rejection_codes,
        "built_at_utc": built_at,
        "generated_at_utc": built_at,
        "data_cutoff_utc": data_cutoff,
        "valid_for_session": valid_session,
        "phase_decision_cutoff_utc": first_window["phase_decision_cutoff_utc"],
        "decision_cutoff_utc": first_window["phase_decision_cutoff_utc"],
        "not_before_utc": first_window["not_before_utc"],
        "expires_at_utc": first_window["expires_at_utc"],
        "phase_windows": [dict(value) for value in windows],
        "task_timezone": TASK_TIMEZONE,
        "exchange_timezone": EXCHANGE_TIMEZONE,
        "delivery_timezone": TASK_TIMEZONE,
        "delivery_targets": list(contract.delivery_targets),
        "source_release_tag": source_tag,
        "source_manifest_sha256": source_manifest_sha256,
        "decision_support_database_sha256": database_sha256,
        "source_watermarks": [dict(value) for value in watermarks],
        "auxiliary_assets": dict(auxiliary_assets),
        "required_capabilities": required,
        "optional_capabilities": optional,
        "external_capabilities": external,
        "unavailable_required": unavailable_required,
        "unavailable_optional": unavailable_optional,
        "consumer_live_snapshot_required": bool(external),
        "consumer_live_snapshot_schema": SCHEMA_VERSION if external else None,
        "private_state_owner": "CONSUMER",
        "decision_owner": "CONSUMER",
    }


def file_record(path: Path, relative_to: Path) -> dict[str, object]:
    return {
        "path": path.relative_to(relative_to).as_posix(),
        "bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def compress_database(source: Path, target: Path) -> None:
    executable = shutil.which("zstd")
    if executable is None:
        raise DecisionSupportBuildError("zstd executable is required")
    result = subprocess.run(
        [executable, "-q", "-19", "-T1", "-f", str(source), "-o", str(target)],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode:
        raise DecisionSupportBuildError(
            f"zstd compression failed: {result.stderr.strip()}"
        )


def build_auxiliary_assets(
    database_path: Path,
    out_dir: Path,
    capabilities: Mapping[str, Mapping[str, object]],
    built_at: str,
) -> dict[str, dict[str, object]]:
    funnel_path = out_dir / CANDIDATE_FUNNEL_FILENAME
    duck_connection = duckdb.connect()
    try:
        definitions = ",".join(
            f"{quoted(name)} {kind}" for name, kind in CANDIDATE_FUNNEL_SCHEMA
        )
        duck_connection.execute(f"CREATE TABLE candidate_funnel({definitions})")
        duck_connection.execute(
            "COPY candidate_funnel TO ? (FORMAT PARQUET, COMPRESSION ZSTD)",
            [str(funnel_path)],
        )
    finally:
        duck_connection.close()

    phase_decisions = []
    for phase_id, contract in sorted(PHASES.items()):
        unavailable = sorted(
            capability_id
            for capability_id in contract.required_capabilities
            + contract.optional_capabilities
            if capabilities[capability_id].get("state") != "READY"
        )
        phase_decisions.append(
            {
                "phase_id": phase_id,
                "state": "BLOCKED" if unavailable else "READY",
                "rejection_codes": [
                    f"CAPABILITY_{value.upper()}_{capabilities[value]['state']}"
                    for value in unavailable
                ],
            }
        )
    actionability_path = out_dir / ACTIONABILITY_MATRIX_FILENAME
    actionability_path.write_bytes(
        canonical_json(
            {
                "schema_version": SCHEMA_VERSION,
                "contract_version": CONTRACT_VERSION,
                "built_at_utc": built_at,
                "selector_state": capabilities["candidate_funnel"]["state"],
                "phase_decisions": phase_decisions,
                "securities": [],
            }
        )
    )

    evidence_uncompressed = out_dir / "evidence-packets.jsonl"
    connection = sqlite3.connect(f"file:{database_path}?mode=ro&immutable=1", uri=True)
    try:
        with evidence_uncompressed.open("wb") as handle:
            columns = [name for name, _ in EVIDENCE_COLUMNS]
            if capabilities["candidate_funnel"]["state"] == "READY":
                for row in connection.execute(
                    "SELECT "
                    + ",".join(quoted(value) for value in columns)
                    + " FROM evidence ORDER BY evidence_id"
                ):
                    packet = dict(zip(columns, row, strict=True))
                    handle.write(canonical_json(packet))
    finally:
        connection.close()
    evidence_path = out_dir / EVIDENCE_PACKETS_FILENAME
    compress_database(evidence_uncompressed, evidence_path)
    evidence_uncompressed.unlink()

    return {
        "candidate_funnel": file_record(funnel_path, out_dir),
        "actionability_matrix": file_record(actionability_path, out_dir),
        "evidence_packets": file_record(evidence_path, out_dir),
    }


def build_database(
    release_dir: Path,
    database_path: Path,
    manifest: Mapping[str, object],
    source_tag: str,
    source_manifest_sha256: str,
    built_at: str,
    data_cutoff: str,
    valid_session: str,
    producer_commit: str,
    capabilities: Mapping[str, Mapping[str, object]],
) -> dict[str, int]:
    sqlite_connection = sqlite3.connect(database_path)
    duck_connection = duckdb.connect()
    row_counts: dict[str, int] = {}
    try:
        sqlite_connection.execute("PRAGMA page_size=4096")
        sqlite_connection.execute("PRAGMA auto_vacuum=NONE")
        sqlite_connection.execute("PRAGMA journal_mode=OFF")
        sqlite_connection.execute("PRAGMA synchronous=OFF")
        sqlite_connection.execute("PRAGMA temp_store=MEMORY")
        sqlite_connection.execute("PRAGMA foreign_keys=ON")
        sqlite_connection.execute("BEGIN")

        create_table(
            sqlite_connection,
            "metadata",
            (("key", "TEXT NOT NULL"), ("value", "TEXT NOT NULL")),
            ("key",),
        )
        metadata = {
            "schema_version": SCHEMA_VERSION,
            "contract_version": CONTRACT_VERSION,
            "source_release_tag": source_tag,
            "source_manifest_sha256": source_manifest_sha256,
            "source_created_at_utc": str(manifest.get("created_at_utc") or ""),
            "source_cutoff_date": str(manifest.get("cutoff_date") or ""),
            "built_at_utc": built_at,
            "generated_at_utc": built_at,
            "data_cutoff_utc": data_cutoff,
            "data_session": valid_session,
            "valid_for_session": valid_session,
            "producer_commit": producer_commit,
            "task_timezone": TASK_TIMEZONE,
            "exchange_timezone": EXCHANGE_TIMEZONE,
            "market_data_role": "NON_EXECUTABLE_RESEARCH_PROXY",
            "price_adjustment": "RAW_CLOSE_NOT_DIVIDEND_ADJUSTED",
            "private_state_owner": "CONSUMER",
            "decision_owner": "CONSUMER",
            "recent_market_sessions": str(RECENT_MARKET_SESSIONS),
        }
        row_counts["metadata"] = insert_rows(
            sqlite_connection,
            "metadata",
            ("key", "value"),
            sorted(metadata.items()),
        )

        create_table(
            sqlite_connection,
            "dataset_health",
            (
                ("group_id", "TEXT NOT NULL"),
                ("state", "TEXT NOT NULL"),
                ("mode", "TEXT"),
                ("source_release_tag", "TEXT"),
                ("group_sha256", "TEXT"),
                ("freshness_json", "TEXT NOT NULL"),
                ("exclusions_json", "TEXT NOT NULL"),
            ),
            ("group_id",),
        )
        groups = group_records(manifest)
        dataset_health_rows = []
        for group_id, record in sorted(groups.items()):
            dataset_health_rows.append(
                (
                    group_id,
                    record.get("state"),
                    record.get("mode"),
                    record.get("source_release_tag"),
                    record.get("group_sha256"),
                    json.dumps(
                        record.get("freshness") or {},
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    json.dumps(
                        record.get("exclusions") or [],
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                )
            )
        row_counts["dataset_health"] = insert_rows(
            sqlite_connection,
            "dataset_health",
            (
                "group_id",
                "state",
                "mode",
                "source_release_tag",
                "group_sha256",
                "freshness_json",
                "exclusions_json",
            ),
            dataset_health_rows,
        )

        create_table(
            sqlite_connection,
            "capability_health",
            (
                ("capability_id", "TEXT NOT NULL"),
                ("state", "TEXT NOT NULL"),
                ("reason", "TEXT"),
                ("source_group", "TEXT"),
                ("observed_at", "TEXT"),
                ("maximum_age_seconds", "INTEGER"),
                ("sessions", "INTEGER"),
                ("weekly_observations", "INTEGER"),
                ("minimum_sessions", "INTEGER"),
                ("minimum_weekly_observations", "INTEGER"),
                ("securities", "INTEGER"),
                ("coverage", "REAL"),
                ("expected_coverage", "REAL"),
                ("maximum_session", "TEXT"),
                ("identity_subset_sha256", "TEXT"),
            ),
            ("capability_id",),
        )
        row_counts["capability_health"] = insert_rows(
            sqlite_connection,
            "capability_health",
            (
                "capability_id",
                "state",
                "reason",
                "source_group",
                "observed_at",
                "maximum_age_seconds",
                "sessions",
                "weekly_observations",
                "minimum_sessions",
                "minimum_weekly_observations",
                "securities",
                "coverage",
                "expected_coverage",
                "maximum_session",
                "identity_subset_sha256",
            ),
            (
                (
                    capability_id,
                    record["state"],
                    record["reason"],
                    record["source_group"],
                    record["observed_at"],
                    record["maximum_age_seconds"],
                    record.get("sessions"),
                    record.get("weekly_observations"),
                    record.get("minimum_sessions"),
                    record.get("minimum_weekly_observations"),
                    record.get("securities"),
                    record.get("coverage"),
                    record.get("expected_coverage"),
                    record.get("maximum_session"),
                    record.get("identity_subset_sha256"),
                )
                for capability_id, record in sorted(capabilities.items())
            ),
        )

        master_rows = duck_connection.execute(
            """
            SELECT security_id, ticker, exchange_mic, cik, registrant_name, sic,
                   mapping_status
            FROM read_parquet(?) ORDER BY security_id
            """,
            [str(release_dir / "security-master.parquet")],
        ).fetchall()
        master = {str(row[0]): row for row in master_rows}
        active: dict[str, dict[str, str]] = {}
        with (release_dir / "security-universe.csv").open(
            newline="", encoding="utf-8-sig"
        ) as handle:
            reader = csv.DictReader(handle)
            required = {"security_id", "ticker", "universe_admission_status"}
            if not required <= set(reader.fieldnames or ()):
                raise DecisionSupportBuildError(
                    "Security universe lacks required columns"
                )
            for row in reader:
                if (
                    str(row.get("universe_admission_status") or "").upper()
                    in ADMITTED_STATES
                ):
                    active[str(row["security_id"])] = {
                        key: str(value or "") for key, value in row.items()
                    }
        create_table(
            sqlite_connection,
            "security",
            (
                ("security_id", "TEXT NOT NULL"),
                ("ticker", "TEXT NOT NULL"),
                ("exchange_mic", "TEXT"),
                ("security_type", "TEXT"),
                ("universe_admission_status", "TEXT"),
                ("cik", "TEXT"),
                ("registrant_name", "TEXT"),
                ("sic", "TEXT"),
                ("mapping_status", "TEXT"),
                ("is_current", "INTEGER NOT NULL"),
            ),
            ("security_id",),
        )
        security_rows = []
        for security_id in sorted(set(master) | set(active)):
            universe_row = active.get(security_id, {})
            master_row = master.get(security_id)
            security_rows.append(
                (
                    security_id,
                    universe_row.get("ticker") or (master_row[1] if master_row else ""),
                    universe_row.get("exchange_mic")
                    or (master_row[2] if master_row else None),
                    universe_row.get("security_type") or None,
                    universe_row.get("universe_admission_status") or None,
                    master_row[3] if master_row else None,
                    master_row[4] if master_row else None,
                    master_row[5] if master_row else None,
                    master_row[6] if master_row else None,
                    int(security_id in active),
                )
            )
        row_counts["security"] = insert_rows(
            sqlite_connection,
            "security",
            (
                "security_id",
                "ticker",
                "exchange_mic",
                "security_type",
                "universe_admission_status",
                "cik",
                "registrant_name",
                "sic",
                "mapping_status",
                "is_current",
            ),
            security_rows,
        )

        market_path = str(release_dir / "yahoo-ohlcv-320.parquet")
        market_snapshot_columns = (
            ("security_id", "TEXT"),
            ("ticker", "TEXT"),
            ("session_date", "TEXT"),
            ("open", "REAL"),
            ("high", "REAL"),
            ("low", "REAL"),
            ("close", "REAL"),
            ("volume", "INTEGER"),
            ("price_return_1d", "REAL"),
            ("price_return_5d", "REAL"),
            ("price_return_20d", "REAL"),
            ("price_return_63d", "REAL"),
            ("average_volume_20d", "REAL"),
            ("average_dollar_volume_20d", "REAL"),
            ("source_dataset", "TEXT"),
            ("source_revision", "TEXT"),
            ("observed_at_utc", "TEXT"),
        )
        row_counts["market_snapshot"] = copy_query(
            sqlite_connection,
            duck_connection,
            "market_snapshot",
            market_snapshot_columns,
            """
            WITH ranked AS (
              SELECT *, row_number() OVER (
                PARTITION BY security_id ORDER BY session_date DESC
              ) AS rn
              FROM read_parquet(?)
            ), summarized AS (
              SELECT security_id,
                max(CASE WHEN rn=1 THEN ticker END) AS ticker,
                max(CASE WHEN rn=1 THEN session_date END) AS session_date,
                max(CASE WHEN rn=1 THEN open END) AS open,
                max(CASE WHEN rn=1 THEN high END) AS high,
                max(CASE WHEN rn=1 THEN low END) AS low,
                max(CASE WHEN rn=1 THEN close END) AS close,
                max(CASE WHEN rn=1 THEN volume END) AS volume,
                max(CASE WHEN rn=2 THEN close END) AS close_1d,
                max(CASE WHEN rn=6 THEN close END) AS close_5d,
                max(CASE WHEN rn=21 THEN close END) AS close_20d,
                max(CASE WHEN rn=64 THEN close END) AS close_63d,
                avg(CASE WHEN rn <= 20 THEN volume END) AS average_volume_20d,
                avg(CASE WHEN rn <= 20 THEN close * volume END) AS average_dollar_volume_20d,
                max(CASE WHEN rn=1 THEN source_dataset END) AS source_dataset,
                max(CASE WHEN rn=1 THEN source_revision END) AS source_revision,
                max(CASE WHEN rn=1 THEN observed_at_utc END) AS observed_at_utc
              FROM ranked GROUP BY security_id
            )
            SELECT security_id, ticker, session_date, open, high, low, close, volume,
              close / nullif(close_1d, 0) - 1 AS price_return_1d,
              close / nullif(close_5d, 0) - 1 AS price_return_5d,
              close / nullif(close_20d, 0) - 1 AS price_return_20d,
              close / nullif(close_63d, 0) - 1 AS price_return_63d,
              average_volume_20d, average_dollar_volume_20d,
              source_dataset, source_revision, observed_at_utc
            FROM summarized ORDER BY security_id
            """,
            (market_path,),
        )
        market_history_columns = (
            ("security_id", "TEXT"),
            ("session_date", "TEXT"),
            ("open", "REAL"),
            ("high", "REAL"),
            ("low", "REAL"),
            ("close", "REAL"),
            ("volume", "INTEGER"),
            ("source_revision", "TEXT"),
            ("observed_at_utc", "TEXT"),
        )
        row_counts["market_history_recent"] = copy_query(
            sqlite_connection,
            duck_connection,
            "market_history_recent",
            market_history_columns,
            """
            SELECT security_id, session_date, open, high, low, close, volume,
                   source_revision, observed_at_utc
            FROM (
              SELECT *, row_number() OVER (
                PARTITION BY security_id ORDER BY session_date DESC
              ) AS rn
              FROM read_parquet(?)
            )
            WHERE rn <= ? ORDER BY security_id, session_date
            """,
            (market_path, RECENT_MARKET_SESSIONS),
        )

        latest_contracts = (
            (
                "fundamental_factors_latest",
                "fundamental-factors.parquet",
                "security_id",
                "factor_as_of_date",
            ),
            (
                "insider_signals_latest",
                "insider-signals.parquet",
                "security_id",
                "signal_as_of_date",
            ),
            (
                "institutional_signals_latest",
                "institutional-ownership-signals.parquet",
                "security_id",
                "report_period",
            ),
            (
                "short_interest_latest",
                "finra-short-interest.parquet",
                "security_id",
                "settlement_date",
            ),
        )
        for table, filename, partition_column, order_column in latest_contracts:
            contract = CONTRACTS[filename]
            names = [name for name, _ in contract.columns]
            selected = ",".join(quoted(name) for name in names)
            row_counts[table] = copy_query(
                sqlite_connection,
                duck_connection,
                table,
                sqlite_columns(contract),
                f"""
                SELECT {selected} FROM read_parquet(?)
                QUALIFY row_number() OVER (
                  PARTITION BY {quoted(partition_column)}
                  ORDER BY {quoted(order_column)} DESC NULLS LAST,
                           source_retrieved_at_utc DESC NULLS LAST
                ) = 1
                ORDER BY {quoted(partition_column)}
                """,
                (str(release_dir / filename),),
            )

        cutoff = str(manifest.get("cutoff_date") or date.today().isoformat())
        event_contract = CONTRACTS["corporate-events.parquet"]
        event_names = [name for name, _ in event_contract.columns]
        row_counts["corporate_events"] = copy_query(
            sqlite_connection,
            duck_connection,
            "corporate_events",
            sqlite_columns(event_contract),
            f"""
            SELECT {",".join(quoted(name) for name in event_names)}
            FROM read_parquet(?)
            WHERE coalesce(effective_date, source_event_date, source_filing_date)
              BETWEEN cast(? AS DATE) - INTERVAL 180 DAY
                  AND cast(? AS DATE) + INTERVAL 365 DAY
            ORDER BY coalesce(effective_date, source_event_date, source_filing_date),
                     security_id, event_id
            """,
            (str(release_dir / event_contract.filename), cutoff, cutoff),
        )
        earnings_contract = CONTRACTS["earnings-and-guidance-events.parquet"]
        earnings_names = [name for name, _ in earnings_contract.columns]
        row_counts["earnings_events"] = copy_query(
            sqlite_connection,
            duck_connection,
            "earnings_events",
            sqlite_columns(earnings_contract),
            f"""
            SELECT {",".join(quoted(name) for name in earnings_names)}
            FROM read_parquet(?)
            WHERE coalesce(event_date, source_event_date, source_filing_date)
              BETWEEN cast(? AS DATE) - INTERVAL 180 DAY
                  AND cast(? AS DATE) + INTERVAL 365 DAY
            ORDER BY coalesce(event_date, source_event_date, source_filing_date),
                     security_id, event_id
            """,
            (str(release_dir / earnings_contract.filename), cutoff, cutoff),
        )

        filing_contract = CONTRACTS["sec-filings.parquet"]
        filing_names = [name for name, _ in filing_contract.columns]
        row_counts["primary_filings_latest"] = copy_query(
            sqlite_connection,
            duck_connection,
            "primary_filings_latest",
            sqlite_columns(filing_contract),
            f"""
            SELECT {",".join(quoted(name) for name in filing_names)}
            FROM read_parquet(?)
            QUALIFY row_number() OVER (
              PARTITION BY security_id, form
              ORDER BY acceptance_datetime_utc DESC NULLS LAST,
                       source_retrieved_at_utc DESC NULLS LAST,
                       accession_number DESC
            ) = 1
            ORDER BY security_id, form
            """,
            (str(release_dir / filing_contract.filename),),
        )

        evidence_query = """
            SELECT 'sec-filing:' || security_id || ':' || accession_number AS evidence_id,
                   security_id, 'SEC_FILING' AS evidence_kind,
                   coalesce(acceptance_datetime_utc,
                            source_acceptance_datetime_utc,
                            source_retrieved_at_utc) AS known_at_utc,
                   accession_number AS revision,
                   coalesce(acceptance_datetime_utc,
                            source_acceptance_datetime_utc) AS source_event_at,
                   primary_document_url AS source_locator,
                   form || ' filing' AS headline,
                   primary_document AS summary
            FROM read_parquet(?)
            WHERE accession_number IS NOT NULL AND security_id IS NOT NULL
              AND primary_document_url IS NOT NULL
              AND coalesce(acceptance_datetime_utc,
                           source_acceptance_datetime_utc,
                           source_retrieved_at_utc) IS NOT NULL
            QUALIFY row_number() OVER (
              PARTITION BY security_id, accession_number
              ORDER BY acceptance_datetime_utc DESC NULLS LAST,
                       source_retrieved_at_utc DESC NULLS LAST,
                       accession_number DESC
            ) = 1
            UNION ALL
            SELECT 'corporate-event:' || security_id || ':' || event_id,
                   security_id, 'CORPORATE_EVENT',
                   coalesce(announcement_datetime_utc,
                            source_acceptance_datetime_utc,
                            source_retrieved_at_utc),
                   event_id,
                   coalesce(announcement_datetime_utc,
                            source_acceptance_datetime_utc),
                   source_document_url, headline, summary
            FROM read_parquet(?)
            WHERE event_id IS NOT NULL AND security_id IS NOT NULL
              AND source_document_url IS NOT NULL
              AND coalesce(announcement_datetime_utc,
                           source_acceptance_datetime_utc,
                           source_retrieved_at_utc) IS NOT NULL
              AND coalesce(effective_date, source_event_date, source_filing_date)
                BETWEEN cast(? AS DATE) - INTERVAL 180 DAY
                    AND cast(? AS DATE) + INTERVAL 365 DAY
            QUALIFY row_number() OVER (
              PARTITION BY security_id, event_id
              ORDER BY source_retrieved_at_utc DESC NULLS LAST,
                       announcement_datetime_utc DESC NULLS LAST
            ) = 1
            UNION ALL
            SELECT 'earnings-event:' || security_id || ':' || event_id,
                   security_id, 'EARNINGS_EVENT',
                   coalesce(event_datetime_utc,
                            source_acceptance_datetime_utc,
                            source_retrieved_at_utc),
                   event_id,
                   coalesce(event_datetime_utc,
                            source_acceptance_datetime_utc),
                   source_document_url,
                   event_type || ' ' || coalesce(fiscal_period, ''),
                   guidance_direction
            FROM read_parquet(?)
            WHERE event_id IS NOT NULL AND security_id IS NOT NULL
              AND source_document_url IS NOT NULL
              AND coalesce(event_datetime_utc,
                           source_acceptance_datetime_utc,
                           source_retrieved_at_utc) IS NOT NULL
              AND coalesce(event_date, source_event_date, source_filing_date)
                BETWEEN cast(? AS DATE) - INTERVAL 180 DAY
                    AND cast(? AS DATE) + INTERVAL 365 DAY
            QUALIFY row_number() OVER (
              PARTITION BY security_id, event_id
              ORDER BY source_retrieved_at_utc DESC NULLS LAST,
                       event_datetime_utc DESC NULLS LAST
            ) = 1
            ORDER BY evidence_id
        """
        row_counts["evidence"] = copy_query(
            sqlite_connection,
            duck_connection,
            "evidence",
            EVIDENCE_COLUMNS,
            evidence_query,
            (
                str(release_dir / filing_contract.filename),
                str(release_dir / event_contract.filename),
                cutoff,
                cutoff,
                str(release_dir / earnings_contract.filename),
                cutoff,
                cutoff,
            ),
        )

        create_table(
            sqlite_connection,
            "candidate_funnel",
            tuple((name, SQLITE_TYPE[kind]) for name, kind in CANDIDATE_FUNNEL_SCHEMA),
        )
        row_counts["candidate_funnel"] = 0
        create_table(
            sqlite_connection,
            "actionability_matrix",
            (
                ("phase_id", "TEXT"),
                ("security_id", "TEXT"),
                ("actionability_state", "TEXT"),
                ("rejection_codes_json", "TEXT"),
                ("known_at_utc", "TEXT"),
                ("revision", "TEXT"),
            ),
        )
        row_counts["actionability_matrix"] = 0

        analyst_path = release_dir / ANALYST_ESTIMATES_CONTRACT.filename
        analyst_names = [name for name, _ in ANALYST_ESTIMATES_CONTRACT.columns]
        if analyst_path.is_file():
            row_counts["analyst_estimates_latest"] = copy_query(
                sqlite_connection,
                duck_connection,
                "analyst_estimates_latest",
                sqlite_columns(ANALYST_ESTIMATES_CONTRACT),
                f"""
                SELECT {",".join(quoted(name) for name in analyst_names)}
                FROM read_parquet(?)
                QUALIFY row_number() OVER (
                  PARTITION BY security_id, fiscal_period, metric, provider
                  ORDER BY estimate_as_of_utc DESC, source_retrieved_at_utc DESC
                ) = 1
                ORDER BY security_id, fiscal_period, metric, provider
                """,
                (str(analyst_path),),
            )
        else:
            create_table(
                sqlite_connection,
                "analyst_estimates_latest",
                sqlite_columns(ANALYST_ESTIMATES_CONTRACT),
            )
            row_counts["analyst_estimates_latest"] = 0

        future_sources = {
            "distributions": "benchmark-distributions.parquet",
            "benchmark_total_returns": "benchmark-total-returns.parquet",
        }
        for table, columns in FUTURE_TABLE_COLUMNS.items():
            source_path = release_dir / future_sources[table]
            if source_path.is_file():
                names = [name for name, _ in columns]
                row_counts[table] = copy_query(
                    sqlite_connection,
                    duck_connection,
                    table,
                    columns,
                    f"SELECT {','.join(quoted(name) for name in names)} "
                    f"FROM read_parquet(?) ORDER BY {','.join(quoted(name) for name in names[:2])}",
                    (str(source_path),),
                )
            else:
                create_table(sqlite_connection, table, columns)
                row_counts[table] = 0

        sqlite_connection.execute(
            "CREATE INDEX market_history_security_date ON market_history_recent(security_id, session_date)"
        )
        sqlite_connection.execute(
            "CREATE INDEX corporate_events_security_date ON corporate_events(security_id, effective_date)"
        )
        sqlite_connection.execute(
            "CREATE INDEX earnings_events_security_date ON earnings_events(security_id, event_date)"
        )
        sqlite_connection.execute(
            "CREATE UNIQUE INDEX evidence_identity ON evidence(evidence_id)"
        )
        sqlite_connection.execute(
            "CREATE INDEX factors_quality ON fundamental_factors_latest(factor_quality_status)"
        )
        sqlite_connection.commit()
        check = sqlite_connection.execute("PRAGMA integrity_check").fetchone()
        if check != ("ok",):
            raise DecisionSupportBuildError(f"SQLite integrity check failed: {check}")
        sqlite_connection.execute("VACUUM")
    finally:
        duck_connection.close()
        sqlite_connection.close()
    return row_counts


def build(args: argparse.Namespace) -> dict[str, object]:
    release_dir = Path(args.release_dir).resolve()
    out_dir = Path(args.out_dir).resolve()
    if out_dir == release_dir:
        raise DecisionSupportBuildError(
            "Output directory must differ from source release"
        )
    out_dir.mkdir(parents=True, exist_ok=True)
    phase_dir = out_dir / PHASE_PACK_DIRECTORY
    phase_dir.mkdir(parents=True, exist_ok=True)
    generated = (
        parse_utc(args.generated_at)
        if args.generated_at
        else datetime.now(timezone.utc)
    )
    generated_at = format_utc(generated)
    target_phase = str(getattr(args, "target_phase", None) or "")
    target_window = str(getattr(args, "target_window", None) or "")
    reference_session_text = str(getattr(args, "reference_session", None) or "")
    scheduled_time_text = str(getattr(args, "scheduled_time", None) or "")
    artifact_deadline_text = str(getattr(args, "artifact_deadline", None) or "")
    idempotency_key = str(getattr(args, "idempotency_key", None) or "")
    publication_target: dict[str, object] | None = None
    if any((target_phase, target_window, reference_session_text, artifact_deadline_text)):
        if (
            target_phase not in PHASES
            or not target_window
            or not reference_session_text
            or not scheduled_time_text
            or not artifact_deadline_text
            or not idempotency_key
        ):
            raise DecisionSupportBuildError(
                "Targeted publication requires phase, window, reference session, "
                "artifact deadline, and idempotency key"
            )
        try:
            reference_session = date.fromisoformat(reference_session_text)
        except ValueError as exc:
            raise DecisionSupportBuildError(
                "Publication reference session is invalid"
            ) from exc
        target_windows = phase_windows(target_phase, reference_session)
        matching_window = next(
            (window for window in target_windows if window.get("window_id") == target_window),
            None,
        )
        if matching_window is None:
            raise DecisionSupportBuildError(
                "Publication window does not match the phase/reference session"
            )
        artifact_deadline = parse_utc(artifact_deadline_text)
        scheduled_time = parse_utc(scheduled_time_text)
        if artifact_deadline != parse_utc(
            str(matching_window["phase_decision_cutoff_utc"])
        ):
            raise DecisionSupportBuildError(
                "Artifact deadline must equal the phase decision cutoff"
            )
        if generated > artifact_deadline:
            raise DecisionSupportBuildError(
                "Artifact generation started after its phase deadline"
            )
        if generated < scheduled_time or scheduled_time > artifact_deadline:
            raise DecisionSupportBuildError(
                "Artifact generation time is inconsistent with its scheduled time"
            )
        publication_target = {
            "phase_id": target_phase,
            "window_id": target_window,
            "reference_session": reference_session.isoformat(),
            "scheduled_time_utc": format_utc(scheduled_time),
            "artifact_deadline_utc": format_utc(artifact_deadline),
            "idempotency_key": idempotency_key,
        }
    producer_commit = str(getattr(args, "producer_commit", None) or "LOCAL_TEST")
    if (
        producer_commit != "LOCAL_TEST"
        and re.fullmatch(r"[0-9a-f]{40}", producer_commit) is None
    ):
        raise DecisionSupportBuildError(
            "producer_commit must be a lowercase 40-character Git SHA"
        )
    source_manifest_path = release_dir / "manifest.json"
    source_manifest = load_manifest(source_manifest_path)
    source_manifest_sha256 = sha256_file(source_manifest_path)
    overlay_files, benchmark_group, benchmark_overlay = load_benchmark_overlay(
        release_dir, source_manifest_sha256
    )
    source_files = validate_source_files(
        release_dir, source_manifest, overlay_records=overlay_files
    )
    groups = group_records(source_manifest)
    if benchmark_group is not None:
        groups["total_returns"] = benchmark_group
    capabilities = capability_records(groups, release_dir, as_of=generated)
    valid_session_date = source_session(source_manifest)
    if publication_target is not None:
        apply_target_session_gates(
            capabilities,
            target_phase,
            reference_session,
            valid_session_date,
            release_dir,
        )
    valid_session = valid_session_date.isoformat()
    calendar = xcals.get_calendar("XNYS")
    market_session = calendar.date_to_session(valid_session_date, direction="none")
    data_cutoff = format_utc(calendar.session_close(market_session).to_pydatetime())
    watermarks = source_watermarks(groups, capabilities)
    validators = validator_identity(producer_commit)

    with tempfile.TemporaryDirectory(dir=out_dir, prefix="decision-support-") as temp:
        uncompressed = Path(temp) / "decision-support.sqlite"
        row_counts = build_database(
            release_dir,
            uncompressed,
            source_manifest,
            args.source_tag,
            source_manifest_sha256,
            generated_at,
            data_cutoff,
            valid_session,
            producer_commit,
            capabilities,
        )
        uncompressed_sha256 = sha256_file(uncompressed)
        uncompressed_bytes = uncompressed.stat().st_size
        auxiliary_assets = build_auxiliary_assets(
            uncompressed, out_dir, capabilities, generated_at
        )
        compressed = out_dir / DATABASE_FILENAME
        compress_database(uncompressed, compressed)

    if compressed.stat().st_size > MAX_COMPRESSED_DATABASE_BYTES:
        raise DecisionSupportBuildError(
            f"Compressed decision-support database is {compressed.stat().st_size:,} bytes; "
            f"limit is {MAX_COMPRESSED_DATABASE_BYTES:,} bytes"
        )
    database_sha256 = sha256_file(compressed)
    phase_records: list[dict[str, object]] = []
    phase_status_counts: dict[str, int] = {}
    for phase_id in sorted(PHASES):
        pack_reference_session = (
            reference_session
            if publication_target is not None and phase_id == target_phase
            else valid_session_date
        )
        pack = build_phase_pack(
            phase_id,
            capabilities,
            args.source_tag,
            source_manifest_sha256,
            database_sha256,
            generated_at,
            data_cutoff,
            phase_valid_session(phase_id, pack_reference_session),
            watermarks,
            phase_windows(phase_id, pack_reference_session),
            auxiliary_assets,
        )
        path = phase_dir / f"{phase_id}.json"
        path.write_bytes(canonical_json(pack))
        phase_records.append(
            {
                "phase_id": phase_id,
                "status": pack["status"],
                **file_record(path, out_dir),
            }
        )
        status = str(pack["status"])
        phase_status_counts[status] = phase_status_counts.get(status, 0) + 1

    warnings = [
        f"{capability_id}: {record['reason']}"
        for capability_id, record in sorted(capabilities.items())
        if record["state"] not in {"READY", "CONSUMER_REQUIRED"}
    ]
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "contract_version": CONTRACT_VERSION,
        "status": "READY",
        "built_at_utc": generated_at,
        "generated_at_utc": generated_at,
        "data_cutoff_utc": data_cutoff,
        "data_session": valid_session,
        "valid_for_session": valid_session,
        "task_timezone": TASK_TIMEZONE,
        "exchange_timezone": EXCHANGE_TIMEZONE,
        "source_release": {
            "tag": args.source_tag,
            "manifest_sha256": source_manifest_sha256,
            "schema_version": source_manifest.get("schema_version"),
            "created_at_utc": source_manifest.get("created_at_utc"),
            "cutoff_date": source_manifest.get("cutoff_date"),
            "status": source_manifest.get("status"),
        },
        "source_files": source_files,
        "hot_lane_overlays": (
            [benchmark_overlay] if benchmark_overlay is not None else []
        ),
        "publication_target": publication_target,
        "database": {
            **file_record(compressed, out_dir),
            "uncompressed_bytes": uncompressed_bytes,
            "uncompressed_sha256": uncompressed_sha256,
            "sqlite_version": sqlite3.sqlite_version,
            "recent_market_sessions": RECENT_MARKET_SESSIONS,
            "tables": [
                {"name": name, "rows": rows}
                for name, rows in sorted(row_counts.items())
            ],
            "market_data_role": "NON_EXECUTABLE_RESEARCH_PROXY",
            "price_adjustment": "RAW_CLOSE_NOT_DIVIDEND_ADJUSTED",
        },
        "routine_context_slo_seconds": 60,
        "maximum_compressed_database_bytes": MAX_COMPRESSED_DATABASE_BYTES,
        "capabilities": [capabilities[key] for key in sorted(capabilities)],
        "source_watermarks": watermarks,
        "validator_identity": validators,
        "auxiliary_assets": auxiliary_assets,
        "artifact_operating_modes": sorted(
            {
                mode
                for phase_id in PHASES
                for mode in build_phase_pack(
                    phase_id,
                    capabilities,
                    args.source_tag,
                    source_manifest_sha256,
                    database_sha256,
                    generated_at,
                    data_cutoff,
                    phase_valid_session(phase_id, valid_session_date),
                    watermarks,
                    phase_windows(phase_id, valid_session_date),
                    auxiliary_assets,
                )["operating_modes"]
            },
            key=OPERATING_MODES.index,
        ),
        "phase_packs": phase_records,
        "phase_status_counts": phase_status_counts,
        "private_state_owner": "CONSUMER",
        "decision_owner": "CONSUMER",
        "warnings": warnings,
    }
    (out_dir / MANIFEST_FILENAME).write_bytes(canonical_json(manifest))
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release-dir", required=True)
    parser.add_argument("--source-tag", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--generated-at")
    parser.add_argument("--decision-cutoff")
    parser.add_argument("--producer-commit")
    parser.add_argument("--target-phase", choices=sorted(PHASES))
    parser.add_argument("--target-window")
    parser.add_argument("--reference-session")
    parser.add_argument("--scheduled-time")
    parser.add_argument("--artifact-deadline")
    parser.add_argument("--idempotency-key")
    args = parser.parse_args(argv)
    try:
        manifest = build(args)
    except (DecisionSupportBuildError, OSError, sqlite3.Error, duckdb.Error) as exc:
        parser.error(str(exc))
    print(
        json.dumps(
            {
                "status": manifest["status"],
                "source_release_tag": manifest["source_release"]["tag"],
                "database_bytes": manifest["database"]["bytes"],
                "phase_status_counts": manifest["phase_status_counts"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
