#!/usr/bin/env python3
"""Build the compact, read-only decision-support artifact from a pinned release."""

from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import sqlite3
import subprocess
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping, Sequence

import duckdb

from decision_support_contract import (
    CAPABILITIES,
    DATABASE_FILENAME,
    MANIFEST_FILENAME,
    MAX_COMPRESSED_DATABASE_BYTES,
    OPTIONAL_SOURCE_FILES,
    PHASES,
    PHASE_PACK_DIRECTORY,
    RECENT_MARKET_SESSIONS,
    REQUIRED_SOURCE_FILES,
    SCHEMA_VERSION,
    USABLE_GROUP_STATES,
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
        ("ex_date", "TEXT"),
        ("record_date", "TEXT"),
        ("pay_date", "TEXT"),
        ("cash_amount", "REAL"),
        ("currency", "TEXT"),
        ("distribution_type", "TEXT"),
        ("source_revision", "TEXT"),
        ("source_publication_date", "TEXT"),
        ("source_retrieved_at_utc", "TEXT"),
    ),
    "benchmark_total_returns": (
        ("security_id", "TEXT"),
        ("session_date", "TEXT"),
        ("price_return", "REAL"),
        ("distribution_return", "REAL"),
        ("total_return", "REAL"),
        ("total_return_index", "REAL"),
        ("certification_status", "TEXT"),
        ("source_revision", "TEXT"),
        ("source_retrieved_at_utc", "TEXT"),
    ),
}


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
            raise DecisionSupportBuildError(f"Duplicate source release file: {filename}")
        output[filename] = record
    return output


def validate_source_files(
    release_dir: Path, manifest: Mapping[str, object]
) -> list[dict[str, object]]:
    records = release_records(manifest)
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
        record = records.get(filename)
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
            }
        )
    for filename in OPTIONAL_SOURCE_FILES:
        path = release_dir / filename
        if not path.exists():
            continue
        record = records.get(filename)
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
            }
        )
    return sorted(used, key=lambda item: str(item["file"]))


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
        if value.tzinfo is not None:
            return format_utc(value)
        return value.isoformat(sep="T")
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


def capability_records(
    groups: Mapping[str, Mapping[str, object]], release_dir: Path
) -> dict[str, dict[str, object]]:
    output: dict[str, dict[str, object]] = {}
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
            "certified_total_returns",
            "funded_benchmark_inputs",
        }:
            present = all(
                (release_dir / filename).is_file()
                for filename in (
                    "benchmark-total-returns.parquet",
                    "distributions.parquet",
                )
            )
            state = "READY" if present else "NOT_CONFIGURED"
            reason = None if present else "Certified total-return lane is not configured"
            freshness: Mapping[str, object] = {}
        elif capability_id == "point_in_time_expectations" and not (
            release_dir / ANALYST_ESTIMATES_CONTRACT.filename
        ).is_file():
            state = "NOT_CONFIGURED"
            reason = "Point-in-time analyst expectations are not configured"
            freshness = {}
        elif capability_id in {"rapid_event_news", "macro_industry", "etf_exposure", "survivorship_history"}:
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
            state = "READY" if group_state in USABLE_GROUP_STATES else group_state or "UNAVAILABLE"
            reason = None if state == "READY" else f"Source group state is {group_state or 'missing'}"
            if state == "READY" and capability_id == "historical_market":
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
    return output


def build_phase_pack(
    phase_id: str,
    capabilities: Mapping[str, Mapping[str, object]],
    source_tag: str,
    source_manifest_sha256: str,
    database_sha256: str,
    generated_at: str,
    decision_cutoff: str,
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
    return {
        "schema_version": SCHEMA_VERSION,
        "phase_id": phase_id,
        "status": status,
        "generated_at_utc": generated_at,
        "decision_cutoff_utc": decision_cutoff,
        "delivery_timezone": "America/New_York",
        "delivery_targets": list(contract.delivery_targets),
        "source_release_tag": source_tag,
        "source_manifest_sha256": source_manifest_sha256,
        "decision_support_database_sha256": database_sha256,
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


def build_database(
    release_dir: Path,
    database_path: Path,
    manifest: Mapping[str, object],
    source_tag: str,
    source_manifest_sha256: str,
    generated_at: str,
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
            "source_release_tag": source_tag,
            "source_manifest_sha256": source_manifest_sha256,
            "source_created_at_utc": str(manifest.get("created_at_utc") or ""),
            "source_cutoff_date": str(manifest.get("cutoff_date") or ""),
            "generated_at_utc": generated_at,
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
                    json.dumps(record.get("freshness") or {}, sort_keys=True, separators=(",", ":")),
                    json.dumps(record.get("exclusions") or [], sort_keys=True, separators=(",", ":")),
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
            ),
            (
                (
                    capability_id,
                    record["state"],
                    record["reason"],
                    record["source_group"],
                    record["observed_at"],
                    record["maximum_age_seconds"],
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
                raise DecisionSupportBuildError("Security universe lacks required columns")
            for row in reader:
                if str(row.get("universe_admission_status") or "").upper() in ADMITTED_STATES:
                    active[str(row["security_id"])] = {key: str(value or "") for key, value in row.items()}
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
                    universe_row.get("exchange_mic") or (master_row[2] if master_row else None),
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
            ("fundamental_factors_latest", "fundamental-factors.parquet", "security_id", "factor_as_of_date"),
            ("insider_signals_latest", "insider-signals.parquet", "security_id", "signal_as_of_date"),
            ("institutional_signals_latest", "institutional-ownership-signals.parquet", "security_id", "report_period"),
            ("short_interest_latest", "finra-short-interest.parquet", "security_id", "settlement_date"),
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
            SELECT {','.join(quoted(name) for name in event_names)}
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
            SELECT {','.join(quoted(name) for name in earnings_names)}
            FROM read_parquet(?)
            WHERE coalesce(event_date, source_event_date, source_filing_date)
              BETWEEN cast(? AS DATE) - INTERVAL 180 DAY
                  AND cast(? AS DATE) + INTERVAL 365 DAY
            ORDER BY coalesce(event_date, source_event_date, source_filing_date),
                     security_id, event_id
            """,
            (str(release_dir / earnings_contract.filename), cutoff, cutoff),
        )

        analyst_path = release_dir / ANALYST_ESTIMATES_CONTRACT.filename
        analyst_names = [name for name, _ in ANALYST_ESTIMATES_CONTRACT.columns]
        if analyst_path.is_file():
            row_counts["analyst_estimates_latest"] = copy_query(
                sqlite_connection,
                duck_connection,
                "analyst_estimates_latest",
                sqlite_columns(ANALYST_ESTIMATES_CONTRACT),
                f"""
                SELECT {','.join(quoted(name) for name in analyst_names)}
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
            "distributions": "distributions.parquet",
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
        raise DecisionSupportBuildError("Output directory must differ from source release")
    out_dir.mkdir(parents=True, exist_ok=True)
    phase_dir = out_dir / PHASE_PACK_DIRECTORY
    phase_dir.mkdir(parents=True, exist_ok=True)
    generated = parse_utc(args.generated_at) if args.generated_at else datetime.now(timezone.utc)
    generated_at = format_utc(generated)
    decision_cutoff = (
        format_utc(parse_utc(args.decision_cutoff))
        if args.decision_cutoff
        else generated_at
    )
    source_manifest_path = release_dir / "manifest.json"
    source_manifest = load_manifest(source_manifest_path)
    source_manifest_sha256 = sha256_file(source_manifest_path)
    source_files = validate_source_files(release_dir, source_manifest)
    groups = group_records(source_manifest)
    capabilities = capability_records(groups, release_dir)

    with tempfile.TemporaryDirectory(dir=out_dir, prefix="decision-support-") as temp:
        uncompressed = Path(temp) / "decision-support.sqlite"
        row_counts = build_database(
            release_dir,
            uncompressed,
            source_manifest,
            args.source_tag,
            source_manifest_sha256,
            generated_at,
            capabilities,
        )
        uncompressed_sha256 = sha256_file(uncompressed)
        uncompressed_bytes = uncompressed.stat().st_size
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
        pack = build_phase_pack(
            phase_id,
            capabilities,
            args.source_tag,
            source_manifest_sha256,
            database_sha256,
            generated_at,
            decision_cutoff,
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
        "status": "READY",
        "generated_at_utc": generated_at,
        "decision_cutoff_utc": decision_cutoff,
        "source_release": {
            "tag": args.source_tag,
            "manifest_sha256": source_manifest_sha256,
            "schema_version": source_manifest.get("schema_version"),
            "created_at_utc": source_manifest.get("created_at_utc"),
            "cutoff_date": source_manifest.get("cutoff_date"),
            "status": source_manifest.get("status"),
        },
        "source_files": source_files,
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
