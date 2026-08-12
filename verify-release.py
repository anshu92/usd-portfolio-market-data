#!/usr/bin/env python3
"""Verify a release directory against its manifest before publication or import."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import re
import sys
from pathlib import Path

import duckdb

from enrichment_contract import CONTRACTS
from reliability_contract import (
    CORE_GROUPS,
    FILE_TO_GROUP,
    GROUPS,
    GROUP_CONTRACT_VERSION,
    GROUP_STATES,
    MARKET_READY_MAX_LAG_SESSIONS,
    ReliabilityContractError,
    group_file_identities,
    group_sha256,
)


SCHEMA_VERSION = "1.0.0"
PRODUCTION_FILES = {
    "yahoo-ohlcv-320.parquet",
    "yahoo-splits.parquet",
    "security-universe.csv",
    "unmatched-tickers.csv",
    "NOTICE.md",
} | set(CONTRACTS)
OPTIONAL_PRODUCTION_FILES = {
    "distributions.parquet",
    "benchmark-total-returns.parquet",
}
BENCHMARK_MINIMUM_SESSIONS = 140
BENCHMARK_MINIMUM_WEEKLY_OBSERVATIONS = 26
BENCHMARK_RAW_CLOSE_RELATIVE_TOLERANCE = 1e-4
BENCHMARK_TOTAL_RETURN_RECONCILIATION_TOLERANCE = 1e-4
BENCHMARK_RETURN_COLUMNS = (
    "security_id",
    "benchmark_id",
    "session_date",
    "price_return",
    "distribution_return",
    "total_return",
    "total_return_index",
    "certification_status",
    "distribution_lineage_sha256",
    "corporate_action_lineage_sha256",
    "known_at_utc",
    "revision",
    "source_locator",
    "source_revision",
    "source_retrieved_at_utc",
)
BENCHMARK_DISTRIBUTION_COLUMNS = (
    "security_id",
    "ex_date",
    "record_date",
    "pay_date",
    "cash_amount",
    "currency",
    "distribution_type",
    "distribution_id",
    "known_at_utc",
    "revision",
    "source_locator",
    "source_revision",
    "source_publication_date",
    "source_retrieved_at_utc",
)
ACCESSION_PATTERN = re.compile(r"^[0-9]{10}-[0-9]{2}-[0-9]{6}$")


class VerificationError(RuntimeError):
    """Raised when release verification fails."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def csv_rows(path: Path) -> int:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.reader(handle)
        try:
            next(reader)
        except StopIteration:
            return 0
        return sum(1 for _ in reader)


def verify_record(
    con: duckdb.DuckDBPyConnection, directory: Path, record: dict[str, object]
) -> None:
    filename = str(record.get("file") or "")
    if not filename or Path(filename).name != filename:
        raise VerificationError(f"Unsafe or blank manifest filename: {filename!r}")
    path = directory / filename
    if not path.is_file():
        raise VerificationError(f"Missing release file: {filename}")
    if path.stat().st_size != int(record.get("bytes", -1)):
        raise VerificationError(f"Byte-size mismatch: {filename}")
    if sha256_file(path) != record.get("sha256"):
        raise VerificationError(f"SHA-256 mismatch: {filename}")

    expected_rows = int(record.get("rows", -1))
    if path.suffix == ".parquet":
        rows = int(
            con.execute("SELECT count(*) FROM read_parquet(?)", [str(path)]).fetchone()[0]
        )
        expected_schema = tuple(record.get("schema") or ())
        actual_schema = tuple(
            str(column[0])
            for column in con.execute(
                "SELECT * FROM read_parquet(?) LIMIT 0", [str(path)]
            ).description
        )
        if expected_schema and actual_schema != expected_schema:
            raise VerificationError(f"Schema mismatch: {filename}")
    elif path.suffix == ".csv":
        rows = csv_rows(path)
    elif path.suffix == ".md":
        rows = 0
    else:
        raise VerificationError(f"Unsupported release file type: {filename}")
    if rows != expected_rows:
        raise VerificationError(
            f"Row-count mismatch for {filename}: manifest={expected_rows}, actual={rows}"
        )


def _parquet_columns(
    con: duckdb.DuckDBPyConnection, path: Path
) -> tuple[tuple[str, str], ...]:
    description = con.execute(
        "SELECT * FROM read_parquet(?) LIMIT 0", [str(path)]
    ).description
    return tuple((str(column[0]), str(column[1])) for column in description)


def _count(con: duckdb.DuckDBPyConnection, sql: str, path: Path) -> int:
    return int(con.execute(sql, [str(path)]).fetchone()[0])


def insider_cik_violation_count(
    con: duckdb.DuckDBPyConnection, insider_path: Path, master_path: Path
) -> int:
    return int(
        con.execute(
            """
            SELECT count(*)
            FROM read_parquet(?) insider
            LEFT JOIN read_parquet(?) master USING (security_id)
            WHERE master.security_id IS NULL
               OR master.cik IS NULL
               OR NOT regexp_full_match(master.cik, '[0-9]{10}')
               OR lpad(regexp_replace(cast(insider.issuer_cik AS VARCHAR),
                                      '[^0-9]', '', 'g'), 10, '0') <> master.cik
            """,
            [str(insider_path), str(master_path)],
        ).fetchone()[0]
    )


def verify_identity_references(
    con: duckdb.DuckDBPyConnection, directory: Path, filenames: set[str]
) -> None:
    """Require every packaged dataset identity to join the packaged master."""
    master = directory / "security-master.parquet"
    if not master.is_file():
        raise VerificationError("Release lacks security-master.parquet")
    for filename in sorted(filenames):
        path = directory / filename
        if path.suffix != ".parquet" or filename == master.name:
            continue
        columns = {
            str(column[0])
            for column in con.execute(
                "SELECT * FROM read_parquet(?) LIMIT 0", [str(path)]
            ).description
        }
        if "security_id" not in columns:
            continue
        unknown = int(
            con.execute(
                """
                SELECT count(*)
                FROM read_parquet(?) dataset
                LEFT JOIN read_parquet(?) master USING (security_id)
                WHERE dataset.security_id IS NOT NULL
                  AND master.security_id IS NULL
                """,
                [str(path), str(master)],
            ).fetchone()[0]
        )
        if unknown:
            raise VerificationError(
                f"Unknown security_id in {filename}: {unknown} rows"
            )


def verify_benchmark_total_returns(
    con: duckdb.DuckDBPyConnection,
    directory: Path,
    manifest: dict[str, object],
) -> None:
    returns_path = directory / "benchmark-total-returns.parquet"
    distributions_path = directory / "distributions.parquet"
    present = (returns_path.is_file(), distributions_path.is_file())
    if not any(present):
        return
    if not all(present):
        raise VerificationError("Benchmark total-return lane is incomplete")
    return_columns = tuple(
        str(column[0])
        for column in con.execute(
            "SELECT * FROM read_parquet(?) LIMIT 0", [str(returns_path)]
        ).description
    )
    distribution_columns = tuple(
        str(column[0])
        for column in con.execute(
            "SELECT * FROM read_parquet(?) LIMIT 0", [str(distributions_path)]
        ).description
    )
    if return_columns != BENCHMARK_RETURN_COLUMNS:
        raise VerificationError("Benchmark total-return schema mismatch")
    if distribution_columns != BENCHMARK_DISTRIBUTION_COLUMNS:
        raise VerificationError("Benchmark distribution schema mismatch")
    stats = con.execute(
        """
        SELECT count(*), count(DISTINCT session_date),
               count(DISTINCT date_trunc('week', session_date)),
               min(session_date), max(session_date),
               count(DISTINCT security_id), count(DISTINCT benchmark_id),
               count(*) - count(DISTINCT (security_id, session_date)),
               count(*) FILTER (
                 WHERE certification_status <> 'CERTIFIED'
                    OR price_return IS NULL OR NOT isfinite(price_return)
                    OR distribution_return IS NULL OR NOT isfinite(distribution_return)
                    OR total_return IS NULL OR NOT isfinite(total_return)
                    OR total_return <= -1
                    OR abs(total_return - price_return - distribution_return) > 0.0001
                    OR total_return_index IS NULL OR NOT isfinite(total_return_index)
                    OR total_return_index <= 0
                    OR NOT regexp_full_match(distribution_lineage_sha256, '[0-9a-f]{64}')
                    OR NOT regexp_full_match(corporate_action_lineage_sha256, '[0-9a-f]{64}')
                    OR known_at_utc IS NULL OR revision IS NULL
                    OR source_locator IS NULL OR source_revision IS NULL
                    OR source_retrieved_at_utc IS NULL
               )
        FROM read_parquet(?)
        """,
        [str(returns_path)],
    ).fetchone()
    if int(stats[0] or 0) != int(stats[1] or 0):
        raise VerificationError("Benchmark total-return keys are not unique")
    if int(stats[1] or 0) < BENCHMARK_MINIMUM_SESSIONS:
        raise VerificationError(
            "Benchmark total-return history is shorter than 140 sessions"
        )
    if int(stats[2] or 0) < BENCHMARK_MINIMUM_WEEKLY_OBSERVATIONS:
        raise VerificationError(
            "Benchmark total-return history is shorter than 26 weeks"
        )
    if int(stats[5] or 0) != 1 or int(stats[6] or 0) != 1:
        raise VerificationError("Benchmark total-return lane is not single-benchmark")
    if int(stats[7] or 0) or int(stats[8] or 0):
        raise VerificationError("Benchmark total-return lane has invalid rows")
    index_errors = int(
        con.execute(
            """
            WITH ordered AS (
              SELECT security_id, session_date, total_return, total_return_index,
                     lag(total_return_index) OVER (
                       PARTITION BY security_id ORDER BY session_date
                     ) AS previous_index
              FROM read_parquet(?)
            )
            SELECT count(*) FROM ordered
            WHERE previous_index IS NOT NULL
              AND abs(total_return_index / previous_index - 1 - total_return) > 1e-10
            """,
            [str(returns_path)],
        ).fetchone()[0]
    )
    if index_errors:
        raise VerificationError("Benchmark total-return index does not compound")
    expected = str(
        (manifest.get("validation") or {}).get("expected_latest_xnys_session") or ""
    )
    market_maximum = str((manifest.get("aggregate") or {}).get("max_date") or "")
    if str(stats[4]) != expected or str(stats[4]) != market_maximum:
        raise VerificationError(
            "Benchmark total-return lane is not current with market data"
        )
    distribution_stats = con.execute(
        """
        SELECT count(*), count(*) - count(DISTINCT distribution_id),
               count(*) FILTER (
                 WHERE security_id IS NULL OR ex_date IS NULL
                    OR cash_amount IS NULL OR NOT isfinite(cash_amount)
                    OR cash_amount <= 0 OR currency <> 'USD'
                    OR distribution_type <> 'CASH_DIVIDEND'
                    OR distribution_id IS NULL OR known_at_utc IS NULL
                    OR revision IS NULL OR source_locator IS NULL
                    OR source_revision IS NULL OR source_retrieved_at_utc IS NULL
               )
        FROM read_parquet(?)
        """,
        [str(distributions_path)],
    ).fetchone()
    if int(distribution_stats[0] or 0) < 1:
        raise VerificationError("Benchmark distribution lane is empty")
    if int(distribution_stats[1] or 0) or int(distribution_stats[2] or 0):
        raise VerificationError("Benchmark distribution lane has invalid rows")
    distribution_errors = int(
        con.execute(
            """
            WITH prices AS (
              SELECT security_id, session_date, close,
                     lag(close) OVER (
                       PARTITION BY security_id ORDER BY session_date
                     ) AS previous_close
              FROM read_parquet(?)
            ), cash AS (
              SELECT security_id, ex_date, sum(cash_amount) AS cash_amount
              FROM read_parquet(?) GROUP BY security_id, ex_date
            )
            SELECT count(*)
            FROM read_parquet(?) returns
            JOIN prices USING (security_id, session_date)
            LEFT JOIN cash
              ON cash.security_id = returns.security_id
             AND cash.ex_date = returns.session_date
            WHERE prices.previous_close IS NOT NULL
              AND abs(returns.distribution_return
                      - coalesce(cash.cash_amount, 0) / prices.previous_close) > 0.0001
            """,
            [
                str(directory / "yahoo-ohlcv-320.parquet"),
                str(distributions_path),
                str(returns_path),
            ],
        ).fetchone()[0]
    )
    if distribution_errors:
        raise VerificationError(
            "Benchmark distribution returns do not reconcile to cash events"
        )
    certification = manifest.get("benchmark_total_returns")
    if (
        not isinstance(certification, dict)
        or certification.get("status") != "CERTIFIED"
    ):
        raise VerificationError("Benchmark certification metadata is missing")
    identity = con.execute(
        "SELECT min(security_id), min(benchmark_id), "
        "count(DISTINCT source_locator), count(DISTINCT source_revision) "
        "FROM read_parquet(?)",
        [str(returns_path)],
    ).fetchone()
    if (
        certification.get("certification_method")
        != "YAHOO_ADJUSTED_CLOSE_RECONCILED_TO_CANONICAL_RAW_CLOSE_AND_CASH_EVENTS"
        or certification.get("security_id") != identity[0]
        or certification.get("benchmark_id") != identity[1]
        or certification.get("currency") != "USD"
        or int(identity[2] or 0) != 1
        or int(identity[3] or 0) != 1
        or not re.fullmatch(
            r"[0-9a-f]{64}", str(certification.get("source_revision") or "")
        )
    ):
        raise VerificationError("Benchmark certification identity is invalid")
    provenance_errors = int(
        con.execute(
            """
            SELECT
              (SELECT count(*) FROM read_parquet(?)
               WHERE source_locator <> ? OR source_revision <> ? OR revision <> ?)
              +
              (SELECT count(*) FROM read_parquet(?)
               WHERE source_locator <> ? OR source_revision <> ? OR revision <> ?)
            """,
            [
                str(returns_path),
                certification.get("source_locator"),
                certification.get("source_revision"),
                certification.get("source_revision"),
                str(distributions_path),
                certification.get("source_locator"),
                certification.get("source_revision"),
                certification.get("source_revision"),
            ],
        ).fetchone()[0]
    )
    if provenance_errors:
        raise VerificationError("Benchmark row provenance does not match certification")
    expected_metadata = {
        "maximum_session": str(stats[4]),
        "expected_latest_xnys_session": expected,
        "sessions": int(stats[1]),
        "weekly_observations": int(stats[2]),
        "distributions": int(distribution_stats[0]),
    }
    if any(certification.get(key) != value for key, value in expected_metadata.items()):
        raise VerificationError(
            "Benchmark certification metadata does not match its assets"
        )
    measured_thresholds = {
        "maximum_raw_close_relative_error": (
            BENCHMARK_RAW_CLOSE_RELATIVE_TOLERANCE
        ),
        "maximum_total_return_reconciliation_error": (
            BENCHMARK_TOTAL_RETURN_RECONCILIATION_TOLERANCE
        ),
    }
    for key, maximum in measured_thresholds.items():
        value = certification.get(key)
        if (
            not isinstance(value, (int, float))
            or not math.isfinite(float(value))
            or float(value) > maximum
        ):
            raise VerificationError(f"Benchmark certification exceeds {key}")
    expected_thresholds = {
        "minimum_sessions": BENCHMARK_MINIMUM_SESSIONS,
        "minimum_weekly_observations": BENCHMARK_MINIMUM_WEEKLY_OBSERVATIONS,
        "maximum_raw_close_relative_error": (
            BENCHMARK_RAW_CLOSE_RELATIVE_TOLERANCE
        ),
        "maximum_total_return_reconciliation_error": (
            BENCHMARK_TOTAL_RETURN_RECONCILIATION_TOLERANCE
        ),
    }
    if certification.get("thresholds") != expected_thresholds:
        raise VerificationError("Benchmark certification thresholds are invalid")


def verify_enrichment(
    con: duckdb.DuckDBPyConnection,
    directory: Path,
    manifest: dict[str, object],
    *,
    require_production: bool,
) -> None:
    present = {filename for filename in CONTRACTS if (directory / filename).is_file()}
    if not present:
        if require_production:
            raise VerificationError("Production release lacks enrichment datasets")
        return
    if present != set(CONTRACTS):
        raise VerificationError(
            "Incomplete enrichment file set: "
            f"missing={sorted(set(CONTRACTS) - present)}"
        )

    master_path = directory / "security-master.parquet"
    master_ids = {
        str(row[0])
        for row in con.execute(
            "SELECT security_id FROM read_parquet(?)", [str(master_path)]
        ).fetchall()
    }
    with (directory / "security-universe.csv").open(
        newline="", encoding="utf-8-sig"
    ) as handle:
        admitted_ids = {
            str(row.get("security_id") or "")
            for row in csv.DictReader(handle)
            if str(row.get("universe_admission_status") or "").upper() in {"ADMITTED", "ADMITTED_ETF"}
        }
    if not admitted_ids <= master_ids:
        raise VerificationError(
            "Security master does not cover every admitted universe security"
        )

    for filename, contract in CONTRACTS.items():
        path = directory / filename
        actual_columns = _parquet_columns(con, path)
        if actual_columns != contract.columns:
            raise VerificationError(
                f"Versioned schema/type mismatch: {filename}; "
                f"expected={contract.columns}, actual={actual_columns}"
            )
        if _count(con, "SELECT count(*) FROM read_parquet(?)", path) == 0:
            raise VerificationError(f"Required enrichment dataset is empty: {filename}")
        key_sql = ", ".join(f'"{name}"' for name in contract.primary_key)
        if _count(
            con,
            f"SELECT count(*) FROM ("
            f"SELECT {key_sql} FROM read_parquet(?) "
            f"GROUP BY {key_sql} HAVING count(*) > 1)",
            path,
        ):
            raise VerificationError(f"Duplicate primary key: {filename}")
        if not contract.allow_null_primary_key:
            null_sql = " OR ".join(
                f'"{name}" IS NULL' for name in contract.primary_key
            )
            if _count(
                con, f"SELECT count(*) FROM read_parquet(?) WHERE {null_sql}", path
            ):
                raise VerificationError(f"Null primary-key field: {filename}")
        for name, sql_type in contract.columns:
            if sql_type == "DOUBLE" and _count(
                con,
                f'SELECT count(*) FROM read_parquet(?) '
                f'WHERE "{name}" IS NOT NULL AND NOT isfinite("{name}")',
                path,
            ):
                raise VerificationError(f"Non-finite value in {filename}:{name}")
        if _count(
            con,
            "SELECT count(*) FROM read_parquet(?) "
            "WHERE source_retrieved_at_utc IS NULL",
            path,
        ):
            raise VerificationError(f"Missing retrieval provenance: {filename}")
        if filename != "security-master.parquet":
            referenced_ids = {
                str(row[0])
                for row in con.execute(
                    "SELECT DISTINCT security_id FROM read_parquet(?) "
                    "WHERE security_id IS NOT NULL",
                    [str(path)],
                ).fetchall()
            }
            if not referenced_ids <= master_ids:
                raise VerificationError(f"Unknown security_id in {filename}")

    accession_fields = {
        "sec-company-facts.parquet": "accession_number",
        "normalized-fundamentals-quarterly.parquet": "accession_number",
        "sec-filings.parquet": "accession_number",
        "corporate-events.parquet": "accession_number",
        "insider-transactions.parquet": "accession_number",
        "institutional-holdings-13f.parquet": "accession_number",
        "earnings-and-guidance-events.parquet": "filing_accession_number",
    }
    for filename, field in accession_fields.items():
        values = con.execute(
            f'SELECT DISTINCT "{field}" FROM read_parquet(?)',
            [str(directory / filename)],
        ).fetchall()
        if any(
            value is None or ACCESSION_PATTERN.fullmatch(str(value)) is None
            for (value,) in values
        ):
            raise VerificationError(f"Invalid SEC accession number: {filename}")

    facts_path = directory / "sec-company-facts.parquet"
    if _count(
        con,
        "SELECT count(*) FROM read_parquet(?) "
        "WHERE unit NOT IN ('USD', 'shares', 'USD/shares', 'pure') "
        "AND unit_status <> 'UNSUPPORTED'",
        facts_path,
    ):
        raise VerificationError("Unsupported SEC fact unit is not marked unsupported")
    if _count(
        con,
        "SELECT count(*) FROM read_parquet(?) "
        "WHERE period_end IS NOT NULL AND filed_date < period_end",
        facts_path,
    ):
        raise VerificationError("SEC fact was filed before its period ended")
    if _count(
        con,
        "SELECT count(*) FROM read_parquet(?) "
        "WHERE filing_available_date > factor_as_of_date",
        directory / "fundamental-factors.parquet",
    ):
        raise VerificationError("Derived factor uses an unavailable filing")
    if _count(
        con,
        "SELECT count(*) FROM read_parquet(?) "
        "WHERE publication_date < settlement_date "
        "OR source_publication_date < settlement_date",
        directory / "finra-short-interest.parquet",
    ):
        raise VerificationError("Short-interest publication precedes settlement")
    if _count(
        con,
        "SELECT count(*) FROM read_parquet(?) "
        "WHERE source_publication_date < filing_date",
        directory / "institutional-holdings-13f.parquet",
    ):
        raise VerificationError("13F position is public before its filing date")
    if _count(
        con,
        "SELECT count(*) FROM read_parquet(?) "
        "WHERE filing_date IS NULL OR accession_number IS NULL",
        directory / "insider-transactions.parquet",
    ):
        raise VerificationError("Insider transaction lacks filing provenance")
    if _count(
        con,
        "SELECT count(*) FROM read_parquet(?) "
        "WHERE cik IS NOT NULL AND NOT regexp_full_match(cik, '[0-9]{10}')",
        master_path,
    ):
        raise VerificationError("Security master contains a non-canonical CIK")
    insider_cik_violations = insider_cik_violation_count(
        con,
        directory / "insider-transactions.parquet",
        master_path,
    )
    if insider_cik_violations:
        raise VerificationError(
            "Insider canonical-CIK join contains "
            f"{insider_cik_violations} violations"
        )
    if _count(
        con,
        "SELECT count(*) FROM read_parquet(?) "
        "WHERE accession_number IS NULL OR source_document_url IS NULL",
        directory / "corporate-events.parquet",
    ):
        raise VerificationError("Corporate event lacks a traceable source filing")

    datasets = manifest.get("datasets")
    if not isinstance(datasets, list):
        raise VerificationError("Manifest has no dataset entries")
    by_path: dict[str, dict[str, object]] = {}
    for raw in datasets:
        if not isinstance(raw, dict):
            raise VerificationError("Manifest datasets contains a non-object")
        filename = str(raw.get("path") or "")
        if filename in by_path:
            raise VerificationError(f"Duplicate manifest dataset entry: {filename}")
        by_path[filename] = raw
    release_records = {
        str(raw.get("file")): raw
        for raw in manifest.get("release_files", [])
        if isinstance(raw, dict) and raw.get("file") != "NOTICE.md"
    }
    if set(by_path) != set(release_records):
        raise VerificationError("Manifest dataset entries do not cover every data asset")
    for filename, record in by_path.items():
        release = release_records[filename]
        expected = {
            "row_count": int(release.get("rows", -1)),
            "byte_size": int(release.get("bytes", -1)),
            "sha256": str(release.get("sha256") or ""),
        }
        if any(record.get(key) != value for key, value in expected.items()):
            raise VerificationError(f"Manifest dataset metadata mismatch: {filename}")
        if record.get("point_in_time_safe") is not True:
            raise VerificationError(f"Dataset is not marked point-in-time safe: {filename}")

    coverage = manifest.get("coverage")
    if not isinstance(coverage, dict):
        raise VerificationError("Manifest enrichment coverage is missing")
    for name in (
        "fundamentals",
        "filings_and_events",
        "insider_transactions",
        "institutional_ownership",
        "short_interest",
    ):
        section = coverage.get(name)
        if not isinstance(section, dict) or section.get("status") != "READY":
            raise VerificationError(f"Enrichment coverage is not READY: {name}")
    analyst = coverage.get("analyst_estimates")
    if not isinstance(analyst, dict) or analyst.get("status") != "NOT_CONFIGURED":
        raise VerificationError("Unexpected analyst-estimates status")


def verify_dataset_groups(
    con: duckdb.DuckDBPyConnection,
    directory: Path,
    manifest: dict[str, object],
    *,
    require_production: bool,
) -> None:
    raw_datasets = manifest.get("datasets")
    raw_groups = manifest.get("dataset_groups")
    if not isinstance(raw_groups, list):
        if require_production:
            raise VerificationError("Production manifest has no dataset_groups")
        return
    if not isinstance(raw_datasets, list):
        raise VerificationError("Grouped manifest has no datasets")
    datasets: dict[str, dict[str, object]] = {}
    for raw in raw_datasets:
        if not isinstance(raw, dict):
            raise VerificationError("datasets contains a non-object")
        filename = str(raw.get("path") or "")
        if filename in datasets:
            raise VerificationError(f"Duplicate dataset identity: {filename}")
        datasets[filename] = raw
        expected_group = FILE_TO_GROUP.get(filename)
        if expected_group is None or raw.get("group_id") != expected_group:
            raise VerificationError(f"Invalid dataset group assignment: {filename}")
        path = directory / filename
        if not path.is_file():
            raise VerificationError(f"Grouped dataset file is missing: {filename}")
        if raw.get("logical_columns") != [
            item.get("name")
            for item in raw.get("physical_schema", [])
            if isinstance(item, dict)
        ]:
            raise VerificationError(f"Logical/physical schema mismatch: {filename}")
        actual = _parquet_columns(con, path) if path.suffix == ".parquet" else None
        if actual is not None and raw.get("physical_schema") != [
            {"name": name, "type": sql_type} for name, sql_type in actual
        ]:
            raise VerificationError(f"Physical schema mismatch: {filename}")
        if not isinstance(raw.get("nullability_policy"), dict):
            raise VerificationError(f"Missing nullability policy: {filename}")
        required_dataset_fields = {
            "schema_version",
            "sha256",
            "byte_size",
            "row_count",
            "primary_key",
            "source_name",
            "immutable_source_revision",
            "source_retrieval_time",
            "point_in_time_safe",
            "validation_result",
        }
        if not required_dataset_fields <= set(raw):
            raise VerificationError(f"Incomplete dataset identity: {filename}")
        if raw.get("validation_result") != "PASS":
            raise VerificationError(f"Dataset validation did not pass: {filename}")

    groups: dict[str, dict[str, object]] = {}
    for raw in raw_groups:
        if not isinstance(raw, dict):
            raise VerificationError("dataset_groups contains a non-object")
        group_id = str(raw.get("group_id") or "")
        if group_id in groups or group_id not in GROUPS:
            raise VerificationError(f"Unknown or duplicate dataset group: {group_id}")
        groups[group_id] = raw
    if set(groups) != set(GROUPS):
        raise VerificationError("dataset_groups does not cover the group contract")

    for group_id, contract in GROUPS.items():
        record = groups[group_id]
        state = str(record.get("state") or "")
        if record.get("group_contract_version") != GROUP_CONTRACT_VERSION:
            raise VerificationError(f"Unsupported group contract: {group_id}")
        if state not in GROUP_STATES:
            raise VerificationError(f"Invalid group state: {group_id}:{state}")
        expected_files = [] if state == "NOT_CONFIGURED" else sorted(contract.files)
        if record.get("files") != expected_files:
            raise VerificationError(f"Group file set mismatch: {group_id}")
        if state == "NOT_CONFIGURED":
            if not contract.optional or record.get("group_sha256") is not None:
                raise VerificationError(f"Required group is NOT_CONFIGURED: {group_id}")
            continue
        identities = group_file_identities(contract, datasets)
        actual_digest = group_sha256(identities)
        if record.get("group_sha256") != actual_digest:
            raise VerificationError(f"Group digest mismatch: {group_id}")
        for key in (
            "freshness",
            "validation_errors",
            "validation_warnings",
            "exclusions",
            "dependencies",
            "dependency_group_identities",
        ):
            if key not in record:
                raise VerificationError(f"Group {group_id} lacks {key}")
        if record.get("dependencies") != list(contract.dependencies):
            raise VerificationError(f"Group dependencies mismatch: {group_id}")
        freshness = record.get("freshness")
        if not isinstance(freshness, dict) or not {
            "expected",
            "observed",
            "state",
        } <= set(freshness):
            raise VerificationError(f"Group freshness is incomplete: {group_id}")
        if state == "READY_REUSED":
            for key in (
                "source_release_tag",
                "source_manifest_sha256",
                "source_group_sha256",
            ):
                if not record.get(key):
                    raise VerificationError(f"Reused group lacks {key}: {group_id}")
            if record.get("source_group_sha256") != actual_digest:
                raise VerificationError(f"Reused group bytes changed: {group_id}")
        if "GROUP_REUSE" in str(record.get("mode") or ""):
            if record.get("source_release_immutable") is not True:
                raise VerificationError(
                    f"Reused group lacks immutable release identity: {group_id}"
                )
            for filename in contract.files:
                dataset = datasets[filename]
                for key in (
                    "source_release_tag",
                    "source_manifest_sha256",
                    "source_group_sha256",
                ):
                    if not dataset.get(key):
                        raise VerificationError(
                            f"Reused dataset lacks {key}: {filename}"
                        )
                if dataset.get("source_release_immutable") is not True:
                    raise VerificationError(
                        f"Reused dataset lacks immutable source release: {filename}"
                    )
                if dataset.get("source_release_tag") != record.get("source_release_tag"):
                    raise VerificationError(
                        f"Reused dataset release identity mismatch: {filename}"
                    )
                if dataset.get("source_manifest_sha256") != record.get("source_manifest_sha256"):
                    raise VerificationError(
                        f"Reused dataset manifest identity mismatch: {filename}"
                    )
                if dataset.get("source_group_sha256") != record.get("source_group_sha256"):
                    raise VerificationError(
                        f"Reused dataset group identity mismatch: {filename}"
                    )
        if group_id == "market":
            lag = freshness.get("lag_eligible_sessions")
            if state == "READY_NEW":
                if lag != 0 or float(
                    (manifest.get("validation") or {}).get(
                        "latest_session_coverage", 0.0
                    )
                ) < 0.99:
                    raise VerificationError(
                        "READY_NEW market group misses freshness/coverage threshold"
                    )
                if require_production and (
                    (manifest.get("validation") or {}).get("benchmark_valid") is not True
                ):
                    raise VerificationError("READY_NEW market benchmark is invalid")
            elif state == "READY_WITH_EXCLUSIONS":
                coverage = float(
                    (manifest.get("validation") or {}).get(
                        "latest_session_coverage", 0.0
                    )
                )
                if lag != 0 or coverage < 0.95 or not record.get("exclusions"):
                    raise VerificationError(
                        "READY_WITH_EXCLUSIONS market group violates threshold"
                    )
                if require_production and (
                    (manifest.get("validation") or {}).get("benchmark_valid") is not True
                ):
                    raise VerificationError(
                        "READY_WITH_EXCLUSIONS market benchmark is invalid"
                    )
            elif state == "READY_REUSED" and (
                not isinstance(lag, int) or lag > MARKET_READY_MAX_LAG_SESSIONS
            ):
                raise VerificationError("READY_REUSED market group is too stale")
            if state == "READY_REUSED" and float(
                (manifest.get("validation") or {}).get(
                    "latest_session_coverage", 0.0
                )
            ) < 0.95:
                raise VerificationError(
                    "READY_REUSED market group misses coverage threshold"
                )
        if state in {"READY_NEW", "READY_REUSED", "READY_WITH_EXCLUSIONS"}:
            if record.get("validation_errors"):
                raise VerificationError(f"Usable group has validation errors: {group_id}")
        if group_id in CORE_GROUPS and state not in {
            "READY_NEW",
            "READY_REUSED",
            "READY_WITH_EXCLUSIONS",
        }:
            raise VerificationError(f"Core group is not usable: {group_id}")

    for group_id, record in groups.items():
        if record.get("state") not in {
            "READY_NEW",
            "READY_REUSED",
            "READY_WITH_EXCLUSIONS",
        }:
            continue
        dependency_ids = record.get("dependency_group_identities")
        if not isinstance(dependency_ids, dict):
            raise VerificationError(f"Dependency identities are invalid: {group_id}")
        for dependency in GROUPS[group_id].dependencies:
            dependency_record = groups[dependency]
            if dependency_record.get("state") not in {
                "READY_NEW",
                "READY_REUSED",
                "READY_WITH_EXCLUSIONS",
            }:
                raise VerificationError(
                    f"Usable group {group_id} has disabled dependency {dependency}"
                )
            if dependency_ids.get(dependency) != dependency_record.get("group_sha256"):
                raise VerificationError(
                    f"Dependency identity mismatch: {group_id}->{dependency}"
                )

    failures = manifest.get("candidate_group_failures")
    if not isinstance(failures, list):
        raise VerificationError("Manifest lacks candidate_group_failures")
    for failure in failures:
        if (
            not isinstance(failure, dict)
            or failure.get("state") != "QUARANTINED"
            or failure.get("group_id") not in GROUPS
            or not isinstance(failure.get("diagnostics"), list)
        ):
            raise VerificationError("Invalid quarantined candidate diagnostics")
    if require_production and failures:
        raise VerificationError(
            "Production manifest mixes released readiness with legacy quarantine state"
        )

    attempts = manifest.get("candidate_attempt_failures")
    if not isinstance(attempts, list):
        raise VerificationError("Manifest lacks candidate_attempt_failures")
    seen_attempts: set[str] = set()
    usable = {"READY_NEW", "READY_REUSED", "READY_WITH_EXCLUSIONS"}
    for attempt in attempts:
        if not isinstance(attempt, dict):
            raise VerificationError("candidate_attempt_failures contains a non-object")
        group_id = str(attempt.get("group_id") or "")
        if (
            group_id not in GROUPS
            or group_id in seen_attempts
            or attempt.get("attempt_state") != "QUARANTINED"
            or not isinstance(attempt.get("diagnostics"), list)
            or attempt.get("released_state") != groups[group_id].get("state")
        ):
            raise VerificationError("Invalid candidate-attempt diagnostics")
        seen_attempts.add(group_id)
        if group_id in CORE_GROUPS and attempt.get("released_state") not in usable:
            raise VerificationError(f"Unresolved core-group quarantine: {group_id}")


def verify(directory: Path, require_ready: bool, require_production: bool) -> dict[str, object]:
    manifest_path = directory / "manifest.json"
    if not manifest_path.is_file():
        raise VerificationError("manifest.json is missing")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise VerificationError(f"Invalid manifest JSON: {exc}") from exc
    if manifest.get("schema_version") != SCHEMA_VERSION:
        raise VerificationError("Unsupported manifest schema_version")
    if require_ready and manifest.get("status") != "READY":
        raise VerificationError(f"Manifest status is {manifest.get('status')}, not READY")
    records = manifest.get("release_files")
    if not isinstance(records, list) or not records:
        raise VerificationError("Manifest has no release_files")

    filenames = {str(record.get("file")) for record in records if isinstance(record, dict)}
    if len(filenames) != len(records):
        raise VerificationError("release_files contains duplicate filenames")
    if require_production:
        missing = PRODUCTION_FILES - filenames
        unexpected = filenames - PRODUCTION_FILES - OPTIONAL_PRODUCTION_FILES
        if missing or unexpected:
            raise VerificationError(
                "Production file set mismatch: "
                f"missing={sorted(missing)}, unexpected={sorted(unexpected)}"
            )

    con = duckdb.connect()
    try:
        for record in records:
            if not isinstance(record, dict):
                raise VerificationError("release_files contains a non-object entry")
            verify_record(con, directory, record)
        if (directory / "security-master.parquet").is_file():
            verify_identity_references(con, directory, filenames)
        elif require_production:
            raise VerificationError("Production release lacks security-master.parquet")
        verify_benchmark_total_returns(con, directory, manifest)
        verify_enrichment(
            con, directory, manifest, require_production=require_production
        )
        verify_dataset_groups(
            con, directory, manifest, require_production=require_production
        )
    finally:
        con.close()
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dist", required=True)
    parser.add_argument("--require-ready", action="store_true")
    parser.add_argument("--require-production", action="store_true")
    args = parser.parse_args(argv)
    try:
        manifest = verify(
            Path(args.dist).resolve(), args.require_ready, args.require_production
        )
    except (
        VerificationError,
        ReliabilityContractError,
        OSError,
        duckdb.Error,
        ValueError,
    ) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(
        json.dumps(
            {
                "status": manifest["status"],
                "schema_version": manifest["schema_version"],
                "verified_files": len(manifest["release_files"]),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
