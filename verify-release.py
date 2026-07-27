#!/usr/bin/env python3
"""Verify a release directory against its manifest before publication or import."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
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
                not isinstance(lag, int) or lag > 1
            ):
                raise VerificationError("READY_REUSED market group is too stale")
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
    if require_production and filenames != PRODUCTION_FILES:
        raise VerificationError(
            f"Production file set mismatch: expected={sorted(PRODUCTION_FILES)}, "
            f"actual={sorted(filenames)}"
        )

    con = duckdb.connect()
    try:
        for record in records:
            if not isinstance(record, dict):
                raise VerificationError("release_files contains a non-object entry")
            verify_record(con, directory, record)
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
