"""Additive producer reliability contract for dataset-level and group identities."""

from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Mapping, Sequence

import duckdb

from enrichment_contract import CONTRACTS, SCHEMA_VERSION, sha256_file


GROUP_CONTRACT_VERSION = "1.0.0"
MARKET_READY_MAX_LAG_SESSIONS = 2
GROUP_STATES = {
    "READY_NEW",
    "READY_REUSED",
    "READY_WITH_EXCLUSIONS",
    "STALE_DISABLED",
    "NOT_CONFIGURED",
}
CORE_GROUPS = {"identity", "market"}


@dataclass(frozen=True)
class GroupContract:
    group_id: str
    files: tuple[str, ...]
    dependencies: tuple[str, ...] = ()
    optional: bool = False


GROUPS: dict[str, GroupContract] = {
    "identity": GroupContract(
        "identity",
        ("security-universe.csv", "security-master.parquet", "unmatched-tickers.csv"),
    ),
    "market": GroupContract(
        "market",
        ("yahoo-ohlcv-320.parquet", "yahoo-splits.parquet"),
        ("identity",),
    ),
    "fundamentals": GroupContract(
        "fundamentals",
        (
            "sec-company-facts.parquet",
            "normalized-fundamentals-quarterly.parquet",
            "fundamental-factors.parquet",
        ),
        ("identity",),
    ),
    "filings_events": GroupContract(
        "filings_events",
        (
            "sec-filings.parquet",
            "corporate-events.parquet",
            "earnings-and-guidance-events.parquet",
        ),
        ("identity",),
    ),
    "insiders": GroupContract(
        "insiders",
        ("insider-transactions.parquet", "insider-signals.parquet"),
        ("identity", "filings_events"),
    ),
    "institutional": GroupContract(
        "institutional",
        (
            "institutional-holdings-13f.parquet",
            "institutional-ownership-signals.parquet",
        ),
        ("identity",),
    ),
    "short_interest": GroupContract(
        "short_interest",
        ("finra-short-interest.parquet",),
        ("identity",),
    ),
    "analyst_estimates": GroupContract(
        "analyst_estimates",
        ("analyst-estimates.parquet",),
        ("identity",),
        optional=True,
    ),
}
FILE_TO_GROUP = {
    filename: group.group_id for group in GROUPS.values() for filename in group.files
}

LEGACY_PRIMARY_KEYS: dict[str, tuple[str, ...]] = {
    "security-universe.csv": ("security_id",),
    "unmatched-tickers.csv": ("security_id",),
    "yahoo-ohlcv-320.parquet": ("security_id", "session_date"),
    "yahoo-splits.parquet": ("security_id", "event_date", "split_factor"),
}
LEGACY_SOURCES = {
    "security-universe.csv": "Nasdaq Trader Symbol Directory",
    "unmatched-tickers.csv": "Derived identifier reconciliation",
    "yahoo-ohlcv-320.parquet": "defeatbeta/yahoo-finance-data and Yahoo Finance Chart API",
    "yahoo-splits.parquet": "defeatbeta/yahoo-finance-data and Yahoo Finance Chart API",
}


class ReliabilityContractError(RuntimeError):
    """Raised when additive reliability metadata cannot be constructed safely."""


def canonical_json(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def normalize_cik(value: object) -> str | None:
    text = "".join(character for character in str(value or "") if character.isdigit())
    if not text or len(text) > 10:
        return None
    return text.zfill(10)


def _csv_schema(path: Path) -> tuple[tuple[str, str], ...]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        fields = csv.DictReader(handle).fieldnames
    if not fields or any(not str(field).strip() for field in fields):
        raise ReliabilityContractError(f"CSV has no valid header: {path.name}")
    return tuple((str(field), "VARCHAR") for field in fields)


def physical_schema(
    con: duckdb.DuckDBPyConnection, path: Path
) -> tuple[tuple[str, str], ...]:
    if path.suffix == ".parquet":
        description = con.execute(
            "SELECT * FROM read_parquet(?) LIMIT 0", [str(path)]
        ).description
        return tuple((str(column[0]), str(column[1])) for column in description)
    if path.suffix == ".csv":
        return _csv_schema(path)
    raise ReliabilityContractError(f"Unsupported dataset file: {path.name}")


def _row_count(con: duckdb.DuckDBPyConnection, path: Path) -> int:
    if path.suffix == ".parquet":
        return int(
            con.execute("SELECT count(*) FROM read_parquet(?)", [str(path)]).fetchone()[0]
        )
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.reader(handle)
        next(reader, None)
        return sum(1 for _ in reader)


def _release_records(manifest: Mapping[str, object]) -> dict[str, dict[str, object]]:
    raw_records = manifest.get("release_files")
    if not isinstance(raw_records, list):
        raise ReliabilityContractError("Manifest release_files is not a list")
    records: dict[str, dict[str, object]] = {}
    for raw in raw_records:
        if not isinstance(raw, dict):
            raise ReliabilityContractError("Manifest release_files contains a non-object")
        filename = str(raw.get("file") or "")
        if not filename or Path(filename).name != filename or filename in records:
            raise ReliabilityContractError(f"Unsafe or duplicate release file: {filename!r}")
        records[filename] = raw
    return records


def _existing_datasets(manifest: Mapping[str, object]) -> dict[str, dict[str, object]]:
    values = manifest.get("datasets")
    if not isinstance(values, list):
        return {}
    output: dict[str, dict[str, object]] = {}
    for value in values:
        if isinstance(value, dict) and value.get("path"):
            output[str(value["path"])] = dict(value)
    return output


def dataset_identity_records(
    manifest: Mapping[str, object], directory: Path
) -> list[dict[str, object]]:
    """Return complete additive dataset records from final on-disk bytes."""
    release = _release_records(manifest)
    existing = _existing_datasets(manifest)
    con = duckdb.connect()
    try:
        output: list[dict[str, object]] = []
        for filename in sorted(FILE_TO_GROUP):
            if filename not in release:
                if GROUPS[FILE_TO_GROUP[filename]].optional:
                    continue
                raise ReliabilityContractError(f"Missing required grouped file: {filename}")
            path = directory / filename
            if path.is_symlink() or not path.is_file():
                raise ReliabilityContractError(f"Grouped asset is not a regular file: {filename}")
            schema = physical_schema(con, path)
            rows = _row_count(con, path)
            raw = release[filename]
            if (
                int(raw.get("rows", -1)) != rows
                or int(raw.get("bytes", -1)) != path.stat().st_size
                or str(raw.get("sha256") or "") != sha256_file(path)
            ):
                raise ReliabilityContractError(f"Release identity mismatch: {filename}")
            prior = existing.get(filename, {})
            contract = CONTRACTS.get(filename)
            primary_key = (
                tuple(contract.primary_key)
                if contract is not None
                else LEGACY_PRIMARY_KEYS.get(filename, ())
            )
            nullability = {
                name: ("NULLABLE" if contract and contract.allow_null_primary_key else "NOT_NULL")
                if name in primary_key
                else "DATASET_SPECIFIC"
                for name, _ in schema
            }
            source_name = str(
                prior.get("source_name")
                or prior.get("source")
                or (contract.source if contract else LEGACY_SOURCES.get(filename, "Packaged producer data"))
            )
            source_revision = str(
                prior.get("immutable_source_revision")
                or prior.get("source_revision")
                or (manifest.get("source") or {}).get("revision")
                or ""
            )
            source_retrieved = str(
                prior.get("source_retrieval_time")
                or prior.get("source_retrieved_at_utc")
                or manifest.get("created_at_utc")
                or ""
            )
            record = dict(prior)
            record.update(
                {
                    "path": filename,
                    "group_id": FILE_TO_GROUP[filename],
                    "schema_version": str(prior.get("schema_version") or SCHEMA_VERSION),
                    "row_count": rows,
                    "byte_size": path.stat().st_size,
                    "sha256": sha256_file(path),
                    "logical_columns": [name for name, _ in schema],
                    "physical_schema": [
                        {"name": name, "type": sql_type} for name, sql_type in schema
                    ],
                    "primary_key": list(primary_key),
                    "nullability_policy": nullability,
                    "source": source_name,
                    "source_name": source_name,
                    "source_revision": source_revision,
                    "immutable_source_revision": source_revision,
                    "source_retrieved_at_utc": source_retrieved,
                    "source_retrieval_time": source_retrieved,
                    "minimum_event_date": prior.get("minimum_event_date"),
                    "maximum_event_date": prior.get("maximum_event_date"),
                    "point_in_time_safe": prior.get("point_in_time_safe", True),
                    "validation_result": "PASS",
                }
            )
            output.append(record)
        return output
    finally:
        con.close()


def group_file_identities(
    group: GroupContract, datasets: Mapping[str, Mapping[str, object]]
) -> list[dict[str, object]]:
    output = []
    for filename in sorted(group.files):
        if filename not in datasets:
            if group.optional:
                continue
            raise ReliabilityContractError(
                f"Group {group.group_id} lacks dataset identity for {filename}"
            )
        dataset = datasets[filename]
        output.append(
            {
                "path": filename,
                "sha256": dataset["sha256"],
                "bytes": dataset["byte_size"],
                "rows": dataset["row_count"],
                "physical_schema": dataset["physical_schema"],
                "primary_key": dataset["primary_key"],
            }
        )
    return output


def group_sha256(file_identities: Sequence[Mapping[str, object]]) -> str:
    return hashlib.sha256(canonical_json(list(file_identities))).hexdigest()


def _parse_timestamp(value: object) -> datetime | None:
    text = str(value or "")
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def default_freshness(
    group_id: str,
    manifest: Mapping[str, object],
    group_datasets: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    created = _parse_timestamp(manifest.get("created_at_utc"))
    if group_id == "market":
        validation = manifest.get("validation") or {}
        expected = validation.get("expected_latest_xnys_session")
        observed = (manifest.get("aggregate") or {}).get("max_date")
        lag = validation.get("missing_eligible_sessions")
        return {
            "clock": "XNYS_ELIGIBLE_SESSIONS",
            "expected": expected,
            "observed": observed,
            "lag_eligible_sessions": lag,
            "state": (
                "READY"
                if isinstance(lag, int) and lag <= MARKET_READY_MAX_LAG_SESSIONS
                else "STALE"
            ),
        }
    observed_values = [
        _parse_timestamp(item.get("source_retrieval_time")) for item in group_datasets
    ]
    observed_values = [value for value in observed_values if value is not None]
    observed = min(observed_values) if observed_values else None
    lag_hours = (
        max(0.0, (created - observed).total_seconds() / 3600)
        if created is not None and observed is not None
        else None
    )
    ready_hours = 24 if group_id == "identity" else 8 * 24
    disabled_hours = 3 * 24 if group_id == "identity" else 14 * 24
    state = (
        "UNKNOWN"
        if lag_hours is None
        else "READY"
        if lag_hours <= ready_hours
        else "REUSED_WITH_PENALTY"
        if lag_hours <= disabled_hours
        else "DISABLED"
    )
    return {
        "clock": "SOURCE_RETRIEVAL_TIME",
        "expected": manifest.get("created_at_utc"),
        "observed": observed.isoformat().replace("+00:00", "Z") if observed else None,
        "lag_hours": lag_hours,
        "lag_calendar_days": int(lag_hours // 24) if lag_hours is not None else None,
        "state": state,
    }


def apply_dataset_groups(
    manifest: dict[str, object],
    directory: Path,
    *,
    group_overrides: Mapping[str, Mapping[str, object]] | None = None,
    candidate_group_failures: Sequence[Mapping[str, object]] | None = None,
    candidate_attempt_failures: Sequence[Mapping[str, object]] | None = None,
) -> dict[str, object]:
    """Finalize additive dataset and group identities after all bytes exist."""
    datasets = dataset_identity_records(manifest, directory)
    by_path = {str(record["path"]): record for record in datasets}
    overrides = group_overrides or {}
    records: list[dict[str, object]] = []
    for group_id, group in GROUPS.items():
        present = [filename for filename in group.files if filename in by_path]
        if group.optional and not present:
            records.append(
                {
                    "group_id": group_id,
                    "group_contract_version": GROUP_CONTRACT_VERSION,
                    "state": "NOT_CONFIGURED",
                    "mode": "OPTIONAL_NOT_CONFIGURED",
                    "files": [],
                    "group_sha256": None,
                    "freshness": {
                        "clock": "NOT_CONFIGURED",
                        "expected": None,
                        "observed": None,
                        "state": "NOT_CONFIGURED",
                    },
                    "validation_errors": [],
                    "validation_warnings": [],
                    "exclusions": [],
                    "dependencies": list(group.dependencies),
                    "dependency_group_identities": {},
                }
            )
            continue
        identities = group_file_identities(group, by_path)
        override = dict(overrides.get(group_id, {}))
        state = str(override.pop("state", "READY_NEW"))
        if state not in GROUP_STATES:
            raise ReliabilityContractError(f"Invalid state for {group_id}: {state}")
        group_datasets = [by_path[filename] for filename in sorted(group.files)]
        record: dict[str, object] = {
            "group_id": group_id,
            "group_contract_version": GROUP_CONTRACT_VERSION,
            "state": state,
            "mode": str(override.pop("mode", "FRESH_CANDIDATE")),
            "files": sorted(group.files),
            "group_sha256": group_sha256(identities),
            "freshness": override.pop(
                "freshness", default_freshness(group_id, manifest, group_datasets)
            ),
            "validation_errors": override.pop("validation_errors", []),
            "validation_warnings": override.pop("validation_warnings", []),
            "exclusions": override.pop("exclusions", []),
            "dependencies": list(group.dependencies),
            "dependency_group_identities": {},
        }
        record.update(override)
        records.append(record)
    by_group = {str(record["group_id"]): record for record in records}
    for record in records:
        record["dependency_group_identities"] = {
            dependency: by_group[dependency].get("group_sha256")
            for dependency in record["dependencies"]
        }
    manifest["datasets"] = datasets
    manifest["dataset_groups"] = records
    manifest["candidate_group_failures"] = [
        dict(value) for value in (candidate_group_failures or [])
    ]
    manifest["candidate_attempt_failures"] = [
        dict(value) for value in (candidate_attempt_failures or [])
    ]
    return manifest


def freshness_state_for_reuse(
    group_id: str, freshness: Mapping[str, object]
) -> str:
    if group_id == "market":
        lag = freshness.get("lag_eligible_sessions")
        return (
            "READY_REUSED"
            if isinstance(lag, int) and lag <= MARKET_READY_MAX_LAG_SESSIONS
            else "STALE_DISABLED"
        )
    lag_hours = freshness.get("lag_hours")
    if not isinstance(lag_hours, (int, float)):
        return "STALE_DISABLED"
    limit = 3 * 24 if group_id == "identity" else 14 * 24
    return "READY_REUSED" if lag_hours <= limit else "STALE_DISABLED"


def iso_date(value: object) -> date | None:
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return None
