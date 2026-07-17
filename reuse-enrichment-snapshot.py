#!/usr/bin/env python3
"""Attach a validated immutable enrichment snapshot to a fresh market build."""

from __future__ import annotations

import argparse
import copy
import csv
import json
import os
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path

import duckdb

from enrichment_contract import (
    CONTRACTS,
    SCHEMA_VERSION,
    dataset_record,
    release_file_record,
    sha256_file,
    write_parquet,
)


LEGACY_PRODUCTION_FILES = {
    "security-universe.csv",
    "unmatched-tickers.csv",
    "yahoo-ohlcv-320.parquet",
    "yahoo-splits.parquet",
}
TAG_PATTERN = re.compile(r"market-data-[0-9]{8}T[0-9]{6}Z")
NASDAQ_SOURCE_URL = "https://www.nasdaqtrader.com/trader.aspx?id=symboldirdefs"


class SnapshotReuseError(RuntimeError):
    """Raised when an enrichment snapshot cannot be reused safely."""


def load_manifest(path: Path, label: str) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        raise SnapshotReuseError(f"Cannot read {label} manifest: {exc}") from exc
    if not isinstance(value, dict):
        raise SnapshotReuseError(f"{label} manifest root is not an object")
    if value.get("schema_version") != SCHEMA_VERSION:
        raise SnapshotReuseError(f"{label} manifest schema is not {SCHEMA_VERSION}")
    if value.get("status") != "READY":
        raise SnapshotReuseError(f"{label} manifest is not READY")
    return value


def records_by_name(
    records: object, key: str, label: str
) -> dict[str, dict[str, object]]:
    if not isinstance(records, list):
        raise SnapshotReuseError(f"{label} is not a list")
    output: dict[str, dict[str, object]] = {}
    for record in records:
        if not isinstance(record, dict):
            raise SnapshotReuseError(f"{label} contains a non-object")
        name = str(record.get(key) or "")
        if not name or Path(name).name != name:
            raise SnapshotReuseError(f"{label} contains an unsafe filename: {name!r}")
        if name in output:
            raise SnapshotReuseError(f"{label} contains duplicate filename: {name}")
        output[name] = record
    return output


def verify_record_file(
    directory: Path, filename: str, record: dict[str, object]
) -> Path:
    path = directory / filename
    if path.is_symlink() or not path.is_file():
        raise SnapshotReuseError(f"Snapshot asset is not a regular file: {filename}")
    if path.stat().st_size != int(record.get("bytes", -1)):
        raise SnapshotReuseError(f"Snapshot byte-size mismatch: {filename}")
    if sha256_file(path) != record.get("sha256"):
        raise SnapshotReuseError(f"Snapshot SHA-256 mismatch: {filename}")
    return path


def validate_previous_snapshot(
    directory: Path, manifest: dict[str, object]
) -> tuple[dict[str, dict[str, object]], dict[str, dict[str, object]]]:
    release = records_by_name(manifest.get("release_files"), "file", "release_files")
    required = set(CONTRACTS) | {"NOTICE.md"}
    missing = required - set(release)
    if missing:
        raise SnapshotReuseError(
            f"Previous release lacks enrichment assets: {sorted(missing)}"
        )
    datasets = records_by_name(manifest.get("datasets"), "path", "datasets")
    missing_datasets = set(CONTRACTS) - set(datasets)
    if missing_datasets:
        raise SnapshotReuseError(
            f"Previous manifest lacks enrichment datasets: {sorted(missing_datasets)}"
        )

    con = duckdb.connect()
    try:
        for filename, contract in CONTRACTS.items():
            record = release[filename]
            path = verify_record_file(directory, filename, record)
            rows = int(
                con.execute(
                    "SELECT count(*) FROM read_parquet(?)", [str(path)]
                ).fetchone()[0]
            )
            if rows != int(record.get("rows", -1)):
                raise SnapshotReuseError(f"Snapshot row-count mismatch: {filename}")
            actual = tuple(
                (str(column[0]), str(column[1]))
                for column in con.execute(
                    "SELECT * FROM read_parquet(?) LIMIT 0", [str(path)]
                ).description
            )
            if actual != contract.columns:
                raise SnapshotReuseError(f"Snapshot contract mismatch: {filename}")
            dataset = datasets[filename]
            expected = {
                "row_count": rows,
                "byte_size": path.stat().st_size,
                "sha256": str(record.get("sha256") or ""),
            }
            if any(dataset.get(key) != value for key, value in expected.items()):
                raise SnapshotReuseError(
                    f"Snapshot dataset metadata mismatch: {filename}"
                )
            if dataset.get("point_in_time_safe") is not True:
                raise SnapshotReuseError(f"Snapshot is not point-in-time safe: {filename}")
    finally:
        con.close()
    verify_record_file(directory, "NOTICE.md", release["NOTICE.md"])
    return release, datasets


def validate_current_market(
    directory: Path, manifest: dict[str, object]
) -> dict[str, dict[str, object]]:
    release = records_by_name(manifest.get("release_files"), "file", "release_files")
    if set(release) != LEGACY_PRODUCTION_FILES:
        raise SnapshotReuseError(
            "Fresh market manifest file set is not the four production market assets"
        )
    for filename, record in release.items():
        verify_record_file(directory, filename, record)
    return release


def parse_retrieved_at(value: object) -> datetime:
    text = str(value or "")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SnapshotReuseError("Fresh manifest has an invalid created_at_utc") from exc
    if parsed.tzinfo is None:
        raise SnapshotReuseError("Fresh manifest created_at_utc has no timezone")
    return parsed


def admitted_securities(path: Path) -> dict[str, dict[str, str]]:
    try:
        handle = path.open(newline="", encoding="utf-8-sig")
    except OSError as exc:
        raise SnapshotReuseError(f"Cannot read fresh security universe: {exc}") from exc
    with handle:
        reader = csv.DictReader(handle)
        required = {"security_id", "ticker", "exchange_mic", "universe_admission_status"}
        if not reader.fieldnames or not required <= set(reader.fieldnames):
            raise SnapshotReuseError("Fresh security universe schema is incomplete")
        output: dict[str, dict[str, str]] = {}
        for row in reader:
            if str(row.get("universe_admission_status") or "").upper() != "ADMITTED":
                continue
            security_id = str(row.get("security_id") or "")
            if not security_id or security_id in output:
                raise SnapshotReuseError(
                    f"Blank or duplicate admitted security_id: {security_id!r}"
                )
            output[security_id] = {
                "ticker": str(row.get("ticker") or ""),
                "exchange_mic": str(row.get("exchange_mic") or ""),
            }
    if not output:
        raise SnapshotReuseError("Fresh security universe has no admitted securities")
    return output


def read_master(path: Path) -> list[dict[str, object]]:
    contract = CONTRACTS["security-master.parquet"]
    columns = tuple(name for name, _ in contract.columns)
    con = duckdb.connect()
    try:
        description = con.execute(
            "SELECT * FROM read_parquet(?) LIMIT 0", [str(path)]
        ).description
        if tuple(str(column[0]) for column in description) != columns:
            raise SnapshotReuseError("Previous security-master columns are invalid")
        values = con.execute("SELECT * FROM read_parquet(?)", [str(path)]).fetchall()
    finally:
        con.close()
    return [dict(zip(columns, row)) for row in values]


def copy_atomic(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + ".snapshot.tmp")
    shutil.copyfile(source, temporary)
    os.replace(temporary, target)


def legacy_dataset_records(
    manifest: dict[str, object], release: dict[str, dict[str, object]]
) -> list[dict[str, object]]:
    primary_keys = {
        "yahoo-ohlcv-320.parquet": ["security_id", "session_date"],
        "yahoo-splits.parquet": ["security_id", "event_date", "split_factor"],
        "security-universe.csv": ["security_id"],
        "unmatched-tickers.csv": ["ticker"],
    }
    sources = {
        "yahoo-ohlcv-320.parquet": "defeatbeta/yahoo-finance-data",
        "yahoo-splits.parquet": "defeatbeta/yahoo-finance-data",
        "security-universe.csv": "Nasdaq Trader Symbol Directory",
        "unmatched-tickers.csv": "Derived identifier reconciliation",
    }
    created_at = str(manifest.get("created_at_utc") or "")
    revision = str((manifest.get("source") or {}).get("revision") or "")
    minimum = (manifest.get("aggregate") or {}).get("min_date")
    maximum = (manifest.get("aggregate") or {}).get("max_date")
    output = []
    for filename in (
        "yahoo-ohlcv-320.parquet",
        "yahoo-splits.parquet",
        "security-universe.csv",
        "unmatched-tickers.csv",
    ):
        record = release[filename]
        output.append(
            {
                "path": filename,
                "schema_version": str(manifest.get("schema_version") or ""),
                "row_count": int(record.get("rows", 0)),
                "byte_size": int(record.get("bytes", 0)),
                "sha256": str(record.get("sha256") or ""),
                "primary_key": primary_keys[filename],
                "source": sources[filename],
                "source_revision": revision,
                "source_retrieved_at_utc": created_at,
                "minimum_event_date": minimum,
                "maximum_event_date": maximum,
                "point_in_time_safe": True,
            }
        )
    return output


def reuse_snapshot(
    current_manifest_path: Path,
    previous_directory: Path,
    out_dir: Path,
    source_tag: str,
) -> dict[str, object]:
    if TAG_PATTERN.fullmatch(source_tag) is None:
        raise SnapshotReuseError(f"Invalid source release tag: {source_tag!r}")
    current = load_manifest(current_manifest_path, "fresh market")
    previous_manifest_path = previous_directory / "manifest.json"
    previous_manifest_sha256 = sha256_file(previous_manifest_path)
    previous = load_manifest(previous_manifest_path, "previous release")
    current_release = validate_current_market(out_dir, current)
    previous_release, previous_datasets = validate_previous_snapshot(
        previous_directory, previous
    )
    coverage = previous.get("coverage")
    if not isinstance(coverage, dict):
        raise SnapshotReuseError("Previous manifest has no enrichment coverage")

    for filename in CONTRACTS:
        if filename == "security-master.parquet":
            continue
        copy_atomic(previous_directory / filename, out_dir / filename)
    copy_atomic(previous_directory / "NOTICE.md", out_dir / "NOTICE.md")

    admitted = admitted_securities(out_dir / "security-universe.csv")
    master_rows = read_master(previous_directory / "security-master.parquet")
    master_ids = {str(row["security_id"]) for row in master_rows}
    retrieved_at = parse_retrieved_at(current.get("created_at_utc"))
    universe_sha = str(current_release["security-universe.csv"].get("sha256") or "")
    new_security_ids = sorted(set(admitted) - master_ids)
    for security_id in new_security_ids:
        security = admitted[security_id]
        master_rows.append(
            {
                "security_id": security_id,
                "ticker": security["ticker"],
                "exchange_mic": security["exchange_mic"],
                "cik": None,
                "registrant_name": None,
                "sic": None,
                "mapping_status": "UNMAPPED_DAILY_ADMISSION",
                "source_url": NASDAQ_SOURCE_URL,
                "source_revision": f"nasdaq-symbol-directory:{universe_sha}",
                "source_event_date": None,
                "source_filing_date": None,
                "source_acceptance_datetime_utc": None,
                "source_publication_date": None,
                "source_retrieved_at_utc": retrieved_at,
            }
        )

    master_path = out_dir / "security-master.parquet"
    master_contract = CONTRACTS[master_path.name]
    if new_security_ids:
        master_rows_count = write_parquet(master_path, master_contract, master_rows)
        previous_revision = str(
            previous_datasets[master_path.name].get("source_revision") or ""
        )
        master_dataset = dataset_record(
            master_path,
            master_contract,
            rows=master_rows_count,
            source_revision=(
                f"{previous_revision}+daily-universe:{universe_sha}"
            ),
            source_retrieved_at_utc=str(current.get("created_at_utc") or ""),
        )
        master_release = release_file_record(
            master_path, master_contract, master_rows_count
        )
    else:
        copy_atomic(previous_directory / master_path.name, master_path)
        master_rows_count = len(master_rows)
        master_dataset = copy.deepcopy(previous_datasets[master_path.name])
        master_release = copy.deepcopy(previous_release[master_path.name])

    enrichment_release: list[dict[str, object]] = []
    enrichment_datasets: list[dict[str, object]] = []
    for filename in CONTRACTS:
        if filename == master_path.name:
            enrichment_release.append(master_release)
            enrichment_datasets.append(master_dataset)
        else:
            enrichment_release.append(copy.deepcopy(previous_release[filename]))
            enrichment_datasets.append(copy.deepcopy(previous_datasets[filename]))

    current["release_files"] = (
        [copy.deepcopy(record) for record in current_release.values()]
        + enrichment_release
        + [copy.deepcopy(previous_release["NOTICE.md"])]
    )
    current["datasets"] = legacy_dataset_records(current, current_release) + enrichment_datasets
    current["coverage"] = copy.deepcopy(coverage)
    master_coverage = current["coverage"].setdefault("security_master", {})
    if not isinstance(master_coverage, dict):
        raise SnapshotReuseError("Previous security-master coverage is invalid")
    unresolved_daily_admissions = sum(
        row.get("mapping_status") == "UNMAPPED_DAILY_ADMISSION"
        for row in master_rows
    )
    master_coverage.update(
        {
            "status": "READY",
            "security_count": master_rows_count,
            "sec_cik_mapped_count": sum(row.get("cik") is not None for row in master_rows),
            "current_admitted_count": len(admitted),
            "historical_security_count": len(master_ids - set(admitted)),
            "new_unmapped_admissions_count": len(new_security_ids),
            "unmapped_daily_admission_count": unresolved_daily_admissions,
        }
    )
    snapshot = {
        "mode": "REUSED_VALIDATED_IMMUTABLE_RELEASE",
        "source_release_tag": source_tag,
        "source_manifest_sha256": previous_manifest_sha256,
        "source_created_at_utc": previous.get("created_at_utc"),
        "reused_at_utc": current.get("created_at_utc"),
        "new_unmapped_admissions_count": len(new_security_ids),
    }
    current["enrichment_snapshot"] = snapshot
    source = current.setdefault("source", {})
    if not isinstance(source, dict):
        raise SnapshotReuseError("Fresh manifest source is invalid")
    source["enrichment_snapshot"] = copy.deepcopy(snapshot)
    warnings = current.setdefault("validation", {}).setdefault("warnings", [])
    if not isinstance(warnings, list):
        raise SnapshotReuseError("Fresh manifest validation warnings is invalid")
    warning = (
        f"Enrichment assets reuse validated immutable release {source_tag}; "
        "each dataset retains its original source retrieval time"
    )
    if warning not in warnings:
        warnings.append(warning)
    if unresolved_daily_admissions:
        warnings.append(
            f"{unresolved_daily_admissions} admissions await SEC mapping at the next "
            "explicit enrichment refresh"
        )

    temporary = current_manifest_path.with_suffix(".json.snapshot.tmp")
    temporary.write_text(
        json.dumps(current, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, current_manifest_path)
    return current


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--current-manifest", required=True)
    parser.add_argument("--previous-release", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--source-tag", required=True)
    args = parser.parse_args(argv)
    try:
        manifest = reuse_snapshot(
            Path(args.current_manifest).resolve(),
            Path(args.previous_release).resolve(),
            Path(args.out_dir).resolve(),
            args.source_tag,
        )
    except (SnapshotReuseError, OSError, ValueError, duckdb.Error) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(
        json.dumps(
            {
                "enrichment_snapshot": manifest["enrichment_snapshot"],
                "status": manifest["status"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
