#!/usr/bin/env python3
"""Verify a release directory against its manifest before publication or import."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from pathlib import Path

import duckdb


SCHEMA_VERSION = "1.0.0"
PRODUCTION_FILES = {
    "yahoo-ohlcv-320.parquet",
    "yahoo-splits.parquet",
    "security-universe.csv",
    "unmatched-tickers.csv",
}


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
    else:
        raise VerificationError(f"Unsupported release file type: {filename}")
    if rows != expected_rows:
        raise VerificationError(
            f"Row-count mismatch for {filename}: manifest={expected_rows}, actual={rows}"
        )


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
    except (VerificationError, OSError, duckdb.Error, ValueError) as exc:
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
