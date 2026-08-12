#!/usr/bin/env python3
"""Verify compact decision-support bytes, provenance, schemas, and phase packs."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Mapping

from decision_support_contract import (
    CAPABILITIES,
    DATABASE_FILENAME,
    EXPECTED_TABLES,
    MANIFEST_FILENAME,
    MAX_COMPRESSED_DATABASE_BYTES,
    PHASES,
    PHASE_PACK_DIRECTORY,
    PRIVATE_SCHEMA_TOKENS,
    OPTIONAL_SOURCE_FILES,
    RECENT_MARKET_SESSIONS,
    REQUIRED_SOURCE_FILES,
    SCHEMA_VERSION,
    sha256_file,
)


TAG_PATTERN = re.compile(r"market-data-[0-9]{8}T[0-9]{6}Z")
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")


class DecisionSupportVerificationError(RuntimeError):
    """Raised when a decision-support artifact is not safe to consume."""


def load_json(path: Path, label: str) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DecisionSupportVerificationError(f"Cannot read {label}: {exc}") from exc
    if not isinstance(value, dict):
        raise DecisionSupportVerificationError(f"{label} root is not an object")
    return value


def verify_file_record(root: Path, record: Mapping[str, object], label: str) -> Path:
    relative = record.get("path")
    if not isinstance(relative, str) or not relative or Path(relative).is_absolute():
        raise DecisionSupportVerificationError(f"Invalid {label} path")
    path = root / relative
    if path.is_symlink() or not path.is_file():
        raise DecisionSupportVerificationError(f"Missing {label}: {relative}")
    size = record.get("bytes")
    digest = record.get("sha256")
    if isinstance(size, bool) or not isinstance(size, int) or size < 0:
        raise DecisionSupportVerificationError(f"Invalid {label} byte size")
    if not isinstance(digest, str) or SHA256_PATTERN.fullmatch(digest) is None:
        raise DecisionSupportVerificationError(f"Invalid {label} SHA-256")
    if path.stat().st_size != size:
        raise DecisionSupportVerificationError(f"{label} byte-size mismatch")
    if sha256_file(path) != digest:
        raise DecisionSupportVerificationError(f"{label} SHA-256 mismatch")
    return path


def decompress_database(source: Path, target: Path) -> None:
    executable = shutil.which("zstd")
    if executable is None:
        raise DecisionSupportVerificationError("zstd executable is required")
    with target.open("wb") as output:
        result = subprocess.run(
            [executable, "-q", "-d", "-c", str(source)],
            stdout=output,
            stderr=subprocess.PIPE,
            check=False,
        )
    if result.returncode:
        raise DecisionSupportVerificationError(
            f"zstd decompression failed: {result.stderr.decode(errors='replace').strip()}"
        )


def table_names(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
    }


def verify_private_schema(connection: sqlite3.Connection) -> None:
    names: list[str] = []
    for table in table_names(connection):
        names.append(table.lower())
        names.extend(
            str(row[1]).lower()
            for row in connection.execute(f'PRAGMA table_info("{table}")')
        )
    violations = sorted(
        {token for token in PRIVATE_SCHEMA_TOKENS if token in set(names)}
    )
    if violations:
        raise DecisionSupportVerificationError(
            f"Private-state schema tokens are forbidden: {violations}"
        )


def verify_phase_pack(
    pack: Mapping[str, object], manifest: Mapping[str, object], phase_id: str
) -> None:
    contract = PHASES[phase_id]
    manifest_capabilities = {
        str(record["capability_id"]): record
        for record in manifest["capabilities"]
    }
    if pack.get("schema_version") != SCHEMA_VERSION or pack.get("phase_id") != phase_id:
        raise DecisionSupportVerificationError(f"Invalid phase-pack identity: {phase_id}")
    source_release = manifest["source_release"]
    database = manifest["database"]
    expected_identity = {
        "source_release_tag": source_release["tag"],
        "source_manifest_sha256": source_release["manifest_sha256"],
        "decision_support_database_sha256": database["sha256"],
        "private_state_owner": "CONSUMER",
        "decision_owner": "CONSUMER",
    }
    for key, value in expected_identity.items():
        if pack.get(key) != value:
            raise DecisionSupportVerificationError(
                f"Phase-pack identity mismatch for {phase_id}: {key}"
            )
    capability_lists = {
        "required_capabilities": contract.required_capabilities,
        "optional_capabilities": contract.optional_capabilities,
        "external_capabilities": contract.external_capabilities,
    }
    for field, expected_ids in capability_lists.items():
        records = pack.get(field)
        if not isinstance(records, list) or any(
            not isinstance(record, dict) for record in records
        ):
            raise DecisionSupportVerificationError(
                f"Invalid {field} in phase pack: {phase_id}"
            )
        actual_ids = tuple(str(record.get("capability_id")) for record in records)
        if actual_ids != expected_ids:
            raise DecisionSupportVerificationError(
                f"Capability contract mismatch in phase pack: {phase_id}/{field}"
            )
        for record in records:
            capability_id = str(record["capability_id"])
            if record != manifest_capabilities[capability_id]:
                raise DecisionSupportVerificationError(
                    f"Capability snapshot mismatch in phase pack: {phase_id}/{capability_id}"
                )
    required = pack["required_capabilities"]
    optional = pack["optional_capabilities"]
    unavailable_required = [
        item["capability_id"] for item in required if item.get("state") != "READY"
    ]
    unavailable_optional = [
        item["capability_id"] for item in optional if item.get("state") != "READY"
    ]
    expected_status = (
        "BLOCKED"
        if unavailable_required
        else "DEGRADED"
        if unavailable_optional
        else "READY"
    )
    if pack.get("status") != expected_status:
        raise DecisionSupportVerificationError(
            f"Incorrect phase status for {phase_id}: {pack.get('status')!r}"
        )
    if pack.get("unavailable_required") != unavailable_required:
        raise DecisionSupportVerificationError(
            f"Incorrect required-capability diagnostics for {phase_id}"
        )
    if pack.get("unavailable_optional") != unavailable_optional:
        raise DecisionSupportVerificationError(
            f"Incorrect optional-capability diagnostics for {phase_id}"
        )
    if pack.get("delivery_timezone") != "America/New_York" or pack.get(
        "delivery_targets"
    ) != list(contract.delivery_targets):
        raise DecisionSupportVerificationError(
            f"Incorrect delivery contract for {phase_id}"
        )
    expected_live_schema = SCHEMA_VERSION if contract.external_capabilities else None
    if pack.get("consumer_live_snapshot_schema") != expected_live_schema:
        raise DecisionSupportVerificationError(
            f"Incorrect live-snapshot schema reference for {phase_id}"
        )


def verify(directory: Path) -> dict[str, object]:
    root = directory.resolve()
    manifest = load_json(root / MANIFEST_FILENAME, "decision-support manifest")
    if manifest.get("schema_version") != SCHEMA_VERSION:
        raise DecisionSupportVerificationError("Unsupported decision-support schema")
    if manifest.get("status") != "READY":
        raise DecisionSupportVerificationError("Decision-support manifest is not READY")
    if manifest.get("private_state_owner") != "CONSUMER":
        raise DecisionSupportVerificationError("Private-state ownership is not consumer-only")
    if manifest.get("decision_owner") != "CONSUMER":
        raise DecisionSupportVerificationError("Decision ownership is not consumer-only")
    if manifest.get("routine_context_slo_seconds") != 60:
        raise DecisionSupportVerificationError("Routine-context SLO is not 60 seconds")
    if manifest.get("maximum_compressed_database_bytes") != MAX_COMPRESSED_DATABASE_BYTES:
        raise DecisionSupportVerificationError("Compressed database limit is inconsistent")

    source = manifest.get("source_release")
    if not isinstance(source, dict):
        raise DecisionSupportVerificationError("Missing source release identity")
    tag = source.get("tag")
    if not isinstance(tag, str) or TAG_PATTERN.fullmatch(tag) is None:
        raise DecisionSupportVerificationError("Invalid source release tag")
    source_digest = source.get("manifest_sha256")
    if not isinstance(source_digest, str) or SHA256_PATTERN.fullmatch(source_digest) is None:
        raise DecisionSupportVerificationError("Invalid source manifest SHA-256")
    if source.get("status") != "READY":
        raise DecisionSupportVerificationError("Pinned source release was not READY")

    source_files = manifest.get("source_files")
    if not isinstance(source_files, list):
        raise DecisionSupportVerificationError("Source-file identities are missing")
    source_file_names: set[str] = set()
    for record in source_files:
        if not isinstance(record, dict) or not isinstance(record.get("file"), str):
            raise DecisionSupportVerificationError("Invalid source-file identity")
        filename = str(record["file"])
        if filename in source_file_names:
            raise DecisionSupportVerificationError(f"Duplicate source-file identity: {filename}")
        if filename not in set(REQUIRED_SOURCE_FILES) | set(OPTIONAL_SOURCE_FILES):
            raise DecisionSupportVerificationError(f"Unexpected source-file identity: {filename}")
        size = record.get("bytes")
        digest = record.get("sha256")
        if (
            isinstance(size, bool)
            or not isinstance(size, int)
            or size < 0
            or not isinstance(digest, str)
            or SHA256_PATTERN.fullmatch(digest) is None
        ):
            raise DecisionSupportVerificationError(f"Invalid source-file identity: {filename}")
        source_file_names.add(filename)
    if not set(REQUIRED_SOURCE_FILES) <= source_file_names:
        raise DecisionSupportVerificationError(
            f"Missing required source-file identities: {sorted(set(REQUIRED_SOURCE_FILES) - source_file_names)}"
        )
    source_manifest_record = next(
        record for record in source_files if record["file"] == "manifest.json"
    )
    if source_manifest_record["sha256"] != source_digest:
        raise DecisionSupportVerificationError("Source manifest identity is inconsistent")

    capability_records = manifest.get("capabilities")
    if not isinstance(capability_records, list):
        raise DecisionSupportVerificationError("Capability records are missing")
    capabilities: dict[str, Mapping[str, object]] = {}
    for record in capability_records:
        if not isinstance(record, dict) or not isinstance(record.get("capability_id"), str):
            raise DecisionSupportVerificationError("Invalid capability record")
        capability_id = str(record["capability_id"])
        if capability_id in capabilities or capability_id not in CAPABILITIES:
            raise DecisionSupportVerificationError(
                f"Unexpected or duplicate capability record: {capability_id}"
            )
        if not isinstance(record.get("state"), str):
            raise DecisionSupportVerificationError(
                f"Invalid capability state: {capability_id}"
            )
        contract = CAPABILITIES[capability_id]
        if record.get("source_group") != contract.source_group:
            raise DecisionSupportVerificationError(
                f"Capability source-group mismatch: {capability_id}"
            )
        if record.get("maximum_age_seconds") != contract.maximum_age_seconds:
            raise DecisionSupportVerificationError(
                f"Capability freshness contract mismatch: {capability_id}"
            )
        capabilities[capability_id] = record
    if set(capabilities) != set(CAPABILITIES):
        raise DecisionSupportVerificationError("Capability set mismatch")

    database = manifest.get("database")
    if not isinstance(database, dict) or database.get("path") != DATABASE_FILENAME:
        raise DecisionSupportVerificationError("Invalid database identity")
    compressed = verify_file_record(root, database, "decision-support database")
    if compressed.stat().st_size > MAX_COMPRESSED_DATABASE_BYTES:
        raise DecisionSupportVerificationError("Compressed database exceeds size limit")
    uncompressed_size = database.get("uncompressed_bytes")
    uncompressed_sha = database.get("uncompressed_sha256")
    if (
        isinstance(uncompressed_size, bool)
        or not isinstance(uncompressed_size, int)
        or uncompressed_size <= 0
        or not isinstance(uncompressed_sha, str)
        or SHA256_PATTERN.fullmatch(uncompressed_sha) is None
    ):
        raise DecisionSupportVerificationError("Invalid uncompressed database identity")
    if database.get("market_data_role") != "NON_EXECUTABLE_RESEARCH_PROXY":
        raise DecisionSupportVerificationError("Market-data role disclaimer is missing")
    if database.get("price_adjustment") != "RAW_CLOSE_NOT_DIVIDEND_ADJUSTED":
        raise DecisionSupportVerificationError("Price-adjustment disclaimer is missing")

    table_records = database.get("tables")
    if not isinstance(table_records, list):
        raise DecisionSupportVerificationError("Database table records are missing")
    expected_rows: dict[str, int] = {}
    for record in table_records:
        if not isinstance(record, dict) or not isinstance(record.get("name"), str):
            raise DecisionSupportVerificationError("Invalid database table record")
        name = str(record["name"])
        rows = record.get("rows")
        if name in expected_rows or isinstance(rows, bool) or not isinstance(rows, int) or rows < 0:
            raise DecisionSupportVerificationError("Invalid database table row count")
        expected_rows[name] = rows
    if set(expected_rows) != set(EXPECTED_TABLES):
        raise DecisionSupportVerificationError(
            "Database table contract mismatch: "
            f"missing={sorted(set(EXPECTED_TABLES) - set(expected_rows))}, "
            f"unexpected={sorted(set(expected_rows) - set(EXPECTED_TABLES))}"
        )

    with tempfile.TemporaryDirectory(prefix="verify-decision-support-") as temp:
        database_path = Path(temp) / "decision-support.sqlite"
        decompress_database(compressed, database_path)
        if database_path.stat().st_size != uncompressed_size:
            raise DecisionSupportVerificationError("Uncompressed database byte-size mismatch")
        if sha256_file(database_path) != uncompressed_sha:
            raise DecisionSupportVerificationError("Uncompressed database SHA-256 mismatch")
        uri = f"file:{database_path.as_posix()}?mode=ro&immutable=1"
        connection = sqlite3.connect(uri, uri=True)
        try:
            connection.execute("PRAGMA query_only=ON")
            if connection.execute("PRAGMA quick_check").fetchone() != ("ok",):
                raise DecisionSupportVerificationError("SQLite quick_check failed")
            actual_tables = table_names(connection)
            if actual_tables != set(EXPECTED_TABLES):
                raise DecisionSupportVerificationError("SQLite table set mismatch")
            verify_private_schema(connection)
            for table, expected in expected_rows.items():
                actual = int(
                    connection.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0]
                )
                if actual != expected:
                    raise DecisionSupportVerificationError(
                        f"SQLite row-count mismatch: {table}"
                    )
            metadata = dict(connection.execute("SELECT key, value FROM metadata"))
            metadata_expected = {
                "schema_version": SCHEMA_VERSION,
                "source_release_tag": tag,
                "source_manifest_sha256": source_digest,
                "market_data_role": "NON_EXECUTABLE_RESEARCH_PROXY",
                "price_adjustment": "RAW_CLOSE_NOT_DIVIDEND_ADJUSTED",
                "private_state_owner": "CONSUMER",
                "decision_owner": "CONSUMER",
            }
            for key, value in metadata_expected.items():
                if metadata.get(key) != value:
                    raise DecisionSupportVerificationError(
                        f"SQLite metadata mismatch: {key}"
                    )
            database_capabilities = {
                str(row[0]): {
                    "capability_id": row[0],
                    "state": row[1],
                    "reason": row[2],
                    "source_group": row[3],
                    "observed_at": row[4],
                    "maximum_age_seconds": row[5],
                }
                for row in connection.execute(
                    """
                    SELECT capability_id, state, reason, source_group, observed_at,
                           maximum_age_seconds
                    FROM capability_health
                    """
                )
            }
            if database_capabilities != capabilities:
                raise DecisionSupportVerificationError(
                    "SQLite capability health does not match the manifest"
                )
            security_tables = (
                "market_snapshot",
                "market_history_recent",
                "fundamental_factors_latest",
                "corporate_events",
                "earnings_events",
                "insider_signals_latest",
                "institutional_signals_latest",
                "short_interest_latest",
                "analyst_estimates_latest",
                "distributions",
                "benchmark_total_returns",
            )
            for table in security_tables:
                orphaned = int(
                    connection.execute(
                        f"""
                        SELECT count(*) FROM "{table}" AS candidate
                        LEFT JOIN security USING (security_id)
                        WHERE security.security_id IS NULL
                        """
                    ).fetchone()[0]
                )
                if orphaned:
                    raise DecisionSupportVerificationError(
                        f"{table} has {orphaned} unknown security IDs"
                    )
            excessive_history = int(
                connection.execute(
                    """
                    SELECT count(*) FROM (
                      SELECT security_id FROM market_history_recent
                      GROUP BY security_id HAVING count(*) > ?
                    )
                    """,
                    (RECENT_MARKET_SESSIONS,),
                ).fetchone()[0]
            )
            if excessive_history:
                raise DecisionSupportVerificationError(
                    "Recent market-history window exceeds its bounded contract"
                )
            duplicate_keys = int(
                connection.execute(
                    """
                    SELECT count(*) FROM (
                      SELECT security_id, session_date
                      FROM market_history_recent
                      GROUP BY security_id, session_date HAVING count(*) > 1
                    )
                    """
                ).fetchone()[0]
            )
            if duplicate_keys:
                raise DecisionSupportVerificationError(
                    "Recent market history contains duplicate keys"
                )
        finally:
            connection.close()

    phase_records = manifest.get("phase_packs")
    if not isinstance(phase_records, list):
        raise DecisionSupportVerificationError("Phase-pack records are missing")
    by_phase: dict[str, Mapping[str, object]] = {}
    for record in phase_records:
        if not isinstance(record, dict) or not isinstance(record.get("phase_id"), str):
            raise DecisionSupportVerificationError("Invalid phase-pack record")
        phase_id = str(record["phase_id"])
        if phase_id in by_phase:
            raise DecisionSupportVerificationError(f"Duplicate phase pack: {phase_id}")
        by_phase[phase_id] = record
    if set(by_phase) != set(PHASES):
        raise DecisionSupportVerificationError("Phase-pack set mismatch")
    for phase_id, record in by_phase.items():
        expected_path = f"{PHASE_PACK_DIRECTORY}/{phase_id}.json"
        if record.get("path") != expected_path:
            raise DecisionSupportVerificationError(f"Invalid phase-pack path: {phase_id}")
        path = verify_file_record(root, record, f"phase pack {phase_id}")
        pack = load_json(path, f"phase pack {phase_id}")
        verify_phase_pack(pack, manifest, phase_id)
        if record.get("status") != pack.get("status"):
            raise DecisionSupportVerificationError(
                f"Phase-pack manifest status mismatch: {phase_id}"
            )
    actual_status_counts: dict[str, int] = {}
    for record in by_phase.values():
        status = str(record.get("status"))
        actual_status_counts[status] = actual_status_counts.get(status, 0) + 1
    if manifest.get("phase_status_counts") != actual_status_counts:
        raise DecisionSupportVerificationError("Phase status counts are inconsistent")

    expected_entries = {
        MANIFEST_FILENAME,
        DATABASE_FILENAME,
        PHASE_PACK_DIRECTORY,
    }
    actual_entries = {entry.name for entry in root.iterdir()}
    if actual_entries != expected_entries:
        raise DecisionSupportVerificationError(
            f"Artifact file-set mismatch: {sorted(actual_entries)}"
        )
    phase_entries = {entry.name for entry in (root / PHASE_PACK_DIRECTORY).iterdir()}
    if phase_entries != {f"{phase_id}.json" for phase_id in PHASES}:
        raise DecisionSupportVerificationError("Phase-pack directory contains unexpected files")
    return {
        "status": "VALID",
        "source_release_tag": tag,
        "database_bytes": compressed.stat().st_size,
        "phase_packs": len(PHASES),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dist", required=True)
    args = parser.parse_args(argv)
    try:
        result = verify(Path(args.dist))
    except (DecisionSupportVerificationError, OSError, sqlite3.Error) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
