#!/usr/bin/env python3
"""Verify compact decision-support bytes, provenance, schemas, and phase packs."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Mapping

import duckdb

from decision_support_contract import (
    ACTIONABILITY_MATRIX_FILENAME,
    BENCHMARK_MINIMUM_SESSIONS,
    BENCHMARK_MINIMUM_WEEKLY_OBSERVATIONS,
    CAPABILITIES,
    CANDIDATE_FUNNEL_FILENAME,
    CANDIDATE_FUNNEL_SCHEMA,
    CONTRACT_VERSION,
    DATABASE_FILENAME,
    EVIDENCE_PACKETS_FILENAME,
    EXCHANGE_TIMEZONE,
    EXPECTED_TABLES,
    MANIFEST_FILENAME,
    MAX_COMPRESSED_DATABASE_BYTES,
    PHASES,
    PHASE_PACK_DIRECTORY,
    PRIVATE_SCHEMA_TOKENS,
    OPTIONAL_SOURCE_FILES,
    OPERATING_MODES,
    RECENT_MARKET_SESSIONS,
    REQUIRED_SOURCE_FILES,
    SCHEMA_VERSION,
    TASK_TIMEZONE,
    VALIDATOR_FILES,
    canonical_json,
    sha256_file,
)


TAG_PATTERN = re.compile(r"market-data-[0-9]{8}T[0-9]{6}Z")
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
EVIDENCE_FIELDS = {
    "evidence_id",
    "security_id",
    "evidence_kind",
    "known_at_utc",
    "revision",
    "source_event_at",
    "source_locator",
    "headline",
    "summary",
}


class DecisionSupportVerificationError(RuntimeError):
    """Raised when a decision-support artifact is not safe to consume."""


def parse_utc(value: object, label: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise DecisionSupportVerificationError(f"Invalid {label}") from exc
    if parsed.tzinfo is None:
        raise DecisionSupportVerificationError(f"{label} has no timezone")
    return parsed.astimezone(timezone.utc)


def expected_validator_identity() -> tuple[list[dict[str, str]], str]:
    root = Path(__file__).resolve().parent
    files = [
        {"path": filename, "sha256": sha256_file(root / filename)}
        for filename in VALIDATOR_FILES
    ]
    return files, hashlib.sha256(canonical_json(files)).hexdigest()


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


def expected_operating_modes(
    phase_id: str,
    required: list[Mapping[str, object]],
    optional: list[Mapping[str, object]],
    external: list[Mapping[str, object]],
    capabilities: Mapping[str, Mapping[str, object]],
) -> list[str]:
    unavailable_required = [
        value for value in required if value.get("state") != "READY"
    ]
    unavailable_optional = [
        value for value in optional if value.get("state") != "READY"
    ]
    modes = ["ARTIFACT_VALID"]
    if phase_id in {"sunday", "accounting"} and all(
        capabilities[value].get("state") == "READY"
        for value in (
            "benchmark_identity",
            "benchmark_market_current",
            "certified_total_returns",
            "funded_benchmark_inputs",
        )
    ):
        modes.append("BENCHMARK_ONLY_SAFE")
    if phase_id != "accounting" and not unavailable_required and not unavailable_optional:
        modes.append("CHALLENGER_RESEARCH_READY")
    if external:
        modes.append("LIVE_SNAPSHOT_REQUIRED")
    if unavailable_required or unavailable_optional:
        modes.append("CHALLENGER_BLOCKED")
    return [mode for mode in OPERATING_MODES if mode in modes]


def verify_windows(pack: Mapping[str, object], phase_id: str) -> None:
    windows = pack.get("phase_windows")
    if not isinstance(windows, list) or not windows:
        raise DecisionSupportVerificationError(f"Missing phase windows: {phase_id}")
    ids: set[str] = set()
    for window in windows:
        if not isinstance(window, dict) or not isinstance(window.get("window_id"), str):
            raise DecisionSupportVerificationError(f"Invalid phase window: {phase_id}")
        if str(window["window_id"]) in ids:
            raise DecisionSupportVerificationError(
                f"Duplicate phase window: {phase_id}"
            )
        ids.add(str(window["window_id"]))
        not_before = parse_utc(window.get("not_before_utc"), "not_before_utc")
        decision = parse_utc(
            window.get("phase_decision_cutoff_utc"), "phase_decision_cutoff_utc"
        )
        expires = parse_utc(window.get("expires_at_utc"), "expires_at_utc")
        if not not_before <= decision < expires:
            raise DecisionSupportVerificationError(
                f"Invalid phase-window ordering: {phase_id}"
            )
    first = windows[0]
    for key in ("phase_decision_cutoff_utc", "not_before_utc", "expires_at_utc"):
        if pack.get(key) != first.get(key):
            raise DecisionSupportVerificationError(
                f"Phase-window alias mismatch: {phase_id}/{key}"
            )
    if pack.get("decision_cutoff_utc") != pack.get("phase_decision_cutoff_utc"):
        raise DecisionSupportVerificationError(
            f"Legacy cutoff alias mismatch: {phase_id}"
        )


def verify_phase_pack(
    pack: Mapping[str, object], manifest: Mapping[str, object], phase_id: str
) -> None:
    contract = PHASES[phase_id]
    manifest_capabilities = {
        str(record["capability_id"]): record for record in manifest["capabilities"]
    }
    if (
        pack.get("schema_version") != SCHEMA_VERSION
        or pack.get("contract_version") != CONTRACT_VERSION
        or pack.get("phase_id") != phase_id
    ):
        raise DecisionSupportVerificationError(
            f"Invalid phase-pack identity: {phase_id}"
        )
    source_release = manifest["source_release"]
    database = manifest["database"]
    expected_identity = {
        "source_release_tag": source_release["tag"],
        "source_manifest_sha256": source_release["manifest_sha256"],
        "decision_support_database_sha256": database["sha256"],
        "built_at_utc": manifest["built_at_utc"],
        "generated_at_utc": manifest["built_at_utc"],
        "data_cutoff_utc": manifest["data_cutoff_utc"],
        "source_watermarks": manifest["source_watermarks"],
        "auxiliary_assets": manifest["auxiliary_assets"],
        "private_state_owner": "CONSUMER",
        "decision_owner": "CONSUMER",
    }
    for key, value in expected_identity.items():
        if pack.get(key) != value:
            raise DecisionSupportVerificationError(
                f"Phase-pack identity mismatch for {phase_id}: {key}"
            )
    try:
        date.fromisoformat(str(pack.get("valid_for_session")))
    except ValueError as exc:
        raise DecisionSupportVerificationError(
            f"Invalid phase valid_for_session: {phase_id}"
        ) from exc
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
    modes = expected_operating_modes(
        phase_id,
        required,
        optional,
        pack["external_capabilities"],
        manifest_capabilities,
    )
    if pack.get("operating_modes") != modes or pack.get("decision_mode") != modes[-1]:
        raise DecisionSupportVerificationError(
            f"Incorrect operating modes for {phase_id}"
        )
    expected_rejections = sorted(
        {
            f"CAPABILITY_{item['capability_id'].upper()}_{item['state']}"
            for item in required + optional
            if item.get("state") != "READY"
        }
    )
    if pack.get("rejection_codes") != expected_rejections:
        raise DecisionSupportVerificationError(
            f"Incorrect rejection codes for {phase_id}"
        )
    if (
        pack.get("task_timezone") != TASK_TIMEZONE
        or pack.get("exchange_timezone") != EXCHANGE_TIMEZONE
        or pack.get("delivery_timezone") != TASK_TIMEZONE
        or pack.get("delivery_targets") != list(contract.delivery_targets)
    ):
        raise DecisionSupportVerificationError(
            f"Incorrect delivery contract for {phase_id}"
        )
    expected_live_schema = SCHEMA_VERSION if contract.external_capabilities else None
    if pack.get("consumer_live_snapshot_schema") != expected_live_schema:
        raise DecisionSupportVerificationError(
            f"Incorrect live-snapshot schema reference for {phase_id}"
        )
    verify_windows(pack, phase_id)


def verify(directory: Path) -> dict[str, object]:
    verification_started = time.perf_counter()
    decompression_seconds = 0.0
    root = directory.resolve()
    manifest = load_json(root / MANIFEST_FILENAME, "decision-support manifest")
    if manifest.get("schema_version") != SCHEMA_VERSION:
        raise DecisionSupportVerificationError("Unsupported decision-support schema")
    if manifest.get("contract_version") != CONTRACT_VERSION:
        raise DecisionSupportVerificationError("Unsupported decision-support contract")
    if manifest.get("status") != "READY":
        raise DecisionSupportVerificationError("Decision-support manifest is not READY")
    if manifest.get("private_state_owner") != "CONSUMER":
        raise DecisionSupportVerificationError(
            "Private-state ownership is not consumer-only"
        )
    if manifest.get("decision_owner") != "CONSUMER":
        raise DecisionSupportVerificationError(
            "Decision ownership is not consumer-only"
        )
    if manifest.get("routine_context_slo_seconds") != 60:
        raise DecisionSupportVerificationError("Routine-context SLO is not 60 seconds")
    if (
        manifest.get("maximum_compressed_database_bytes")
        != MAX_COMPRESSED_DATABASE_BYTES
    ):
        raise DecisionSupportVerificationError(
            "Compressed database limit is inconsistent"
        )
    if (
        manifest.get("task_timezone") != TASK_TIMEZONE
        or manifest.get("exchange_timezone") != EXCHANGE_TIMEZONE
    ):
        raise DecisionSupportVerificationError(
            "Decision-support timezone contract mismatch"
        )
    built_at = parse_utc(manifest.get("built_at_utc"), "built_at_utc")
    if manifest.get("generated_at_utc") != manifest.get("built_at_utc"):
        raise DecisionSupportVerificationError("Legacy generated-at alias mismatch")
    data_cutoff = parse_utc(manifest.get("data_cutoff_utc"), "data_cutoff_utc")
    if data_cutoff > built_at:
        raise DecisionSupportVerificationError(
            "Data cutoff occurs after artifact build"
        )
    try:
        date.fromisoformat(str(manifest.get("valid_for_session")))
    except ValueError as exc:
        raise DecisionSupportVerificationError("Invalid valid_for_session") from exc
    if manifest.get("data_session") != manifest.get("valid_for_session"):
        raise DecisionSupportVerificationError(
            "Artifact data-session identity mismatch"
        )

    validator = manifest.get("validator_identity")
    expected_files, expected_validator_sha = expected_validator_identity()
    if not isinstance(validator, dict):
        raise DecisionSupportVerificationError("Missing validator identity")
    if (
        validator.get("contract_version") != CONTRACT_VERSION
        or validator.get("files") != expected_files
        or validator.get("validator_set_sha256") != expected_validator_sha
    ):
        raise DecisionSupportVerificationError(
            "Validator identity does not match checked-out code"
        )
    producer_commit = validator.get("producer_commit")
    if producer_commit != "LOCAL_TEST" and (
        not isinstance(producer_commit, str)
        or re.fullmatch(r"[0-9a-f]{40}", producer_commit) is None
    ):
        raise DecisionSupportVerificationError("Invalid validator producer commit")

    source = manifest.get("source_release")
    if not isinstance(source, dict):
        raise DecisionSupportVerificationError("Missing source release identity")
    tag = source.get("tag")
    if not isinstance(tag, str) or TAG_PATTERN.fullmatch(tag) is None:
        raise DecisionSupportVerificationError("Invalid source release tag")
    source_digest = source.get("manifest_sha256")
    if (
        not isinstance(source_digest, str)
        or SHA256_PATTERN.fullmatch(source_digest) is None
    ):
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
            raise DecisionSupportVerificationError(
                f"Duplicate source-file identity: {filename}"
            )
        if filename not in set(REQUIRED_SOURCE_FILES) | set(OPTIONAL_SOURCE_FILES):
            raise DecisionSupportVerificationError(
                f"Unexpected source-file identity: {filename}"
            )
        size = record.get("bytes")
        digest = record.get("sha256")
        if (
            isinstance(size, bool)
            or not isinstance(size, int)
            or size < 0
            or not isinstance(digest, str)
            or SHA256_PATTERN.fullmatch(digest) is None
        ):
            raise DecisionSupportVerificationError(
                f"Invalid source-file identity: {filename}"
            )
        source_file_names.add(filename)
    if not set(REQUIRED_SOURCE_FILES) <= source_file_names:
        raise DecisionSupportVerificationError(
            f"Missing required source-file identities: {sorted(set(REQUIRED_SOURCE_FILES) - source_file_names)}"
        )
    source_manifest_record = next(
        record for record in source_files if record["file"] == "manifest.json"
    )
    if source_manifest_record["sha256"] != source_digest:
        raise DecisionSupportVerificationError(
            "Source manifest identity is inconsistent"
        )

    capability_records = manifest.get("capabilities")
    if not isinstance(capability_records, list):
        raise DecisionSupportVerificationError("Capability records are missing")
    capabilities: dict[str, Mapping[str, object]] = {}
    for record in capability_records:
        if not isinstance(record, dict) or not isinstance(
            record.get("capability_id"), str
        ):
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
    watermarks = manifest.get("source_watermarks")
    if not isinstance(watermarks, list) or any(
        not isinstance(value, dict) for value in watermarks
    ):
        raise DecisionSupportVerificationError("Source watermarks are missing")
    watermark_ids = [str(value.get("capability_id")) for value in watermarks]
    if watermark_ids != sorted(CAPABILITIES):
        raise DecisionSupportVerificationError(
            "Source-watermark capability set mismatch"
        )
    for record in watermarks:
        capability_id = str(record["capability_id"])
        if record.get("capability_state") != capabilities[capability_id].get("state"):
            raise DecisionSupportVerificationError(
                f"Source-watermark state mismatch: {capability_id}"
            )
    for capability_id in (
        "benchmark_identity",
        "benchmark_market_current",
        "certified_total_returns",
        "funded_benchmark_inputs",
    ):
        record = capabilities[capability_id]
        if record.get("state") == "READY" and (
            record.get("sessions", 0) < BENCHMARK_MINIMUM_SESSIONS
            or record.get("weekly_observations", 0)
            < BENCHMARK_MINIMUM_WEEKLY_OBSERVATIONS
            or record.get("securities") != 3
            or record.get("coverage") != 1.0
        ):
            raise DecisionSupportVerificationError(
                f"Certified benchmark history is insufficient: {capability_id}"
            )

    auxiliary_assets = manifest.get("auxiliary_assets")
    if not isinstance(auxiliary_assets, dict) or set(auxiliary_assets) != {
        "candidate_funnel",
        "actionability_matrix",
        "evidence_packets",
    }:
        raise DecisionSupportVerificationError("Auxiliary asset set mismatch")
    expected_asset_paths = {
        "candidate_funnel": CANDIDATE_FUNNEL_FILENAME,
        "actionability_matrix": ACTIONABILITY_MATRIX_FILENAME,
        "evidence_packets": EVIDENCE_PACKETS_FILENAME,
    }
    for key, expected_path in expected_asset_paths.items():
        record = auxiliary_assets[key]
        if not isinstance(record, dict) or record.get("path") != expected_path:
            raise DecisionSupportVerificationError(f"Invalid auxiliary asset: {key}")
        verify_file_record(root, record, f"auxiliary asset {key}")
    candidate_path = root / CANDIDATE_FUNNEL_FILENAME
    duck_connection = duckdb.connect()
    try:
        candidate_description = duck_connection.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)", [str(candidate_path)]
        ).fetchall()
        actual_candidate_schema = tuple(
            (str(row[0]), str(row[1])) for row in candidate_description
        )
        if actual_candidate_schema != CANDIDATE_FUNNEL_SCHEMA:
            raise DecisionSupportVerificationError("Candidate-funnel schema mismatch")
        candidate_rows = int(
            duck_connection.execute(
                "SELECT count(*) FROM read_parquet(?)", [str(candidate_path)]
            ).fetchone()[0]
        )
        if capabilities["candidate_funnel"]["state"] != "READY" and candidate_rows:
            raise DecisionSupportVerificationError(
                "Unconfigured candidate funnel must be empty"
            )
    finally:
        duck_connection.close()

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
        if (
            name in expected_rows
            or isinstance(rows, bool)
            or not isinstance(rows, int)
            or rows < 0
        ):
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
        evidence_path = Path(temp) / "evidence-packets.jsonl"
        decompression_started = time.perf_counter()
        decompress_database(compressed, database_path)
        decompress_database(root / EVIDENCE_PACKETS_FILENAME, evidence_path)
        decompression_seconds = time.perf_counter() - decompression_started
        packet_ids: list[str] = []
        with evidence_path.open(encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                try:
                    packet = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise DecisionSupportVerificationError(
                        f"Invalid evidence packet JSON at line {line_number}"
                    ) from exc
                if not isinstance(packet, dict) or set(packet) != EVIDENCE_FIELDS:
                    raise DecisionSupportVerificationError(
                        f"Evidence packet schema mismatch at line {line_number}"
                    )
                if any(
                    not isinstance(packet.get(field), str) or not packet.get(field)
                    for field in (
                        "evidence_id",
                        "security_id",
                        "evidence_kind",
                        "known_at_utc",
                        "revision",
                        "source_locator",
                    )
                ):
                    raise DecisionSupportVerificationError(
                        f"Evidence packet lacks provenance at line {line_number}"
                    )
                parse_utc(packet["known_at_utc"], "evidence known_at_utc")
                packet_ids.append(str(packet["evidence_id"]))
        if packet_ids != sorted(set(packet_ids)):
            raise DecisionSupportVerificationError(
                "Evidence packet IDs are duplicate or non-deterministically ordered"
            )
        if database_path.stat().st_size != uncompressed_size:
            raise DecisionSupportVerificationError(
                "Uncompressed database byte-size mismatch"
            )
        if sha256_file(database_path) != uncompressed_sha:
            raise DecisionSupportVerificationError(
                "Uncompressed database SHA-256 mismatch"
            )
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
                "contract_version": CONTRACT_VERSION,
                "source_release_tag": tag,
                "source_manifest_sha256": source_digest,
                "built_at_utc": manifest["built_at_utc"],
                "data_cutoff_utc": manifest["data_cutoff_utc"],
                "data_session": manifest["data_session"],
                "valid_for_session": manifest["valid_for_session"],
                "producer_commit": producer_commit,
                "task_timezone": TASK_TIMEZONE,
                "exchange_timezone": EXCHANGE_TIMEZONE,
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
                    **(
                        {
                            "sessions": row[6],
                            "weekly_observations": row[7],
                            "minimum_sessions": row[8],
                            "minimum_weekly_observations": row[9],
                            "securities": row[10],
                            "coverage": row[11],
                            "expected_coverage": row[12],
                            "maximum_session": row[13],
                            "identity_subset_sha256": row[14],
                        }
                        if row[6] is not None
                        else {}
                    ),
                }
                for row in connection.execute(
                    """
                    SELECT capability_id, state, reason, source_group, observed_at,
                           maximum_age_seconds, sessions, weekly_observations,
                           minimum_sessions, minimum_weekly_observations,
                           securities, coverage, expected_coverage,
                           maximum_session, identity_subset_sha256
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
                "primary_filings_latest",
                "evidence",
                "candidate_funnel",
                "actionability_matrix",
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
            invalid_evidence = int(
                connection.execute(
                    """
                    SELECT count(*) FROM evidence
                    WHERE evidence_id IS NULL OR known_at_utc IS NULL
                       OR revision IS NULL OR source_locator IS NULL
                    """
                ).fetchone()[0]
            )
            if invalid_evidence:
                raise DecisionSupportVerificationError(
                    "Evidence records lack stable identity or point-in-time provenance"
                )
            database_evidence_ids = [
                str(row[0])
                for row in connection.execute(
                    "SELECT evidence_id FROM evidence ORDER BY evidence_id"
                )
            ]
            expected_packet_ids = (
                database_evidence_ids
                if capabilities["candidate_funnel"]["state"] == "READY"
                else []
            )
            if packet_ids != expected_packet_ids:
                raise DecisionSupportVerificationError(
                    "Evidence packets do not match the SQLite evidence index"
                )
            if capabilities["certified_total_returns"]["state"] == "READY":
                invalid_benchmark = int(
                    connection.execute(
                        """
                        SELECT count(*) FROM benchmark_total_returns
                        WHERE certification_status <> 'CERTIFIED'
                           OR distribution_lineage_sha256 IS NULL
                           OR corporate_action_lineage_sha256 IS NULL
                           OR known_at_utc IS NULL OR source_revision IS NULL
                           OR source_locator IS NULL
                        """
                    ).fetchone()[0]
                )
                if invalid_benchmark:
                    raise DecisionSupportVerificationError(
                        "Certified benchmark rows lack certification or lineage"
                    )
        finally:
            connection.close()

    actionability = load_json(
        root / ACTIONABILITY_MATRIX_FILENAME, "actionability matrix"
    )
    if (
        actionability.get("schema_version") != SCHEMA_VERSION
        or actionability.get("contract_version") != CONTRACT_VERSION
        or actionability.get("selector_state")
        != capabilities["candidate_funnel"]["state"]
        or not isinstance(actionability.get("phase_decisions"), list)
        or not isinstance(actionability.get("securities"), list)
    ):
        raise DecisionSupportVerificationError("Invalid actionability matrix contract")
    if capabilities["candidate_funnel"]["state"] != "READY" and actionability.get(
        "securities"
    ):
        raise DecisionSupportVerificationError(
            "Unconfigured candidate funnel must not publish security actionability"
        )
    if actionability.get("built_at_utc") != manifest.get("built_at_utc"):
        raise DecisionSupportVerificationError("Actionability build identity mismatch")
    phase_decisions = actionability["phase_decisions"]
    if any(not isinstance(value, dict) for value in phase_decisions) or {
        str(value.get("phase_id")) for value in phase_decisions
    } != set(PHASES):
        raise DecisionSupportVerificationError("Actionability phase set mismatch")
    for decision in phase_decisions:
        if decision.get("state") not in {"READY", "BLOCKED"} or not isinstance(
            decision.get("rejection_codes"), list
        ):
            raise DecisionSupportVerificationError(
                "Invalid actionability phase decision"
            )
        phase_id = str(decision["phase_id"])
        phase_contract = PHASES[phase_id]
        unavailable = sorted(
            capability_id
            for capability_id in phase_contract.required_capabilities
            + phase_contract.optional_capabilities
            if capabilities[capability_id].get("state") != "READY"
        )
        expected_state = "BLOCKED" if unavailable else "READY"
        expected_rejections = [
            f"CAPABILITY_{value.upper()}_{capabilities[value]['state']}"
            for value in unavailable
        ]
        if (
            decision.get("state") != expected_state
            or decision.get("rejection_codes") != expected_rejections
        ):
            raise DecisionSupportVerificationError(
                f"Incorrect actionability decision for {phase_id}"
            )

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
            raise DecisionSupportVerificationError(
                f"Invalid phase-pack path: {phase_id}"
            )
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
    expected_artifact_modes: set[str] = set()
    for phase_id, record in by_phase.items():
        pack = load_json(root / str(record["path"]), f"phase pack {phase_id}")
        expected_artifact_modes.update(str(value) for value in pack["operating_modes"])
    expected_modes = sorted(expected_artifact_modes, key=OPERATING_MODES.index)
    if manifest.get("artifact_operating_modes") != expected_modes:
        raise DecisionSupportVerificationError(
            "Artifact operating modes are inconsistent"
        )

    expected_entries = {
        MANIFEST_FILENAME,
        DATABASE_FILENAME,
        PHASE_PACK_DIRECTORY,
        CANDIDATE_FUNNEL_FILENAME,
        ACTIONABILITY_MATRIX_FILENAME,
        EVIDENCE_PACKETS_FILENAME,
    }
    actual_entries = {entry.name for entry in root.iterdir()}
    if actual_entries != expected_entries:
        raise DecisionSupportVerificationError(
            f"Artifact file-set mismatch: {sorted(actual_entries)}"
        )
    phase_entries = {entry.name for entry in (root / PHASE_PACK_DIRECTORY).iterdir()}
    if phase_entries != {f"{phase_id}.json" for phase_id in PHASES}:
        raise DecisionSupportVerificationError(
            "Phase-pack directory contains unexpected files"
        )
    total_seconds = time.perf_counter() - verification_started
    return {
        "status": "VALID",
        "source_release_tag": tag,
        "database_bytes": compressed.stat().st_size,
        "database_representation": "COMPRESSED_ZSTANDARD",
        "decompressed_database_bytes": uncompressed_size,
        "phase_packs": len(PHASES),
        "latency": {
            "download_seconds": None,
            "download_included": False,
            "decompression_seconds": decompression_seconds,
            "integrity_schema_validation_seconds_excluding_decompression": max(
                0.0, total_seconds - decompression_seconds
            ),
            "total_post_download_seconds": total_seconds,
        },
    }


def evaluate_phase_at(
    directory: Path,
    phase_id: str,
    as_of: datetime,
    *,
    allow_degraded: bool = False,
) -> dict[str, object]:
    if phase_id not in PHASES:
        raise DecisionSupportVerificationError(f"Unknown phase: {phase_id}")
    pack = load_json(
        directory.resolve() / PHASE_PACK_DIRECTORY / f"{phase_id}.json",
        f"phase pack {phase_id}",
    )
    instant = as_of.astimezone(timezone.utc)
    active_window = next(
        (
            window
            for window in pack["phase_windows"]
            if parse_utc(window["not_before_utc"], "not_before_utc")
            <= instant
            < parse_utc(window["expires_at_utc"], "expires_at_utc")
        ),
        None,
    )
    reasons = list(pack.get("rejection_codes") or [])
    freshness_reasons: list[str] = []
    if active_window is None:
        reasons.append("PHASE_WINDOW_NOT_ACTIVE")
    if pack.get("status") == "DEGRADED" and not allow_degraded:
        reasons.append("DEGRADED_NOT_ALLOWED")
    for capability in pack.get("required_capabilities") or []:
        if not isinstance(capability, dict) or capability.get("state") != "READY":
            continue
        maximum_age = capability.get("maximum_age_seconds")
        observed = capability.get("observed_at")
        if not isinstance(maximum_age, int) or maximum_age < 0 or not observed:
            continue
        observed_at = parse_utc(observed, f"{capability.get('capability_id')}.observed_at")
        age_seconds = (instant - observed_at).total_seconds()
        capability_id = str(capability.get("capability_id") or "UNKNOWN").upper()
        if age_seconds < 0:
            freshness_reasons.append(
                f"CAPABILITY_{capability_id}_NOT_KNOWN_AT_AS_OF"
            )
        elif age_seconds > maximum_age:
            freshness_reasons.append(f"CAPABILITY_{capability_id}_STALE_AT_AS_OF")
    reasons.extend(freshness_reasons)
    usable = active_window is not None and (
        pack.get("status") == "READY"
        or (pack.get("status") == "DEGRADED" and allow_degraded)
    ) and not freshness_reasons
    return {
        "status": "USABLE" if usable else "BLOCKED",
        "phase_id": phase_id,
        "as_of_utc": instant.isoformat().replace("+00:00", "Z"),
        "active_window_id": active_window.get("window_id") if active_window else None,
        "decision_mode": pack.get("decision_mode"),
        "rejection_codes": sorted(set(reasons)),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dist", required=True)
    parser.add_argument("--phase", choices=sorted(PHASES))
    parser.add_argument("--as-of")
    parser.add_argument("--allow-degraded", action="store_true")
    args = parser.parse_args(argv)
    try:
        result = verify(Path(args.dist))
        if args.phase:
            as_of = (
                parse_utc(args.as_of, "as_of")
                if args.as_of
                else datetime.now(timezone.utc)
            )
            result["phase_evaluation"] = evaluate_phase_at(
                Path(args.dist), args.phase, as_of, allow_degraded=args.allow_degraded
            )
            if result["phase_evaluation"]["status"] != "USABLE":
                raise DecisionSupportVerificationError(
                    "Phase is not usable at the requested decision instant: "
                    + ",".join(result["phase_evaluation"]["rejection_codes"])
                )
    except (
        DecisionSupportVerificationError,
        OSError,
        sqlite3.Error,
        duckdb.Error,
    ) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
    (OPERATING_MODES,)
