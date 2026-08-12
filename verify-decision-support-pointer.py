#!/usr/bin/env python3
"""Verify the exact identity of a published decision-support Actions artifact."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from decision_support_contract import MANIFEST_FILENAME, SCHEMA_VERSION, sha256_file
from decision_support_contract import (
    CONTRACT_VERSION,
    EXPECTED_BRANCH,
    EXPECTED_EVENTS,
    EXPECTED_REPOSITORY,
    EXPECTED_WORKFLOW_ID,
    EXPECTED_WORKFLOW_PATH,
    VALIDATOR_FILES,
    canonical_json,
)


REQUIRED_FIELDS = {
    "artifact_id",
    "artifact_name",
    "artifact_sha256",
    "artifact_size_bytes",
    "artifact_expires_at",
    "decision_support_manifest_sha256",
    "generated_at_utc",
    "repository",
    "workflow_id",
    "workflow_path",
    "workflow_branch",
    "workflow_event",
    "validator_contract_version",
    "validator_set_sha256",
    "promotion_key",
    "producer_commit",
    "source_manifest_sha256",
    "source_release_tag",
    "source_release_immutable",
    "workflow_run_id",
}
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
COMMIT_PATTERN = re.compile(r"[0-9a-f]{40}")


class DecisionSupportPointerError(RuntimeError):
    """Raised when the pointer does not name one exact trusted artifact."""


def load_json(path: Path, label: str) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DecisionSupportPointerError(f"Cannot read {label}: {exc}") from exc
    if not isinstance(value, dict):
        raise DecisionSupportPointerError(f"{label} root is not an object")
    return value


def parse_time(value: object, label: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise DecisionSupportPointerError(f"Invalid {label}") from exc
    if parsed.tzinfo is None:
        raise DecisionSupportPointerError(f"{label} has no timezone")
    return parsed.astimezone(timezone.utc)


def verify_pointer(
    pointer: dict[str, object],
    artifact: dict[str, object],
    run: dict[str, object],
    release: dict[str, object],
    artifact_directory: Path,
    now: datetime,
) -> None:
    if pointer.get("schema_version") != SCHEMA_VERSION:
        raise DecisionSupportPointerError("Unsupported pointer schema")
    if not REQUIRED_FIELDS <= set(pointer):
        raise DecisionSupportPointerError(
            f"Pointer lacks required fields: {sorted(REQUIRED_FIELDS - set(pointer))}"
        )
    if run.get("status") != "completed" or run.get("conclusion") != "success":
        raise DecisionSupportPointerError("Decision-support run did not succeed")
    head_repository = run.get("head_repository")
    if (
        not isinstance(head_repository, dict)
        or head_repository.get("full_name") != EXPECTED_REPOSITORY
    ):
        raise DecisionSupportPointerError("Workflow repository is not trusted")
    run_identity = {
        "repository": EXPECTED_REPOSITORY,
        "workflow_id": EXPECTED_WORKFLOW_ID,
        "workflow_path": EXPECTED_WORKFLOW_PATH,
        "workflow_branch": EXPECTED_BRANCH,
        "workflow_event": str(run.get("event") or ""),
    }
    if (
        int(run.get("workflow_id", -1)) != EXPECTED_WORKFLOW_ID
        or run.get("path") != EXPECTED_WORKFLOW_PATH
        or run.get("head_branch") != EXPECTED_BRANCH
        or run.get("event") not in EXPECTED_EVENTS
    ):
        raise DecisionSupportPointerError("Workflow run identity is not trusted")
    for key, value in run_identity.items():
        if pointer.get(key) != value:
            raise DecisionSupportPointerError(f"Pointer mismatch: {key}")
    workflow_run = artifact.get("workflow_run")
    if not isinstance(workflow_run, dict):
        raise DecisionSupportPointerError("Artifact lacks workflow_run identity")
    digest = str(artifact.get("digest") or "").removeprefix("sha256:")
    expected = {
        "workflow_run_id": int(run.get("id", -1)),
        "artifact_id": int(artifact.get("id", -1)),
        "artifact_name": str(artifact.get("name") or ""),
        "artifact_sha256": digest,
        "artifact_size_bytes": int(artifact.get("size_in_bytes", -1)),
        "artifact_expires_at": str(artifact.get("expires_at") or ""),
        "producer_commit": str(run.get("head_sha") or ""),
    }
    if (
        expected["workflow_run_id"] <= 0
        or expected["artifact_id"] <= 0
        or SHA256_PATTERN.fullmatch(str(expected["artifact_sha256"])) is None
        or COMMIT_PATTERN.fullmatch(str(expected["producer_commit"])) is None
    ):
        raise DecisionSupportPointerError(
            "Invalid workflow/artifact cryptographic identity"
        )
    for key, value in expected.items():
        if pointer.get(key) != value:
            raise DecisionSupportPointerError(f"Pointer mismatch: {key}")
    if int(workflow_run.get("id", -1)) != expected["workflow_run_id"]:
        raise DecisionSupportPointerError("Artifact workflow_run.id mismatch")
    if str(workflow_run.get("head_sha") or "") != expected["producer_commit"]:
        raise DecisionSupportPointerError("Artifact workflow_run.head_sha mismatch")
    if artifact.get("expired") is not False:
        raise DecisionSupportPointerError("Artifact is expired")
    expires_at = parse_time(artifact.get("expires_at"), "artifact expires_at")
    if expires_at - now < timedelta(days=7):
        raise DecisionSupportPointerError("Artifact has less than seven days remaining")
    generated = parse_time(pointer.get("generated_at_utc"), "pointer generated_at_utc")
    completed = parse_time(run.get("updated_at"), "run updated_at")
    if generated < completed or generated - completed > timedelta(minutes=60):
        raise DecisionSupportPointerError(
            "Pointer was not generated within 60 minutes after the successful run"
        )

    source_tag = str(pointer.get("source_release_tag") or "")
    if (
        pointer.get("artifact_name")
        != f"decision-support-{source_tag}-{expected['workflow_run_id']}"
    ):
        raise DecisionSupportPointerError("Artifact name does not match source tag")
    if pointer.get("promotion_key") != f"{source_tag}/{expected['artifact_id']}":
        raise DecisionSupportPointerError(
            "Promotion key does not bind source tag and artifact ID"
        )
    if release.get("tag_name") != source_tag:
        raise DecisionSupportPointerError("Source release tag mismatch")
    if release.get("draft") is not False or release.get("prerelease") is not False:
        raise DecisionSupportPointerError(
            "Source release is not published production data"
        )
    if release.get("immutable") is not True:
        raise DecisionSupportPointerError("Source release is not immutable")
    if pointer.get("source_release_immutable") is not True:
        raise DecisionSupportPointerError(
            "Pointer does not assert immutable source release"
        )

    manifest_path = artifact_directory / MANIFEST_FILENAME
    manifest_sha = sha256_file(manifest_path)
    if manifest_sha != pointer.get("decision_support_manifest_sha256"):
        raise DecisionSupportPointerError("Decision-support manifest SHA-256 mismatch")
    manifest = load_json(manifest_path, "decision-support manifest")
    source = manifest.get("source_release")
    if not isinstance(source, dict):
        raise DecisionSupportPointerError(
            "Decision-support manifest lacks source identity"
        )
    if source.get("tag") != source_tag:
        raise DecisionSupportPointerError("Embedded source release tag mismatch")
    if source.get("manifest_sha256") != pointer.get("source_manifest_sha256"):
        raise DecisionSupportPointerError("Embedded source manifest SHA-256 mismatch")
    validator = manifest.get("validator_identity")
    if not isinstance(validator, dict):
        raise DecisionSupportPointerError(
            "Decision-support manifest lacks validator identity"
        )
    if validator.get("producer_commit") != expected["producer_commit"]:
        raise DecisionSupportPointerError(
            "Validator commit does not match workflow commit"
        )
    if (
        pointer.get("validator_contract_version") != CONTRACT_VERSION
        or validator.get("contract_version") != CONTRACT_VERSION
    ):
        raise DecisionSupportPointerError("Validator contract version mismatch")
    if pointer.get("validator_set_sha256") != validator.get("validator_set_sha256"):
        raise DecisionSupportPointerError("Validator set SHA-256 mismatch")
    validator_root = Path(__file__).resolve().parent
    expected_validator_files = [
        {"path": filename, "sha256": sha256_file(validator_root / filename)}
        for filename in VALIDATOR_FILES
    ]
    expected_validator_set_sha256 = hashlib.sha256(
        canonical_json(expected_validator_files)
    ).hexdigest()
    if (
        validator.get("files") != expected_validator_files
        or validator.get("validator_set_sha256") != expected_validator_set_sha256
    ):
        raise DecisionSupportPointerError(
            "Validator identity does not match checked-out code"
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pointer", required=True)
    parser.add_argument("--artifact-metadata", required=True)
    parser.add_argument("--run-metadata", required=True)
    parser.add_argument("--release-metadata", required=True)
    parser.add_argument("--artifact-dir", required=True)
    parser.add_argument("--now")
    args = parser.parse_args(argv)
    try:
        now = parse_time(args.now, "now") if args.now else datetime.now(timezone.utc)
        verify_pointer(
            load_json(Path(args.pointer), "pointer"),
            load_json(Path(args.artifact_metadata), "artifact metadata"),
            load_json(Path(args.run_metadata), "run metadata"),
            load_json(Path(args.release_metadata), "release metadata"),
            Path(args.artifact_dir).resolve(),
            now,
        )
    except (DecisionSupportPointerError, OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(json.dumps({"status": "VALID", "pointer": args.pointer}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
