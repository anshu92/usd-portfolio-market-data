#!/usr/bin/env python3
"""Verify the exact identity of a published decision-support Actions artifact."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from decision_support_contract import MANIFEST_FILENAME, SCHEMA_VERSION, sha256_file


REQUIRED_FIELDS = {
    "artifact_id",
    "artifact_name",
    "artifact_sha256",
    "artifact_size_bytes",
    "decision_support_manifest_sha256",
    "generated_at_utc",
    "producer_commit",
    "source_manifest_sha256",
    "source_release_tag",
    "workflow_run_id",
}


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
        "producer_commit": str(run.get("head_sha") or ""),
    }
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
    if pointer.get("artifact_name") != f"decision-support-{source_tag}":
        raise DecisionSupportPointerError("Artifact name does not match source tag")
    if release.get("tag_name") != source_tag:
        raise DecisionSupportPointerError("Source release tag mismatch")
    if release.get("draft") is not False or release.get("prerelease") is not False:
        raise DecisionSupportPointerError("Source release is not published production data")
    if release.get("immutable") is not True:
        raise DecisionSupportPointerError("Source release is not immutable")

    manifest_path = artifact_directory / MANIFEST_FILENAME
    manifest_sha = sha256_file(manifest_path)
    if manifest_sha != pointer.get("decision_support_manifest_sha256"):
        raise DecisionSupportPointerError("Decision-support manifest SHA-256 mismatch")
    manifest = load_json(manifest_path, "decision-support manifest")
    source = manifest.get("source_release")
    if not isinstance(source, dict):
        raise DecisionSupportPointerError("Decision-support manifest lacks source identity")
    if source.get("tag") != source_tag:
        raise DecisionSupportPointerError("Embedded source release tag mismatch")
    if source.get("manifest_sha256") != pointer.get("source_manifest_sha256"):
        raise DecisionSupportPointerError("Embedded source manifest SHA-256 mismatch")


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
