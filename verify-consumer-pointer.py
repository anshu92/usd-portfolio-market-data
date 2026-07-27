#!/usr/bin/env python3
"""Verify exact consumer pointer/run/artifact/release identity before commit."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path


REQUIRED_FIELDS = {
    "release_tag",
    "workflow_run_id",
    "artifact_id",
    "artifact_name",
    "artifact_sha256",
    "artifact_size_bytes",
    "producer_commit",
    "manifest_sha256",
}


class PointerVerificationError(RuntimeError):
    """Raised when discovery metadata is not the exact exported artifact identity."""


def load_json(path: Path, label: str) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PointerVerificationError(f"Cannot read {label}: {exc}") from exc
    if not isinstance(value, dict):
        raise PointerVerificationError(f"{label} root is not an object")
    return value


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parse_time(value: object, label: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise PointerVerificationError(f"Invalid {label}") from exc
    if parsed.tzinfo is None:
        raise PointerVerificationError(f"{label} has no timezone")
    return parsed.astimezone(timezone.utc)


def verify_pointer(
    pointer: dict[str, object],
    artifact: dict[str, object],
    run: dict[str, object],
    release: dict[str, object],
    artifact_directory: Path,
    now: datetime,
) -> None:
    if not REQUIRED_FIELDS <= set(pointer):
        raise PointerVerificationError(
            f"Pointer lacks required fields: {sorted(REQUIRED_FIELDS - set(pointer))}"
        )
    if run.get("status") != "completed" or run.get("conclusion") != "success":
        raise PointerVerificationError("Export workflow run is not successfully completed")
    workflow_run = artifact.get("workflow_run")
    if not isinstance(workflow_run, dict):
        raise PointerVerificationError("Artifact lacks workflow_run identity")
    digest = str(artifact.get("digest") or "")
    expected = {
        "workflow_run_id": int(run.get("id", -1)),
        "artifact_id": int(artifact.get("id", -1)),
        "artifact_name": str(artifact.get("name") or ""),
        "artifact_sha256": digest.removeprefix("sha256:"),
        "artifact_size_bytes": int(artifact.get("size_in_bytes", -1)),
        "producer_commit": str(run.get("head_sha") or ""),
    }
    for key, value in expected.items():
        if pointer.get(key) != value:
            raise PointerVerificationError(f"Pointer mismatch: {key}")
    if int(workflow_run.get("id", -1)) != expected["workflow_run_id"]:
        raise PointerVerificationError("Artifact workflow_run.id mismatch")
    if str(workflow_run.get("head_sha") or "") != expected["producer_commit"]:
        raise PointerVerificationError("Artifact workflow_run.head_sha mismatch")
    if artifact.get("expired") is not False:
        raise PointerVerificationError("Artifact is expired")
    expires_at = parse_time(artifact.get("expires_at"), "artifact expires_at")
    if expires_at - now < timedelta(days=7):
        raise PointerVerificationError("Artifact has less than seven days remaining")
    generated = parse_time(pointer.get("generated_at_utc"), "pointer generated_at_utc")
    completed = parse_time(run.get("updated_at"), "run updated_at")
    if generated < completed or generated - completed > timedelta(minutes=60):
        raise PointerVerificationError(
            "Pointer was not generated within 60 minutes after successful export"
        )
    release_tag = str(pointer.get("release_tag") or "")
    if pointer.get("artifact_name") != f"validated-market-data-{release_tag}":
        raise PointerVerificationError("Artifact name does not match release tag")
    if release.get("tag_name") != release_tag:
        raise PointerVerificationError("GitHub release tag mismatch")
    if release.get("draft") is not False or release.get("prerelease") is not False:
        raise PointerVerificationError("Release is not published production data")
    if release.get("immutable") is not True:
        raise PointerVerificationError("Release is not immutable")
    resolved_tag = (artifact_directory / "resolved-tag.txt").read_text(
        encoding="utf-8"
    ).strip()
    if resolved_tag != release_tag:
        raise PointerVerificationError("resolved-tag.txt mismatch")
    embedded_release = load_json(
        artifact_directory / "github-release.json", "embedded release metadata"
    )
    if embedded_release.get("tag_name") != release_tag:
        raise PointerVerificationError("Embedded release tag mismatch")
    manifest_path = artifact_directory / "manifest.json"
    if sha256_file(manifest_path) != pointer.get("manifest_sha256"):
        raise PointerVerificationError("Downloaded manifest SHA-256 mismatch")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pointer", required=True)
    parser.add_argument("--artifact-metadata", required=True)
    parser.add_argument("--run-metadata", required=True)
    parser.add_argument("--release-metadata", required=True)
    parser.add_argument("--artifact-dir", required=True)
    parser.add_argument("--now")
    args = parser.parse_args(argv)
    try:
        now = (
            parse_time(args.now, "now")
            if args.now
            else datetime.now(timezone.utc)
        )
        verify_pointer(
            load_json(Path(args.pointer), "pointer"),
            load_json(Path(args.artifact_metadata), "artifact metadata"),
            load_json(Path(args.run_metadata), "run metadata"),
            load_json(Path(args.release_metadata), "release metadata"),
            Path(args.artifact_dir).resolve(),
            now,
        )
    except (PointerVerificationError, OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(json.dumps({"status": "VALID", "pointer": args.pointer}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
