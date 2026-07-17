#!/usr/bin/env python3
"""Validate immutable GitHub release metadata and downloaded asset digests."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path

from enrichment_contract import CONTRACTS


EXPECTED_ASSETS = tuple(sorted({
    "manifest.json",
    "NOTICE.md",
    "security-universe.csv",
    "unmatched-tickers.csv",
    "yahoo-ohlcv-320.parquet",
    "yahoo-splits.parquet",
} | set(CONTRACTS)))
EXPORT_METADATA_FILES = {"github-release.json", "resolved-tag.txt"}
TAG_PATTERN = re.compile(r"market-data-[0-9]{8}T[0-9]{6}Z")
DIGEST_PATTERN = re.compile(r"sha256:([0-9a-f]{64})")


class ReleaseMetadataError(RuntimeError):
    """Raised when GitHub release metadata or downloaded assets are invalid."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_metadata(
    metadata: dict[str, object],
) -> tuple[str, dict[str, dict[str, object]]]:
    if metadata.get("draft") is not False:
        raise ReleaseMetadataError("Latest release is a draft")
    if metadata.get("prerelease") is not False:
        raise ReleaseMetadataError("Latest release is a prerelease")
    if metadata.get("immutable") is not True:
        raise ReleaseMetadataError("Latest release is not immutable")

    tag = metadata.get("tag_name")
    if not isinstance(tag, str) or TAG_PATTERN.fullmatch(tag) is None:
        raise ReleaseMetadataError(f"Unexpected release tag: {tag!r}")

    raw_assets = metadata.get("assets")
    if not isinstance(raw_assets, list):
        raise ReleaseMetadataError("Release assets are missing or invalid")
    assets: dict[str, dict[str, object]] = {}
    for asset in raw_assets:
        if not isinstance(asset, dict):
            raise ReleaseMetadataError("Release assets contains a non-object entry")
        name = asset.get("name")
        if not isinstance(name, str) or name not in EXPECTED_ASSETS:
            raise ReleaseMetadataError(f"Unexpected release asset: {name!r}")
        if name in assets:
            raise ReleaseMetadataError(f"Duplicate release asset: {name}")
        if asset.get("state") != "uploaded":
            raise ReleaseMetadataError(f"Release asset is not uploaded: {name}")
        size = asset.get("size")
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise ReleaseMetadataError(f"Invalid release asset size: {name}")
        digest = asset.get("digest")
        if not isinstance(digest, str) or DIGEST_PATTERN.fullmatch(digest) is None:
            raise ReleaseMetadataError(f"Invalid SHA-256 digest for release asset: {name}")
        assets[name] = asset

    if set(assets) != set(EXPECTED_ASSETS):
        missing = sorted(set(EXPECTED_ASSETS) - set(assets))
        raise ReleaseMetadataError(f"Missing required release assets: {missing}")
    return tag, assets


def verify_downloads(directory: Path, assets: dict[str, dict[str, object]]) -> None:
    if not directory.is_dir():
        raise ReleaseMetadataError(f"Release directory is missing: {directory}")
    entries = {entry.name for entry in directory.iterdir()}
    missing = set(EXPECTED_ASSETS) - entries
    unexpected = entries - set(EXPECTED_ASSETS) - EXPORT_METADATA_FILES
    if missing or unexpected:
        raise ReleaseMetadataError(
            "Downloaded file set mismatch: "
            f"missing={sorted(missing)}, unexpected={sorted(unexpected)}"
        )
    for name in EXPECTED_ASSETS:
        path = directory / name
        if path.is_symlink() or not path.is_file():
            raise ReleaseMetadataError(f"Downloaded asset is not a regular file: {name}")
        asset = assets[name]
        if path.stat().st_size != asset["size"]:
            raise ReleaseMetadataError(f"GitHub byte-size mismatch: {name}")
        expected_digest = DIGEST_PATTERN.fullmatch(str(asset["digest"]))
        assert expected_digest is not None
        if sha256_file(path) != expected_digest.group(1):
            raise ReleaseMetadataError(f"GitHub SHA-256 mismatch: {name}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--metadata", required=True)
    parser.add_argument("--dist")
    args = parser.parse_args(argv)
    try:
        metadata = json.loads(Path(args.metadata).read_text(encoding="utf-8"))
        if not isinstance(metadata, dict):
            raise ReleaseMetadataError("Release metadata root is not an object")
        tag, assets = validate_metadata(metadata)
        if args.dist:
            verify_downloads(Path(args.dist), assets)
    except (json.JSONDecodeError, OSError, ReleaseMetadataError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(
        json.dumps(
            {
                "assets_verified": len(assets) if args.dist else 0,
                "immutable": True,
                "tag": tag,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
