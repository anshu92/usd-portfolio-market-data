#!/usr/bin/env python3
"""Reject workflow artifact inputs that cannot stay below 512 MiB."""

from __future__ import annotations

import argparse
import tempfile
import zipfile
from pathlib import Path
from typing import Iterable, NamedTuple


MAX_ARTIFACT_BYTES = 512 * 1024 * 1024
PACKAGING_HEADROOM_BYTES = 1024 * 1024
MAX_PREVIEW_BYTES = MAX_ARTIFACT_BYTES - PACKAGING_HEADROOM_BYTES


class ArtifactSizeError(RuntimeError):
    """Raised when workflow artifact inputs are unsafe or too large."""


class ArtifactSize(NamedTuple):
    input_bytes: int
    preview_bytes: int


def artifact_input_files(
    paths: Iterable[Path], allow_missing: bool = False
) -> dict[Path, int]:
    files: dict[Path, int] = {}
    for path in paths:
        if path.is_symlink():
            raise ArtifactSizeError(
                f"Artifact input must not contain symlinks: {path}"
            )
        if not path.exists():
            if allow_missing:
                continue
            raise ArtifactSizeError(f"Artifact input does not exist: {path}")
        candidates = path.rglob("*") if path.is_dir() else (path,)
        for candidate in candidates:
            if candidate.is_symlink():
                raise ArtifactSizeError(
                    f"Artifact input must not contain symlinks: {candidate}"
                )
            if candidate.is_file():
                files[candidate.resolve()] = candidate.stat().st_size
    return files


def compressed_preview_size(files: Iterable[Path]) -> int:
    with tempfile.TemporaryFile() as preview:
        with zipfile.ZipFile(
            preview,
            mode="w",
            compression=zipfile.ZIP_DEFLATED,
            compresslevel=9,
            allowZip64=True,
        ) as archive:
            for index, path in enumerate(sorted(files), start=1):
                archive.write(path, arcname=f"{index:04d}-{path.name}")
        return preview.tell()


def verify_artifact_inputs(
    paths: Iterable[Path], allow_missing: bool = False
) -> ArtifactSize:
    files = artifact_input_files(paths, allow_missing=allow_missing)
    size = ArtifactSize(
        input_bytes=sum(files.values()),
        preview_bytes=compressed_preview_size(files),
    )
    if size.preview_bytes >= MAX_PREVIEW_BYTES:
        raise ArtifactSizeError(
            f"Compressed workflow artifact preview totals {size.preview_bytes:,} "
            f"bytes from {size.input_bytes:,} input bytes; the preview must be less "
            f"than {MAX_PREVIEW_BYTES:,} bytes to reserve "
            f"{PACKAGING_HEADROOM_BYTES:,} bytes and keep the uploaded artifact below "
            f"{MAX_ARTIFACT_BYTES:,} bytes"
        )
    return size


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="+", type=Path)
    parser.add_argument(
        "--allow-missing",
        action="store_true",
        help="Ignore missing paths for best-effort diagnostic uploads",
    )
    args = parser.parse_args()
    try:
        size = verify_artifact_inputs(args.paths, allow_missing=args.allow_missing)
    except ArtifactSizeError as exc:
        parser.error(str(exc))
    print(
        f"Workflow artifact inputs: {size.input_bytes:,} bytes; "
        f"compressed preview: {size.preview_bytes:,} bytes "
        f"(preview limit with packaging headroom: {MAX_PREVIEW_BYTES - 1:,} bytes)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
