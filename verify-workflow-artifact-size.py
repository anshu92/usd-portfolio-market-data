#!/usr/bin/env python3
"""Reject workflow artifact inputs that cannot stay below 512 MiB."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable


MAX_ARTIFACT_BYTES = 512 * 1024 * 1024
PACKAGING_HEADROOM_BYTES = 1024 * 1024
MAX_INPUT_BYTES = MAX_ARTIFACT_BYTES - PACKAGING_HEADROOM_BYTES


class ArtifactSizeError(RuntimeError):
    """Raised when workflow artifact inputs are unsafe or too large."""


def artifact_input_size(paths: Iterable[Path], allow_missing: bool = False) -> int:
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
    return sum(files.values())


def verify_artifact_inputs(
    paths: Iterable[Path], allow_missing: bool = False
) -> int:
    size = artifact_input_size(paths, allow_missing=allow_missing)
    if size >= MAX_INPUT_BYTES:
        raise ArtifactSizeError(
            f"Workflow artifact inputs total {size:,} bytes; they must be less than "
            f"{MAX_INPUT_BYTES:,} bytes to reserve {PACKAGING_HEADROOM_BYTES:,} bytes "
            f"and keep the packaged artifact below {MAX_ARTIFACT_BYTES:,} bytes"
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
        f"Workflow artifact inputs: {size:,} bytes "
        f"(limit with packaging headroom: {MAX_INPUT_BYTES - 1:,} bytes)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
