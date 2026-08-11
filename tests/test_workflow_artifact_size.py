from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


def load_module():
    path = (
        Path(__file__).resolve().parents[1] / "verify-workflow-artifact-size.py"
    )
    spec = importlib.util.spec_from_file_location("workflow_artifact_size", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_accepts_inputs_below_limit_and_deduplicates_paths(tmp_path: Path):
    module = load_module()
    payload = tmp_path / "payload.bin"
    payload.write_bytes(b"artifact" * 1_000)

    size = module.verify_artifact_inputs([tmp_path, payload])
    assert size.input_bytes == len(b"artifact" * 1_000)
    assert size.preview_bytes < size.input_bytes


def test_rejects_preview_without_packaging_headroom(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    module = load_module()
    payload = tmp_path / "payload.bin"
    payload.write_bytes(b"artifact")

    monkeypatch.setattr(
        module, "compressed_preview_size", lambda _: module.MAX_PREVIEW_BYTES
    )

    with pytest.raises(module.ArtifactSizeError, match="uploaded artifact below"):
        module.verify_artifact_inputs([payload])


def test_missing_inputs_require_explicit_best_effort_mode(tmp_path: Path):
    module = load_module()
    missing = tmp_path / "missing"

    with pytest.raises(module.ArtifactSizeError, match="does not exist"):
        module.verify_artifact_inputs([missing])
    size = module.verify_artifact_inputs([missing], allow_missing=True)
    assert size.input_bytes == 0
    assert size.preview_bytes > 0
