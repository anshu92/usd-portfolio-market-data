from __future__ import annotations

import json
from pathlib import Path

import pytest


def test_verifier_rejects_hash_mismatch(
    aggregate_module, verify_module, aggregate_inputs, tmp_path: Path
):
    out_dir = tmp_path / "dist"
    args = [
        "--universe",
        str(aggregate_inputs["universe"]),
        "--universe-metadata",
        str(aggregate_inputs["metadata"]),
        "--out-dir",
        str(out_dir),
        "--prices-file",
        str(aggregate_inputs["prices"]),
        "--splits-file",
        str(aggregate_inputs["splits"]),
        "--sessions",
        "3",
        "--minimum-history",
        "2",
        "--minimum-coverage",
        "1",
        "--minimum-adequate-history-coverage",
        "1",
        "--cutoff-date",
        "2024-01-10",
        "--observed-at",
        "2024-01-11T01:00:00Z",
    ]
    assert aggregate_module.main(args) == 0
    manifest_path = out_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["aggregate"]["sha256"] = "0" * 64
    manifest["release_files"][0]["sha256"] = "0" * 64
    manifest_path.write_text(json.dumps(manifest) + "\n")
    with pytest.raises(verify_module.VerificationError, match="SHA-256 mismatch"):
        verify_module.verify(out_dir, require_ready=True, require_production=False)
