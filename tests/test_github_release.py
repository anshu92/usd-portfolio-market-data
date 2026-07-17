from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest


def release_metadata(github_release_module, directory: Path) -> dict[str, object]:
    assets = []
    for index, name in enumerate(github_release_module.EXPECTED_ASSETS, start=1):
        path = directory / name
        path.write_bytes(f"fixture-{name}\n".encode())
        assets.append(
            {
                "digest": f"sha256:{hashlib.sha256(path.read_bytes()).hexdigest()}",
                "id": index,
                "name": name,
                "size": path.stat().st_size,
                "state": "uploaded",
            }
        )
    return {
        "assets": assets,
        "draft": False,
        "immutable": True,
        "prerelease": False,
        "tag_name": "market-data-20260717T032654Z",
    }


def test_validates_immutable_release_and_downloads(
    github_release_module, tmp_path: Path
):
    metadata = release_metadata(github_release_module, tmp_path)
    tag, assets = github_release_module.validate_metadata(metadata)
    assert tag == "market-data-20260717T032654Z"
    github_release_module.verify_downloads(tmp_path, assets)
    (tmp_path / "github-release.json").write_text("{}\n", encoding="utf-8")
    (tmp_path / "resolved-tag.txt").write_text(f"{tag}\n", encoding="utf-8")
    github_release_module.verify_downloads(tmp_path, assets)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("draft", True, "draft"),
        ("prerelease", True, "prerelease"),
        ("immutable", False, "not immutable"),
        ("tag_name", "latest", "Unexpected release tag"),
    ],
)
def test_rejects_unqualified_release(
    github_release_module,
    tmp_path: Path,
    field: str,
    value: object,
    message: str,
):
    metadata = release_metadata(github_release_module, tmp_path)
    metadata[field] = value
    with pytest.raises(github_release_module.ReleaseMetadataError, match=message):
        github_release_module.validate_metadata(metadata)


def test_rejects_missing_asset_digest(github_release_module, tmp_path: Path):
    metadata = release_metadata(github_release_module, tmp_path)
    metadata["assets"][0]["digest"] = None
    with pytest.raises(
        github_release_module.ReleaseMetadataError, match="Invalid SHA-256 digest"
    ):
        github_release_module.validate_metadata(metadata)


def test_rejects_tampered_download(github_release_module, tmp_path: Path):
    metadata = release_metadata(github_release_module, tmp_path)
    _, assets = github_release_module.validate_metadata(metadata)
    (tmp_path / "manifest.json").write_text("tampered\n", encoding="utf-8")
    with pytest.raises(
        github_release_module.ReleaseMetadataError,
        match="GitHub (byte-size|SHA-256) mismatch",
    ):
        github_release_module.verify_downloads(tmp_path, assets)


def test_rejects_unexpected_download(github_release_module, tmp_path: Path):
    metadata = release_metadata(github_release_module, tmp_path)
    _, assets = github_release_module.validate_metadata(metadata)
    (tmp_path / "unexpected.txt").write_text("unexpected\n", encoding="utf-8")
    with pytest.raises(
        github_release_module.ReleaseMetadataError, match="unexpected.txt"
    ):
        github_release_module.verify_downloads(tmp_path, assets)


def test_export_workflow_is_read_only_and_sha_pinned():
    workflow = (
        Path(__file__).resolve().parents[1]
        / ".github/workflows/export-release-for-consumer.yml"
    ).read_text(encoding="utf-8")
    assert "permissions:\n  contents: read" in workflow
    assert "verify-github-release.py" in workflow
    assert "verify-release.py" in workflow
    assert "--require-production" in workflow
    assert "publish_pointer:" in workflow
    assert "actions: read\n      contents: write" in workflow
    assert "consumer/latest-production-artifact.json" in workflow
    assert "actions/artifacts/${EXPECTED_ARTIFACT_ID}" in workflow
    for line in workflow.splitlines():
        if "uses:" in line:
            reference = line.split("uses:", 1)[1].split("#", 1)[0].strip()
            assert re_full_sha_reference(reference), reference


def test_consumer_pointer_schema_and_identity():
    pointer = json.loads(
        (
            Path(__file__).resolve().parents[1]
            / "consumer/latest-production-artifact.json"
        ).read_text(encoding="utf-8")
    )
    assert set(pointer) == {
        "artifact_expires_at",
        "artifact_id",
        "artifact_name",
        "artifact_sha256",
        "artifact_size_bytes",
        "generated_at_utc",
        "manifest_sha256",
        "producer_commit",
        "release_immutable",
        "release_tag",
        "repository",
        "schema_version",
        "workflow_run_id",
    }
    assert pointer["schema_version"] == "1.0.0"
    assert pointer["repository"] == "anshu92/usd-portfolio-market-data"
    assert pointer["release_immutable"] is True
    assert pointer["artifact_name"] == f"validated-market-data-{pointer['release_tag']}"
    assert isinstance(pointer["workflow_run_id"], int)
    assert isinstance(pointer["artifact_id"], int)
    assert isinstance(pointer["artifact_size_bytes"], int)
    assert len(pointer["artifact_sha256"]) == 64
    assert len(pointer["manifest_sha256"]) == 64
    assert len(pointer["producer_commit"]) == 40


def test_production_publish_dispatches_consumer_export():
    workflow = (
        Path(__file__).resolve().parents[1]
        / ".github/workflows/build-market-data.yml"
    ).read_text(encoding="utf-8")
    assert "actions: write" in workflow
    assert "gh workflow run export-release-for-consumer.yml" in workflow


def re_full_sha_reference(reference: str) -> bool:
    owner_action, separator, revision = reference.rpartition("@")
    return bool(owner_action and separator and len(revision) == 40) and all(
        character in "0123456789abcdef" for character in revision
    )
