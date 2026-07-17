from __future__ import annotations

import hashlib
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
    for line in workflow.splitlines():
        if "uses:" in line:
            reference = line.split("uses:", 1)[1].split("#", 1)[0].strip()
            assert re_full_sha_reference(reference), reference


def re_full_sha_reference(reference: str) -> bool:
    owner_action, separator, revision = reference.rpartition("@")
    return bool(owner_action and separator and len(revision) == 40) and all(
        character in "0123456789abcdef" for character in revision
    )
