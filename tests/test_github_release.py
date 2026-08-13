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


def test_validates_a_nonempty_downloaded_release_subset(
    github_release_module, tmp_path: Path
):
    metadata = release_metadata(github_release_module, tmp_path)
    _, assets = github_release_module.validate_metadata(metadata)
    for path in tuple(tmp_path.iterdir()):
        if path.name not in {"manifest.json", "security-universe.csv"}:
            path.unlink()
    github_release_module.verify_downloads(tmp_path, assets, allow_subset=True)

    (tmp_path / "manifest.json").unlink()
    (tmp_path / "security-universe.csv").unlink()
    with pytest.raises(
        github_release_module.ReleaseMetadataError, match="subset is empty"
    ):
        github_release_module.verify_downloads(tmp_path, assets, allow_subset=True)


def test_export_workflow_is_read_only_and_sha_pinned():
    workflow = (
        Path(__file__).resolve().parents[1]
        / ".github/workflows/export-release-for-consumer.yml"
    ).read_text(encoding="utf-8")
    assert "permissions:\n  contents: read" in workflow
    assert "verify-github-release.py" in workflow
    assert "verify-release.py" in workflow
    assert "--require-production" in workflow
    assert "contents: write" not in workflow
    pointer_workflow = (
        Path(__file__).resolve().parents[1]
        / ".github/workflows/publish-consumer-pointer.yml"
    ).read_text(encoding="utf-8")
    assert "workflow_run:" in pointer_workflow
    assert "workflow_dispatch:" in pointer_workflow
    assert "github.event.workflow_run.conclusion == 'success'" in pointer_workflow
    assert "actions: read\n      contents: write" in pointer_workflow
    assert "verify-consumer-pointer.py" in pointer_workflow
    assert "consumer/latest-production-artifact.json" in pointer_workflow
    assert "actions/artifacts/${artifact_id}" in pointer_workflow
    assert ".workflow_id == 314869492" in pointer_workflow
    assert "steps.artifact.outputs.run_id" in pointer_workflow
    assert "gh workflow run publish-consumer-pointer.yml" in workflow
    for line in (workflow + pointer_workflow).splitlines():
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
    assert "gh workflow run build-decision-support.yml" in workflow
    assert '-f source_tag="$TAG"' in workflow


def test_decision_support_workflows_are_pinned_and_least_privilege():
    workflows = Path(__file__).resolve().parents[1] / ".github/workflows"
    build = (workflows / "build-decision-support.yml").read_text(encoding="utf-8")
    pointer = (workflows / "publish-decision-support-pointer.yml").read_text(
        encoding="utf-8"
    )
    assert "permissions:\n  contents: read" in build
    assert "request_pointer_publication:" in build
    assert "contents: write" not in build
    assert build.count("actions: write") == 1
    assert "gh workflow run publish-decision-support-pointer.yml" in build
    assert 'source_run_id="$GITHUB_RUN_ID"' in build
    assert "sec-company-facts.parquet" not in build
    assert "institutional-holdings-13f.parquet" not in build
    assert "verify-decision-support.py --dist dist" in build
    assert "workflow_run:" in pointer
    assert '.path == ".github/workflows/build-decision-support.yml"' in pointer
    assert "actions: read\n      contents: write" in pointer
    assert "verify-decision-support-pointer.py" in pointer
    assert "consumer/latest-decision-support-artifact.json" in pointer
    for line in (build + pointer).splitlines():
        if "uses:" in line:
            reference = line.split("uses:", 1)[1].split("#", 1)[0].strip()
            assert re_full_sha_reference(reference), reference


def test_producer_workflow_artifacts_are_bounded_and_compressed():
    workflows = Path(__file__).resolve().parents[1] / ".github/workflows"
    combined = "\n".join(
        path.read_text(encoding="utf-8") for path in sorted(workflows.glob("*.yml"))
    )
    upload_count = combined.count("actions/upload-artifact@")
    assert upload_count == 6
    assert combined.count("compression-level: 9") == upload_count
    assert "compression-level: 0" not in combined
    assert combined.count("verify-workflow-artifact-size.py") == upload_count


def test_recovery_workflow_is_bounded_and_trusts_only_production_artifacts():
    workflow = (
        Path(__file__).resolve().parents[1]
        / ".github/workflows/recover-market-data-release.yml"
    ).read_text(encoding="utf-8")
    assert "timeout-minutes: 20" in workflow
    assert "timeout-minutes: 10" in workflow
    assert ".workflow_id == 314836103" in workflow
    assert '.head_branch == "main"' in workflow
    assert "run-id: ${{ inputs.source_run_id }}" in workflow
    assert "actions: write\n      contents: write" in workflow
    assert (
        "python verify-release.py --dist dist --require-ready --require-production"
        in workflow
    )
    for line in workflow.splitlines():
        if "uses:" in line:
            reference = line.split("uses:", 1)[1].split("#", 1)[0].strip()
            assert re_full_sha_reference(reference), reference


def re_full_sha_reference(reference: str) -> bool:
    owner_action, separator, revision = reference.rpartition("@")
    return bool(owner_action and separator and len(revision) == 40) and all(
        character in "0123456789abcdef" for character in revision
    )
