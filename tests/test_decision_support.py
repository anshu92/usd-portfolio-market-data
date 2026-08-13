from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
import duckdb

from decision_support_contract import (
    ACTIONABILITY_MATRIX_FILENAME,
    CANDIDATE_FUNNEL_FILENAME,
    DATABASE_FILENAME,
    EVIDENCE_PACKETS_FILENAME,
    MANIFEST_FILENAME,
    PHASES,
)
from test_reliability_contract import _write_grouped_release


pytestmark = pytest.mark.skipif(shutil.which("zstd") is None, reason="zstd is required")


def source_release(tmp_path: Path) -> Path:
    release = tmp_path / "source"
    manifest = _write_grouped_release(release)
    manifest["cutoff_date"] = "2026-07-24"
    (release / "manifest.json").write_text(
        json.dumps(manifest, sort_keys=True), encoding="utf-8"
    )
    return release


def build_artifact(
    decision_builder_module,
    release: Path,
    output: Path,
    producer_commit: str = "LOCAL_TEST",
):
    return decision_builder_module.build(
        SimpleNamespace(
            release_dir=str(release),
            source_tag="market-data-20260725T000000Z",
            out_dir=str(output),
            generated_at="2026-07-25T01:00:00Z",
            decision_cutoff="2026-07-25T01:00:00Z",
            producer_commit=producer_commit,
        )
    )


def test_builds_and_verifies_compact_read_model(
    decision_builder_module, decision_verify_module, tmp_path: Path
):
    release = source_release(tmp_path)
    output = tmp_path / "decision"

    manifest = build_artifact(decision_builder_module, release, output)
    result = decision_verify_module.verify(output)

    assert result["status"] == "VALID"
    assert result["phase_packs"] == len(PHASES)
    assert (output / DATABASE_FILENAME).stat().st_size < 50 * 1024 * 1024
    assert manifest["database"]["market_data_role"] == "NON_EXECUTABLE_RESEARCH_PROXY"
    assert manifest["contract_version"] == "1.1.0"
    assert manifest["valid_for_session"] == "2026-07-24"
    assert manifest["data_cutoff_utc"] == "2026-07-24T20:00:00Z"
    assert manifest["task_timezone"] == "America/Toronto"
    assert manifest["exchange_timezone"] == "America/New_York"
    assert manifest["validator_identity"]["contract_version"] == "1.1.0"
    for filename in (
        CANDIDATE_FUNNEL_FILENAME,
        ACTIONABILITY_MATRIX_FILENAME,
        EVIDENCE_PACKETS_FILENAME,
    ):
        assert (output / filename).is_file()
    packs = {
        phase_id: json.loads((output / "phase-packs" / f"{phase_id}.json").read_text())
        for phase_id in PHASES
    }
    assert packs["accounting"]["status"] == "BLOCKED"
    assert packs["execution_research"]["consumer_live_snapshot_required"] is True
    assert (
        packs["execution_research"]["external_capabilities"][0]["state"]
        == "CONSUMER_REQUIRED"
    )
    assert "LIVE_SNAPSHOT_REQUIRED" in packs["execution_research"]["operating_modes"]
    assert "CHALLENGER_BLOCKED" in packs["pre_open"]["operating_modes"]
    assert packs["pre_open"]["not_before_utc"] < packs["pre_open"]["expires_at_utc"]
    assert packs["pre_open"]["valid_for_session"] == "2026-07-27"
    assert len(packs["exception_monitoring"]["phase_windows"]) == 2
    execution_evaluation = decision_verify_module.evaluate_phase_at(
        output,
        "execution_research",
        datetime(2026, 7, 27, 13, 38, tzinfo=timezone.utc),
        allow_degraded=True,
    )
    assert execution_evaluation["status"] == "BLOCKED"
    assert "CONSUMER_LIVE_SNAPSHOT_REQUIRED" in execution_evaluation[
        "rejection_codes"
    ]

    database = tmp_path / "read-model.sqlite"
    decision_verify_module.decompress_database(output / DATABASE_FILENAME, database)
    connection = sqlite3.connect(f"file:{database}?mode=ro&immutable=1", uri=True)
    try:
        metadata = dict(connection.execute("SELECT key, value FROM metadata"))
        assert metadata["market_data_role"] == "NON_EXECUTABLE_RESEARCH_PROXY"
        assert metadata["price_adjustment"] == "RAW_CLOSE_NOT_DIVIDEND_ADJUSTED"
        assert connection.execute(
            "SELECT count(*) FROM market_snapshot"
        ).fetchone() == (1,)
        assert connection.execute(
            "SELECT count(*) FROM security WHERE is_current=1"
        ).fetchone() == (1,)
    finally:
        connection.close()


def test_build_is_deterministic_for_identical_inputs(
    decision_builder_module, tmp_path: Path
):
    release = source_release(tmp_path)
    first = tmp_path / "first"
    second = tmp_path / "second"

    build_artifact(decision_builder_module, release, first)
    build_artifact(decision_builder_module, release, second)

    assert (
        hashlib.sha256((first / DATABASE_FILENAME).read_bytes()).digest()
        == hashlib.sha256((second / DATABASE_FILENAME).read_bytes()).digest()
    )
    assert (first / MANIFEST_FILENAME).read_bytes() == (
        second / MANIFEST_FILENAME
    ).read_bytes()


def test_targeted_artifact_is_bound_to_one_pre_cutoff_window(
    decision_builder_module, decision_verify_module, tmp_path: Path
):
    release = source_release(tmp_path)
    output = tmp_path / "targeted"

    manifest = decision_builder_module.build(
        SimpleNamespace(
            release_dir=str(release),
            source_tag="market-data-20260725T000000Z",
            out_dir=str(output),
            generated_at="2026-07-27T11:56:00Z",
            decision_cutoff=None,
            producer_commit="LOCAL_TEST",
            target_phase="pre_open",
            target_window="PRE_OPEN",
            reference_session="2026-07-24",
            scheduled_time="2026-07-27T11:55:00Z",
            artifact_deadline="2026-07-27T12:10:00Z",
            idempotency_key="decision:pre_open:2026-07-24:pre_open",
        )
    )

    decision_verify_module.verify(output)
    assert manifest["publication_target"] == {
        "phase_id": "pre_open",
        "window_id": "PRE_OPEN",
        "reference_session": "2026-07-24",
        "scheduled_time_utc": "2026-07-27T11:55:00Z",
        "artifact_deadline_utc": "2026-07-27T12:10:00Z",
        "idempotency_key": "decision:pre_open:2026-07-24:pre_open",
    }


def test_targeted_artifact_rejects_generation_after_deadline(
    decision_builder_module, tmp_path: Path
):
    release = source_release(tmp_path)

    with pytest.raises(
        decision_builder_module.DecisionSupportBuildError,
        match="after its phase deadline",
    ):
        decision_builder_module.build(
            SimpleNamespace(
                release_dir=str(release),
                source_tag="market-data-20260725T000000Z",
                out_dir=str(tmp_path / "late"),
                generated_at="2026-07-27T12:10:01Z",
                decision_cutoff=None,
                producer_commit="LOCAL_TEST",
                target_phase="pre_open",
                target_window="PRE_OPEN",
                reference_session="2026-07-24",
                scheduled_time="2026-07-27T11:55:00Z",
                artifact_deadline="2026-07-27T12:10:00Z",
                idempotency_key="decision:pre_open:2026-07-24:pre_open",
            )
        )


def test_terminal_target_cannot_treat_prior_session_market_as_current(
    decision_builder_module, tmp_path: Path
):
    release = source_release(tmp_path)
    output = tmp_path / "terminal"

    decision_builder_module.build(
        SimpleNamespace(
            release_dir=str(release),
            source_tag="market-data-20260725T000000Z",
            out_dir=str(output),
            generated_at="2026-07-27T20:06:00Z",
            decision_cutoff=None,
            producer_commit="LOCAL_TEST",
            target_phase="terminal_review",
            target_window="TERMINAL_REVIEW",
            reference_session="2026-07-27",
            scheduled_time="2026-07-27T20:05:00Z",
            artifact_deadline="2026-07-27T20:20:00Z",
            idempotency_key="decision:terminal_review:2026-07-27:terminal_review",
        )
    )

    pack = json.loads((output / "phase-packs/terminal_review.json").read_text())
    broad_market = next(
        record
        for record in pack["required_capabilities"]
        if record["capability_id"] == "broad_market_current"
    )
    assert pack["status"] == "BLOCKED"
    assert broad_market["state"] == "STALE"
    assert "required=2026-07-27" in broad_market["reason"]


def test_evidence_index_keeps_latest_revision_for_duplicate_event_ids(
    decision_builder_module, decision_verify_module, tmp_path: Path
):
    release = source_release(tmp_path)
    events = release / "corporate-events.parquet"
    connection = duckdb.connect()
    replacement = release / "corporate-events-revised.parquet"
    try:
        connection.execute(
            "CREATE TABLE revised AS SELECT * FROM read_parquet(?)", [str(events)]
        )
        columns = [row[0] for row in connection.execute("DESCRIBE revised").fetchall()]
        base = {column: None for column in columns}
        base.update(
            {
                "event_id": "duplicate-event",
                "security_id": "ARCX:VTI",
                "event_type": "CORPORATE_ACTION",
                "announcement_datetime_utc": "2026-07-24T12:00:00Z",
                "effective_date": "2026-07-24",
                "source_document_url": "https://www.sec.gov/Archives/duplicate",
                "headline": "Older revision",
                "source_retrieved_at_utc": "2026-07-24T12:01:00Z",
            }
        )
        newer = dict(base)
        newer["headline"] = "Newer revision"
        newer["source_retrieved_at_utc"] = "2026-07-24T12:02:00Z"
        placeholders = ",".join("?" for _ in columns)
        connection.executemany(
            f"INSERT INTO revised VALUES ({placeholders})",
            [[row[column] for column in columns] for row in (base, newer)],
        )
        connection.execute("COPY revised TO ? (FORMAT PARQUET)", [str(replacement)])
    finally:
        connection.close()
    replacement.replace(events)
    manifest = json.loads((release / "manifest.json").read_text())
    for record in manifest["release_files"]:
        if record["file"] == events.name:
            record["bytes"] = events.stat().st_size
            record["sha256"] = hashlib.sha256(events.read_bytes()).hexdigest()
            record["rows"] = 2
    (release / "manifest.json").write_text(json.dumps(manifest, sort_keys=True))

    output = tmp_path / "decision"
    build_artifact(decision_builder_module, release, output)
    assert decision_verify_module.verify(output)["status"] == "VALID"


def test_verifier_rejects_tampered_phase_pack(
    decision_builder_module, decision_verify_module, tmp_path: Path
):
    release = source_release(tmp_path)
    output = tmp_path / "decision"
    build_artifact(decision_builder_module, release, output)
    phase = output / "phase-packs" / "pre_open.json"
    phase.write_text("{}\n", encoding="utf-8")

    with pytest.raises(
        decision_verify_module.DecisionSupportVerificationError,
        match="phase pack pre_open (byte-size|SHA-256) mismatch",
    ):
        decision_verify_module.verify(output)


def test_pointer_binds_artifact_run_and_source_release(
    decision_builder_module, decision_pointer_module, tmp_path: Path
):
    release_dir = source_release(tmp_path)
    artifact_dir = tmp_path / "decision"
    manifest = build_artifact(
        decision_builder_module, release_dir, artifact_dir, "b" * 40
    )
    manifest_sha = hashlib.sha256(
        (artifact_dir / MANIFEST_FILENAME).read_bytes()
    ).hexdigest()
    source_tag = manifest["source_release"]["tag"]
    completed = "2026-07-25T01:05:00Z"
    artifact = {
        "id": 10,
        "name": f"decision-support-{source_tag}-20",
        "digest": f"sha256:{'a' * 64}",
        "size_in_bytes": 123,
        "expired": False,
        "expires_at": "2026-08-24T01:05:00Z",
        "workflow_run": {"id": 20, "head_sha": "b" * 40},
    }
    run = {
        "id": 20,
        "head_sha": "b" * 40,
        "head_repository": {"full_name": "anshu92/usd-portfolio-market-data"},
        "head_branch": "main",
        "path": ".github/workflows/build-decision-support.yml",
        "workflow_id": 332931733,
        "event": "workflow_dispatch",
        "status": "completed",
        "conclusion": "success",
        "updated_at": completed,
    }
    release = {
        "tag_name": source_tag,
        "draft": False,
        "prerelease": False,
        "immutable": True,
    }
    pointer = {
        "schema_version": "1.0.0",
        "source_release_tag": source_tag,
        "source_manifest_sha256": manifest["source_release"]["manifest_sha256"],
        "workflow_run_id": 20,
        "artifact_id": 10,
        "artifact_name": artifact["name"],
        "artifact_sha256": "a" * 64,
        "artifact_size_bytes": 123,
        "artifact_expires_at": "2026-08-24T01:05:00Z",
        "source_release_immutable": True,
        "producer_commit": "b" * 40,
        "repository": "anshu92/usd-portfolio-market-data",
        "workflow_id": 332931733,
        "workflow_path": ".github/workflows/build-decision-support.yml",
        "workflow_branch": "main",
        "workflow_event": "workflow_dispatch",
        "validator_contract_version": "1.1.0",
        "validator_set_sha256": manifest["validator_identity"]["validator_set_sha256"],
        "promotion_key": f"{source_tag}/10",
        "decision_support_manifest_sha256": manifest_sha,
        "generated_at_utc": "2026-07-25T01:06:00Z",
    }

    decision_pointer_module.verify_pointer(
        pointer,
        artifact,
        run,
        release,
        artifact_dir,
        datetime(2026, 7, 25, 1, 6, tzinfo=timezone.utc),
    )
    pointer["source_manifest_sha256"] = "c" * 64
    with pytest.raises(
        decision_pointer_module.DecisionSupportPointerError,
        match="Embedded source manifest SHA-256 mismatch",
    ):
        decision_pointer_module.verify_pointer(
            pointer,
            artifact,
            run,
            release,
            artifact_dir,
            datetime(2026, 7, 25, 1, 6, tzinfo=timezone.utc),
        )
