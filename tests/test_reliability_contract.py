from __future__ import annotations

import hashlib
import json
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pytest

from enrichment_contract import CONTRACTS, write_parquet
from reliability_contract import apply_dataset_groups


def _file_record(path: Path, rows: int) -> dict[str, object]:
    return {
        "file": path.name,
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "bytes": path.stat().st_size,
        "rows": rows,
    }


def _write_grouped_release(directory: Path) -> dict[str, object]:
    directory.mkdir()
    universe = directory / "security-universe.csv"
    universe.write_text(
        "security_id,ticker,exchange_mic,universe_admission_status\n"
        "ARCX:VTI,VTI,ARCX,ADMITTED_ETF\n",
        encoding="utf-8",
    )
    unmatched = directory / "unmatched-tickers.csv"
    unmatched.write_text("security_id,ticker,reason\n", encoding="utf-8")
    notice = directory / "NOTICE.md"
    notice.write_text("Fixture attribution.\n", encoding="utf-8")
    rows_by_file: dict[str, int] = {
        universe.name: 1,
        unmatched.name: 0,
        notice.name: 1,
    }
    for filename, contract in CONTRACTS.items():
        rows = []
        if filename == "security-master.parquet":
            rows = [{"security_id": "ARCX:VTI", "ticker": "VTI", "cik": "0000000001"}]
        elif filename == "insider-transactions.parquet":
            rows = [{
                "security_id": "ARCX:VTI",
                "issuer_cik": "0000000001",
                "reporting_owner_cik": "0000000099",
                "accession_number": "0000000001-26-000001",
                "transaction_id": "fixture-transaction",
            }]
        rows_by_file[filename] = write_parquet(directory / filename, contract, rows)

    prices = directory / "yahoo-ohlcv-320.parquet"
    splits = directory / "yahoo-splits.parquet"
    con = duckdb.connect()
    try:
        con.execute(
            "CREATE TABLE prices(security_id VARCHAR, ticker VARCHAR, source_symbol VARCHAR, "
            "session_date DATE, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, "
            "volume BIGINT, source_dataset VARCHAR, source_revision VARCHAR, "
            "observed_at_utc VARCHAR)"
        )
        con.execute(
            "INSERT INTO prices VALUES ('ARCX:VTI','VTI','VTI',DATE '2026-07-24',"
            "100,102,99,101,1000,'fixture','market-rev','2026-07-25T00:00:00Z')"
        )
        con.execute("COPY prices TO ? (FORMAT PARQUET)", [str(prices)])
        con.execute(
            "CREATE TABLE splits(security_id VARCHAR, ticker VARCHAR, source_symbol VARCHAR, "
            "event_date DATE, split_factor VARCHAR, source_dataset VARCHAR, "
            "source_revision VARCHAR, observed_at_utc VARCHAR)"
        )
        con.execute("COPY splits TO ? (FORMAT PARQUET)", [str(splits)])
    finally:
        con.close()
    rows_by_file[prices.name] = 1
    rows_by_file[splits.name] = 0
    manifest: dict[str, object] = {
        "schema_version": "1.0.0",
        "status": "READY",
        "created_at_utc": "2026-07-25T00:00:00Z",
        "source": {"revision": "market-rev"},
        "aggregate": {"min_date": "2026-07-24", "max_date": "2026-07-24"},
        "splits": {"rows": 0},
        "validation": {
            "errors": [],
            "warnings": [],
            "expected_latest_xnys_session": "2026-07-24",
            "missing_eligible_sessions": 0,
            "latest_session_coverage": 1.0,
            "benchmark_valid": True,
        },
        "release_files": [
            _file_record(directory / filename, rows)
            for filename, rows in sorted(rows_by_file.items())
        ],
    }
    apply_dataset_groups(manifest, directory)
    (directory / "manifest.json").write_text(
        json.dumps(manifest, sort_keys=True), encoding="utf-8"
    )
    return manifest


def test_failed_insiders_reuses_only_that_group_and_promotes_market(
    compose_module, tmp_path: Path
):
    previous = tmp_path / "previous"
    previous_manifest = _write_grouped_release(previous)
    candidate = tmp_path / "candidate"
    output = tmp_path / "output"
    shutil.copytree(previous, candidate)
    shutil.copytree(previous, output)
    con = duckdb.connect()
    try:
        market_source = str(candidate / "yahoo-ohlcv-320.parquet").replace("'", "''")
        market_target = str(candidate / "market.tmp.parquet").replace("'", "''")
        con.execute(
            f"COPY (SELECT * REPLACE(volume + 1 AS volume) FROM read_parquet('{market_source}')) "
            f"TO '{market_target}' (FORMAT PARQUET)"
        )
        (candidate / "market.tmp.parquet").replace(candidate / "yahoo-ohlcv-320.parquet")
        insider_source = str(candidate / "insider-transactions.parquet").replace("'", "''")
        insider_target = str(candidate / "insider.tmp.parquet").replace("'", "''")
        con.execute(
            f"COPY (SELECT * REPLACE('0000000002' AS issuer_cik) FROM read_parquet('{insider_source}')) "
            f"TO '{insider_target}' (FORMAT PARQUET)"
        )
        (candidate / "insider.tmp.parquet").replace(candidate / "insider-transactions.parquet")
    finally:
        con.close()
    candidate_manifest = json.loads((candidate / "manifest.json").read_text())
    records = {record["file"]: record for record in candidate_manifest["release_files"]}
    records["yahoo-ohlcv-320.parquet"] = _file_record(
        candidate / "yahoo-ohlcv-320.parquet", 1
    )
    records["insider-transactions.parquet"] = _file_record(
        candidate / "insider-transactions.parquet", 1
    )
    candidate_manifest["release_files"] = list(records.values())
    apply_dataset_groups(candidate_manifest, candidate)
    (candidate / "manifest.json").write_text(
        json.dumps(candidate_manifest, sort_keys=True), encoding="utf-8"
    )

    result = compose_module.compose_release(
        candidate, previous, output, "market-data-pinned"
    )
    groups = {record["group_id"]: record for record in result["dataset_groups"]}
    previous_groups = {
        record["group_id"]: record for record in previous_manifest["dataset_groups"]
    }
    assert groups["market"]["state"] == "READY_NEW"
    assert groups["insiders"]["state"] == "READY_REUSED"
    assert groups["insiders"]["group_sha256"] == previous_groups["insiders"]["group_sha256"]
    assert groups["insiders"]["source_group_sha256"] == previous_groups["insiders"]["group_sha256"]
    assert [failure["group_id"] for failure in result["candidate_group_failures"]] == ["insiders"]
    assert hashlib.sha256((output / "insider-transactions.parquet").read_bytes()).hexdigest() == hashlib.sha256((previous / "insider-transactions.parquet").read_bytes()).hexdigest()
    assert hashlib.sha256((output / "yahoo-ohlcv-320.parquet").read_bytes()).hexdigest() == hashlib.sha256((candidate / "yahoo-ohlcv-320.parquet").read_bytes()).hexdigest()


def test_stale_noncore_group_disables_only_its_dependents(
    verify_module, tmp_path: Path
):
    release = tmp_path / "release"
    manifest = _write_grouped_release(release)
    stale = {
        "state": "STALE_DISABLED",
        "mode": "FRESHNESS_GATE",
        "freshness": {
            "clock": "SOURCE_RETRIEVAL_TIME",
            "expected": "2026-07-25T00:00:00Z",
            "observed": "2026-07-01T00:00:00Z",
            "lag_hours": 576.0,
            "lag_calendar_days": 24,
            "state": "DISABLED",
        },
    }
    apply_dataset_groups(
        manifest,
        release,
        group_overrides={"filings_events": stale, "insiders": stale},
    )
    groups = {record["group_id"]: record for record in manifest["dataset_groups"]}
    assert groups["filings_events"]["state"] == "STALE_DISABLED"
    assert groups["insiders"]["state"] == "STALE_DISABLED"
    assert groups["fundamentals"]["state"] == "READY_NEW"
    assert groups["institutional"]["state"] == "READY_NEW"
    con = duckdb.connect()
    try:
        verify_module.verify_dataset_groups(
            con, release, manifest, require_production=True
        )
    finally:
        con.close()


def test_insider_resolution_is_cik_first_and_share_class_constrained(
    enrichment_module,
):
    masters = {
        "XNAS:ONE": {"cik": "0000000001"},
        "XNAS:MULTI.A": {"cik": "0000000002"},
        "XNAS:MULTI.B": {"cik": "0000000002"},
        "XNYS:OTHER": {"cik": "0000000003"},
        "XNYS:NULL": {"cik": None},
    }
    tickers = {
        "ONE": "XNAS:ONE",
        "MULTI.A": "XNAS:MULTI.A",
        "MULTI.B": "XNAS:MULTI.B",
        "OTHER": "XNYS:OTHER",
        "NULL": "XNYS:NULL",
    }

    assert enrichment_module.resolve_insider_security(
        "0000000001", "WRONG", masters, tickers
    ) == ("XNAS:ONE", None)
    assert enrichment_module.resolve_insider_security(
        "0000000002", "MULTI.B", masters, tickers
    ) == ("XNAS:MULTI.B", None)
    assert enrichment_module.resolve_insider_security(
        "0000000002", "OTHER", masters, tickers
    ) == (None, "TICKER_CIK_CONFLICT")
    assert enrichment_module.resolve_insider_security(
        "0000000099", "NULL", masters, tickers
    ) == (None, "MISSING_CANONICAL_CIK")


def test_historical_538_conflicts_and_13_null_ciks_are_all_detected(
    verify_module, tmp_path: Path
):
    master = tmp_path / "security-master.parquet"
    insiders = tmp_path / "insider-transactions.parquet"
    con = duckdb.connect()
    try:
        con.execute(
            "CREATE TABLE master(security_id VARCHAR, cik VARCHAR)"
        )
        con.execute(
            "INSERT INTO master VALUES ('XNAS:CONFLICT', '0000000001'), "
            "('XNAS:NULL', NULL), ('XNAS:GOOD', '0000000003')"
        )
        con.execute("COPY master TO ? (FORMAT PARQUET)", [str(master)])
        con.execute(
            "CREATE TABLE insiders(security_id VARCHAR, issuer_cik VARCHAR)"
        )
        con.execute(
            "INSERT INTO insiders "
            "SELECT 'XNAS:CONFLICT', '0000000002' FROM range(538)"
        )
        con.execute(
            "INSERT INTO insiders "
            "SELECT 'XNAS:NULL', '0000000004' FROM range(13)"
        )
        con.execute("INSERT INTO insiders VALUES ('XNAS:GOOD', '3')")
        con.execute("COPY insiders TO ? (FORMAT PARQUET)", [str(insiders)])
        assert verify_module.insider_cik_violation_count(
            con, insiders, master
        ) == 551
    finally:
        con.close()


def _pointer_fixture(directory: Path):
    tag = "market-data-example"
    commit = "a" * 40
    manifest = directory / "manifest.json"
    manifest.write_text('{"status":"READY"}\n', encoding="utf-8")
    manifest_sha = hashlib.sha256(manifest.read_bytes()).hexdigest()
    (directory / "resolved-tag.txt").write_text(tag + "\n", encoding="utf-8")
    (directory / "github-release.json").write_text(
        '{"tag_name":"market-data-example"}\n', encoding="utf-8"
    )
    now = datetime(2026, 7, 27, 12, 30, tzinfo=timezone.utc)
    artifact = {
        "id": 22,
        "name": f"validated-market-data-{tag}",
        "digest": "sha256:" + "b" * 64,
        "size_in_bytes": 1234,
        "expired": False,
        "expires_at": (now + timedelta(days=8)).isoformat(),
        "workflow_run": {"id": 11, "head_sha": commit},
    }
    run = {
        "id": 11,
        "head_sha": commit,
        "status": "completed",
        "conclusion": "success",
        "updated_at": (now - timedelta(minutes=5)).isoformat(),
    }
    release = {
        "tag_name": tag,
        "draft": False,
        "prerelease": False,
        "immutable": True,
    }
    pointer = {
        "release_tag": tag,
        "workflow_run_id": 11,
        "artifact_id": 22,
        "artifact_name": artifact["name"],
        "artifact_sha256": "b" * 64,
        "artifact_size_bytes": 1234,
        "producer_commit": commit,
        "manifest_sha256": manifest_sha,
        "generated_at_utc": now.isoformat(),
    }
    return pointer, artifact, run, release, now


@pytest.mark.parametrize(
    ("field", "bad_value"),
    [
        ("workflow_run_id", 12),
        ("artifact_id", 23),
        ("artifact_name", "wrong"),
        ("artifact_sha256", "c" * 64),
        ("artifact_size_bytes", 1235),
        ("producer_commit", "d" * 40),
        ("release_tag", "wrong-tag"),
        ("manifest_sha256", "e" * 64),
    ],
)
def test_pointer_rejects_every_identity_mismatch(
    pointer_module, tmp_path: Path, field: str, bad_value: object
):
    pointer, artifact, run, release, now = _pointer_fixture(tmp_path)
    pointer[field] = bad_value
    with pytest.raises(pointer_module.PointerVerificationError):
        pointer_module.verify_pointer(
            pointer, artifact, run, release, tmp_path, now
        )


def test_pointer_accepts_exact_identity_and_rejects_short_retention(
    pointer_module, tmp_path: Path
):
    pointer, artifact, run, release, now = _pointer_fixture(tmp_path)
    pointer_module.verify_pointer(pointer, artifact, run, release, tmp_path, now)
    artifact["expires_at"] = (now + timedelta(days=6)).isoformat()
    with pytest.raises(pointer_module.PointerVerificationError, match="seven days"):
        pointer_module.verify_pointer(
            pointer, artifact, run, release, tmp_path, now
        )
