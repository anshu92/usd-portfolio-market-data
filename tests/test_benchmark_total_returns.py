from __future__ import annotations

import hashlib
import json
import shutil
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pytest

from enrichment_contract import CONTRACTS, write_parquet
from reliability_contract import apply_dataset_groups
from test_reliability_contract import _file_record, _write_grouped_release


def weekday_sessions(end: date, count: int) -> list[date]:
    sessions: list[date] = []
    current = end
    while len(sessions) < count:
        if current.weekday() < 5:
            sessions.append(current)
        current -= timedelta(days=1)
    return sorted(sessions)


def prepare_release(directory: Path) -> tuple[Path, list[date], list[float]]:
    manifest = _write_grouped_release(directory)
    universe = directory / "security-universe.csv"
    universe.write_text(
        "security_id,ticker,exchange_mic,security_type,currency,universe_admission_status\n"
        "ARCX:VTI,VTI,ARCX,ETF,USD,ADMITTED_ETF\n"
        "ARCX:SPY,SPY,ARCX,ETF,USD,ADMITTED_ETF\n"
        "ARCX:BIL,BIL,ARCX,ETF,USD,ADMITTED_ETF\n",
        encoding="utf-8",
    )
    master = directory / "security-master.parquet"
    master_rows = [
        {"security_id": f"ARCX:{ticker}", "ticker": ticker, "cik": cik}
        for ticker, cik in (("VTI", "0000000001"), ("SPY", "0000000002"), ("BIL", "0000000003"))
    ]
    write_parquet(master, CONTRACTS[master.name], master_rows)
    sessions = weekday_sessions(date(2026, 7, 24), 160)
    closes = [100.0 + index * 0.1 for index in range(len(sessions))]
    prices = directory / "yahoo-ohlcv-320.parquet"
    replacement = directory / "prices-replacement.parquet"
    connection = duckdb.connect()
    try:
        connection.execute(
            "CREATE TABLE prices(security_id VARCHAR, ticker VARCHAR, source_symbol VARCHAR, "
            "session_date DATE, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, "
            "volume BIGINT, source_dataset VARCHAR, source_revision VARCHAR, "
            "observed_at_utc VARCHAR)"
        )
        connection.executemany(
            "INSERT INTO prices VALUES (?, 'VTI', 'VTI', ?, ?, ?, ?, ?, 1000, "
            "'fixture', 'market-rev', '2026-07-24T21:00:00Z')",
            [
                (
                    "ARCX:VTI",
                    session,
                    close - 0.1,
                    close + 0.2,
                    close - 0.2,
                    close,
                )
                for session, close in zip(sessions, closes, strict=True)
            ],
        )
        connection.execute("COPY prices TO ? (FORMAT PARQUET)", [str(replacement)])
    finally:
        connection.close()
    replacement.replace(prices)
    records = {record["file"]: record for record in manifest["release_files"]}
    records[universe.name] = _file_record(universe, 3)
    records[master.name] = _file_record(master, 3)
    records[prices.name] = _file_record(prices, len(sessions))
    records["NOTICE.md"]["rows"] = 0
    manifest["release_files"] = list(records.values())
    manifest["created_at_utc"] = "2026-07-24T21:00:00Z"
    manifest["cutoff_date"] = sessions[-1].isoformat()
    manifest["aggregate"] = {
        **manifest["aggregate"],
        "rows": len(sessions),
        "min_date": sessions[0].isoformat(),
        "max_date": sessions[-1].isoformat(),
        "sha256": hashlib.sha256(prices.read_bytes()).hexdigest(),
    }
    manifest["validation"].update(
        {
            "expected_latest_xnys_session": sessions[-1].isoformat(),
            "missing_eligible_sessions": 0,
            "latest_session_coverage": 1.0,
            "benchmark_valid": True,
        }
    )
    for dataset in manifest["datasets"]:
        dataset["source_retrieved_at_utc"] = "2026-07-24T20:00:00Z"
        dataset["source_retrieval_time"] = "2026-07-24T20:00:00Z"
    apply_dataset_groups(manifest, directory)
    (directory / "manifest.json").write_text(
        json.dumps(manifest, sort_keys=True), encoding="utf-8"
    )
    return directory, sessions, closes


def write_source_payload(
    path: Path,
    sessions: list[date],
    closes: list[float],
    *,
    ticker: str = "VTI",
    scale: float = 1.0,
    include_latest: bool = True,
) -> None:
    prior_session = sessions[0] - timedelta(days=1)
    source_sessions = [prior_session, *sessions]
    source_closes = [(closes[0] - 0.1) * scale, *[value * scale for value in closes]]
    dividends = {
        sessions[40]: 0.50 * scale,
        sessions[100]: 0.55 * scale,
        sessions[150]: 0.60 * scale,
    }
    adjusted = [source_closes[0]]
    for index in range(1, len(source_sessions)):
        cash = dividends.get(source_sessions[index], 0.0)
        total_factor = (source_closes[index] + cash) / source_closes[index - 1]
        adjusted.append(adjusted[-1] * total_factor)
    if not include_latest:
        source_sessions.pop()
        source_closes.pop()
        adjusted.pop()
    timestamps = [
        int(datetime.combine(session, time(13, 30), timezone.utc).timestamp())
        for session in source_sessions
    ]
    events = {
        str(index): {
            "amount": amount,
            "date": int(datetime.combine(session, time.min, timezone.utc).timestamp()),
        }
        for index, (session, amount) in enumerate(dividends.items(), start=1)
        if session in source_sessions
    }
    path.write_text(
        json.dumps(
            {
                "chart": {
                    "error": None,
                    "result": [
                        {
                            "meta": {"symbol": ticker, "currency": "USD"},
                            "timestamp": timestamps,
                            "indicators": {
                                "quote": [
                                    {
                                        "open": [value - 0.1 for value in source_closes],
                                        "high": [value + 0.2 for value in source_closes],
                                        "low": [value - 0.2 for value in source_closes],
                                        "close": source_closes,
                                        "volume": [1000] * len(source_closes),
                                    }
                                ],
                                "adjclose": [{"adjclose": adjusted}],
                            },
                            "events": {"dividends": events, "splits": {}},
                        }
                    ],
                }
            }
        ),
        encoding="utf-8",
    )


def write_sources(directory: Path, sessions: list[date], closes: list[float]) -> Path:
    directory.mkdir()
    for ticker, scale in (("VTI", 1.0), ("SPY", 2.0), ("BIL", 0.9)):
        write_source_payload(
            directory / f"{ticker}.json",
            sessions,
            closes,
            ticker=ticker,
            scale=scale,
        )
    return directory


def arguments(release: Path, source_directory: Path) -> SimpleNamespace:
    return SimpleNamespace(
        release_dir=str(release),
        identity_universe=None,
        identity_observed_at="2026-07-24T20:30:00Z",
        expected_session=None,
        benchmark_tickers="VTI,SPY,BIL",
        source_json_dir=str(source_directory),
        retrieved_at="2026-07-24T20:30:00Z",
        certified_at="2026-07-24T20:30:00Z",
        idempotency_key="accounting:2026-07-24:fixture",
        quarantine_out=None,
    )


def test_certifies_current_distribution_adjusted_benchmark_lane(
    benchmark_builder_module,
    decision_builder_module,
    verify_module,
    tmp_path: Path,
) -> None:
    release, sessions, closes = prepare_release(tmp_path / "release")
    source = write_sources(tmp_path / "sources", sessions, closes)

    certification = benchmark_builder_module.attach(arguments(release, source))
    verified = json.loads((release / "manifest.json").read_text())
    connection = duckdb.connect()
    try:
        verify_module.verify_benchmark_total_returns(connection, release, verified)
        verify_module.verify_dataset_groups(
            connection, release, verified, require_production=True
        )
    finally:
        connection.close()
    groups = {record["group_id"]: record for record in verified["dataset_groups"]}
    capabilities = decision_builder_module.capability_records(groups, release)

    assert certification["status"] == "CERTIFIED"
    assert certification["coverage"] == {"required": 3, "valid": 3, "ratio": 1.0}
    assert min(item["sessions"] for item in certification["securities"]) == 160
    assert min(item["weekly_observations"] for item in certification["securities"]) >= 26
    assert sum(item["cash_distributions"] for item in certification["securities"]) == 9
    assert groups["total_returns"]["state"] == "READY_NEW"
    assert capabilities["certified_total_returns"]["state"] == "READY"
    assert capabilities["funded_benchmark_inputs"]["state"] == "READY"


def test_enables_accounting_only_while_benchmark_retrieval_is_fresh(
    benchmark_builder_module,
    decision_builder_module,
    decision_verify_module,
    tmp_path: Path,
) -> None:
    if shutil.which("zstd") is None:
        pytest.skip("zstd is required")
    release, sessions, closes = prepare_release(tmp_path / "release")
    source = write_sources(tmp_path / "sources", sessions, closes)
    benchmark_builder_module.attach(arguments(release, source))
    source_manifest_path = release / "manifest.json"
    source_manifest = json.loads(source_manifest_path.read_text())
    for group in source_manifest["dataset_groups"]:
        if group["group_id"] in {"identity", "market"}:
            group["state"] = "STALE_DISABLED"
            group["freshness"]["state"] = "STALE"
            if group["group_id"] == "market":
                group["freshness"]["lag_eligible_sessions"] = 1
    source_manifest_path.write_text(json.dumps(source_manifest), encoding="utf-8")

    fresh_output = tmp_path / "fresh-decision"
    decision_builder_module.build(
        SimpleNamespace(
            release_dir=str(release),
            source_tag="market-data-20260724T203000Z",
            out_dir=str(fresh_output),
            generated_at="2026-07-24T20:35:00Z",
            decision_cutoff="2026-07-24T20:35:00Z",
            producer_commit="LOCAL_TEST",
        )
    )
    fresh_accounting = json.loads(
        (fresh_output / "phase-packs/accounting.json").read_text()
    )
    assert fresh_accounting["status"] == "READY"
    assert "BENCHMARK_ONLY_SAFE" in fresh_accounting["operating_modes"]
    sunday = json.loads((fresh_output / "phase-packs/sunday.json").read_text())
    assert sunday["status"] == "DEGRADED"
    assert "BENCHMARK_ONLY_SAFE" in sunday["operating_modes"]
    assert "CHALLENGER_BLOCKED" in sunday["operating_modes"]
    phase_result = decision_verify_module.evaluate_phase_at(
        fresh_output,
        "accounting",
        datetime(2026, 7, 24, 20, 45, tzinfo=timezone.utc),
    )
    assert phase_result["status"] == "USABLE"
    expired_result = decision_verify_module.evaluate_phase_at(
        fresh_output,
        "accounting",
        datetime(2026, 7, 24, 21, 31, tzinfo=timezone.utc),
    )
    assert expired_result["status"] == "BLOCKED"
    assert "CAPABILITY_CERTIFIED_TOTAL_RETURNS_STALE_AT_AS_OF" in expired_result[
        "rejection_codes"
    ]

    groups = {
        record["group_id"]: record
        for record in json.loads((release / "manifest.json").read_text())[
            "dataset_groups"
        ]
    }
    stale = decision_builder_module.capability_records(
        groups,
        release,
        as_of=datetime(2026, 7, 24, 21, 31, tzinfo=timezone.utc),
    )
    assert stale["certified_total_returns"]["state"] == "STALE"
    assert stale["funded_benchmark_inputs"]["state"] == "STALE"


def test_stale_source_cannot_publish_benchmark_lane(
    benchmark_builder_module, tmp_path: Path
) -> None:
    release, sessions, closes = prepare_release(tmp_path / "release")
    manifest_before = (release / "manifest.json").read_bytes()
    source = write_sources(tmp_path / "stale-sources", sessions, closes)
    write_source_payload(
        source / "VTI.json", sessions, closes, ticker="VTI", include_latest=False
    )

    assert (
        benchmark_builder_module.main(
            [
                "--release-dir",
                str(release),
                "--source-json-dir",
                str(source),
                "--retrieved-at",
                "2026-07-24T20:30:00Z",
            ]
        )
        == 2
    )
    assert (release / "manifest.json").read_bytes() == manifest_before
    assert not (release / "benchmark-distributions.parquet").exists()
    assert not (release / "benchmark-total-returns.parquet").exists()


def test_unreconciled_adjusted_close_cannot_publish_benchmark_lane(
    benchmark_builder_module, tmp_path: Path
) -> None:
    release, sessions, closes = prepare_release(tmp_path / "release")
    source = write_sources(tmp_path / "unreconciled-sources", sessions, closes)
    source_file = source / "VTI.json"
    payload = json.loads(source_file.read_text())
    payload["chart"]["result"][0]["indicators"]["adjclose"][0]["adjclose"][-1] *= 1.01
    source_file.write_text(json.dumps(payload), encoding="utf-8")

    assert benchmark_builder_module.main(
        [
            "--release-dir",
            str(release),
            "--source-json-dir",
            str(source),
            "--retrieved-at",
            "2026-07-24T20:30:00Z",
        ]
    ) == 2
    assert not (release / "benchmark-distributions.parquet").exists()
    assert not (release / "benchmark-total-returns.parquet").exists()


def test_failed_correction_removes_carried_forward_lane_and_records_quarantine(
    benchmark_builder_module, verify_module, tmp_path: Path
) -> None:
    release, sessions, closes = prepare_release(tmp_path / "release")
    valid = write_sources(tmp_path / "valid-sources", sessions, closes)
    benchmark_builder_module.attach(arguments(release, valid))
    stale = write_sources(tmp_path / "stale-sources", sessions, closes)
    write_source_payload(
        stale / "SPY.json",
        sessions,
        closes,
        ticker="SPY",
        scale=2.0,
        include_latest=False,
    )
    quarantine = tmp_path / "benchmark-quarantine.json"

    assert benchmark_builder_module.main(
        [
            "--release-dir",
            str(release),
            "--source-json-dir",
            str(stale),
            "--retrieved-at",
            "2026-07-24T21:00:00Z",
            "--quarantine-out",
            str(quarantine),
        ]
    ) == 2

    manifest = json.loads((release / "manifest.json").read_text())
    groups = {item["group_id"]: item for item in manifest["dataset_groups"]}
    assert groups["total_returns"]["state"] == "NOT_CONFIGURED"
    assert manifest["candidate_attempt_failures"][-1]["attempt_state"] == "QUARANTINED"
    assert json.loads(quarantine.read_text())["state"] == "QUARANTINED"
    connection = duckdb.connect()
    try:
        verify_module.verify_dataset_groups(
            connection, release, manifest, require_production=True
        )
    finally:
        connection.close()
