from __future__ import annotations

import hashlib
import json
import shutil
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pytest

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
    include_latest: bool = True,
) -> None:
    prior_session = sessions[0] - timedelta(days=1)
    source_sessions = [prior_session, *sessions]
    source_closes = [closes[0] - 0.1, *closes]
    dividends = {sessions[40]: 0.50, sessions[100]: 0.55, sessions[150]: 0.60}
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
                            "meta": {"symbol": "VTI", "currency": "USD"},
                            "timestamp": timestamps,
                            "indicators": {
                                "quote": [{"close": source_closes}],
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


def arguments(release: Path, source: Path) -> SimpleNamespace:
    return SimpleNamespace(
        release_dir=str(release),
        benchmark_ticker="VTI",
        source_json=str(source),
        source_locator="https://query2.finance.yahoo.com/fixture/VTI",
        retrieved_at="2026-07-24T20:30:00Z",
    )


def test_certifies_current_distribution_adjusted_benchmark_lane(
    benchmark_builder_module,
    decision_builder_module,
    verify_module,
    tmp_path: Path,
) -> None:
    release, sessions, closes = prepare_release(tmp_path / "release")
    source = tmp_path / "vti.json"
    write_source_payload(source, sessions, closes)

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
    assert certification["sessions"] == 160
    assert certification["weekly_observations"] >= 26
    assert certification["distributions"] == 3
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
    source = tmp_path / "vti.json"
    write_source_payload(source, sessions, closes)
    benchmark_builder_module.attach(arguments(release, source))

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
    source = tmp_path / "stale-vti.json"
    write_source_payload(source, sessions, closes, include_latest=False)

    assert (
        benchmark_builder_module.main(
            [
                "--release-dir",
                str(release),
                "--source-json",
                str(source),
                "--source-locator",
                "https://query2.finance.yahoo.com/fixture/VTI",
                "--retrieved-at",
                "2026-07-24T20:30:00Z",
            ]
        )
        == 2
    )
    assert (release / "manifest.json").read_bytes() == manifest_before
    assert not (release / "distributions.parquet").exists()
    assert not (release / "benchmark-total-returns.parquet").exists()


def test_unreconciled_adjusted_close_cannot_publish_benchmark_lane(
    benchmark_builder_module, tmp_path: Path
) -> None:
    release, sessions, closes = prepare_release(tmp_path / "release")
    source = tmp_path / "unreconciled-vti.json"
    write_source_payload(source, sessions, closes)
    payload = json.loads(source.read_text())
    payload["chart"]["result"][0]["indicators"]["adjclose"][0]["adjclose"][-1] *= 1.01
    source.write_text(json.dumps(payload), encoding="utf-8")

    assert benchmark_builder_module.main(
        [
            "--release-dir",
            str(release),
            "--source-json",
            str(source),
            "--source-locator",
            "https://query2.finance.yahoo.com/fixture/VTI",
            "--retrieved-at",
            "2026-07-24T20:30:00Z",
        ]
    ) == 2
    assert not (release / "distributions.parquet").exists()
    assert not (release / "benchmark-total-returns.parquet").exists()
