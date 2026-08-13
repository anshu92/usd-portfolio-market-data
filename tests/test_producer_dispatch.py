from __future__ import annotations

from pathlib import Path

import duckdb
import pytest


def test_dispatch_plan_uses_early_close_and_retry_deadlines(
    producer_dispatch_module,
) -> None:
    events = producer_dispatch_module.accounting_plan("2026-11-27")

    assert [event["attempt"] for event in events] == [
        "SOURCE_CLOSE_PLUS_45",
        "SOURCE_CLOSE_PLUS_75",
        "SOURCE_CLOSE_PLUS_120",
    ]
    assert events[0]["scheduled_time"] == "2026-11-27T18:45:00Z"
    assert all(event["early_close"] is True for event in events)
    assert len({event["idempotency_key"] for event in events}) == 3


def test_dispatch_plan_has_distinct_pre_cutoff_exception_artifacts(
    producer_dispatch_module,
) -> None:
    events = [
        event
        for event in producer_dispatch_module.decision_phase_plan("2026-08-12")
        if event["phase"] == "exception_monitoring"
    ]

    assert [event["window_id"] for event in events] == [
        "OPEN_EXCEPTION",
        "CLOSE_EXCEPTION",
    ]
    assert [event["scheduled_time"] for event in events] == [
        "2026-08-13T13:40:00Z",
        "2026-08-13T19:10:00Z",
    ]
    assert [event["artifact_deadline"] for event in events] == [
        "2026-08-13T13:55:00Z",
        "2026-08-13T19:25:00Z",
    ]
    assert len({event["idempotency_key"] for event in events}) == 2


def test_accounting_decision_refresh_starts_inside_early_close_window(
    producer_dispatch_module,
) -> None:
    event = next(
        event
        for event in producer_dispatch_module.decision_phase_plan("2026-11-27")
        if event["phase"] == "accounting"
    )

    assert event["scheduled_time"] == "2026-11-27T18:40:00Z"
    assert event["artifact_deadline"] == "2026-11-27T18:45:00Z"
    assert event["early_close"] is True


def test_dispatch_rejects_xnys_holiday(producer_dispatch_module) -> None:
    with pytest.raises(producer_dispatch_module.DispatchError, match="not an XNYS session"):
        producer_dispatch_module.accounting_plan("2026-11-26")


def test_sunday_dispatch_uses_local_timezone_across_dst(
    producer_dispatch_module,
) -> None:
    event = producer_dispatch_module.sunday_plan("2026-11-27")[0]

    assert event["scheduled_time"] == "2026-11-29T22:15:00Z"
    assert event["decision_cutoff"] == "2026-11-29T22:30:00Z"


def test_coverage_diagnostic_freezes_denominator_and_reconciles_counts(
    coverage_diagnostic_module, tmp_path: Path
) -> None:
    universe = [
        {
            "security_id": "ARCX:VTI",
            "ticker": "VTI",
            "exchange_mic": "ARCX",
            "security_type": "ETF",
        },
        {
            "security_id": "ARCX:SPY",
            "ticker": "SPY",
            "exchange_mic": "ARCX",
            "security_type": "ETF",
        },
    ]
    prices = tmp_path / "prices.parquet"
    connection = duckdb.connect()
    try:
        connection.execute(
            """
            COPY (
              SELECT 'ARCX:VTI' security_id, DATE '2026-11-27' session_date,
                     100.0 AS open, 101.0 AS high, 99.0 AS low,
                     100.5 AS "close", 1000 AS volume
            ) TO ? (FORMAT PARQUET)
            """,
            [str(prices)],
        )
    finally:
        connection.close()
    manifest = {
        "status": "VALIDATION_FAILED",
        "validation": {"expected_latest_xnys_session": "2026-11-27"},
        "universe": {"sha256": "0" * 64},
        "source": {
            "quality": {
                "unresolved_invalid_rows": [],
                "stale_source_symbols": [],
                "common_mode_candidate_failures": [],
                "repair": {"requested_rows": 0, "responses": [], "failures": []},
            },
            "yahoo_chart_supplement": {
                "requested": 2,
                "responses": [{"security_id": "ARCX:VTI"}],
                "failures": [{"security_id": "ARCX:SPY"}],
            },
        },
    }

    diagnostic = coverage_diagnostic_module.build(manifest, universe, prices)

    assert diagnostic["frozen_eligible_universe"]["denominator"] == 2
    assert diagnostic["coverage"] == {
        "valid_current_session_rows": 1,
        "missing": 1,
        "stale": 0,
        "invalid": 0,
        "quarantined": 0,
        "ratio": 0.5,
        "required_ratio": 0.95,
    }
    assert diagnostic["counts_by_failure_reason"] == {
        "PROVIDER_RETRIES_EXHAUSTED": 1
    }
