from __future__ import annotations

import copy

import pytest

from live_snapshot_contract import LiveSnapshotError, validate_snapshot


def ready_snapshot() -> dict[str, object]:
    return {
        "schema_version": "1.0.0",
        "status": "READY",
        "actual_cutoff_utc": "2026-07-25T14:00:00Z",
        "generated_at_utc": "2026-07-25T14:00:01Z",
        "maximum_quote_age_seconds": 5,
        "maximum_clock_skew_seconds": 2,
        "providers": {
            "quote": "fixture-quote-provider",
            "market_status": "fixture-market-status-provider",
            "broker_eligibility": "fixture-broker",
        },
        "securities": [
            {
                "security_id": "XNAS:AAPL",
                "status": "READY",
                "block_reasons": [],
                "requested_at_utc": "2026-07-25T13:59:59Z",
                "received_at_utc": "2026-07-25T14:00:00Z",
                "quote": {
                    "state": "AVAILABLE",
                    "bid": 199.9,
                    "ask": 200.0,
                    "last": 199.95,
                    "bid_size": 100,
                    "ask_size": 200,
                    "session": "REGULAR",
                    "quote_at_utc": "2026-07-25T13:59:59Z",
                },
                "market_status": {
                    "halt_status": "NOT_HALTED",
                    "luld_status": "NORMAL",
                    "lower_band": 180.0,
                    "upper_band": 220.0,
                    "observed_at_utc": "2026-07-25T13:59:59Z",
                },
                "broker_eligibility": {
                    "eligible": True,
                    "reason_codes": [],
                    "observed_at_utc": "2026-07-25T14:00:00Z",
                },
            }
        ],
    }


def test_accepts_fresh_complete_live_snapshot():
    result = validate_snapshot(ready_snapshot())
    assert result == {
        "status": "VALID",
        "snapshot_status": "READY",
        "securities": 1,
        "blocked": 0,
    }


def test_requires_stale_quote_to_block_execution():
    snapshot = ready_snapshot()
    record = snapshot["securities"][0]
    record["quote"]["quote_at_utc"] = "2026-07-25T13:59:50Z"
    record["status"] = "BLOCKED"
    record["block_reasons"] = ["STALE_QUOTE"]
    snapshot["status"] = "BLOCKED"

    result = validate_snapshot(snapshot)
    assert result["blocked"] == 1


def test_rejects_crossed_market_even_if_caller_marks_ready():
    snapshot = ready_snapshot()
    snapshot["securities"][0]["quote"]["bid"] = 201.0
    with pytest.raises(LiveSnapshotError, match="Incorrect status"):
        validate_snapshot(snapshot)


def test_rejects_private_account_or_portfolio_keys():
    for private_key in ("account_id", "portfolio"):
        snapshot = copy.deepcopy(ready_snapshot())
        snapshot[private_key] = "forbidden"
        with pytest.raises(LiveSnapshotError, match="forbidden"):
            validate_snapshot(snapshot)
