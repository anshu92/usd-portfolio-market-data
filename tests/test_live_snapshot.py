from __future__ import annotations

import copy

import pytest

from live_snapshot_contract import LiveSnapshotError, validate_snapshot


def ready_snapshot() -> dict[str, object]:
    return {
        "schema_version": "1.0.0",
        "contract_version": "1.1.0",
        "status": "READY",
        "actual_cutoff_utc": "2026-07-25T14:00:00Z",
        "generated_at_utc": "2026-07-25T14:00:01Z",
        "maximum_quote_age_seconds": 5,
        "maximum_market_status_age_seconds": 5,
        "maximum_broker_age_seconds": 300,
        "maximum_clock_skew_seconds": 2,
        "spread_policy": {
            "maximum_relative_spread_bps": 10,
            "maximum_absolute_spread": 1.0,
            "block_on_crossed_market": True,
        },
        "providers": {
            "quote": {
                "provider_id": "fixture-quote-provider",
                "feed_type": "SIP_NBBO",
                "entitlement_scope": "US_EQUITIES_REALTIME",
                "request_id": "quote-request-1",
                "provider_response_id": "quote-response-1",
                "observed_at_utc": "2026-07-25T14:00:00Z",
            },
            "market_status": {
                "provider_id": "fixture-status-provider",
                "feed_type": "SIP_STATUS",
                "entitlement_scope": "US_EQUITIES_REALTIME",
                "request_id": "status-request-1",
                "provider_response_id": "status-response-1",
                "observed_at_utc": "2026-07-25T14:00:00Z",
            },
            "broker_eligibility": {
                "provider_id": "fixture-broker",
                "feed_type": "BROKER_ELIGIBILITY",
                "entitlement_scope": "SYMBOL_ELIGIBILITY_ONLY",
                "request_id": "broker-request-1",
                "provider_response_id": "broker-response-1",
                "observed_at_utc": "2026-07-25T14:00:00Z",
            },
        },
        "securities": [
            {
                "security_id": "XNAS:AAPL",
                "symbol": "AAPL",
                "exchange_mic": "XNAS",
                "currency": "USD",
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
                    "feed_type": "SIP_NBBO",
                    "symbol": "AAPL",
                    "exchange_mic": "XNAS",
                    "currency": "USD",
                },
                "market_status": {
                    "halt_status": "NOT_HALTED",
                    "luld_status": "NORMAL",
                    "lower_band": 180.0,
                    "upper_band": 220.0,
                    "observed_at_utc": "2026-07-25T13:59:59Z",
                    "symbol": "AAPL",
                    "exchange_mic": "XNAS",
                },
                "broker_eligibility": {
                    "eligible": True,
                    "reason_codes": [],
                    "observed_at_utc": "2026-07-25T14:00:00Z",
                    "symbol": "AAPL",
                    "exchange_mic": "XNAS",
                    "currency": "USD",
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
        with pytest.raises(LiveSnapshotError, match="field set mismatch"):
            validate_snapshot(snapshot)


def test_strict_allowlist_rejects_private_and_unknown_fields():
    for field in ("account_number", "available_cash", "unexpected"):
        snapshot = copy.deepcopy(ready_snapshot())
        snapshot[field] = "forbidden"
        with pytest.raises(LiveSnapshotError, match="field set mismatch"):
            validate_snapshot(snapshot)


def test_identity_mismatch_is_rejected():
    snapshot = ready_snapshot()
    snapshot["securities"][0]["quote"]["symbol"] = "MSFT"
    with pytest.raises(LiveSnapshotError, match="does not match security identity"):
        validate_snapshot(snapshot)
