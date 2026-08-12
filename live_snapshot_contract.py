"""Strict provider-neutral contract for consumer-built cutoff-time execution facts."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Mapping


SCHEMA_VERSION = "1.0.0"
CONTRACT_VERSION = "1.1.0"
DEFAULT_MAXIMUM_QUOTE_AGE_SECONDS = 5
DEFAULT_MAXIMUM_MARKET_STATUS_AGE_SECONDS = 5
DEFAULT_MAXIMUM_BROKER_AGE_SECONDS = 300
DEFAULT_MAXIMUM_CLOCK_SKEW_SECONDS = 2

TOP_LEVEL_KEYS = {
    "schema_version",
    "contract_version",
    "status",
    "actual_cutoff_utc",
    "generated_at_utc",
    "maximum_quote_age_seconds",
    "maximum_market_status_age_seconds",
    "maximum_broker_age_seconds",
    "maximum_clock_skew_seconds",
    "spread_policy",
    "providers",
    "securities",
}
PROVIDER_KEYS = {
    "provider_id",
    "feed_type",
    "entitlement_scope",
    "request_id",
    "provider_response_id",
    "observed_at_utc",
}
SECURITY_KEYS = {
    "security_id",
    "symbol",
    "exchange_mic",
    "currency",
    "status",
    "block_reasons",
    "requested_at_utc",
    "received_at_utc",
    "quote",
    "market_status",
    "broker_eligibility",
}
QUOTE_KEYS = {
    "state",
    "bid",
    "ask",
    "last",
    "bid_size",
    "ask_size",
    "session",
    "quote_at_utc",
    "feed_type",
    "symbol",
    "exchange_mic",
    "currency",
}
MARKET_STATUS_KEYS = {
    "halt_status",
    "luld_status",
    "lower_band",
    "upper_band",
    "observed_at_utc",
    "symbol",
    "exchange_mic",
}
BROKER_KEYS = {
    "eligible",
    "reason_codes",
    "observed_at_utc",
    "symbol",
    "exchange_mic",
    "currency",
}
SPREAD_POLICY_KEYS = {
    "maximum_relative_spread_bps",
    "maximum_absolute_spread",
    "block_on_crossed_market",
}
PROVIDER_ROLES = {"quote", "market_status", "broker_eligibility"}
ALLOWED_FEEDS = {
    "quote": {"SIP_NBBO", "DIRECT_NBBO"},
    "market_status": {"EXCHANGE_STATUS", "SIP_STATUS"},
    "broker_eligibility": {"BROKER_ELIGIBILITY"},
}


class LiveSnapshotError(RuntimeError):
    """Raised when live facts are not safe to use at a decision cutoff."""


def require_exact_keys(
    value: Mapping[str, object], expected: set[str], label: str
) -> None:
    actual = set(value)
    if actual != expected:
        raise LiveSnapshotError(
            f"{label} field set mismatch: missing={sorted(expected - actual)}, "
            f"unexpected={sorted(actual - expected)}"
        )


def require_text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise LiveSnapshotError(f"Invalid {label}")
    return value


def parse_time(value: object, label: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise LiveSnapshotError(f"Invalid {label}") from exc
    if parsed.tzinfo is None:
        raise LiveSnapshotError(f"{label} has no timezone")
    return parsed.astimezone(timezone.utc)


def finite_number(value: object, label: str, *, positive: bool = False) -> float:
    if isinstance(value, bool):
        raise LiveSnapshotError(f"Invalid {label}")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise LiveSnapshotError(f"Invalid {label}") from exc
    if number != number or number in {float("inf"), float("-inf")}:
        raise LiveSnapshotError(f"Invalid {label}")
    if positive and number <= 0:
        raise LiveSnapshotError(f"{label} must be positive")
    return number


def positive_integer(value: object, label: str, *, allow_zero: bool = False) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise LiveSnapshotError(f"Invalid {label}")
    invalid_range = value < 0 if allow_zero else value <= 0
    if invalid_range:
        raise LiveSnapshotError(f"Invalid {label}")
    return value


def verify_identity(
    record: Mapping[str, object], nested: Mapping[str, object], label: str
) -> None:
    for field in ("symbol", "exchange_mic"):
        if nested.get(field) != record.get(field):
            raise LiveSnapshotError(f"{label} {field} does not match security identity")
    if "currency" in nested and nested.get("currency") != record.get("currency"):
        raise LiveSnapshotError(f"{label} currency does not match security identity")


def expected_record_status(
    record: Mapping[str, object],
    cutoff: datetime,
    generated: datetime,
    maximum_quote_age_seconds: int,
    maximum_market_status_age_seconds: int,
    maximum_broker_age_seconds: int,
    maximum_clock_skew_seconds: int,
    spread_policy: Mapping[str, object],
) -> tuple[str, list[str]]:
    require_exact_keys(record, SECURITY_KEYS, "security")
    for field in ("security_id", "symbol", "exchange_mic", "currency"):
        require_text(record.get(field), f"security {field}")
    reasons: list[str] = []
    quote = record.get("quote")
    if not isinstance(quote, dict):
        raise LiveSnapshotError("Invalid quote object")
    require_exact_keys(quote, QUOTE_KEYS, "quote")
    verify_identity(record, quote, "quote")
    if quote.get("feed_type") not in ALLOWED_FEEDS["quote"]:
        reasons.append("UNACCEPTABLE_QUOTE_FEED")
    if quote.get("state") != "AVAILABLE":
        reasons.append("QUOTE_UNAVAILABLE")
    else:
        bid = finite_number(quote.get("bid"), "quote bid", positive=True)
        ask = finite_number(quote.get("ask"), "quote ask", positive=True)
        if quote.get("last") is not None:
            finite_number(quote.get("last"), "quote last", positive=True)
        if quote.get("session") not in {"PRE_MARKET", "REGULAR", "POST_MARKET"}:
            reasons.append("UNKNOWN_MARKET_SESSION")
        if bid > ask and spread_policy.get("block_on_crossed_market") is True:
            reasons.append("CROSSED_MARKET")
        spread = ask - bid
        maximum_absolute = spread_policy.get("maximum_absolute_spread")
        if maximum_absolute is not None and spread > finite_number(
            maximum_absolute, "maximum_absolute_spread"
        ):
            reasons.append("SPREAD_TOO_WIDE")
        midpoint = (ask + bid) / 2
        relative_bps = spread / midpoint * 10_000
        maximum_relative = finite_number(
            spread_policy.get("maximum_relative_spread_bps"),
            "maximum_relative_spread_bps",
            positive=True,
        )
        if relative_bps > maximum_relative:
            reasons.append("SPREAD_TOO_WIDE")
        for field in ("bid_size", "ask_size"):
            size = finite_number(quote.get(field), f"quote {field}")
            if size < 0:
                reasons.append(f"NEGATIVE_{field.upper()}")
        quote_at = parse_time(quote.get("quote_at_utc"), "quote_at_utc")
        quote_age = (cutoff - quote_at).total_seconds()
        if quote_age > maximum_quote_age_seconds:
            reasons.append("STALE_QUOTE")
        if quote_age < -maximum_clock_skew_seconds:
            reasons.append("QUOTE_AFTER_CUTOFF")
        if quote_at > generated:
            reasons.append("QUOTE_AFTER_SNAPSHOT")

    market = record.get("market_status")
    if not isinstance(market, dict):
        raise LiveSnapshotError("Invalid market_status object")
    require_exact_keys(market, MARKET_STATUS_KEYS, "market_status")
    verify_identity(record, market, "market_status")
    halt = market.get("halt_status")
    if halt == "HALTED":
        reasons.append("SECURITY_HALTED")
    elif halt != "NOT_HALTED":
        reasons.append("HALT_STATUS_UNKNOWN")
    luld = market.get("luld_status")
    if luld == "PAUSED":
        reasons.append("LULD_PAUSED")
    elif luld != "NORMAL":
        reasons.append("LULD_STATUS_UNKNOWN")
    observed = parse_time(
        market.get("observed_at_utc"), "market_status observed_at_utc"
    )
    market_age = (cutoff - observed).total_seconds()
    if market_age > maximum_market_status_age_seconds:
        reasons.append("STALE_MARKET_STATUS")
    if market_age < -maximum_clock_skew_seconds:
        reasons.append("MARKET_STATUS_AFTER_CUTOFF")
    lower = market.get("lower_band")
    upper = market.get("upper_band")
    if lower is not None or upper is not None:
        lower_value = finite_number(lower, "lower LULD band", positive=True)
        upper_value = finite_number(upper, "upper LULD band", positive=True)
        if lower_value >= upper_value:
            reasons.append("INVALID_LULD_BANDS")

    broker = record.get("broker_eligibility")
    if not isinstance(broker, dict):
        raise LiveSnapshotError("Invalid broker_eligibility object")
    require_exact_keys(broker, BROKER_KEYS, "broker_eligibility")
    verify_identity(record, broker, "broker_eligibility")
    if broker.get("eligible") is not True:
        reasons.append("BROKER_INELIGIBLE_OR_UNKNOWN")
    reason_codes = broker.get("reason_codes")
    if not isinstance(reason_codes, list) or any(
        not isinstance(value, str) or not value for value in reason_codes
    ):
        reasons.append("INVALID_BROKER_REASON_CODES")
    broker_observed = parse_time(
        broker.get("observed_at_utc"), "broker eligibility observed_at_utc"
    )
    broker_age = (cutoff - broker_observed).total_seconds()
    if broker_age > maximum_broker_age_seconds:
        reasons.append("STALE_BROKER_ELIGIBILITY")
    if broker_age < -maximum_clock_skew_seconds:
        reasons.append("BROKER_STATUS_AFTER_CUTOFF")
    if broker_observed > generated:
        reasons.append("BROKER_STATUS_AFTER_SNAPSHOT")

    requested = parse_time(record.get("requested_at_utc"), "requested_at_utc")
    received = parse_time(record.get("received_at_utc"), "received_at_utc")
    if requested > received:
        reasons.append("RESPONSE_PRECEDES_REQUEST")
    if received > generated:
        reasons.append("RESPONSE_AFTER_SNAPSHOT")
    return ("READY" if not reasons else "BLOCKED", sorted(set(reasons)))


def validate_snapshot(snapshot: Mapping[str, object]) -> dict[str, object]:
    require_exact_keys(snapshot, TOP_LEVEL_KEYS, "live snapshot")
    if snapshot.get("schema_version") != SCHEMA_VERSION:
        raise LiveSnapshotError("Unsupported live-snapshot schema")
    if snapshot.get("contract_version") != CONTRACT_VERSION:
        raise LiveSnapshotError("Unsupported live-snapshot contract")
    providers = snapshot.get("providers")
    if not isinstance(providers, dict) or set(providers) != PROVIDER_ROLES:
        raise LiveSnapshotError("Live snapshot lacks exact provider roles")
    cutoff = parse_time(snapshot.get("actual_cutoff_utc"), "actual_cutoff_utc")
    generated = parse_time(snapshot.get("generated_at_utc"), "generated_at_utc")
    if generated < cutoff:
        raise LiveSnapshotError("Snapshot was generated before its actual cutoff")
    if (generated - cutoff).total_seconds() > 60:
        raise LiveSnapshotError(
            "Snapshot was generated more than 60 seconds after cutoff"
        )
    maximum_quote_age = positive_integer(
        snapshot.get("maximum_quote_age_seconds"), "maximum_quote_age_seconds"
    )
    maximum_market_age = positive_integer(
        snapshot.get("maximum_market_status_age_seconds"),
        "maximum_market_status_age_seconds",
    )
    maximum_broker_age = positive_integer(
        snapshot.get("maximum_broker_age_seconds"), "maximum_broker_age_seconds"
    )
    maximum_clock_skew = positive_integer(
        snapshot.get("maximum_clock_skew_seconds"),
        "maximum_clock_skew_seconds",
        allow_zero=True,
    )
    provider_maximum_ages = {
        "quote": maximum_quote_age,
        "market_status": maximum_market_age,
        "broker_eligibility": maximum_broker_age,
    }
    for role, provider in providers.items():
        if not isinstance(provider, dict):
            raise LiveSnapshotError(f"Invalid provider identity: {role}")
        require_exact_keys(provider, PROVIDER_KEYS, f"provider {role}")
        for field in (
            "provider_id",
            "entitlement_scope",
            "request_id",
            "provider_response_id",
        ):
            require_text(provider.get(field), f"provider {role} {field}")
        if provider.get("feed_type") not in ALLOWED_FEEDS[role]:
            raise LiveSnapshotError(f"Unacceptable provider feed type: {role}")
        provider_observed = parse_time(
            provider.get("observed_at_utc"), f"provider {role} observed_at_utc"
        )
        provider_age = (cutoff - provider_observed).total_seconds()
        if provider_age > provider_maximum_ages[role]:
            raise LiveSnapshotError(f"Stale provider observation: {role}")
        if provider_age < -maximum_clock_skew or provider_observed > generated:
            raise LiveSnapshotError(f"Future provider observation: {role}")
    spread_policy = snapshot.get("spread_policy")
    if not isinstance(spread_policy, dict):
        raise LiveSnapshotError("Invalid spread policy")
    require_exact_keys(spread_policy, SPREAD_POLICY_KEYS, "spread policy")
    if not isinstance(spread_policy.get("block_on_crossed_market"), bool):
        raise LiveSnapshotError("Invalid block_on_crossed_market")

    records = snapshot.get("securities")
    if not isinstance(records, list) or not records:
        raise LiveSnapshotError("Live snapshot contains no securities")
    seen: set[str] = set()
    blocked = 0
    for index, record in enumerate(records):
        if not isinstance(record, dict):
            raise LiveSnapshotError(f"Security record {index} is not an object")
        security_id = record.get("security_id")
        if not isinstance(security_id, str) or not security_id or security_id in seen:
            raise LiveSnapshotError(
                f"Invalid or duplicate security_id: {security_id!r}"
            )
        seen.add(security_id)
        quote = record.get("quote")
        if isinstance(quote, dict) and quote.get("feed_type") != providers["quote"].get(
            "feed_type"
        ):
            raise LiveSnapshotError(
                f"Quote feed does not match provider identity for {security_id}"
            )
        expected_status, expected_reasons = expected_record_status(
            record,
            cutoff,
            generated,
            maximum_quote_age,
            maximum_market_age,
            maximum_broker_age,
            maximum_clock_skew,
            spread_policy,
        )
        if record.get("status") != expected_status:
            raise LiveSnapshotError(f"Incorrect status for {security_id}")
        if record.get("block_reasons") != expected_reasons:
            raise LiveSnapshotError(f"Incorrect block reasons for {security_id}")
        blocked += expected_status == "BLOCKED"
    expected_snapshot_status = "READY" if blocked == 0 else "BLOCKED"
    if snapshot.get("status") != expected_snapshot_status:
        raise LiveSnapshotError("Incorrect aggregate live-snapshot status")
    return {
        "status": "VALID",
        "snapshot_status": expected_snapshot_status,
        "securities": len(records),
        "blocked": blocked,
    }
