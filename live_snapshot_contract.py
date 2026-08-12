"""Provider-neutral contract for consumer-built cutoff-time execution facts."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Mapping


SCHEMA_VERSION = "1.0.0"
DEFAULT_MAXIMUM_QUOTE_AGE_SECONDS = 5
DEFAULT_MAXIMUM_CLOCK_SKEW_SECONDS = 2
PRIVATE_KEYS = {
    "account_id",
    "cash",
    "confirmation",
    "holdings",
    "lot",
    "portfolio",
    "position",
    "trade",
}


class LiveSnapshotError(RuntimeError):
    """Raised when live facts are not safe to use at a decision cutoff."""


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


def find_private_keys(value: object) -> set[str]:
    violations: set[str] = set()
    if isinstance(value, dict):
        for key, nested in value.items():
            normalized = str(key).strip().lower()
            if normalized in PRIVATE_KEYS:
                violations.add(normalized)
            violations.update(find_private_keys(nested))
    elif isinstance(value, list):
        for nested in value:
            violations.update(find_private_keys(nested))
    return violations


def expected_record_status(
    record: Mapping[str, object],
    cutoff: datetime,
    generated: datetime,
    maximum_quote_age_seconds: int,
    maximum_clock_skew_seconds: int,
) -> tuple[str, list[str]]:
    reasons: list[str] = []
    quote = record.get("quote")
    if not isinstance(quote, dict) or quote.get("state") != "AVAILABLE":
        reasons.append("QUOTE_UNAVAILABLE")
    else:
        bid = finite_number(quote.get("bid"), "quote bid", positive=True)
        ask = finite_number(quote.get("ask"), "quote ask", positive=True)
        if quote.get("last") is not None:
            finite_number(quote.get("last"), "quote last", positive=True)
        if quote.get("session") not in {"PRE_MARKET", "REGULAR", "POST_MARKET"}:
            reasons.append("UNKNOWN_MARKET_SESSION")
        if bid > ask:
            reasons.append("CROSSED_MARKET")
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
        reasons.append("MARKET_STATUS_UNAVAILABLE")
    else:
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
        if (cutoff - observed).total_seconds() > maximum_quote_age_seconds:
            reasons.append("STALE_MARKET_STATUS")
        if (cutoff - observed).total_seconds() < -maximum_clock_skew_seconds:
            reasons.append("MARKET_STATUS_AFTER_CUTOFF")
        lower = market.get("lower_band")
        upper = market.get("upper_band")
        if lower is not None or upper is not None:
            lower_value = finite_number(lower, "lower LULD band", positive=True)
            upper_value = finite_number(upper, "upper LULD band", positive=True)
            if lower_value >= upper_value:
                reasons.append("INVALID_LULD_BANDS")

    broker = record.get("broker_eligibility")
    if not isinstance(broker, dict) or broker.get("eligible") is not True:
        reasons.append("BROKER_INELIGIBLE_OR_UNKNOWN")
    else:
        reason_codes = broker.get("reason_codes")
        if not isinstance(reason_codes, list) or any(
            not isinstance(value, str) for value in reason_codes
        ):
            reasons.append("INVALID_BROKER_REASON_CODES")
        observed = parse_time(
            broker.get("observed_at_utc"), "broker eligibility observed_at_utc"
        )
        if observed > generated:
            reasons.append("BROKER_STATUS_AFTER_SNAPSHOT")

    requested = parse_time(record.get("requested_at_utc"), "requested_at_utc")
    received = parse_time(record.get("received_at_utc"), "received_at_utc")
    if requested > received:
        reasons.append("RESPONSE_PRECEDES_REQUEST")
    if received > generated:
        reasons.append("RESPONSE_AFTER_SNAPSHOT")
    return ("READY" if not reasons else "BLOCKED", sorted(set(reasons)))


def validate_snapshot(snapshot: Mapping[str, object]) -> dict[str, object]:
    if snapshot.get("schema_version") != SCHEMA_VERSION:
        raise LiveSnapshotError("Unsupported live-snapshot schema")
    violations = find_private_keys(snapshot)
    if violations:
        raise LiveSnapshotError(
            f"Private portfolio/account keys are forbidden: {sorted(violations)}"
        )
    providers = snapshot.get("providers")
    if not isinstance(providers, dict) or set(providers) != {
        "quote",
        "market_status",
        "broker_eligibility",
    } or any(not isinstance(value, str) or not value for value in providers.values()):
        raise LiveSnapshotError("Live snapshot lacks exact provider identities")
    cutoff = parse_time(snapshot.get("actual_cutoff_utc"), "actual_cutoff_utc")
    generated = parse_time(snapshot.get("generated_at_utc"), "generated_at_utc")
    if generated < cutoff:
        raise LiveSnapshotError("Snapshot was generated before its actual cutoff")
    if (generated - cutoff).total_seconds() > 60:
        raise LiveSnapshotError("Snapshot was generated more than 60 seconds after cutoff")
    maximum_quote_age = snapshot.get(
        "maximum_quote_age_seconds", DEFAULT_MAXIMUM_QUOTE_AGE_SECONDS
    )
    maximum_clock_skew = snapshot.get(
        "maximum_clock_skew_seconds", DEFAULT_MAXIMUM_CLOCK_SKEW_SECONDS
    )
    if (
        isinstance(maximum_quote_age, bool)
        or not isinstance(maximum_quote_age, int)
        or maximum_quote_age <= 0
        or isinstance(maximum_clock_skew, bool)
        or not isinstance(maximum_clock_skew, int)
        or maximum_clock_skew < 0
    ):
        raise LiveSnapshotError("Invalid live-snapshot timing thresholds")
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
            raise LiveSnapshotError(f"Invalid or duplicate security_id: {security_id!r}")
        seen.add(security_id)
        expected_status, expected_reasons = expected_record_status(
            record,
            cutoff,
            generated,
            maximum_quote_age,
            maximum_clock_skew,
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
