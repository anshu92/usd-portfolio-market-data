"""Versioned contracts for compact producer decision-support artifacts."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path


SCHEMA_VERSION = "1.0.0"
DATABASE_FILENAME = "decision-support.sqlite.zst"
MANIFEST_FILENAME = "decision-support-manifest.json"
PHASE_PACK_DIRECTORY = "phase-packs"
RECENT_MARKET_SESSIONS = 64
MAX_COMPRESSED_DATABASE_BYTES = 50 * 1024 * 1024

USABLE_GROUP_STATES = {
    "READY_NEW",
    "READY_REUSED",
    "READY_WITH_EXCLUSIONS",
}

REQUIRED_SOURCE_FILES = (
    "manifest.json",
    "security-universe.csv",
    "security-master.parquet",
    "yahoo-ohlcv-320.parquet",
    "fundamental-factors.parquet",
    "corporate-events.parquet",
    "earnings-and-guidance-events.parquet",
    "insider-signals.parquet",
    "institutional-ownership-signals.parquet",
    "finra-short-interest.parquet",
)

OPTIONAL_SOURCE_FILES = (
    "analyst-estimates.parquet",
    "distributions.parquet",
    "benchmark-total-returns.parquet",
)

EXPECTED_TABLES = (
    "metadata",
    "dataset_health",
    "capability_health",
    "security",
    "market_snapshot",
    "market_history_recent",
    "fundamental_factors_latest",
    "corporate_events",
    "earnings_events",
    "insider_signals_latest",
    "institutional_signals_latest",
    "short_interest_latest",
    "analyst_estimates_latest",
    "distributions",
    "benchmark_total_returns",
)

PRIVATE_SCHEMA_TOKENS = {
    "account_id",
    "arbitration",
    "cash_balance",
    "confirmation",
    "portfolio",
    "tax_lot",
    "trade_order",
}


@dataclass(frozen=True)
class CapabilityContract:
    capability_id: str
    source_group: str | None
    maximum_age_seconds: int | None = None
    external: bool = False


CAPABILITIES = {
    "identity": CapabilityContract("identity", "identity", 24 * 60 * 60),
    "historical_market": CapabilityContract("historical_market", "market"),
    "current_catalysts": CapabilityContract(
        "current_catalysts", "filings_events", 20 * 60
    ),
    "point_in_time_expectations": CapabilityContract(
        "point_in_time_expectations", "analyst_estimates", 25 * 60
    ),
    "rapid_event_news": CapabilityContract("rapid_event_news", "news_events", 20 * 60),
    "macro_industry": CapabilityContract("macro_industry", "macro_industry", 24 * 60 * 60),
    "etf_exposure": CapabilityContract("etf_exposure", "etf_exposure", 24 * 60 * 60),
    "certified_total_returns": CapabilityContract(
        "certified_total_returns", "total_returns", 60 * 60
    ),
    "funded_benchmark_inputs": CapabilityContract(
        "funded_benchmark_inputs", "total_returns", 60 * 60
    ),
    "canonical_replay": CapabilityContract("canonical_replay", "market"),
    "survivorship_history": CapabilityContract(
        "survivorship_history", "survivorship_history"
    ),
    "execution_snapshot": CapabilityContract(
        "execution_snapshot", None, external=True
    ),
}


@dataclass(frozen=True)
class PhaseContract:
    phase_id: str
    required_capabilities: tuple[str, ...]
    optional_capabilities: tuple[str, ...] = ()
    external_capabilities: tuple[str, ...] = ()
    delivery_targets: tuple[str, ...] = ()


PHASES = {
    "sunday": PhaseContract(
        "sunday",
        ("identity", "historical_market", "current_catalysts"),
        ("point_in_time_expectations", "macro_industry", "etf_exposure"),
        (),
        ("SUNDAY_CONFIGURED_CUTOFF",),
    ),
    "pre_open": PhaseContract(
        "pre_open",
        (
            "identity",
            "historical_market",
            "current_catalysts",
            "point_in_time_expectations",
            "rapid_event_news",
        ),
        ("macro_industry", "etf_exposure"),
        (),
        ("08:10",),
    ),
    "execution_research": PhaseContract(
        "execution_research",
        (
            "identity",
            "historical_market",
            "current_catalysts",
            "point_in_time_expectations",
        ),
        ("rapid_event_news", "macro_industry", "etf_exposure"),
        ("execution_snapshot",),
        ("ACTUAL_DECISION_CUTOFF",),
    ),
    "exception_monitoring": PhaseContract(
        "exception_monitoring",
        ("identity", "current_catalysts", "rapid_event_news"),
        ("point_in_time_expectations",),
        ("execution_snapshot",),
        ("09:55", "15:25"),
    ),
    "terminal_review": PhaseContract(
        "terminal_review",
        ("identity", "historical_market", "current_catalysts"),
        ("point_in_time_expectations", "rapid_event_news"),
        (),
        ("XNYS_CLOSE+00:20",),
    ),
    "accounting": PhaseContract(
        "accounting",
        ("identity", "certified_total_returns", "funded_benchmark_inputs"),
        (),
        (),
        ("XNYS_CLOSE+00:45",),
    ),
    "saturday_replay": PhaseContract(
        "saturday_replay",
        ("identity", "canonical_replay", "survivorship_history"),
        (),
        (),
        ("SATURDAY_08:30",),
    ),
}


def canonical_json(value: object) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        + "\n"
    ).encode("utf-8")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
