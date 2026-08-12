"""Versioned contracts for compact producer decision-support artifacts."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path


SCHEMA_VERSION = "1.0.0"
CONTRACT_VERSION = "1.1.0"
DATABASE_FILENAME = "decision-support.sqlite.zst"
MANIFEST_FILENAME = "decision-support-manifest.json"
PHASE_PACK_DIRECTORY = "phase-packs"
CANDIDATE_FUNNEL_FILENAME = "candidate-funnel.parquet"
ACTIONABILITY_MATRIX_FILENAME = "actionability-matrix.json"
EVIDENCE_PACKETS_FILENAME = "evidence-packets.jsonl.zst"
RECENT_MARKET_SESSIONS = 64
BENCHMARK_MINIMUM_SESSIONS = 140
BENCHMARK_MINIMUM_WEEKLY_OBSERVATIONS = 26
MAX_COMPRESSED_DATABASE_BYTES = 50 * 1024 * 1024
TASK_TIMEZONE = "America/Toronto"
EXCHANGE_TIMEZONE = "America/New_York"
EXPECTED_REPOSITORY = "anshu92/usd-portfolio-market-data"
EXPECTED_WORKFLOW_PATH = ".github/workflows/build-decision-support.yml"
EXPECTED_WORKFLOW_ID = 332931733
EXPECTED_BRANCH = "main"
EXPECTED_EVENTS = ("workflow_dispatch", "schedule")

OPERATING_MODES = (
    "ARTIFACT_VALID",
    "BENCHMARK_ONLY_SAFE",
    "CHALLENGER_RESEARCH_READY",
    "LIVE_SNAPSHOT_REQUIRED",
    "CHALLENGER_BLOCKED",
)

VALIDATOR_FILES = (
    "decision_support_contract.py",
    "reliability_contract.py",
    "build-benchmark-total-returns.py",
    "build-decision-support.py",
    "verify-release.py",
    "verify-decision-support.py",
    "verify-decision-support-pointer.py",
    "live_snapshot_contract.py",
    "verify-live-snapshot.py",
)

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
    "sec-filings.parquet",
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
    "primary_filings_latest",
    "evidence",
    "candidate_funnel",
    "actionability_matrix",
)

CANDIDATE_FUNNEL_SCHEMA = (
    ("candidate_id", "VARCHAR"),
    ("security_id", "VARCHAR"),
    ("phase_id", "VARCHAR"),
    ("funnel_state", "VARCHAR"),
    ("candidate_rank", "BIGINT"),
    ("rejection_codes_json", "VARCHAR"),
    ("evidence_ids_json", "VARCHAR"),
    ("known_at_utc", "TIMESTAMP"),
    ("revision", "VARCHAR"),
    ("source_retrieved_at_utc", "TIMESTAMP"),
)

PRIVATE_SCHEMA_TOKENS = {
    "account_id",
    "arbitration",
    "cash_balance",
    "confirmation",
    "portfolio",
    "tax_lot",
    "trade_order",
    "account_number",
    "available_cash",
    "tax_lots",
    "open_orders",
    "filled_quantity",
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
    "macro_industry": CapabilityContract(
        "macro_industry", "macro_industry", 24 * 60 * 60
    ),
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
    "execution_snapshot": CapabilityContract("execution_snapshot", None, external=True),
    "primary_evidence": CapabilityContract(
        "primary_evidence", "filings_events", 20 * 60
    ),
    "candidate_funnel": CapabilityContract(
        "candidate_funnel", "candidate_funnel", 15 * 60
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
        phase_id="sunday",
        required_capabilities=(
            "identity",
            "historical_market",
            "certified_total_returns",
            "funded_benchmark_inputs",
        ),
        optional_capabilities=(
            "current_catalysts",
            "point_in_time_expectations",
            "macro_industry",
            "etf_exposure",
            "candidate_funnel",
        ),
        delivery_targets=("SUNDAY_17:30",),
    ),
    "pre_open": PhaseContract(
        phase_id="pre_open",
        required_capabilities=(
            "identity",
            "historical_market",
            "current_catalysts",
            "point_in_time_expectations",
            "rapid_event_news",
            "primary_evidence",
        ),
        optional_capabilities=("macro_industry", "etf_exposure", "candidate_funnel"),
        delivery_targets=("08:10",),
    ),
    "execution_research": PhaseContract(
        phase_id="execution_research",
        required_capabilities=(
            "identity",
            "historical_market",
            "current_catalysts",
            "point_in_time_expectations",
            "primary_evidence",
        ),
        optional_capabilities=(
            "rapid_event_news",
            "macro_industry",
            "etf_exposure",
            "candidate_funnel",
        ),
        external_capabilities=("execution_snapshot",),
        delivery_targets=("09:38",),
    ),
    "exception_monitoring": PhaseContract(
        phase_id="exception_monitoring",
        required_capabilities=(
            "identity",
            "current_catalysts",
            "rapid_event_news",
            "primary_evidence",
        ),
        optional_capabilities=("point_in_time_expectations", "candidate_funnel"),
        external_capabilities=("execution_snapshot",),
        delivery_targets=("09:55", "15:25"),
    ),
    "terminal_review": PhaseContract(
        phase_id="terminal_review",
        required_capabilities=(
            "identity",
            "historical_market",
            "current_catalysts",
            "primary_evidence",
        ),
        optional_capabilities=(
            "point_in_time_expectations",
            "rapid_event_news",
            "candidate_funnel",
        ),
        delivery_targets=("XNYS_CLOSE+00:20",),
    ),
    "accounting": PhaseContract(
        phase_id="accounting",
        required_capabilities=(
            "identity",
            "certified_total_returns",
            "funded_benchmark_inputs",
        ),
        delivery_targets=("XNYS_CLOSE+00:45",),
    ),
    "saturday_replay": PhaseContract(
        phase_id="saturday_replay",
        required_capabilities=(
            "identity",
            "canonical_replay",
            "survivorship_history",
        ),
        delivery_targets=("SATURDAY_08:30",),
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
