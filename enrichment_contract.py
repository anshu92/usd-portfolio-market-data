"""Versioned schemas and deterministic Parquet helpers for enrichment assets."""

from __future__ import annotations

import hashlib
import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping

import duckdb


SCHEMA_VERSION = "1.0.0"
POINT_IN_TIME_COLUMNS = (
    ("source_event_date", "DATE"),
    ("source_filing_date", "DATE"),
    ("source_acceptance_datetime_utc", "TIMESTAMP"),
    ("source_publication_date", "DATE"),
    ("source_retrieved_at_utc", "TIMESTAMP"),
)


@dataclass(frozen=True)
class DatasetContract:
    filename: str
    columns: tuple[tuple[str, str], ...]
    primary_key: tuple[str, ...]
    source: str
    point_in_time_safe: bool = True
    allow_null_primary_key: bool = False


def _columns(*values: tuple[str, str]) -> tuple[tuple[str, str], ...]:
    names = [name for name, _ in values]
    if len(names) != len(set(names)):
        raise ValueError("Dataset contract contains duplicate columns")
    return tuple(values)


def _pit(*values: tuple[str, str]) -> tuple[tuple[str, str], ...]:
    return _columns(*values, *POINT_IN_TIME_COLUMNS)


CONTRACTS = {
    "security-master.parquet": DatasetContract(
        "security-master.parquet",
        _pit(
            ("security_id", "VARCHAR"),
            ("ticker", "VARCHAR"),
            ("exchange_mic", "VARCHAR"),
            ("cik", "VARCHAR"),
            ("registrant_name", "VARCHAR"),
            ("sic", "VARCHAR"),
            ("mapping_status", "VARCHAR"),
            ("source_url", "VARCHAR"),
            ("source_revision", "VARCHAR"),
        ),
        ("security_id",),
        "SEC EDGAR submissions",
    ),
    "sec-company-facts.parquet": DatasetContract(
        "sec-company-facts.parquet",
        _pit(
            ("security_id", "VARCHAR"),
            ("cik", "VARCHAR"),
            ("taxonomy", "VARCHAR"),
            ("concept", "VARCHAR"),
            ("label", "VARCHAR"),
            ("description", "VARCHAR"),
            ("unit", "VARCHAR"),
            ("unit_status", "VARCHAR"),
            ("value", "DOUBLE"),
            ("fiscal_year", "BIGINT"),
            ("fiscal_period", "VARCHAR"),
            ("form", "VARCHAR"),
            ("filed_date", "DATE"),
            ("period_start", "DATE"),
            ("period_end", "DATE"),
            ("frame", "VARCHAR"),
            ("accession_number", "VARCHAR"),
            ("source_url", "VARCHAR"),
            ("source_revision", "VARCHAR"),
        ),
        (
            "cik",
            "taxonomy",
            "concept",
            "unit",
            "accession_number",
            "period_start",
            "period_end",
            "fiscal_year",
            "fiscal_period",
        ),
        "SEC EDGAR company facts",
        allow_null_primary_key=True,
    ),
    "normalized-fundamentals-quarterly.parquet": DatasetContract(
        "normalized-fundamentals-quarterly.parquet",
        _pit(
            ("security_id", "VARCHAR"),
            ("cik", "VARCHAR"),
            ("fiscal_year", "BIGINT"),
            ("fiscal_period", "VARCHAR"),
            ("period_start", "DATE"),
            ("period_end", "DATE"),
            ("filing_date", "DATE"),
            ("filing_available_date", "DATE"),
            ("accession_number", "VARCHAR"),
            ("currency", "VARCHAR"),
            ("revenue", "DOUBLE"),
            ("gross_profit", "DOUBLE"),
            ("operating_income", "DOUBLE"),
            ("ebit", "DOUBLE"),
            ("pretax_income", "DOUBLE"),
            ("net_income", "DOUBLE"),
            ("operating_cash_flow", "DOUBLE"),
            ("capital_expenditure", "DOUBLE"),
            ("free_cash_flow", "DOUBLE"),
            ("cash_and_equivalents", "DOUBLE"),
            ("current_assets", "DOUBLE"),
            ("current_liabilities", "DOUBLE"),
            ("total_assets", "DOUBLE"),
            ("total_liabilities", "DOUBLE"),
            ("total_debt", "DOUBLE"),
            ("short_term_debt", "DOUBLE"),
            ("long_term_debt", "DOUBLE"),
            ("stockholders_equity", "DOUBLE"),
            ("tangible_book_value", "DOUBLE"),
            ("shares_basic", "DOUBLE"),
            ("shares_diluted", "DOUBLE"),
            ("stock_based_compensation", "DOUBLE"),
            ("interest_expense", "DOUBLE"),
            ("accounts_receivable", "DOUBLE"),
            ("inventory", "DOUBLE"),
            ("source_concept_map_version", "VARCHAR"),
            ("normalization_status", "VARCHAR"),
            ("missing_required_fields", "VARCHAR"),
        ),
        ("security_id", "fiscal_year", "fiscal_period"),
        "SEC EDGAR company facts",
    ),
    "fundamental-factors.parquet": DatasetContract(
        "fundamental-factors.parquet",
        _pit(
            ("security_id", "VARCHAR"),
            ("factor_as_of_date", "DATE"),
            ("latest_filing_date", "DATE"),
            ("filing_available_date", "DATE"),
            ("trailing_revenue", "DOUBLE"),
            ("trailing_operating_income", "DOUBLE"),
            ("trailing_net_income", "DOUBLE"),
            ("trailing_operating_cash_flow", "DOUBLE"),
            ("trailing_capex", "DOUBLE"),
            ("trailing_free_cash_flow", "DOUBLE"),
            ("net_debt", "DOUBLE"),
            ("market_cap", "DOUBLE"),
            ("enterprise_value", "DOUBLE"),
            ("earnings_yield", "DOUBLE"),
            ("free_cash_flow_yield", "DOUBLE"),
            ("ev_to_ebit", "DOUBLE"),
            ("price_to_tangible_book", "DOUBLE"),
            ("current_ratio", "DOUBLE"),
            ("interest_coverage", "DOUBLE"),
            ("debt_to_ebitda", "DOUBLE"),
            ("gross_margin", "DOUBLE"),
            ("operating_margin", "DOUBLE"),
            ("free_cash_flow_margin", "DOUBLE"),
            ("cash_conversion", "DOUBLE"),
            ("roic", "DOUBLE"),
            ("incremental_roic", "DOUBLE"),
            ("reinvestment_rate", "DOUBLE"),
            ("revenue_growth_yoy", "DOUBLE"),
            ("revenue_growth_cagr_3y", "DOUBLE"),
            ("earnings_growth_yoy", "DOUBLE"),
            ("free_cash_flow_growth_yoy", "DOUBLE"),
            ("share_count_change_yoy", "DOUBLE"),
            ("gross_margin_stability_3y", "DOUBLE"),
            ("free_cash_flow_consistency_3y", "DOUBLE"),
            ("factor_quality_status", "VARCHAR"),
            ("factor_missing_fields", "VARCHAR"),
            ("model_version", "VARCHAR"),
        ),
        ("security_id", "factor_as_of_date"),
        "SEC EDGAR and packaged market history",
    ),
    "sec-filings.parquet": DatasetContract(
        "sec-filings.parquet",
        _pit(
            ("security_id", "VARCHAR"),
            ("cik", "VARCHAR"),
            ("accession_number", "VARCHAR"),
            ("form", "VARCHAR"),
            ("filing_date", "DATE"),
            ("acceptance_datetime_utc", "TIMESTAMP"),
            ("report_date", "DATE"),
            ("primary_document", "VARCHAR"),
            ("primary_document_url", "VARCHAR"),
            ("is_amendment", "BOOLEAN"),
            ("items", "VARCHAR"),
            ("file_number", "VARCHAR"),
            ("film_number", "VARCHAR"),
        ),
        ("cik", "accession_number"),
        "SEC EDGAR submissions",
    ),
    "corporate-events.parquet": DatasetContract(
        "corporate-events.parquet",
        _pit(
            ("event_id", "VARCHAR"),
            ("security_id", "VARCHAR"),
            ("cik", "VARCHAR"),
            ("event_type", "VARCHAR"),
            ("event_subtype", "VARCHAR"),
            ("announcement_datetime_utc", "TIMESTAMP"),
            ("effective_date", "DATE"),
            ("expiration_date", "DATE"),
            ("filing_form", "VARCHAR"),
            ("accession_number", "VARCHAR"),
            ("source_document_url", "VARCHAR"),
            ("headline", "VARCHAR"),
            ("summary", "VARCHAR"),
            ("counterparty", "VARCHAR"),
            ("cash_value", "DOUBLE"),
            ("share_value", "DOUBLE"),
            ("exchange_ratio", "DOUBLE"),
            ("ownership_percentage", "DOUBLE"),
            ("board_seats_requested", "BIGINT"),
            ("event_status", "VARCHAR"),
            ("event_confidence", "VARCHAR"),
            ("materiality_score", "DOUBLE"),
            ("parser_version", "VARCHAR"),
        ),
        ("event_id",),
        "SEC EDGAR filings",
    ),
    "insider-transactions.parquet": DatasetContract(
        "insider-transactions.parquet",
        _pit(
            ("security_id", "VARCHAR"),
            ("issuer_cik", "VARCHAR"),
            ("reporting_owner_cik", "VARCHAR"),
            ("reporting_owner_name", "VARCHAR"),
            ("is_director", "BOOLEAN"),
            ("is_officer", "BOOLEAN"),
            ("is_ten_percent_owner", "BOOLEAN"),
            ("officer_title", "VARCHAR"),
            ("filing_date", "DATE"),
            ("transaction_date", "DATE"),
            ("form", "VARCHAR"),
            ("accession_number", "VARCHAR"),
            ("transaction_id", "VARCHAR"),
            ("security_title", "VARCHAR"),
            ("transaction_code", "VARCHAR"),
            ("acquired_disposed", "VARCHAR"),
            ("shares", "DOUBLE"),
            ("price_per_share", "DOUBLE"),
            ("ownership_nature", "VARCHAR"),
            ("shares_owned_after", "DOUBLE"),
            ("is_derivative", "BOOLEAN"),
            ("exercise_price", "DOUBLE"),
            ("expiration_date", "DATE"),
            ("is_10b5_1", "BOOLEAN"),
        ),
        ("accession_number", "reporting_owner_cik", "transaction_id"),
        "SEC insider transactions data sets",
    ),
    "insider-signals.parquet": DatasetContract(
        "insider-signals.parquet",
        _pit(
            ("security_id", "VARCHAR"),
            ("signal_as_of_date", "DATE"),
            ("open_market_purchase_value_30d", "DOUBLE"),
            ("open_market_purchase_value_90d", "DOUBLE"),
            ("open_market_sale_value_30d", "DOUBLE"),
            ("open_market_sale_value_90d", "DOUBLE"),
            ("unique_buyers_90d", "BIGINT"),
            ("unique_sellers_90d", "BIGINT"),
            ("cluster_buy_flag", "BOOLEAN"),
            ("director_purchase_flag", "BOOLEAN"),
            ("ceo_purchase_flag", "BOOLEAN"),
            ("cfo_purchase_flag", "BOOLEAN"),
            ("insider_signal_score", "DOUBLE"),
            ("signal_quality_status", "VARCHAR"),
        ),
        ("security_id", "signal_as_of_date"),
        "Derived from SEC insider transactions",
    ),
    "institutional-holdings-13f.parquet": DatasetContract(
        "institutional-holdings-13f.parquet",
        _pit(
            ("manager_cik", "VARCHAR"),
            ("manager_name", "VARCHAR"),
            ("report_period", "DATE"),
            ("filing_date", "DATE"),
            ("accession_number", "VARCHAR"),
            ("cusip", "VARCHAR"),
            ("security_id", "VARCHAR"),
            ("security_mapping_method", "VARCHAR"),
            ("issuer_name", "VARCHAR"),
            ("class_title", "VARCHAR"),
            ("market_value_thousands", "DOUBLE"),
            ("shares_or_principal", "DOUBLE"),
            ("share_type", "VARCHAR"),
            ("put_call", "VARCHAR"),
            ("investment_discretion", "VARCHAR"),
            ("voting_authority_sole", "DOUBLE"),
            ("voting_authority_shared", "DOUBLE"),
            ("voting_authority_none", "DOUBLE"),
        ),
        ("manager_cik", "security_id", "report_period", "accession_number"),
        "SEC Form 13F data sets",
    ),
    "institutional-ownership-signals.parquet": DatasetContract(
        "institutional-ownership-signals.parquet",
        _pit(
            ("security_id", "VARCHAR"),
            ("report_period", "DATE"),
            ("known_13f_shares", "DOUBLE"),
            ("manager_count", "BIGINT"),
            ("new_position_manager_count", "BIGINT"),
            ("closed_position_manager_count", "BIGINT"),
            ("increased_position_manager_count", "BIGINT"),
            ("decreased_position_manager_count", "BIGINT"),
            ("net_share_change", "DOUBLE"),
            ("net_manager_breadth", "BIGINT"),
            ("top_10_manager_concentration", "DOUBLE"),
            ("ownership_signal_score", "DOUBLE"),
            ("publication_lag_days", "BIGINT"),
            ("signal_quality_status", "VARCHAR"),
        ),
        ("security_id", "report_period"),
        "Derived from SEC Form 13F data sets",
    ),
    "finra-short-interest.parquet": DatasetContract(
        "finra-short-interest.parquet",
        _pit(
            ("security_id", "VARCHAR"),
            ("symbol", "VARCHAR"),
            ("issue_name", "VARCHAR"),
            ("market", "VARCHAR"),
            ("settlement_date", "DATE"),
            ("publication_date", "DATE"),
            ("current_short_shares", "DOUBLE"),
            ("previous_short_shares", "DOUBLE"),
            ("short_change", "DOUBLE"),
            ("short_change_percent", "DOUBLE"),
            ("average_daily_volume", "DOUBLE"),
            ("days_to_cover", "DOUBLE"),
            ("revision_flag", "VARCHAR"),
            ("short_interest_change_1m", "DOUBLE"),
            ("short_interest_change_3m", "DOUBLE"),
            ("days_to_cover_change", "DOUBLE"),
            ("short_interest_percent_float", "DOUBLE"),
            ("short_interest_percent_float_quality", "VARCHAR"),
        ),
        ("security_id", "settlement_date"),
        "FINRA Equity Short Interest",
    ),
    "earnings-and-guidance-events.parquet": DatasetContract(
        "earnings-and-guidance-events.parquet",
        _pit(
            ("event_id", "VARCHAR"),
            ("security_id", "VARCHAR"),
            ("event_datetime_utc", "TIMESTAMP"),
            ("event_date", "DATE"),
            ("event_time_classification", "VARCHAR"),
            ("event_type", "VARCHAR"),
            ("fiscal_period", "VARCHAR"),
            ("revenue_reported", "DOUBLE"),
            ("eps_reported", "DOUBLE"),
            ("revenue_guidance_low", "DOUBLE"),
            ("revenue_guidance_high", "DOUBLE"),
            ("eps_guidance_low", "DOUBLE"),
            ("eps_guidance_high", "DOUBLE"),
            ("guidance_direction", "VARCHAR"),
            ("source_type", "VARCHAR"),
            ("filing_accession_number", "VARCHAR"),
            ("source_document_url", "VARCHAR"),
        ),
        ("event_id",),
        "SEC EDGAR filings and issuer releases",
    ),
}


ANALYST_ESTIMATES_CONTRACT = DatasetContract(
    "analyst-estimates.parquet",
    _pit(
        ("security_id", "VARCHAR"),
        ("estimate_as_of_utc", "TIMESTAMP"),
        ("fiscal_period", "VARCHAR"),
        ("metric", "VARCHAR"),
        ("consensus_value", "DOUBLE"),
        ("estimate_count", "BIGINT"),
        ("high_estimate", "DOUBLE"),
        ("low_estimate", "DOUBLE"),
        ("median_estimate", "DOUBLE"),
        ("revision_up_count_7d", "BIGINT"),
        ("revision_down_count_7d", "BIGINT"),
        ("revision_up_count_30d", "BIGINT"),
        ("revision_down_count_30d", "BIGINT"),
        ("prior_consensus_7d", "DOUBLE"),
        ("prior_consensus_30d", "DOUBLE"),
        ("provider", "VARCHAR"),
        ("provider_license", "VARCHAR"),
    ),
    ("security_id", "estimate_as_of_utc", "fiscal_period", "metric", "provider"),
    "Licensed analyst-estimate provider",
)
OPTIONAL_CONTRACTS = {ANALYST_ESTIMATES_CONTRACT.filename: ANALYST_ESTIMATES_CONTRACT}


REQUIRED_ENRICHMENT_FILES = frozenset(CONTRACTS)


class ContractError(RuntimeError):
    """Raised when an enrichment asset violates its versioned contract."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _quoted(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def write_parquet(
    path: Path,
    contract: DatasetContract,
    rows: Iterable[Mapping[str, object]],
) -> int:
    column_names = tuple(name for name, _ in contract.columns)
    allowed = set(column_names)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    database = path.with_suffix(path.suffix + ".duckdb.tmp")
    if tmp.exists():
        tmp.unlink()
    if database.exists():
        database.unlink()
    con = duckdb.connect(str(database))
    row_count = 0
    try:
        definitions = ", ".join(
            f"{_quoted(name)} {sql_type}" for name, sql_type in contract.columns
        )
        con.execute(f"CREATE TABLE output ({definitions})")
        placeholders = ", ".join("?" for _ in column_names)
        batch: list[tuple[object, ...]] = []
        for index, row in enumerate(rows):
            extra = set(row) - allowed
            if extra:
                raise ContractError(
                    f"{contract.filename} row {index} has extra columns: {sorted(extra)}"
                )
            for name, sql_type in contract.columns:
                value = row.get(name)
                if sql_type == "DOUBLE" and value is not None:
                    try:
                        finite = math.isfinite(float(value))
                    except (TypeError, ValueError) as exc:
                        raise ContractError(
                            f"{contract.filename} row {index} has invalid {name}"
                        ) from exc
                    if not finite:
                        raise ContractError(
                            f"{contract.filename} row {index} has non-finite {name}"
                        )
            batch.append(tuple(row.get(name) for name in column_names))
            row_count += 1
            if len(batch) == 10_000:
                con.executemany(f"INSERT INTO output VALUES ({placeholders})", batch)
                batch.clear()
        if batch:
            con.executemany(f"INSERT INTO output VALUES ({placeholders})", batch)
        key_sql = ", ".join(_quoted(name) for name in contract.primary_key)
        duplicate_count = con.execute(
            f"SELECT count(*) - count(DISTINCT ({key_sql})) FROM output"
        ).fetchone()[0]
        if duplicate_count:
            raise ContractError(
                f"{contract.filename} has {int(duplicate_count)} duplicate primary keys"
            )
        null_key = " OR ".join(
            f"{_quoted(name)} IS NULL" for name in contract.primary_key
        )
        if (
            not contract.allow_null_primary_key
            and con.execute(f"SELECT count(*) FROM output WHERE {null_key}").fetchone()[0]
        ):
            raise ContractError(f"{contract.filename} has a null primary-key value")
        order_sql = ", ".join(_quoted(name) for name in contract.primary_key)
        con.execute(
            f"COPY (SELECT * FROM output ORDER BY {order_sql}) TO ? "
            "(FORMAT PARQUET, COMPRESSION ZSTD)",
            [str(tmp)],
        )
    finally:
        con.close()
        if database.exists():
            database.unlink()
    os.replace(tmp, path)
    return row_count


def dataset_record(
    path: Path,
    contract: DatasetContract,
    *,
    rows: int,
    source_revision: str,
    source_retrieved_at_utc: str,
    minimum_event_date: str | None = None,
    maximum_event_date: str | None = None,
) -> dict[str, object]:
    return {
        "path": contract.filename,
        "schema_version": SCHEMA_VERSION,
        "row_count": rows,
        "byte_size": path.stat().st_size,
        "sha256": sha256_file(path),
        "primary_key": list(contract.primary_key),
        "source": contract.source,
        "source_revision": source_revision,
        "source_retrieved_at_utc": source_retrieved_at_utc,
        "minimum_event_date": minimum_event_date,
        "maximum_event_date": maximum_event_date,
        "point_in_time_safe": contract.point_in_time_safe,
    }


def release_file_record(
    path: Path, contract: DatasetContract, rows: int
) -> dict[str, object]:
    return {
        "bytes": path.stat().st_size,
        "file": contract.filename,
        "rows": rows,
        "schema": [name for name, _ in contract.columns],
        "sha256": sha256_file(path),
    }
