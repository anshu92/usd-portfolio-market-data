#!/usr/bin/env python3
"""Build point-in-time SEC and FINRA enrichment assets from official sources."""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import io
import json
import math
import os
import re
import shutil
import statistics
import time
import urllib.request
import zipfile
from collections import defaultdict
from datetime import date, datetime, time as datetime_time, timedelta, timezone
from pathlib import Path
from typing import Iterable, Iterator, Mapping
from zoneinfo import ZoneInfo

import duckdb

from enrichment_contract import (
    CONTRACTS,
    ContractError,
    dataset_record,
    release_file_record,
    sha256_file,
    write_parquet,
)


ACCESSION_PATTERN = re.compile(r"^[0-9]{10}-[0-9]{2}-[0-9]{6}$")
FINRA_SHORT_INTEREST_URL = (
    "https://api.finra.org/data/group/otcmarket/name/consolidatedShortInterest"
)
SEC_FORMS = {
    "10-K",
    "10-K/A",
    "10-Q",
    "10-Q/A",
    "8-K",
    "8-K/A",
    "6-K",
    "20-F",
    "40-F",
    "S-1",
    "S-3",
    "DEF 14A",
    "DEFA14A",
    "SC 13D",
    "SC 13D/A",
    "SC 13G",
    "SC 13G/A",
    "SCHEDULE 13D",
    "SCHEDULE 13D/A",
    "SCHEDULE 13G",
    "SCHEDULE 13G/A",
    "SC TO",
    "SC TO-I",
    "SC TO-T",
    "PREM14A",
    "DEFM14A",
}
FLOW_FIELDS = {
    "revenue",
    "gross_profit",
    "operating_income",
    "pretax_income",
    "net_income",
    "operating_cash_flow",
    "capital_expenditure",
    "shares_basic",
    "shares_diluted",
    "stock_based_compensation",
    "interest_expense",
}
SHARE_FIELDS = {"shares_basic", "shares_diluted"}
DERIVABLE_FLOW_FIELDS = FLOW_FIELDS - SHARE_FIELDS
CORE_NORMALIZED_FIELDS = {
    "revenue",
    "operating_income",
    "net_income",
    "operating_cash_flow",
    "total_assets",
    "stockholders_equity",
}
SUPPORTED_UNITS = {"USD", "shares", "USD/shares", "pure"}
PARSER_VERSION = "sec-events-1.0.0"
FACTOR_MODEL_VERSION = "fundamental-factors-1.0.0"
NORMALIZATION_FACT_COLUMNS = (
    "security_id",
    "cik",
    "taxonomy",
    "concept",
    "unit",
    "unit_status",
    "value",
    "fiscal_year",
    "fiscal_period",
    "filed_date",
    "period_start",
    "period_end",
    "accession_number",
    "source_acceptance_datetime_utc",
    "source_retrieved_at_utc",
)


class EnrichmentError(RuntimeError):
    """Raised when an official-source or point-in-time invariant fails."""


def parse_date(value: object) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    if (
        len(text) == 10
        and text[4] == "-"
        and text[7] == "-"
        and text[:4].isdigit()
        and text[5:7].isdigit()
        and text[8:].isdigit()
    ):
        try:
            return date.fromisoformat(text)
        except ValueError:
            pass
    for pattern in ("%Y-%m-%d", "%d-%b-%Y", "%d-%B-%Y"):
        try:
            return datetime.strptime(text.upper(), pattern).date()
        except ValueError:
            continue
    raise EnrichmentError(f"Invalid date: {text!r}")


def parse_datetime(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise EnrichmentError(f"Timestamp lacks timezone: {text!r}")
    return parsed.astimezone(timezone.utc).replace(tzinfo=None)


def format_utc(value: datetime) -> str:
    aware = value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value
    return aware.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def cik10(value: object) -> str:
    text = re.sub(r"\D", "", str(value or ""))
    if not text:
        raise EnrichmentError(f"Invalid CIK: {value!r}")
    return text.zfill(10)


def finite_number(value: object) -> float | None:
    if value is None or str(value).strip() == "":
        return None
    number = float(value)
    if not math.isfinite(number):
        raise EnrichmentError(f"Non-finite numeric value: {value!r}")
    return number


def first_not_none(*values: object) -> object | None:
    return next((value for value in values if value is not None and str(value).strip() != ""), None)


def bool_value(value: object) -> bool | None:
    text = str(value or "").strip().lower()
    if not text:
        return None
    if text in {"1", "true", "y", "yes"}:
        return True
    if text in {"0", "false", "n", "no"}:
        return False
    raise EnrichmentError(f"Invalid boolean value: {value!r}")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def combined_revision(
    paths: Iterable[Path], known_revisions: Mapping[Path, str] | None = None
) -> str:
    digest = hashlib.sha256()
    for path in sorted(paths, key=lambda item: item.name):
        digest.update(path.name.encode())
        revision = (known_revisions or {}).get(path)
        digest.update(bytes.fromhex(revision or sha256_file(path)))
    return digest.hexdigest()


def log_stage(stage: str, phase: str, started_at: float) -> None:
    print(
        f"enrichment_stage stage={stage} phase={phase} "
        f"elapsed_seconds={time.monotonic() - started_at:.1f}",
        flush=True,
    )


def normalize_name(value: object) -> str:
    text = re.sub(r"[^A-Z0-9]+", " ", str(value or "").upper()).strip()
    return re.sub(r"\s+", " ", text)


def safe_divide(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator == 0:
        return None
    value = numerator / denominator
    return value if math.isfinite(value) else None


def safe_sum(values: Iterable[float | None]) -> float | None:
    materialized = list(values)
    if not materialized or any(value is None for value in materialized):
        return None
    return float(sum(value for value in materialized if value is not None))


def load_json(path: Path) -> object:
    data = path.read_bytes()
    if data.startswith(b"\x1f\x8b"):
        data = gzip.decompress(data)
    return json.loads(data)


def iter_json_documents(path: Path) -> Iterator[tuple[str, dict[str, object]]]:
    if zipfile.is_zipfile(path):
        with zipfile.ZipFile(path) as archive:
            for name in sorted(archive.namelist()):
                if not name.lower().endswith(".json"):
                    continue
                with archive.open(name) as handle:
                    value = json.load(io.TextIOWrapper(handle, encoding="utf-8"))
                if isinstance(value, dict):
                    yield name, value
        return
    value = load_json(path)
    if not isinstance(value, dict):
        raise EnrichmentError(f"JSON source root is not an object: {path}")
    yield path.name, value


def zip_tsv(path: Path, name: str) -> Iterator[dict[str, str]]:
    with zipfile.ZipFile(path) as archive:
        try:
            handle = archive.open(name)
        except KeyError as exc:
            raise EnrichmentError(f"{path.name} lacks {name}") from exc
        with handle, io.TextIOWrapper(handle, encoding="utf-8-sig", newline="") as text:
            reader = csv.DictReader(text, delimiter="\t")
            for raw in reader:
                yield {str(key): str(value or "").strip() for key, value in raw.items()}


def load_universe(path: Path) -> tuple[dict[str, dict[str, str]], dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        required = {
            "security_id",
            "ticker",
            "exchange_mic",
            "universe_admission_status",
        }
        if not required.issubset(reader.fieldnames or []):
            raise EnrichmentError("Security universe lacks required identity columns")
        by_id: dict[str, dict[str, str]] = {}
        ticker_to_id: dict[str, str] = {}
        for raw in reader:
            row = {str(key): str(value or "").strip() for key, value in raw.items()}
            if row["universe_admission_status"] != "ADMITTED":
                continue
            security_id = row["security_id"]
            ticker = row["ticker"].upper()
            by_id[security_id] = row
            if ticker in ticker_to_id and ticker_to_id[ticker] != security_id:
                raise EnrichmentError(f"Ambiguous admitted ticker: {ticker}")
            ticker_to_id[ticker] = security_id
    if not by_id:
        raise EnrichmentError("Security universe has no admitted securities")
    return by_id, ticker_to_id


def resolve_ticker(ticker: str, ticker_to_id: Mapping[str, str]) -> str | None:
    ticker = ticker.strip().upper()
    candidates = (ticker, ticker.replace("-", "."), ticker.replace("/", "."))
    matches = {ticker_to_id[value] for value in candidates if value in ticker_to_id}
    if len(matches) > 1:
        raise EnrichmentError(f"Ambiguous SEC/FINRA ticker alias: {ticker}")
    return next(iter(matches)) if matches else None


def conservative_available_date(
    filing_date: date | None, acceptance: datetime | None
) -> date | None:
    if acceptance is not None:
        eastern = acceptance.replace(tzinfo=timezone.utc).astimezone(
            ZoneInfo("America/New_York")
        )
        return eastern.date() if eastern.time() <= datetime_time(16, 0) else eastern.date() + timedelta(days=1)
    return filing_date + timedelta(days=1) if filing_date else None


def pit_fields(
    *,
    event_date: date | None,
    filing_date: date | None,
    acceptance: datetime | None,
    publication_date: date | None,
    retrieved_at: datetime,
) -> dict[str, object]:
    return {
        "source_event_date": event_date,
        "source_filing_date": filing_date,
        "source_acceptance_datetime_utc": acceptance,
        "source_publication_date": publication_date,
        "source_retrieved_at_utc": retrieved_at,
    }


def filing_document_url(cik: str, accession: str, primary_document: str) -> str:
    return (
        "https://www.sec.gov/Archives/edgar/data/"
        f"{int(cik)}/{accession.replace('-', '')}/{primary_document}"
    )


def recent_rows(document: Mapping[str, object]) -> Iterator[dict[str, object]]:
    filings = document.get("filings")
    if not isinstance(filings, dict):
        return
    recent = filings.get("recent")
    if not isinstance(recent, dict):
        return
    accession_values = recent.get("accessionNumber")
    if not isinstance(accession_values, list):
        return
    keys = tuple(recent)
    for index in range(len(accession_values)):
        yield {
            key: values[index] if isinstance(values, list) and index < len(values) else None
            for key, values in ((key, recent.get(key)) for key in keys)
        }


def parse_submissions(
    path: Path,
    ticker_to_id: Mapping[str, str],
    universe: Mapping[str, Mapping[str, str]],
    cutoff: date,
    retrieved_at: datetime,
    source_revision: str | None = None,
) -> tuple[
    list[dict[str, object]],
    list[dict[str, object]],
    dict[tuple[str, str], dict[str, object]],
    dict[str, str],
]:
    master_by_id: dict[str, dict[str, object]] = {}
    all_filings: dict[tuple[str, str], dict[str, object]] = {}
    cik_to_security: dict[str, str] = {}
    revision = source_revision or sha256_file(path)
    candidates: dict[str, tuple[dict[str, object], list[str], date]] = {}
    security_candidates: dict[str, list[tuple[date, str]]] = defaultdict(list)
    for _, document in iter_json_documents(path):
        raw_tickers = document.get("tickers")
        if not isinstance(raw_tickers, list):
            continue
        resolved_ids = [
            security_id
            for ticker in raw_tickers
            if (security_id := resolve_ticker(str(ticker), ticker_to_id)) is not None
        ]
        if not resolved_ids:
            continue
        cik = cik10(document.get("cik"))
        filings = document.get("filings")
        recent = filings.get("recent") if isinstance(filings, dict) else None
        filing_dates = recent.get("filingDate") if isinstance(recent, dict) else None
        activity_dates = [
            filing_date for value in filing_dates or []
            if (filing_date := parse_date(value)) is not None
            and filing_date <= cutoff
        ]
        latest_activity = max(activity_dates, default=date.min)
        ordered_ids = list(dict.fromkeys(resolved_ids))
        candidates[cik] = (document, ordered_ids, latest_activity)
        for security_id in ordered_ids:
            security_candidates[security_id].append((latest_activity, cik))

    selected_ciks_by_security: dict[str, str] = {}
    for security_id, choices in security_candidates.items():
        ordered = sorted(choices, reverse=True)
        if len(ordered) > 1 and ordered[0][0] == ordered[1][0]:
            raise EnrichmentError(
                f"Security {security_id} has an ambiguous SEC registrant tie at "
                f"{ordered[0][0]}: {ordered[0][1]} and {ordered[1][1]}"
            )
        selected_ciks_by_security[security_id] = ordered[0][1]

    securities_by_cik: dict[str, list[str]] = defaultdict(list)
    for security_id, cik in selected_ciks_by_security.items():
        securities_by_cik[cik].append(security_id)

    for cik, security_ids in securities_by_cik.items():
        document, ticker_order, _ = candidates[cik]
        ordered_ids = [value for value in ticker_order if value in security_ids]
        ordered_ids.extend(sorted(set(security_ids) - set(ordered_ids)))
        security_id = ordered_ids[0]
        cik_to_security[cik] = security_id
        for mapped_id in ordered_ids:
            collision_resolved = len(security_candidates[mapped_id]) > 1
            master_by_id[mapped_id] = {
                "security_id": mapped_id,
                "ticker": universe[mapped_id]["ticker"],
                "exchange_mic": universe[mapped_id]["exchange_mic"],
                "cik": cik,
                "registrant_name": str(document.get("name") or "").strip(),
                "sic": str(document.get("sic") or "").strip() or None,
                "mapping_status": (
                    "LATEST_SEC_FILING_TICKER_COLLISION"
                    if collision_resolved
                    else "EXACT_SEC_PRIMARY_TICKER"
                    if mapped_id == security_id
                    else "EXACT_SEC_ADDITIONAL_CLASS"
                ),
                "source_url": f"https://data.sec.gov/submissions/CIK{cik}.json",
                "source_revision": revision,
                **pit_fields(
                    event_date=None,
                    filing_date=None,
                    acceptance=None,
                    publication_date=None,
                    retrieved_at=retrieved_at,
                ),
            }

        for row in recent_rows(document):
            accession = str(row.get("accessionNumber") or "").strip()
            if not ACCESSION_PATTERN.fullmatch(accession):
                raise EnrichmentError(f"Invalid SEC accession number: {accession!r}")
            filing_date = parse_date(row.get("filingDate"))
            acceptance = parse_datetime(row.get("acceptanceDateTime"))
            if filing_date is None or filing_date > cutoff:
                continue
            form = str(row.get("form") or "").strip()
            subject_indexed_form = (
                "13D" in form
                or "13G" in form
                or form.removesuffix("/A") in {"3", "4", "5"}
            )
            is_accession_filer = cik == accession[:10]
            if (subject_indexed_form and is_accession_filer) or (
                not subject_indexed_form and not is_accession_filer
            ):
                continue
            report_date = parse_date(row.get("reportDate"))
            periodic_form = form.removesuffix("/A") in {
                "10-K",
                "10-Q",
                "20-F",
                "40-F",
            }
            if periodic_form and report_date and filing_date < report_date:
                raise EnrichmentError(
                    f"Filing {accession} precedes its report period: "
                    f"{filing_date} < {report_date}"
                )
            primary_document = str(row.get("primaryDocument") or "").strip()
            document_url = (
                filing_document_url(cik, accession, primary_document)
                if primary_document
                else f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace('-', '')}/"
            )
            filing_record = {
                "security_id": security_id,
                "cik": cik,
                "accession_number": accession,
                "form": form,
                "filing_date": filing_date,
                "acceptance_datetime_utc": acceptance,
                "report_date": report_date,
                "primary_document": primary_document or None,
                "primary_document_url": document_url,
                "is_amendment": form.endswith("/A"),
                "items": str(row.get("items") or "").strip() or None,
                "file_number": str(row.get("fileNumber") or "").strip() or None,
                "film_number": str(row.get("filmNumber") or "").strip() or None,
                **pit_fields(
                    event_date=report_date or filing_date,
                    filing_date=filing_date,
                    acceptance=acceptance,
                    publication_date=conservative_available_date(
                        filing_date, acceptance
                    ),
                    retrieved_at=retrieved_at,
                ),
            }
            filing_key = (cik, accession)
            previous = all_filings.get(filing_key)
            if previous is not None and previous != filing_record:
                raise EnrichmentError(
                    f"SEC filing key {filing_key} has conflicting metadata"
                )
            else:
                all_filings[filing_key] = filing_record
    for security_id, identity in universe.items():
        if security_id in master_by_id:
            continue
        master_by_id[security_id] = {
            "security_id": security_id,
            "ticker": identity["ticker"],
            "exchange_mic": identity["exchange_mic"],
            "cik": None,
            "registrant_name": None,
            "sic": None,
            "mapping_status": "UNMAPPED_SEC_CIK",
            "source_url": None,
            "source_revision": revision,
            **pit_fields(
                event_date=None,
                filing_date=None,
                acceptance=None,
                publication_date=None,
                retrieved_at=retrieved_at,
            ),
        }
    emitted = [
        row
        for row in all_filings.values()
        if row["form"] in SEC_FORMS or str(row["form"]).startswith("424B")
    ]
    return list(master_by_id.values()), emitted, all_filings, cik_to_security


def iter_company_facts(
    path: Path,
    cik_to_security: Mapping[str, str],
    filings: Mapping[tuple[str, str], Mapping[str, object]],
    cutoff: date,
    retrieved_at: datetime,
    source_revision: str | None = None,
) -> Iterator[dict[str, object]]:
    revision = source_revision or sha256_file(path)
    availability_cache: dict[tuple[date, datetime | None], date | None] = {}
    source_urls: dict[str, str] = {}
    for _, document in iter_json_documents(path):
        try:
            cik = cik10(document.get("cik"))
        except EnrichmentError:
            continue
        security_id = cik_to_security.get(cik)
        if security_id is None:
            continue
        facts = document.get("facts")
        if not isinstance(facts, dict):
            continue
        for taxonomy, concepts in facts.items():
            if not isinstance(concepts, dict):
                continue
            for concept, detail in concepts.items():
                if not isinstance(detail, dict):
                    continue
                units = detail.get("units")
                if not isinstance(units, dict):
                    continue
                for unit, observations in units.items():
                    if not isinstance(observations, list):
                        continue
                    for observation in observations:
                        if not isinstance(observation, dict):
                            continue
                        accession = str(observation.get("accn") or "").strip()
                        filed_date = parse_date(observation.get("filed"))
                        if not accession or filed_date is None or filed_date > cutoff:
                            continue
                        if not ACCESSION_PATTERN.fullmatch(accession):
                            raise EnrichmentError(
                                f"Invalid SEC fact accession number: {accession!r}"
                            )
                        period_start = parse_date(observation.get("start"))
                        period_end = parse_date(observation.get("end"))
                        if period_end is not None and filed_date < period_end:
                            # SEC bulk history contains a small number of observations
                            # whose reported period end is after the filing date.  They
                            # cannot be known at the stated filing time, so retaining
                            # them would violate the point-in-time contract.
                            continue
                        fiscal_year = observation.get("fy")
                        fiscal_period = str(observation.get("fp") or "").strip() or None
                        value = finite_number(observation.get("val"))
                        filing = filings.get((cik, accession), {})
                        acceptance = filing.get("acceptance_datetime_utc")
                        if acceptance is not None and not isinstance(acceptance, datetime):
                            acceptance = None
                        availability_key = (filed_date, acceptance)
                        publication = availability_cache.get(availability_key)
                        if availability_key not in availability_cache:
                            publication = conservative_available_date(
                                filed_date, acceptance
                            )
                            availability_cache[availability_key] = publication
                        row = {
                            "security_id": security_id,
                            "cik": cik,
                            "taxonomy": str(taxonomy),
                            "concept": str(concept),
                            "label": str(detail.get("label") or "").strip() or None,
                            "description": str(detail.get("description") or "").strip() or None,
                            "unit": str(unit),
                            "unit_status": "SUPPORTED" if unit in SUPPORTED_UNITS else "UNSUPPORTED",
                            "value": value,
                            "fiscal_year": int(fiscal_year) if fiscal_year is not None else None,
                            "fiscal_period": fiscal_period,
                            "form": str(observation.get("form") or "").strip() or None,
                            "filed_date": filed_date,
                            "period_start": period_start,
                            "period_end": period_end,
                            "frame": str(observation.get("frame") or "").strip() or None,
                            "accession_number": accession,
                            "source_url": source_urls.setdefault(
                                cik,
                                "https://data.sec.gov/api/xbrl/companyfacts/"
                                f"CIK{cik}.json",
                            ),
                            "source_revision": revision,
                            **pit_fields(
                                event_date=period_end,
                                filing_date=filed_date,
                                acceptance=acceptance,
                                publication_date=publication,
                                retrieved_at=retrieved_at,
                            ),
                        }
                        yield row


def build_company_facts_asset(
    source_path: Path,
    output_path: Path,
    cik_to_security: Mapping[str, str],
    filings: Mapping[tuple[str, str], Mapping[str, object]],
    cutoff: date,
    retrieved_at: datetime,
    concept_map: Mapping[str, object],
    source_revision: str | None = None,
) -> tuple[list[dict[str, object]], dict[str, object]]:
    raw_concepts = concept_map.get("concepts")
    if not isinstance(raw_concepts, dict):
        raise EnrichmentError("SEC concept map has no concepts object")
    normalization_concepts = {
        str(concept)
        for precedence in raw_concepts.values()
        if isinstance(precedence, list)
        for concept in precedence
    }
    normalization_floor = cutoff.year - 5
    normalization_rows: list[dict[str, object]] = []
    minimum_event_date: date | None = None
    maximum_event_date: date | None = None
    has_ifrs = False

    def rows() -> Iterator[dict[str, object]]:
        nonlocal minimum_event_date, maximum_event_date, has_ifrs
        for row in iter_company_facts(
            source_path,
            cik_to_security,
            filings,
            cutoff,
            retrieved_at,
            source_revision,
        ):
            event_date = row.get("period_end")
            if isinstance(event_date, date):
                minimum_event_date = (
                    event_date
                    if minimum_event_date is None
                    else min(minimum_event_date, event_date)
                )
                maximum_event_date = (
                    event_date
                    if maximum_event_date is None
                    else max(maximum_event_date, event_date)
                )
            if row.get("taxonomy") == "ifrs-full":
                has_ifrs = True
            fiscal_year = row.get("fiscal_year")
            if (
                row.get("concept") in normalization_concepts
                and isinstance(fiscal_year, int)
                and fiscal_year >= normalization_floor
            ):
                normalization_rows.append(
                    {column: row[column] for column in NORMALIZATION_FACT_COLUMNS}
                )
            yield row

    row_count = write_parquet(
        output_path, CONTRACTS["sec-company-facts.parquet"], rows()
    )
    return normalization_rows, {
        "row_count": row_count,
        "minimum_event_date": minimum_event_date.isoformat()
        if minimum_event_date
        else None,
        "maximum_event_date": maximum_event_date.isoformat()
        if maximum_event_date
        else None,
        "has_ifrs": has_ifrs,
        "normalization_floor_fiscal_year": normalization_floor,
    }


def choose_fact(
    rows: list[dict[str, object]],
    concepts: list[str],
    taxonomy_precedence: list[str],
    target: str,
    fiscal_period: str,
) -> dict[str, object] | None:
    expected_unit = "shares" if target in SHARE_FIELDS else "USD"
    for concept in concepts:
        candidates = []
        for taxonomy in taxonomy_precedence:
            candidates = [
                row
                for row in rows
                if row["concept"] == concept
                and row["taxonomy"] == taxonomy
                and row["unit"] == expected_unit
                and row["unit_status"] == "SUPPORTED"
            ]
            if candidates:
                break
        if target in FLOW_FIELDS:
            candidates = [
                row
                for row in candidates
                if isinstance(row["period_start"], date)
                and isinstance(row["period_end"], date)
            ]
            if fiscal_period == "FY":
                candidates = [
                    row
                    for row in candidates
                    if 250 <= (row["period_end"] - row["period_start"]).days <= 430
                ]
            else:
                maximum_days = {"Q1": 130, "Q2": 220, "Q3": 310}.get(
                    fiscal_period, 130
                )
                candidates = [
                    row
                    for row in candidates
                    if 45
                    <= (row["period_end"] - row["period_start"]).days
                    <= maximum_days
                ]
            candidates.sort(
                key=lambda row: (
                    abs((row["period_end"] - row["period_start"]).days - (365 if fiscal_period == "FY" else 91)),
                    -row["filed_date"].toordinal(),
                )
            )
        else:
            candidates.sort(
                key=lambda row: (str(row["period_end"]), str(row["filed_date"])),
                reverse=True,
            )
        if candidates:
            return candidates[0]
    return None


def normalize_fundamentals(
    facts: list[dict[str, object]],
    concept_map: Mapping[str, object],
) -> list[dict[str, object]]:
    concepts = concept_map.get("concepts")
    if not isinstance(concepts, dict):
        raise EnrichmentError("SEC concept map has no concepts object")
    version = str(concept_map.get("version") or "")
    raw_taxonomies = concept_map.get("taxonomy_precedence")
    if not isinstance(raw_taxonomies, list) or not raw_taxonomies:
        raise EnrichmentError("SEC concept map has no taxonomy_precedence")
    taxonomy_precedence = [str(value) for value in raw_taxonomies]
    grouped: dict[tuple[str, int, str], list[dict[str, object]]] = defaultdict(list)
    for row in facts:
        fiscal_year = row.get("fiscal_year")
        fiscal_period = row.get("fiscal_period")
        if isinstance(fiscal_year, int) and fiscal_period in {"Q1", "Q2", "Q3", "FY"}:
            grouped[(str(row["security_id"]), fiscal_year, str(fiscal_period))].append(row)
    output: list[dict[str, object]] = []
    for (security_id, fiscal_year, fiscal_period), group_rows in sorted(grouped.items()):
        accessions = defaultdict(list)
        for row in group_rows:
            accessions[str(row["accession_number"])].append(row)
        latest_accession = max(
            accessions,
            key=lambda accession: (
                max(str(row["filed_date"]) for row in accessions[accession]),
                accession,
            ),
        )
        selected_rows = accessions[latest_accession]
        values: dict[str, float | None] = {}
        chosen: dict[str, dict[str, object]] = {}
        for target, precedence in concepts.items():
            if not isinstance(precedence, list):
                continue
            fact = choose_fact(
                selected_rows,
                [str(value) for value in precedence],
                taxonomy_precedence,
                str(target),
                fiscal_period,
            )
            values[str(target)] = float(fact["value"]) if fact and fact["value"] is not None else None
            if fact:
                chosen[str(target)] = fact
        capex = values.get("capital_expenditure")
        if capex is not None:
            capex = abs(capex)
        short_debt = values.get("short_term_debt")
        long_debt = values.get("long_term_debt")
        total_debt = values.get("total_debt")
        if total_debt is None and short_debt is not None and long_debt is not None:
            total_debt = short_debt + long_debt
        equity = values.get("stockholders_equity")
        goodwill = values.get("goodwill")
        intangible = values.get("intangible_assets")
        tangible_book = (
            equity - goodwill - intangible
            if equity is not None and goodwill is not None and intangible is not None
            else None
        )
        filing_date = max(
            row["filed_date"] for row in selected_rows if isinstance(row["filed_date"], date)
        )
        acceptance_values = [
            row["source_acceptance_datetime_utc"]
            for row in selected_rows
            if isinstance(row["source_acceptance_datetime_utc"], datetime)
        ]
        acceptance = max(acceptance_values) if acceptance_values else None
        available = conservative_available_date(filing_date, acceptance)
        period_end_values = [
            row["period_end"] for row in selected_rows if isinstance(row["period_end"], date)
        ]
        period_start_values = [
            row["period_start"]
            for row in selected_rows
            if isinstance(row["period_start"], date)
        ]
        missing = sorted(field for field in CORE_NORMALIZED_FIELDS if values.get(field) is None)
        output.append(
            {
                "security_id": security_id,
                "cik": str(selected_rows[0]["cik"]),
                "fiscal_year": fiscal_year,
                "fiscal_period": fiscal_period,
                "period_start": min(period_start_values) if period_start_values else None,
                "period_end": max(period_end_values) if period_end_values else None,
                "filing_date": filing_date,
                "filing_available_date": available,
                "accession_number": latest_accession,
                "currency": "USD",
                "revenue": values.get("revenue"),
                "gross_profit": values.get("gross_profit"),
                "operating_income": values.get("operating_income"),
                "ebit": values.get("operating_income"),
                "pretax_income": values.get("pretax_income"),
                "net_income": values.get("net_income"),
                "operating_cash_flow": values.get("operating_cash_flow"),
                "capital_expenditure": capex,
                "free_cash_flow": (
                    values["operating_cash_flow"] - capex
                    if values.get("operating_cash_flow") is not None and capex is not None
                    else None
                ),
                "cash_and_equivalents": values.get("cash_and_equivalents"),
                "current_assets": values.get("current_assets"),
                "current_liabilities": values.get("current_liabilities"),
                "total_assets": values.get("total_assets"),
                "total_liabilities": values.get("total_liabilities"),
                "total_debt": total_debt,
                "short_term_debt": short_debt,
                "long_term_debt": long_debt,
                "stockholders_equity": equity,
                "tangible_book_value": tangible_book,
                "shares_basic": values.get("shares_basic"),
                "shares_diluted": values.get("shares_diluted"),
                "stock_based_compensation": values.get("stock_based_compensation"),
                "interest_expense": values.get("interest_expense"),
                "accounts_receivable": values.get("accounts_receivable"),
                "inventory": values.get("inventory"),
                "source_concept_map_version": version,
                "normalization_status": "COMPLETE" if not missing else "PARTIAL",
                "missing_required_fields": json.dumps(missing, separators=(",", ":")),
                "_flow_period_days": {
                    name: (
                        (fact["period_end"] - fact["period_start"]).days
                        if fact
                        and isinstance(fact.get("period_start"), date)
                        and isinstance(fact.get("period_end"), date)
                        else None
                    )
                    for name, fact in chosen.items()
                    if name in DERIVABLE_FLOW_FIELDS
                },
                **pit_fields(
                    event_date=max(period_end_values) if period_end_values else None,
                    filing_date=filing_date,
                    acceptance=acceptance,
                    publication_date=available,
                    retrieved_at=max(
                        row["source_retrieved_at_utc"]
                        for row in selected_rows
                        if isinstance(row["source_retrieved_at_utc"], datetime)
                    ),
                ),
            }
        )

    by_year: dict[tuple[str, int], dict[str, dict[str, object]]] = defaultdict(dict)
    for row in output:
        by_year[(str(row["security_id"]), int(row["fiscal_year"]))][
            str(row["fiscal_period"])
        ] = row

    quarterly: list[dict[str, object]] = []
    for _, periods in sorted(by_year.items()):
        completed: list[dict[str, object]] = []
        for fiscal_period in ("Q1", "Q2", "Q3"):
            row = periods.get(fiscal_period)
            if row is None:
                continue
            period_days = row.get("_flow_period_days")
            for field in DERIVABLE_FLOW_FIELDS:
                duration = (
                    period_days.get(field)
                    if isinstance(period_days, dict)
                    else None
                )
                value = finite_number(row.get(field))
                if value is not None and isinstance(duration, int) and duration > 130:
                    prior = [finite_number(item.get(field)) for item in completed]
                    row[field] = (
                        value - sum(item for item in prior if item is not None)
                        if len(prior) == len(completed)
                        and all(item is not None for item in prior)
                        else None
                    )
            capex = finite_number(row.get("capital_expenditure"))
            operating_cash = finite_number(row.get("operating_cash_flow"))
            row["free_cash_flow"] = (
                operating_cash - capex
                if operating_cash is not None and capex is not None
                else None
            )
            completed.append(row)

        annual = periods.get("FY")
        if annual is not None:
            fourth = dict(annual)
            fourth["fiscal_period"] = "Q4"
            if completed and isinstance(completed[-1].get("period_end"), date):
                fourth["period_start"] = completed[-1]["period_end"] + timedelta(days=1)
            for field in DERIVABLE_FLOW_FIELDS:
                annual_value = finite_number(annual.get(field))
                prior = [finite_number(item.get(field)) for item in completed]
                fourth[field] = (
                    annual_value - sum(item for item in prior if item is not None)
                    if annual_value is not None
                    and len(completed) == 3
                    and all(item is not None for item in prior)
                    else None
                )
            fourth["free_cash_flow"] = (
                fourth["operating_cash_flow"] - fourth["capital_expenditure"]
                if fourth.get("operating_cash_flow") is not None
                and fourth.get("capital_expenditure") is not None
                else None
            )
            missing = sorted(
                field for field in CORE_NORMALIZED_FIELDS if fourth.get(field) is None
            )
            fourth["normalization_status"] = "COMPLETE" if not missing else "PARTIAL"
            fourth["missing_required_fields"] = json.dumps(
                missing, separators=(",", ":")
            )
            completed.append(fourth)

        for row in completed:
            row.pop("_flow_period_days", None)
            missing = sorted(
                field for field in CORE_NORMALIZED_FIELDS if row.get(field) is None
            )
            row["normalization_status"] = "COMPLETE" if not missing else "PARTIAL"
            row["missing_required_fields"] = json.dumps(missing, separators=(",", ":"))
            quarterly.append(row)
    return quarterly


def latest_prices(path: Path, cutoff: date) -> dict[str, tuple[date, float]]:
    con = duckdb.connect()
    try:
        rows = con.execute(
            """
            SELECT security_id, session_date, close
              FROM read_parquet(?)
             WHERE session_date <= ?
             QUALIFY row_number() OVER (
               PARTITION BY security_id ORDER BY session_date DESC
             ) = 1
            """,
            [str(path), cutoff],
        ).fetchall()
    finally:
        con.close()
    return {str(security_id): (session_date, float(close)) for security_id, session_date, close in rows}


def growth(current: float | None, previous: float | None) -> float | None:
    if current is None or previous is None or previous == 0:
        return None
    return current / previous - 1


def build_factors(
    normalized: list[dict[str, object]],
    prices: Mapping[str, tuple[date, float]],
    cutoff: date,
    retrieved_at: datetime,
) -> list[dict[str, object]]:
    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in normalized:
        available = row.get("filing_available_date")
        if isinstance(available, date) and available <= cutoff:
            grouped[str(row["security_id"])].append(row)
    output: list[dict[str, object]] = []
    flow_names = {
        "revenue": "trailing_revenue",
        "operating_income": "trailing_operating_income",
        "net_income": "trailing_net_income",
        "operating_cash_flow": "trailing_operating_cash_flow",
        "capital_expenditure": "trailing_capex",
        "free_cash_flow": "trailing_free_cash_flow",
    }
    for security_id, rows in grouped.items():
        rows.sort(key=lambda row: (row.get("period_end") or date.min, row["filing_date"]))
        latest = rows[-1]
        last_four = rows[-4:]
        trailing = {
            output_name: safe_sum(
                finite_number(row.get(input_name)) for row in last_four
            )
            if len(last_four) == 4
            else None
            for input_name, output_name in flow_names.items()
        }
        shares = finite_number(
            first_not_none(latest.get("shares_diluted"), latest.get("shares_basic"))
        )
        price = prices.get(security_id)
        market_cap = price[1] * shares if price and shares is not None else None
        total_debt = finite_number(latest.get("total_debt"))
        cash = finite_number(latest.get("cash_and_equivalents"))
        net_debt = total_debt - cash if total_debt is not None and cash is not None else None
        enterprise_value = (
            market_cap + net_debt if market_cap is not None and net_debt is not None else None
        )
        revenue = trailing["trailing_revenue"]
        operating_income = trailing["trailing_operating_income"]
        net_income = trailing["trailing_net_income"]
        operating_cash = trailing["trailing_operating_cash_flow"]
        free_cash = trailing["trailing_free_cash_flow"]
        interest = safe_sum(
            finite_number(row.get("interest_expense")) for row in last_four
        ) if len(last_four) == 4 else None
        current_assets = finite_number(latest.get("current_assets"))
        current_liabilities = finite_number(latest.get("current_liabilities"))
        equity = finite_number(latest.get("stockholders_equity"))
        tangible_book = finite_number(latest.get("tangible_book_value"))
        gross_margins = [
            margin
            for row in rows[-12:]
            if (margin := safe_divide(finite_number(row.get("gross_profit")), finite_number(row.get("revenue"))))
            is not None
        ]
        fcf_values = [finite_number(row.get("free_cash_flow")) for row in rows[-12:]]
        tax_rate = None
        pretax = safe_sum(finite_number(row.get("pretax_income")) for row in last_four) if len(last_four) == 4 else None
        if pretax and net_income is not None:
            tax_rate = min(0.40, max(0.0, 1 - net_income / pretax))
        nopat = operating_income * (1 - tax_rate) if operating_income is not None and tax_rate is not None else None
        invested_capital = equity + net_debt if equity is not None and net_debt is not None else None
        previous_year = rows[-5] if len(rows) >= 5 else None
        previous_three_year = rows[-13] if len(rows) >= 13 else None
        previous_nopat = None
        previous_invested_capital = None
        if len(rows) >= 8 and previous_year is not None:
            prior_four = rows[-8:-4]
            prior_operating_income = safe_sum(
                finite_number(row.get("operating_income")) for row in prior_four
            )
            prior_pretax = safe_sum(
                finite_number(row.get("pretax_income")) for row in prior_four
            )
            prior_net_income = safe_sum(
                finite_number(row.get("net_income")) for row in prior_four
            )
            prior_tax_rate = None
            if prior_pretax and prior_net_income is not None:
                prior_tax_rate = min(
                    0.40, max(0.0, 1 - prior_net_income / prior_pretax)
                )
            if prior_operating_income is not None and prior_tax_rate is not None:
                previous_nopat = prior_operating_income * (1 - prior_tax_rate)
            prior_debt = finite_number(previous_year.get("total_debt"))
            prior_cash = finite_number(previous_year.get("cash_and_equivalents"))
            prior_equity = finite_number(previous_year.get("stockholders_equity"))
            prior_net_debt = (
                prior_debt - prior_cash
                if prior_debt is not None and prior_cash is not None
                else None
            )
            previous_invested_capital = (
                prior_equity + prior_net_debt
                if prior_equity is not None and prior_net_debt is not None
                else None
            )
        incremental_roic = (
            safe_divide(
                nopat - previous_nopat,
                invested_capital - previous_invested_capital,
            )
            if nopat is not None
            and previous_nopat is not None
            and invested_capital is not None
            and previous_invested_capital is not None
            else None
        )
        latest_available = latest.get("filing_available_date")
        missing_fields: list[str] = []
        factor_values = {
            **trailing,
            "net_debt": net_debt,
            "market_cap": market_cap,
            "enterprise_value": enterprise_value,
            "earnings_yield": safe_divide(net_income, market_cap),
            "free_cash_flow_yield": safe_divide(free_cash, market_cap),
            "ev_to_ebit": safe_divide(enterprise_value, operating_income),
            "price_to_tangible_book": safe_divide(market_cap, tangible_book),
            "current_ratio": safe_divide(current_assets, current_liabilities),
            "interest_coverage": safe_divide(operating_income, interest),
            "debt_to_ebitda": None,
            "gross_margin": safe_divide(
                safe_sum(finite_number(row.get("gross_profit")) for row in last_four)
                if len(last_four) == 4
                else None,
                revenue,
            ),
            "operating_margin": safe_divide(operating_income, revenue),
            "free_cash_flow_margin": safe_divide(free_cash, revenue),
            "cash_conversion": safe_divide(free_cash, net_income),
            "roic": safe_divide(nopat, invested_capital),
            "incremental_roic": incremental_roic,
            "reinvestment_rate": safe_divide(
                None if free_cash is None or nopat is None else nopat - free_cash,
                nopat,
            ),
            "revenue_growth_yoy": growth(
                finite_number(latest.get("revenue")),
                finite_number(previous_year.get("revenue")) if previous_year else None,
            ),
            "revenue_growth_cagr_3y": (
                (finite_number(latest.get("revenue")) / finite_number(previous_three_year.get("revenue"))) ** (1 / 3) - 1
                if previous_three_year
                and finite_number(latest.get("revenue")) is not None
                and finite_number(previous_three_year.get("revenue")) not in {None, 0}
                and finite_number(latest.get("revenue")) > 0
                and finite_number(previous_three_year.get("revenue")) > 0
                else None
            ),
            "earnings_growth_yoy": growth(
                finite_number(latest.get("net_income")),
                finite_number(previous_year.get("net_income")) if previous_year else None,
            ),
            "free_cash_flow_growth_yoy": growth(
                finite_number(latest.get("free_cash_flow")),
                finite_number(previous_year.get("free_cash_flow")) if previous_year else None,
            ),
            "share_count_change_yoy": growth(
                shares,
                (
                    finite_number(
                        first_not_none(
                            previous_year.get("shares_diluted"),
                            previous_year.get("shares_basic"),
                        )
                    )
                    if previous_year
                    else None
                ),
            ),
            "gross_margin_stability_3y": (
                statistics.pstdev(gross_margins) if len(gross_margins) >= 8 else None
            ),
            "free_cash_flow_consistency_3y": (
                sum(value is not None and value > 0 for value in fcf_values) / len(fcf_values)
                if len(fcf_values) >= 8
                else None
            ),
        }
        for name, value in factor_values.items():
            if value is None:
                missing_fields.append(name)
        if isinstance(latest_available, date) and latest_available > cutoff:
            raise EnrichmentError(f"Factor for {security_id} uses an unavailable filing")
        output.append(
            {
                "security_id": security_id,
                "factor_as_of_date": cutoff,
                "latest_filing_date": latest["filing_date"],
                "filing_available_date": latest_available,
                **factor_values,
                "factor_quality_status": "READY" if len(missing_fields) <= 6 else "PARTIAL",
                "factor_missing_fields": json.dumps(sorted(missing_fields), separators=(",", ":")),
                "model_version": FACTOR_MODEL_VERSION,
                **pit_fields(
                    event_date=latest.get("period_end") if isinstance(latest.get("period_end"), date) else None,
                    filing_date=latest["filing_date"],
                    acceptance=latest.get("source_acceptance_datetime_utc") if isinstance(latest.get("source_acceptance_datetime_utc"), datetime) else None,
                    publication_date=latest_available if isinstance(latest_available, date) else None,
                    retrieved_at=retrieved_at,
                ),
            }
        )
    return output


def event_id(*values: object) -> str:
    return hashlib.sha256("|".join(str(value or "") for value in values).encode()).hexdigest()


def build_events(
    filings: Iterable[Mapping[str, object]], retrieved_at: datetime
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    events: list[dict[str, object]] = []
    earnings: list[dict[str, object]] = []
    form_events = {
        "SC 13D": ("ACTIVIST_13D", "INITIAL", "HIGH", 0.9),
        "SC 13D/A": ("ACTIVIST_13D", "AMENDMENT", "HIGH", 0.8),
        "SC 13G": ("PASSIVE_13G", "INITIAL", "HIGH", 0.7),
        "SC 13G/A": ("PASSIVE_13G", "AMENDMENT", "HIGH", 0.6),
        "SCHEDULE 13D": ("ACTIVIST_13D", "INITIAL", "HIGH", 0.9),
        "SCHEDULE 13D/A": ("ACTIVIST_13D", "AMENDMENT", "HIGH", 0.8),
        "SCHEDULE 13G": ("PASSIVE_13G", "INITIAL", "HIGH", 0.7),
        "SCHEDULE 13G/A": ("PASSIVE_13G", "AMENDMENT", "HIGH", 0.6),
        "SC TO": ("TENDER_OFFER", "SCHEDULE_TO", "HIGH", 0.9),
        "SC TO-I": ("TENDER_OFFER", "ISSUER_TENDER", "HIGH", 0.9),
        "SC TO-T": ("TENDER_OFFER", "THIRD_PARTY_TENDER", "HIGH", 0.9),
        "S-1": ("EQUITY_ISSUANCE", "REGISTRATION", "MEDIUM", 0.5),
        "S-3": ("EQUITY_ISSUANCE", "SHELF_REGISTRATION", "MEDIUM", 0.4),
    }
    item_events = {
        "2.01": ("ASSET_SALE", "ACQUISITION_OR_DISPOSITION_COMPLETED", 0.7),
        "2.02": ("EARNINGS_RELEASE", "RESULTS_OF_OPERATIONS", 0.8),
        "2.05": ("STRATEGIC_REVIEW", "EXIT_OR_DISPOSAL_ACTIVITY", 0.6),
        "3.02": ("EQUITY_ISSUANCE", "UNREGISTERED_EQUITY_SALE", 0.7),
        "5.02": ("MANAGEMENT_CHANGE", "DIRECTOR_OR_OFFICER_CHANGE", 0.7),
        "1.05": ("CYBERSECURITY_INCIDENT", "MATERIAL_CYBERSECURITY_INCIDENT", 0.9),
    }
    for filing in filings:
        form = str(filing["form"])
        specs: list[tuple[str, str, str, float]] = []
        if form in form_events:
            specs.append(form_events[form])
        if form.startswith("424B"):
            specs.append(("EQUITY_ISSUANCE", "PROSPECTUS", "MEDIUM", 0.5))
        items = [value.strip() for value in str(filing.get("items") or "").split(",") if value.strip()]
        if form in {"8-K", "8-K/A"}:
            for item in items:
                if item in item_events:
                    event_type, subtype, score = item_events[item]
                    specs.append((event_type, f"8-K_ITEM_{item}:{subtype}", "HIGH", score))
        acceptance = filing.get("acceptance_datetime_utc")
        if acceptance is not None and not isinstance(acceptance, datetime):
            acceptance = None
        for event_type, subtype, confidence, score in specs:
            identifier = event_id(
                filing["security_id"], filing["accession_number"], event_type, subtype
            )
            events.append(
                {
                    "event_id": identifier,
                    "security_id": filing["security_id"],
                    "cik": filing["cik"],
                    "event_type": event_type,
                    "event_subtype": subtype,
                    "announcement_datetime_utc": acceptance,
                    "effective_date": filing.get("report_date") or filing["filing_date"],
                    "expiration_date": None,
                    "filing_form": form,
                    "accession_number": filing["accession_number"],
                    "source_document_url": filing["primary_document_url"],
                    "headline": f"SEC {form} filing: {subtype}",
                    "summary": "Deterministic event classification from SEC form and item metadata.",
                    "counterparty": None,
                    "cash_value": None,
                    "share_value": None,
                    "exchange_ratio": None,
                    "ownership_percentage": None,
                    "board_seats_requested": None,
                    "event_status": "FILED",
                    "event_confidence": confidence,
                    "materiality_score": score,
                    "parser_version": PARSER_VERSION,
                    **pit_fields(
                        event_date=filing.get("report_date") or filing["filing_date"],
                        filing_date=filing["filing_date"],
                        acceptance=acceptance,
                        publication_date=filing.get("source_publication_date"),
                        retrieved_at=retrieved_at,
                    ),
                }
            )
        if form in {"8-K", "8-K/A"} and "2.02" in items:
            event_date_value = (
                acceptance.date() if isinstance(acceptance, datetime) else filing["filing_date"]
            )
            classification = "TIME_UNKNOWN"
            if isinstance(acceptance, datetime):
                eastern = acceptance.replace(tzinfo=timezone.utc).astimezone(
                    ZoneInfo("America/New_York")
                )
                if eastern.time() < datetime_time(9, 30):
                    classification = "BEFORE_MARKET"
                elif eastern.time() <= datetime_time(16, 0):
                    classification = "DURING_MARKET"
                else:
                    classification = "AFTER_MARKET"
            earnings.append(
                {
                    "event_id": event_id(filing["accession_number"], "EARNINGS"),
                    "security_id": filing["security_id"],
                    "event_datetime_utc": acceptance,
                    "event_date": event_date_value,
                    "event_time_classification": classification,
                    "event_type": "EARNINGS_RELEASE",
                    "fiscal_period": None,
                    "revenue_reported": None,
                    "eps_reported": None,
                    "revenue_guidance_low": None,
                    "revenue_guidance_high": None,
                    "eps_guidance_low": None,
                    "eps_guidance_high": None,
                    "guidance_direction": None,
                    "source_type": "SEC_8_K_ITEM_2_02",
                    "filing_accession_number": filing["accession_number"],
                    "source_document_url": filing["primary_document_url"],
                    **pit_fields(
                        event_date=event_date_value,
                        filing_date=filing["filing_date"],
                        acceptance=acceptance,
                        publication_date=filing.get("source_publication_date"),
                        retrieved_at=retrieved_at,
                    ),
                }
            )
    return events, earnings


def parse_insider_archives(
    paths: list[Path],
    cik_to_security: Mapping[str, str],
    ticker_to_id: Mapping[str, str],
    filings: Mapping[tuple[str, str], Mapping[str, object]],
    cutoff: date,
    retrieved_at: datetime,
) -> list[dict[str, object]]:
    output: dict[tuple[str, str, str], dict[str, object]] = {}
    for path in paths:
        submissions = {row["ACCESSION_NUMBER"]: row for row in zip_tsv(path, "SUBMISSION.tsv")}
        owners: dict[str, list[dict[str, str]]] = defaultdict(list)
        for row in zip_tsv(path, "REPORTINGOWNER.tsv"):
            owners[row["ACCESSION_NUMBER"]].append(row)
        for table_name, derivative in (("NONDERIV_TRANS.tsv", False), ("DERIV_TRANS.tsv", True)):
            id_field = "DERIV_TRANS_SK" if derivative else "NONDERIV_TRANS_SK"
            for transaction in zip_tsv(path, table_name):
                accession = transaction["ACCESSION_NUMBER"]
                submission = submissions.get(accession)
                if submission is None or not ACCESSION_PATTERN.fullmatch(accession):
                    raise EnrichmentError(f"Insider transaction lacks filing provenance: {accession}")
                filing_date = parse_date(submission.get("FILING_DATE"))
                transaction_date = parse_date(transaction.get("TRANS_DATE"))
                if filing_date is None or filing_date > cutoff or transaction_date is None:
                    continue
                issuer_cik = cik10(submission.get("ISSUERCIK"))
                security_id = cik_to_security.get(issuer_cik) or resolve_ticker(
                    submission.get("ISSUERTRADINGSYMBOL", ""), ticker_to_id
                )
                if security_id is None:
                    continue
                filing = filings.get((issuer_cik, accession), {})
                acceptance = filing.get("acceptance_datetime_utc")
                if not isinstance(acceptance, datetime):
                    acceptance = None
                publication = conservative_available_date(filing_date, acceptance)
                reporting_owners = owners.get(accession)
                if not reporting_owners:
                    raise EnrichmentError(f"Insider transaction has no reporting owner: {accession}")
                for owner in reporting_owners:
                    relationship = owner.get("RPTOWNER_RELATIONSHIP", "")
                    owner_cik = cik10(owner.get("RPTOWNERCIK"))
                    transaction_id = ("D:" if derivative else "N:") + transaction[id_field]
                    key = (accession, owner_cik, transaction_id)
                    row = {
                        "security_id": security_id,
                        "issuer_cik": issuer_cik,
                        "reporting_owner_cik": owner_cik,
                        "reporting_owner_name": owner.get("RPTOWNERNAME") or None,
                        "is_director": "Director" in relationship,
                        "is_officer": "Officer" in relationship,
                        "is_ten_percent_owner": "TenPercentOwner" in relationship,
                        "officer_title": owner.get("RPTOWNER_TITLE") or None,
                        "filing_date": filing_date,
                        "transaction_date": transaction_date,
                        "form": submission.get("DOCUMENT_TYPE") or transaction.get("TRANS_FORM_TYPE") or None,
                        "accession_number": accession,
                        "transaction_id": transaction_id,
                        "security_title": transaction.get("SECURITY_TITLE") or None,
                        "transaction_code": transaction.get("TRANS_CODE") or None,
                        "acquired_disposed": transaction.get("TRANS_ACQUIRED_DISP_CD") or None,
                        "shares": finite_number(transaction.get("TRANS_SHARES")),
                        "price_per_share": finite_number(transaction.get("TRANS_PRICEPERSHARE")),
                        "ownership_nature": transaction.get("NATURE_OF_OWNERSHIP") or transaction.get("DIRECT_INDIRECT_OWNERSHIP") or None,
                        "shares_owned_after": finite_number(transaction.get("SHRS_OWND_FOLWNG_TRANS")),
                        "is_derivative": derivative,
                        "exercise_price": finite_number(transaction.get("CONV_EXERCISE_PRICE")) if derivative else None,
                        "expiration_date": parse_date(transaction.get("EXPIRATION_DATE")) if derivative else None,
                        "is_10b5_1": bool_value(submission.get("AFF10B5ONE")),
                        **pit_fields(
                            event_date=transaction_date,
                            filing_date=filing_date,
                            acceptance=acceptance,
                            publication_date=publication,
                            retrieved_at=retrieved_at,
                        ),
                    }
                    previous = output.get(key)
                    if previous is not None and previous != row:
                        raise EnrichmentError(f"Conflicting insider transaction {key}")
                    output[key] = row
    return list(output.values())


def build_insider_signals(
    rows: list[dict[str, object]], cutoff: date, retrieved_at: datetime
) -> list[dict[str, object]]:
    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        if row["source_publication_date"] and row["source_publication_date"] <= cutoff:
            grouped[str(row["security_id"])].append(row)
    output = []
    for security_id, transactions in grouped.items():
        def candidate(row: Mapping[str, object], code: str, disposition: str, days: int) -> bool:
            transaction_date = row.get("transaction_date")
            return (
                not row.get("is_derivative")
                and row.get("transaction_code") == code
                and row.get("acquired_disposed") == disposition
                and isinstance(transaction_date, date)
                and cutoff - timedelta(days=days) <= transaction_date <= cutoff
            )

        def value(row: Mapping[str, object]) -> float | None:
            shares = finite_number(row.get("shares"))
            price = finite_number(row.get("price_per_share"))
            return shares * price if shares is not None and price is not None else None

        def total_value(values: list[dict[str, object]]) -> float | None:
            amounts = [value(row) for row in values]
            if any(amount is None for amount in amounts):
                return None
            return float(sum(amount for amount in amounts if amount is not None))

        buys90 = [row for row in transactions if candidate(row, "P", "A", 90)]
        buys30 = [row for row in transactions if candidate(row, "P", "A", 30)]
        sells90 = [row for row in transactions if candidate(row, "S", "D", 90)]
        sells30 = [row for row in transactions if candidate(row, "S", "D", 30)]
        buyer_count = len({str(row["reporting_owner_cik"]) for row in buys90})
        seller_count = len({str(row["reporting_owner_cik"]) for row in sells90})
        director = any(bool(row.get("is_director")) for row in buys90)
        ceo = any("CEO" in str(row.get("officer_title") or "").upper() or "CHIEF EXECUTIVE" in str(row.get("officer_title") or "").upper() for row in buys90)
        cfo = any("CFO" in str(row.get("officer_title") or "").upper() or "CHIEF FINANCIAL" in str(row.get("officer_title") or "").upper() for row in buys90)
        buy_value = total_value(buys90)
        sale_value = total_value(sells90)
        score = (
            min(1.0, math.log1p(buy_value) / 20 + buyer_count * 0.1)
            - min(0.5, math.log1p(sale_value) / 40)
            if buy_value is not None and sale_value is not None
            else None
        )
        latest = max(transactions, key=lambda row: row["source_publication_date"] or date.min)
        output.append(
            {
                "security_id": security_id,
                "signal_as_of_date": cutoff,
                "open_market_purchase_value_30d": total_value(buys30),
                "open_market_purchase_value_90d": buy_value,
                "open_market_sale_value_30d": total_value(sells30),
                "open_market_sale_value_90d": sale_value,
                "unique_buyers_90d": buyer_count,
                "unique_sellers_90d": seller_count,
                "cluster_buy_flag": buyer_count >= 3,
                "director_purchase_flag": director,
                "ceo_purchase_flag": ceo,
                "cfo_purchase_flag": cfo,
                "insider_signal_score": (
                    max(-1.0, min(1.0, score)) if score is not None else None
                ),
                "signal_quality_status": (
                    "PARTIAL_MISSING_TRANSACTION_VALUE"
                    if score is None
                    else "READY"
                    if buys90 or sells90
                    else "NO_OPEN_MARKET_ACTIVITY"
                ),
                **pit_fields(
                    event_date=cutoff,
                    filing_date=latest.get("filing_date"),
                    acceptance=latest.get("source_acceptance_datetime_utc"),
                    publication_date=latest.get("source_publication_date"),
                    retrieved_at=retrieved_at,
                ),
            }
        )
    return output


def load_cusip_overrides(path: Path, security_ids: set[str]) -> dict[str, str]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if tuple(reader.fieldnames or ()) != ("cusip", "security_id", "reason"):
            raise EnrichmentError("CUSIP override columns are invalid")
        output = {}
        for row in reader:
            cusip = str(row.get("cusip") or "").strip().upper()
            security_id = str(row.get("security_id") or "").strip()
            if not cusip and not security_id:
                continue
            if not cusip or security_id not in security_ids or not str(row.get("reason") or "").strip():
                raise EnrichmentError("CUSIP override is incomplete or references an unknown security")
            if cusip in output:
                raise EnrichmentError(f"Duplicate CUSIP override: {cusip}")
            output[cusip] = security_id
    return output


def parse_13f_archives(
    paths: list[Path],
    masters: list[dict[str, object]],
    overrides: Mapping[str, str],
    cutoff: date,
    retrieved_at: datetime,
) -> list[dict[str, object]]:
    name_to_ids: dict[str, set[str]] = defaultdict(set)
    for master in masters:
        name_to_ids[normalize_name(master["registrant_name"])].add(str(master["security_id"]))
    aggregated: dict[tuple[str, str, date, str], dict[str, object]] = {}
    for path in paths:
        submissions = {row["ACCESSION_NUMBER"]: row for row in zip_tsv(path, "SUBMISSION.tsv")}
        covers = {row["ACCESSION_NUMBER"]: row for row in zip_tsv(path, "COVERPAGE.tsv")}
        for info in zip_tsv(path, "INFOTABLE.tsv"):
            accession = info["ACCESSION_NUMBER"]
            submission = submissions.get(accession)
            cover = covers.get(accession)
            if submission is None or cover is None or not ACCESSION_PATTERN.fullmatch(accession):
                raise EnrichmentError(f"13F position lacks filing provenance: {accession}")
            filing_date = parse_date(submission.get("FILING_DATE"))
            report_period = parse_date(submission.get("PERIODOFREPORT"))
            if filing_date is None or report_period is None or filing_date > cutoff:
                continue
            if filing_date < report_period:
                raise EnrichmentError(f"13F filing {accession} predates report period")
            cusip = info.get("CUSIP", "").upper()
            security_id = overrides.get(cusip)
            mapping_method = "REVIEWED_CUSIP_OVERRIDE" if security_id else None
            if security_id is None:
                candidates = name_to_ids.get(normalize_name(info.get("NAMEOFISSUER")), set())
                if len(candidates) == 1:
                    security_id = next(iter(candidates))
                    mapping_method = "UNIQUE_SEC_ISSUER_NAME"
            if security_id is None:
                continue
            manager_cik = cik10(submission.get("CIK"))
            key = (manager_cik, security_id, report_period, accession)
            row = aggregated.get(key)
            publication = filing_date
            market_value = finite_number(info.get("VALUE"))
            shares_or_principal = finite_number(info.get("SSHPRNAMT"))
            if market_value is None or shares_or_principal is None:
                raise EnrichmentError(
                    f"13F position {accession} lacks value or share quantity"
                )
            voting_sole = finite_number(info.get("VOTING_AUTH_SOLE"))
            voting_shared = finite_number(info.get("VOTING_AUTH_SHARED"))
            voting_none = finite_number(info.get("VOTING_AUTH_NONE"))
            if row is None:
                row = {
                    "manager_cik": manager_cik,
                    "manager_name": cover.get("FILINGMANAGER_NAME") or None,
                    "report_period": report_period,
                    "filing_date": filing_date,
                    "accession_number": accession,
                    "cusip": cusip,
                    "security_id": security_id,
                    "security_mapping_method": mapping_method,
                    "issuer_name": info.get("NAMEOFISSUER") or None,
                    "class_title": info.get("TITLEOFCLASS") or None,
                    "market_value_thousands": market_value,
                    "shares_or_principal": shares_or_principal,
                    "share_type": info.get("SSHPRNAMTTYPE") or None,
                    "put_call": info.get("PUTCALL") or None,
                    "investment_discretion": info.get("INVESTMENTDISCRETION") or None,
                    "voting_authority_sole": voting_sole,
                    "voting_authority_shared": voting_shared,
                    "voting_authority_none": voting_none,
                    **pit_fields(
                        event_date=report_period,
                        filing_date=filing_date,
                        acceptance=None,
                        publication_date=publication,
                        retrieved_at=retrieved_at,
                    ),
                }
                aggregated[key] = row
            else:
                row["market_value_thousands"] += market_value
                row["shares_or_principal"] += shares_or_principal
                for field, value in (
                    ("voting_authority_sole", voting_sole),
                    ("voting_authority_shared", voting_shared),
                    ("voting_authority_none", voting_none),
                ):
                    previous = finite_number(row[field])
                    row[field] = (
                        previous + value
                        if previous is not None and value is not None
                        else None
                    )
    return list(aggregated.values())


def build_institutional_signals(
    holdings: list[dict[str, object]], retrieved_at: datetime
) -> list[dict[str, object]]:
    by_period: dict[tuple[str, date], dict[str, dict[str, object]]] = defaultdict(dict)
    for row in holdings:
        by_period[(str(row["security_id"]), row["report_period"])][str(row["manager_cik"])] = row
    periods_by_security: dict[str, list[date]] = defaultdict(list)
    for security_id, report_period in by_period:
        periods_by_security[security_id].append(report_period)
    output = []
    for security_id, periods in periods_by_security.items():
        ordered = sorted(set(periods))
        for index, report_period in enumerate(ordered):
            current = by_period[(security_id, report_period)]
            previous = by_period.get((security_id, ordered[index - 1]), {}) if index else {}
            current_managers = set(current)
            previous_managers = set(previous)
            new = current_managers - previous_managers
            closed = previous_managers - current_managers
            increased = {
                manager
                for manager in current_managers & previous_managers
                if current[manager]["shares_or_principal"] > previous[manager]["shares_or_principal"]
            }
            decreased = {
                manager
                for manager in current_managers & previous_managers
                if current[manager]["shares_or_principal"] < previous[manager]["shares_or_principal"]
            }
            current_shares = sum(float(row["shares_or_principal"]) for row in current.values())
            previous_shares = sum(float(row["shares_or_principal"]) for row in previous.values())
            market_values = sorted(
                (float(row["market_value_thousands"]) for row in current.values()), reverse=True
            )
            total_value = sum(market_values)
            concentration = sum(market_values[:10]) / total_value if total_value else None
            latest_filing = max(row["filing_date"] for row in current.values())
            breadth = len(new) + len(increased) - len(closed) - len(decreased)
            output.append(
                {
                    "security_id": security_id,
                    "report_period": report_period,
                    "known_13f_shares": current_shares,
                    "manager_count": len(current),
                    "new_position_manager_count": len(new),
                    "closed_position_manager_count": len(closed),
                    "increased_position_manager_count": len(increased),
                    "decreased_position_manager_count": len(decreased),
                    "net_share_change": current_shares - previous_shares if previous else None,
                    "net_manager_breadth": breadth,
                    "top_10_manager_concentration": concentration,
                    "ownership_signal_score": math.tanh(breadth / 10),
                    "publication_lag_days": (latest_filing - report_period).days,
                    "signal_quality_status": "PARTIAL_NAME_MAPPED" if any(row["security_mapping_method"] != "REVIEWED_CUSIP_OVERRIDE" for row in current.values()) else "READY",
                    **pit_fields(
                        event_date=report_period,
                        filing_date=latest_filing,
                        acceptance=None,
                        publication_date=latest_filing,
                        retrieved_at=retrieved_at,
                    ),
                }
            )
    return output


def load_finra_schedule(path: Path, cutoff: date) -> dict[date, date]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        output = {}
        for row in reader:
            settlement = parse_date(row.get("settlement_date"))
            publication = parse_date(row.get("publication_date"))
            if settlement and publication and publication <= cutoff:
                if publication < settlement:
                    raise EnrichmentError("FINRA publication date precedes settlement date")
                output[settlement] = publication
    return output


def fetch_finra(schedule: Mapping[date, date]) -> list[dict[str, object]]:
    output: list[dict[str, object]] = []
    for settlement in sorted(schedule):
        offset = 0
        while True:
            payload = json.dumps(
                {
                    "compareFilters": [
                        {
                            "compareType": "EQUAL",
                            "fieldName": "settlementDate",
                            "fieldValue": settlement.isoformat(),
                        }
                    ],
                    "limit": 5000,
                    "offset": offset,
                }
            ).encode()
            request = urllib.request.Request(
                FINRA_SHORT_INTEREST_URL,
                data=payload,
                headers={"Accept": "application/json", "Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(request, timeout=120) as response:
                page = json.load(response)
            if not isinstance(page, list):
                raise EnrichmentError("FINRA short-interest response is not a list")
            output.extend(row for row in page if isinstance(row, dict))
            if len(page) < 5000:
                break
            offset += len(page)
            time.sleep(0.1)
    return output


def parse_finra(
    raw_rows: Iterable[Mapping[str, object]],
    schedule: Mapping[date, date],
    ticker_to_id: Mapping[str, str],
    retrieved_at: datetime,
) -> list[dict[str, object]]:
    rows_by_security: dict[str, list[dict[str, object]]] = defaultdict(list)
    for raw in raw_rows:
        symbol = str(raw.get("symbolCode") or raw.get("issueSymbolIdentifier") or "").strip().upper()
        security_id = resolve_ticker(symbol, ticker_to_id)
        settlement = parse_date(raw.get("settlementDate"))
        if security_id is None or settlement not in schedule:
            continue
        publication = schedule[settlement]
        current = finite_number(
            first_not_none(
                raw.get("currentShortPositionQuantity"),
                raw.get("currentShortShareNumber"),
            )
        )
        previous = finite_number(
            first_not_none(
                raw.get("previousShortPositionQuantity"),
                raw.get("previousShortShareNumber"),
            )
        )
        short_change = finite_number(raw.get("changePreviousNumber"))
        if short_change is None and current is not None and previous is not None:
            short_change = current - previous
        row = {
            "security_id": security_id,
            "symbol": symbol,
            "issue_name": str(raw.get("issueName") or "").strip() or None,
            "market": str(raw.get("marketClassCode") or raw.get("marketCategoryDescription") or "").strip() or None,
            "settlement_date": settlement,
            "publication_date": publication,
            "current_short_shares": current,
            "previous_short_shares": previous,
            "short_change": short_change,
            "short_change_percent": finite_number(raw.get("changePercent")),
            "average_daily_volume": finite_number(
                first_not_none(
                    raw.get("averageDailyVolumeQuantity"),
                    raw.get("averageShortShareNumber"),
                )
            ),
            "days_to_cover": finite_number(
                first_not_none(
                    raw.get("daysToCoverQuantity"), raw.get("daysToCoverNumber")
                )
            ),
            "revision_flag": str(raw.get("revisionFlag") or "").strip() or None,
            "short_interest_change_1m": None,
            "short_interest_change_3m": None,
            "days_to_cover_change": None,
            "short_interest_percent_float": None,
            "short_interest_percent_float_quality": "DENOMINATOR_NOT_CONFIGURED",
            **pit_fields(
                event_date=settlement,
                filing_date=None,
                acceptance=None,
                publication_date=publication,
                retrieved_at=retrieved_at,
            ),
        }
        rows_by_security[security_id].append(row)
    output = []
    for rows in rows_by_security.values():
        rows.sort(key=lambda row: row["settlement_date"])
        for index, row in enumerate(rows):
            current = finite_number(row["current_short_shares"])
            if index >= 2:
                row["short_interest_change_1m"] = growth(
                    current, finite_number(rows[index - 2]["current_short_shares"])
                )
            if index >= 6:
                row["short_interest_change_3m"] = growth(
                    current, finite_number(rows[index - 6]["current_short_shares"])
                )
            if index:
                prior_dtc = finite_number(rows[index - 1]["days_to_cover"])
                dtc = finite_number(row["days_to_cover"])
                row["days_to_cover_change"] = (
                    dtc - prior_dtc if dtc is not None and prior_dtc is not None else None
                )
            output.append(row)
    return output


def atomic_json(path: Path, value: object) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(
        json.dumps(value, indent=2, sort_keys=True, default=json_scalar) + "\n",
        encoding="utf-8",
    )
    os.replace(tmp, path)


def json_scalar(value: object) -> str:
    if isinstance(value, datetime):
        return format_utc(value)
    if isinstance(value, date):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def date_range(rows: list[dict[str, object]], field: str) -> tuple[str | None, str | None]:
    values = sorted(value for row in rows if isinstance((value := row.get(field)), date))
    return (
        values[0].isoformat() if values else None,
        values[-1].isoformat() if values else None,
    )


def merge_manifest(
    manifest_path: Path,
    out_dir: Path,
    datasets: Mapping[str, list[dict[str, object]]],
    revisions: Mapping[str, str],
    retrieved_at: datetime,
    concept_map_version: str,
    notice_path: Path,
    fact_stats: Mapping[str, object],
) -> None:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("status") != "READY":
        raise EnrichmentError("Cannot enrich a market manifest that is not READY")
    event_fields = {
        "sec-company-facts.parquet": "period_end",
        "normalized-fundamentals-quarterly.parquet": "period_end",
        "fundamental-factors.parquet": "factor_as_of_date",
        "sec-filings.parquet": "filing_date",
        "corporate-events.parquet": "effective_date",
        "insider-transactions.parquet": "transaction_date",
        "insider-signals.parquet": "signal_as_of_date",
        "institutional-holdings-13f.parquet": "report_period",
        "institutional-ownership-signals.parquet": "report_period",
        "finra-short-interest.parquet": "settlement_date",
        "earnings-and-guidance-events.parquet": "event_date",
    }
    dataset_records = []
    enrichment_release_records = []
    retrieved_text = format_utc(retrieved_at)
    for filename, rows in datasets.items():
        contract = CONTRACTS[filename]
        path = out_dir / filename
        if filename == "sec-company-facts.parquet":
            row_count = int(fact_stats["row_count"])
            minimum = fact_stats.get("minimum_event_date")
            maximum = fact_stats.get("maximum_event_date")
        else:
            row_count = len(rows)
            field = event_fields.get(filename)
            minimum, maximum = date_range(rows, field) if field else (None, None)
        dataset_records.append(
            dataset_record(
                path,
                contract,
                rows=row_count,
                source_revision=revisions.get(filename, ""),
                source_retrieved_at_utc=retrieved_text,
                minimum_event_date=minimum,
                maximum_event_date=maximum,
            )
        )
        enrichment_release_records.append(
            release_file_record(path, contract, row_count)
        )
    enrichment_names = set(datasets) | {"NOTICE.md"}
    notice_record = {
        "bytes": notice_path.stat().st_size,
        "file": "NOTICE.md",
        "rows": 0,
        "schema": [],
        "sha256": sha256_file(notice_path),
    }
    manifest["release_files"] = [
        record
        for record in manifest.get("release_files", [])
        if record.get("file") not in enrichment_names
    ] + enrichment_release_records + [notice_record]
    legacy_primary_keys = {
        "yahoo-ohlcv-320.parquet": ["security_id", "session_date"],
        "yahoo-splits.parquet": ["security_id", "event_date", "split_factor"],
        "security-universe.csv": ["security_id"],
        "unmatched-tickers.csv": ["ticker"],
    }
    legacy_sources = {
        "yahoo-ohlcv-320.parquet": "defeatbeta/yahoo-finance-data",
        "yahoo-splits.parquet": "defeatbeta/yahoo-finance-data",
        "security-universe.csv": "Nasdaq Trader Symbol Directory",
        "unmatched-tickers.csv": "Derived identifier reconciliation",
    }
    legacy_records = []
    for record in manifest["release_files"]:
        filename = str(record.get("file") or "")
        if filename in enrichment_names:
            continue
        legacy_records.append(
            {
                "path": filename,
                "schema_version": str(manifest.get("schema_version") or ""),
                "row_count": int(record.get("rows", 0)),
                "byte_size": int(record.get("bytes", 0)),
                "sha256": str(record.get("sha256") or ""),
                "primary_key": legacy_primary_keys.get(filename, []),
                "source": legacy_sources.get(filename, "Packaged market-data build"),
                "source_revision": str(
                    manifest.get("source", {}).get("revision") or ""
                ),
                "source_retrieved_at_utc": str(
                    manifest.get("created_at_utc") or retrieved_text
                ),
                "minimum_event_date": manifest.get("aggregate", {}).get("min_date"),
                "maximum_event_date": manifest.get("aggregate", {}).get("max_date"),
                "point_in_time_safe": True,
            }
        )
    manifest["datasets"] = legacy_records + dataset_records
    normalized = datasets["normalized-fundamentals-quarterly.parquet"]
    filings = datasets["sec-filings.parquet"]
    events = datasets["corporate-events.parquet"]
    insiders = datasets["insider-transactions.parquet"]
    holdings = datasets["institutional-holdings-13f.parquet"]
    short_interest = datasets["finra-short-interest.parquet"]
    manifest["coverage"] = {
        "fundamentals": {
            "status": "READY" if normalized else "VALIDATION_FAILED",
            "security_count": len({row["security_id"] for row in normalized}),
            "latest_filing_date": max((row["filing_date"] for row in normalized), default=None),
            "concept_map_version": concept_map_version,
        },
        "filings_and_events": {
            "status": "READY" if filings else "VALIDATION_FAILED",
            "filing_count": len(filings),
            "event_count": len(events),
            "latest_acceptance_datetime_utc": max(
                (row["acceptance_datetime_utc"] for row in filings if row["acceptance_datetime_utc"]),
                default=None,
            ),
        },
        "insider_transactions": {
            "status": "READY" if insiders else "VALIDATION_FAILED",
            "latest_filing_date": max((row["filing_date"] for row in insiders), default=None),
        },
        "institutional_ownership": {
            "status": "READY" if holdings else "VALIDATION_FAILED",
            "latest_report_period": max((row["report_period"] for row in holdings), default=None),
            "latest_filing_date": max((row["filing_date"] for row in holdings), default=None),
        },
        "short_interest": {
            "status": "READY" if short_interest else "VALIDATION_FAILED",
            "latest_settlement_date": max((row["settlement_date"] for row in short_interest), default=None),
            "latest_publication_date": max((row["publication_date"] for row in short_interest), default=None),
        },
        "analyst_estimates": {"status": "NOT_CONFIGURED"},
        "security_master": {
            "status": "READY",
            "security_count": len(datasets["security-master.parquet"]),
            "sec_cik_mapped_count": sum(
                row.get("cik") is not None
                for row in datasets["security-master.parquet"]
            ),
        },
    }
    warnings = manifest.setdefault("validation", {}).setdefault("warnings", [])
    enrichment_warnings = [
        "Analyst estimates are not configured; estimate-specific components are disabled"
    ]
    unmapped_ciks = sum(
        row.get("cik") is None for row in datasets["security-master.parquet"]
    )
    if unmapped_ciks:
        enrichment_warnings.append(
            f"{unmapped_ciks} admitted securities have no exact SEC CIK mapping"
        )
    ticker_collisions = sum(
        row.get("mapping_status") == "LATEST_SEC_FILING_TICKER_COLLISION"
        for row in datasets["security-master.parquet"]
    )
    if ticker_collisions:
        enrichment_warnings.append(
            f"{ticker_collisions} SEC ticker collisions selected the unique latest filer"
        )
    partial_fundamentals = sum(
        row.get("normalization_status") != "COMPLETE" for row in normalized
    )
    if partial_fundamentals:
        enrichment_warnings.append(
            f"{partial_fundamentals} normalized quarters have missing optional or core concepts"
        )
    if fact_stats.get("has_ifrs"):
        enrichment_warnings.append(
            "IFRS facts are present and may not be comparable to US-GAAP concepts"
        )
    name_mapped_positions = sum(
        row.get("security_mapping_method") == "UNIQUE_SEC_ISSUER_NAME"
        for row in holdings
    )
    if name_mapped_positions:
        enrichment_warnings.append(
            f"{name_mapped_positions} 13F rows use unique exact issuer-name mapping; review CUSIP overrides"
        )
    financial_issuers = sum(
        str(row.get("sic") or "").isdigit()
        and 6000 <= int(str(row["sic"])) <= 6799
        for row in datasets["security-master.parquet"]
    )
    if financial_issuers:
        enrichment_warnings.append(
            f"{financial_issuers} financial issuers may not support general-industrial ratios"
        )
    for warning in enrichment_warnings:
        if warning not in warnings:
            warnings.append(warning)
    manifest["source"]["sec_company_facts_observations"] = int(
        fact_stats["row_count"]
    )
    manifest["source"]["normalization_floor_fiscal_year"] = int(
        fact_stats["normalization_floor_fiscal_year"]
    )
    atomic_json(manifest_path, manifest)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--universe", required=True)
    parser.add_argument("--prices", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--companyfacts", required=True)
    parser.add_argument("--submissions", required=True)
    parser.add_argument("--insider-archive", action="append", default=[])
    parser.add_argument("--form13f-archive", action="append", default=[])
    parser.add_argument("--finra-file")
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--cutoff-date", required=True)
    parser.add_argument("--retrieved-at")
    parser.add_argument("--notice", default="NOTICE.md")
    parser.add_argument("--concept-map", default="config/sec-concept-map-v1.json")
    parser.add_argument("--cusip-overrides", default="config/cusip-security-overrides.csv")
    parser.add_argument(
        "--finra-publication-dates",
        default="config/finra-short-interest-publication-dates.csv",
    )
    parser.add_argument("--allow-empty", action="store_true")
    args = parser.parse_args(argv)

    cutoff = parse_date(args.cutoff_date)
    if cutoff is None:
        parser.error("--cutoff-date is required")
    retrieved_at = (
        parse_datetime(args.retrieved_at)
        if args.retrieved_at
        else datetime.now(timezone.utc).replace(tzinfo=None)
    )
    assert retrieved_at is not None
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    universe_path = Path(args.universe).resolve()
    companyfacts_path = Path(args.companyfacts).resolve()
    submissions_path = Path(args.submissions).resolve()
    insider_paths = [Path(value).resolve() for value in args.insider_archive]
    form13f_paths = [Path(value).resolve() for value in args.form13f_archive]
    try:
        source_hash_started = time.monotonic()
        log_stage("source_hashes", "start", source_hash_started)
        source_paths = [companyfacts_path, submissions_path, *insider_paths, *form13f_paths]
        known_revisions = {path: sha256_file(path) for path in source_paths}
        log_stage("source_hashes", "complete", source_hash_started)
        universe, ticker_to_id = load_universe(universe_path)
        submissions_started = time.monotonic()
        log_stage("submissions", "start", submissions_started)
        masters, filing_rows, all_filings, cik_to_security = parse_submissions(
            submissions_path,
            ticker_to_id,
            universe,
            cutoff,
            retrieved_at,
            known_revisions[submissions_path],
        )
        log_stage("submissions", "complete", submissions_started)
        concept_map = load_json(Path(args.concept_map).resolve())
        if not isinstance(concept_map, dict):
            raise EnrichmentError("SEC concept map root is not an object")
        companyfacts_started = time.monotonic()
        log_stage("company_facts", "start", companyfacts_started)
        normalization_facts, fact_stats = build_company_facts_asset(
            companyfacts_path,
            out_dir / "sec-company-facts.parquet",
            cik_to_security,
            all_filings,
            cutoff,
            retrieved_at,
            concept_map,
            known_revisions[companyfacts_path],
        )
        log_stage("company_facts", "complete", companyfacts_started)
        normalization_started = time.monotonic()
        log_stage("normalization_and_factors", "start", normalization_started)
        normalized = normalize_fundamentals(normalization_facts, concept_map)
        factors = build_factors(
            normalized,
            latest_prices(Path(args.prices).resolve(), cutoff),
            cutoff,
            retrieved_at,
        )
        log_stage("normalization_and_factors", "complete", normalization_started)
        events_started = time.monotonic()
        log_stage("events", "start", events_started)
        events, earnings = build_events(filing_rows, retrieved_at)
        log_stage("events", "complete", events_started)
        insiders_started = time.monotonic()
        log_stage("insiders", "start", insiders_started)
        insiders = parse_insider_archives(
            insider_paths,
            cik_to_security,
            ticker_to_id,
            all_filings,
            cutoff,
            retrieved_at,
        )
        insider_signals = build_insider_signals(insiders, cutoff, retrieved_at)
        log_stage("insiders", "complete", insiders_started)
        institutional_started = time.monotonic()
        log_stage("institutional", "start", institutional_started)
        cusip_overrides = load_cusip_overrides(
            Path(args.cusip_overrides).resolve(), set(universe)
        )
        holdings = parse_13f_archives(
            form13f_paths,
            masters,
            cusip_overrides,
            cutoff,
            retrieved_at,
        )
        institutional_signals = build_institutional_signals(holdings, retrieved_at)
        log_stage("institutional", "complete", institutional_started)
        finra_started = time.monotonic()
        log_stage("finra", "start", finra_started)
        schedule = load_finra_schedule(
            Path(args.finra_publication_dates).resolve(), cutoff
        )
        if args.finra_file:
            finra_value = load_json(Path(args.finra_file).resolve())
            if not isinstance(finra_value, list):
                raise EnrichmentError("FINRA JSON root is not a list")
            finra_raw = [row for row in finra_value if isinstance(row, dict)]
        else:
            finra_raw = fetch_finra(schedule)
        short_interest = parse_finra(
            finra_raw, schedule, ticker_to_id, retrieved_at
        )
        log_stage("finra", "complete", finra_started)
        datasets = {
            "security-master.parquet": masters,
            "sec-company-facts.parquet": [],
            "normalized-fundamentals-quarterly.parquet": normalized,
            "fundamental-factors.parquet": factors,
            "sec-filings.parquet": filing_rows,
            "corporate-events.parquet": events,
            "insider-transactions.parquet": insiders,
            "insider-signals.parquet": insider_signals,
            "institutional-holdings-13f.parquet": holdings,
            "institutional-ownership-signals.parquet": institutional_signals,
            "finra-short-interest.parquet": short_interest,
            "earnings-and-guidance-events.parquet": earnings,
        }
        if not args.allow_empty:
            empty = sorted(
                filename
                for filename, rows in datasets.items()
                if (
                    int(fact_stats["row_count"]) == 0
                    if filename == "sec-company-facts.parquet"
                    else not rows
                )
            )
            if empty:
                raise EnrichmentError(f"Required enrichment datasets are empty: {empty}")
        derived_write_started = time.monotonic()
        log_stage("derived_parquet", "start", derived_write_started)
        for filename, rows in datasets.items():
            if filename == "sec-company-facts.parquet":
                continue
            write_parquet(out_dir / filename, CONTRACTS[filename], rows)
        log_stage("derived_parquet", "complete", derived_write_started)
        notice_source = Path(args.notice).resolve()
        notice_target = out_dir / "NOTICE.md"
        if notice_source != notice_target:
            shutil.copyfile(notice_source, notice_target)
        revisions = {
            "security-master.parquet": known_revisions[submissions_path],
            "sec-company-facts.parquet": known_revisions[companyfacts_path],
            "normalized-fundamentals-quarterly.parquet": known_revisions[companyfacts_path],
            "fundamental-factors.parquet": combined_revision(
                [companyfacts_path, Path(args.prices).resolve()], known_revisions
            ),
            "sec-filings.parquet": known_revisions[submissions_path],
            "corporate-events.parquet": known_revisions[submissions_path],
            "earnings-and-guidance-events.parquet": known_revisions[submissions_path],
            "insider-transactions.parquet": combined_revision(insider_paths, known_revisions) if insider_paths else "",
            "insider-signals.parquet": combined_revision(insider_paths, known_revisions) if insider_paths else "",
            "institutional-holdings-13f.parquet": combined_revision(form13f_paths, known_revisions) if form13f_paths else "",
            "institutional-ownership-signals.parquet": combined_revision(form13f_paths, known_revisions) if form13f_paths else "",
            "finra-short-interest.parquet": (
                sha256_file(Path(args.finra_file).resolve()) if args.finra_file else sha256_bytes(json.dumps(finra_raw, sort_keys=True).encode())
            ),
        }
        manifest_started = time.monotonic()
        log_stage("manifest", "start", manifest_started)
        merge_manifest(
            Path(args.manifest).resolve(),
            out_dir,
            datasets,
            revisions,
            retrieved_at,
            str(concept_map.get("version") or ""),
            notice_target,
            fact_stats,
        )
        log_stage("manifest", "complete", manifest_started)
    except (ContractError, EnrichmentError, OSError, ValueError, duckdb.Error, zipfile.BadZipFile) as exc:
        print(f"error: {exc}", file=os.sys.stderr)
        return 2
    print(
        json.dumps(
            {
                "analyst_estimates_status": "NOT_CONFIGURED",
                "datasets": {
                    filename: (
                        int(fact_stats["row_count"])
                        if filename == "sec-company-facts.parquet"
                        else len(rows)
                    )
                    for filename, rows in datasets.items()
                },
                "status": "READY",
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
