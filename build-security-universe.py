#!/usr/bin/env python3
"""Build a reproducible Nasdaq/NYSE security universe from Nasdaq Trader feeds."""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import re
import sys
import urllib.request
from collections import Counter
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from zoneinfo import ZoneInfo


NASDAQ_URL = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"
OTHER_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt"
SCHEMA_VERSION = "1.0.0"
OVERRIDE_FIELDS = (
    "exchange_mic",
    "source_symbol",
    "action",
    "security_id",
    "canonical_ticker",
    "security_type",
    "reason",
)
OUTPUT_FIELDS = (
    "security_id",
    "ticker",
    "source_symbol",
    "legal_name",
    "exchange_mic",
    "security_type",
    "exchange_country",
    "currency",
    "listing_status",
    "cik",
    "cusip",
    "isin",
    "adr_status",
    "adr_ratio",
    "home_exchange",
    "home_country",
    "fund_provider",
    "index_tracked",
    "expense_ratio",
    "asset_class",
    "leveraged",
    "inverse",
    "universe_admission_status",
    "admission_reason",
    "source_file",
    "source_file_created_at_utc",
    "source_file_sha256",
    "generated_at_utc",
)
METADATA_FIELDS = (
    "exchange_mic", "source_symbol", "cik", "cusip", "isin", "adr_status", "adr_ratio",
    "home_exchange", "home_country", "fund_provider", "index_tracked",
    "expense_ratio", "asset_class", "leveraged", "inverse", "currency",
    "listing_status", "exchange_country",
)
OTHER_LISTED_VENUES = {"N": "XNYS", "P": "ARCX", "Z": "BATS", "C": "CBOE"}
ADMITTED_STATUSES = {"ADMITTED", "ADMITTED_ETF"}


class UniverseError(RuntimeError):
    """Raised when an input or universe invariant is violated."""


@dataclass(frozen=True)
class SecurityRow:
    security_id: str
    ticker: str
    source_symbol: str
    legal_name: str
    exchange_mic: str
    security_type: str
    exchange_country: str
    currency: str
    listing_status: str
    cik: str
    cusip: str
    isin: str
    adr_status: str
    adr_ratio: str
    home_exchange: str
    home_country: str
    fund_provider: str
    index_tracked: str
    expense_ratio: str
    asset_class: str
    leveraged: str
    inverse: str
    universe_admission_status: str
    admission_reason: str
    source_file: str
    source_file_created_at_utc: str
    source_file_sha256: str
    generated_at_utc: str


@dataclass(frozen=True)
class SourceSnapshot:
    name: str
    url: str
    sha256: str
    created_at_utc: str
    rows: int


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise UniverseError(f"Timestamp must include a timezone: {value}")
    return parsed.astimezone(timezone.utc)


def format_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def atomic_json(path: Path, value: object) -> None:
    atomic_text(path, json.dumps(value, indent=2, sort_keys=True) + "\n")


def read_source(local_path: str | None, url: str) -> bytes:
    if local_path:
        path = Path(local_path).resolve()
        if not path.is_file():
            raise UniverseError(f"Source file does not exist: {path}")
        return path.read_bytes()
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "usd-portfolio-market-data/1.0 (+https://github.com/anshu92/usd-portfolio-market-data)"},
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.read()
    except OSError as exc:
        raise UniverseError(f"Unable to download {url}: {exc}") from exc


def parse_file_creation_time(lines: list[str], source_name: str) -> datetime:
    prefix = "File Creation Time:"
    trailer = next((line for line in reversed(lines) if line.startswith(prefix)), None)
    if trailer is None:
        raise UniverseError(f"{source_name} has no File Creation Time trailer")
    raw = trailer.split("|", 1)[0].removeprefix(prefix).strip()
    try:
        local = datetime.strptime(raw, "%m%d%Y%H:%M").replace(
            tzinfo=ZoneInfo("America/New_York")
        )
    except ValueError as exc:
        raise UniverseError(f"Invalid File Creation Time in {source_name}: {raw}") from exc
    return local.astimezone(timezone.utc)


def parse_pipe_feed(data: bytes, source_name: str) -> tuple[list[dict[str, str]], datetime]:
    text = data.decode("utf-8-sig")
    lines = [line.rstrip("\r") for line in text.splitlines() if line.strip()]
    created_at = parse_file_creation_time(lines, source_name)
    body = [line for line in lines if not line.startswith("File Creation Time:")]
    reader = csv.DictReader(io.StringIO("\n".join(body)), delimiter="|")
    rows = [{str(k): str(v or "").strip() for k, v in row.items()} for row in reader]
    if not rows:
        raise UniverseError(f"{source_name} contains no data rows")
    return rows, created_at


def normalized_name(value: str) -> str:
    return re.sub(r"\s+", " ", value.upper()).strip()


def classify_security(name: str, is_etf: bool, is_nextshares: bool) -> str:
    text = normalized_name(name)
    if is_etf:
        return "ETF"
    if is_nextshares:
        return "OTHER"
    # Exclusion classes take precedence. For example, a warrant description can
    # mention the common units it purchases, and a preferred issue can be
    # represented by American depositary shares.
    if re.search(r"\bWARRANTS?\b", text):
        return "WARRANT"
    if re.search(r"\bRIGHTS?\b", text):
        return "RIGHT"
    if re.search(r"\bPREFERRED\b|\bPREFERENCE\b", text) or (
        re.search(r"\bDEPOSITARY SHARES?\b", text)
        and not re.search(r"\bAMERICAN DEPOSIT(?:A|O)RY SHARES?\b", text)
    ):
        return "PREFERRED"
    if re.search(r"\bNOTES?\b|\bBONDS?\b|\bDEBENTURES?\b", text):
        return "OTHER"
    if re.search(r"\bAMERICAN DEPOSIT(?:A|O)RY SHARES?\b|\bADRS?\b|\bADS\b", text):
        return "ADR"
    if "SHARES OF BENEFICIAL INTEREST" in text:
        return "REIT"
    if re.search(r"\b(?:COMMON|LIMITED PARTNERSHIP) UNITS?\b", text):
        return "MLP"
    if re.search(r"\bUNITS?\b", text):
        return "UNIT"
    if re.search(r"\bORDINARY SHARES?\b", text):
        return "COMMON"
    if re.search(r"\bCOMMON STOCK\b|\bCOMMON SHARES?\b", text):
        return "COMMON"
    return "OTHER"


def admission(
    *,
    security_type: str,
    is_test: bool,
    financial_status: str | None,
    metadata: dict[str, str],
) -> tuple[str, str]:
    if is_test:
        return "REJECTED_INACTIVE", "TEST_ISSUE"
    if metadata["exchange_country"] != "USA":
        return "REJECTED_NON_US", "EXCHANGE_COUNTRY_NOT_USA"
    if metadata["currency"] != "USD":
        return "REJECTED_NON_USD", "CURRENCY_NOT_USD"
    if metadata["listing_status"] != "ACTIVE":
        return "REJECTED_INACTIVE", "LISTING_NOT_ACTIVE"
    if financial_status is not None and financial_status != "N":
        return "REJECTED_INACTIVE", f"NASDAQ_FINANCIAL_STATUS_{financial_status or 'MISSING'}"
    if security_type == "ETF":
        if metadata["leveraged"] == "true":
            return "REJECTED_LEVERAGED_ETF", "LEVERAGED_ETF"
        if metadata["inverse"] == "true":
            return "REJECTED_INVERSE_ETF", "INVERSE_ETF"
        return "ADMITTED_ETF", "ADMITTED_ELIGIBLE_ETF"
    if security_type == "ADR":
        if not metadata["cik"]:
            return "REJECTED_IDENTITY_CONFLICT", "ADR_CIK_MISSING"
        return "ADMITTED", "ADMITTED_ADR"
    if security_type == "COMMON":
        return "ADMITTED", "ADMITTED_COMMON"
    if security_type == "WARRANT":
        return "REJECTED_WARRANT", "WARRANT"
    if security_type == "RIGHT":
        return "REJECTED_RIGHT", "RIGHT"
    if security_type == "UNIT":
        return "REJECTED_UNIT", "UNIT"
    return "REJECTED_UNKNOWN_SECURITY_TYPE", security_type


def make_row(
    *,
    ticker: str,
    legal_name: str,
    exchange_mic: str,
    source_name: str,
    source_created_at: datetime,
    source_sha: str,
    generated_at: datetime,
    is_test: bool,
    is_etf: bool,
    is_nextshares: bool,
    financial_status: str | None,
    metadata: dict[str, str],
) -> SecurityRow:
    ticker = ticker.strip().upper()
    if not ticker:
        raise UniverseError(f"Blank ticker in {source_name}")
    security_type = classify_security(legal_name, is_etf, is_nextshares)
    # ADR identity is reviewed issuer/security-master metadata, never inferred
    # from the ticker.  Exchange descriptions are not consistently explicit.
    if metadata["adr_status"] == "ADR":
        security_type = "ADR"
    status, reason = admission(
        security_type=security_type,
        is_test=is_test,
        financial_status=financial_status,
        metadata=metadata,
    )
    return SecurityRow(
        security_id=f"{exchange_mic}:{ticker}",
        ticker=ticker,
        source_symbol=ticker,
        legal_name=legal_name.strip(),
        exchange_mic=exchange_mic,
        security_type=security_type,
        exchange_country=metadata["exchange_country"],
        currency=metadata["currency"],
        listing_status=metadata["listing_status"],
        cik=metadata["cik"],
        cusip=metadata["cusip"],
        isin=metadata["isin"],
        adr_status=metadata["adr_status"],
        adr_ratio=metadata["adr_ratio"],
        home_exchange=metadata["home_exchange"],
        home_country=metadata["home_country"],
        fund_provider=metadata["fund_provider"],
        index_tracked=metadata["index_tracked"],
        expense_ratio=metadata["expense_ratio"],
        asset_class=metadata["asset_class"],
        leveraged=metadata["leveraged"],
        inverse=metadata["inverse"],
        universe_admission_status=status,
        admission_reason=reason,
        source_file=source_name,
        source_file_created_at_utc=format_utc(source_created_at),
        source_file_sha256=source_sha,
        generated_at_utc=format_utc(generated_at),
    )


def default_metadata(exchange_mic: str, ticker: str) -> dict[str, str]:
    return {
        "exchange_mic": exchange_mic,
        "source_symbol": ticker,
        "cik": "",
        "cusip": "",
        "isin": "",
        "adr_status": "NOT_ADR",
        "adr_ratio": "",
        "home_exchange": "",
        "home_country": "",
        "fund_provider": "",
        "index_tracked": "",
        "expense_ratio": "",
        "asset_class": "",
        "leveraged": "false",
        "inverse": "false",
        "currency": "USD",
        "listing_status": "ACTIVE",
        "exchange_country": "USA",
    }


def load_security_metadata(path: Path) -> dict[tuple[str, str], dict[str, str]]:
    if not path.exists():
        raise UniverseError(f"Security metadata file does not exist: {path}")
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if tuple(reader.fieldnames or ()) != METADATA_FIELDS:
            raise UniverseError(
                f"Security metadata columns must be exactly {','.join(METADATA_FIELDS)}"
            )
        output: dict[tuple[str, str], dict[str, str]] = {}
        for line_number, raw in enumerate(reader, start=2):
            row = {key: str(value or "").strip() for key, value in raw.items()}
            if not any(row.values()):
                continue
            key = (row["exchange_mic"].upper(), row["source_symbol"].upper())
            if not all(key) or key in output:
                raise UniverseError(f"Invalid or duplicate metadata key at {path}:{line_number}")
            row["exchange_mic"], row["source_symbol"] = key
            for field in ("leveraged", "inverse"):
                value = row[field].lower()
                if value not in {"", "true", "false"}:
                    raise UniverseError(f"Invalid {field} at {path}:{line_number}")
                row[field] = value or "false"
            for field in ("currency", "listing_status", "exchange_country", "adr_status"):
                row[field] = row[field].upper()
            output[key] = row
    return output


def build_rows(
    nasdaq_rows: Iterable[dict[str, str]],
    other_rows: Iterable[dict[str, str]],
    *,
    nasdaq_created_at: datetime,
    other_created_at: datetime,
    nasdaq_sha: str,
    other_sha: str,
    generated_at: datetime,
    security_metadata: dict[tuple[str, str], dict[str, str]],
) -> list[SecurityRow]:
    output: list[SecurityRow] = []
    for row in nasdaq_rows:
        ticker = row.get("Symbol", "").strip().upper()
        metadata = default_metadata("XNAS", ticker)
        metadata.update(security_metadata.get(("XNAS", ticker), {}))
        output.append(
            make_row(
                ticker=ticker,
                legal_name=row.get("Security Name", ""),
                exchange_mic="XNAS",
                source_name="nasdaqlisted.txt",
                source_created_at=nasdaq_created_at,
                source_sha=nasdaq_sha,
                generated_at=generated_at,
                is_test=row.get("Test Issue") != "N",
                is_etf=row.get("ETF") == "Y",
                is_nextshares=row.get("NextShares") == "Y",
                financial_status=row.get("Financial Status", ""),
                metadata=metadata,
            )
        )
    for row in other_rows:
        exchange_mic = OTHER_LISTED_VENUES.get(row.get("Exchange", "").upper())
        if exchange_mic is None:
            continue
        ticker = row.get("ACT Symbol", "").strip().upper()
        metadata = default_metadata(exchange_mic, ticker)
        metadata.update(security_metadata.get((exchange_mic, ticker), {}))
        output.append(
            make_row(
                ticker=ticker,
                legal_name=row.get("Security Name", ""),
                exchange_mic=exchange_mic,
                source_name="otherlisted.txt",
                source_created_at=other_created_at,
                source_sha=other_sha,
                generated_at=generated_at,
                is_test=row.get("Test Issue") != "N",
                is_etf=row.get("ETF") == "Y",
                is_nextshares=False,
                financial_status=None,
                metadata=metadata,
            )
        )
    return output


def load_overrides(path: Path) -> dict[tuple[str, str], dict[str, str]]:
    if not path.exists():
        raise UniverseError(f"Override file does not exist: {path}")
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if tuple(reader.fieldnames or ()) != OVERRIDE_FIELDS:
            raise UniverseError(
                f"Override columns must be exactly {','.join(OVERRIDE_FIELDS)}"
            )
        overrides: dict[tuple[str, str], dict[str, str]] = {}
        for line_number, raw in enumerate(reader, start=2):
            row = {key: str(value or "").strip() for key, value in raw.items()}
            if not any(row.values()):
                continue
            key = (row["exchange_mic"].upper(), row["source_symbol"].upper())
            if not all(key):
                raise UniverseError(f"Blank override key at {path}:{line_number}")
            if key in overrides:
                raise UniverseError(f"Duplicate override for {key} at {path}:{line_number}")
            action = row["action"].upper()
            if action not in {"ADMIT", "EXCLUDE"}:
                raise UniverseError(f"Invalid override action {action!r} at {path}:{line_number}")
            if not row["reason"]:
                raise UniverseError(f"Override reason is required at {path}:{line_number}")
            if action == "ADMIT" and not row["security_type"]:
                raise UniverseError(
                    f"ADMIT override requires security_type at {path}:{line_number}"
                )
            row["action"] = action
            overrides[key] = row
    return overrides


def apply_overrides(
    rows: list[SecurityRow], overrides: dict[tuple[str, str], dict[str, str]]
) -> list[SecurityRow]:
    found: set[tuple[str, str]] = set()
    output: list[SecurityRow] = []
    for row in rows:
        key = (row.exchange_mic, row.source_symbol)
        override = overrides.get(key)
        if override is None:
            output.append(row)
            continue
        found.add(key)
        override_type = override["security_type"] or row.security_type
        status = (
            "ADMITTED_ETF"
            if override["action"] == "ADMIT" and override_type == "ETF"
            else "ADMITTED"
            if override["action"] == "ADMIT"
            else "REJECTED_UNKNOWN_SECURITY_TYPE"
        )
        output.append(
            replace(
                row,
                security_id=override["security_id"] or row.security_id,
                ticker=(override["canonical_ticker"] or row.ticker).upper(),
                security_type=override_type,
                universe_admission_status=status,
                admission_reason=f"OVERRIDE_{override['action']}:{override['reason']}",
            )
        )
    missing = sorted(set(overrides) - found)
    if missing:
        raise UniverseError(f"Overrides reference missing listings: {missing}")
    return output


def validate_identifiers(rows: list[SecurityRow]) -> None:
    source_keys: set[tuple[str, str]] = set()
    security_ids: dict[str, tuple[str, str]] = {}
    admitted_tickers: dict[str, str] = {}
    for row in rows:
        source_key = (row.exchange_mic, row.source_symbol)
        if source_key in source_keys:
            raise UniverseError(f"Duplicate listing key: {source_key}")
        source_keys.add(source_key)
        previous_key = security_ids.get(row.security_id)
        if previous_key is not None and previous_key != source_key:
            raise UniverseError(
                f"security_id {row.security_id!r} collides between {previous_key} and {source_key}"
            )
        security_ids[row.security_id] = source_key
        if row.universe_admission_status in ADMITTED_STATUSES:
            previous_id = admitted_tickers.get(row.ticker)
            if previous_id is not None and previous_id != row.security_id:
                raise UniverseError(
                    f"Admitted ticker {row.ticker!r} maps to both {previous_id} and {row.security_id}"
                )
            admitted_tickers[row.ticker] = row.security_id


def admitted_ids_from_csv(path: Path) -> set[str]:
    if not path.exists():
        raise UniverseError(f"Previous universe does not exist: {path}")
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        required = {"security_id", "universe_admission_status"}
        if not required.issubset(set(reader.fieldnames or ())):
            raise UniverseError(f"Previous universe lacks required columns: {path}")
        return {
            str(row.get("security_id") or "").strip()
            for row in reader
            if str(row.get("universe_admission_status") or "").strip().upper()
            in ADMITTED_STATUSES
        }


def write_universe(path: Path, rows: list[SecurityRow]) -> None:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=OUTPUT_FIELDS, lineterminator="\n")
    writer.writeheader()
    for row in sorted(rows, key=lambda item: (item.exchange_mic, item.ticker, item.security_id)):
        writer.writerow(asdict(row))
    atomic_text(path, buffer.getvalue())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="security-universe.csv")
    parser.add_argument("--metadata-out")
    parser.add_argument("--overrides", default="config/universe-overrides.csv")
    parser.add_argument("--security-metadata", default="config/security-metadata.csv")
    parser.add_argument("--nasdaq-file")
    parser.add_argument("--other-listed-file")
    parser.add_argument("--previous-universe")
    parser.add_argument("--max-admitted-drift", type=float, default=0.05)
    parser.add_argument("--allow-universe-drift", action="store_true")
    parser.add_argument("--max-source-age-hours", type=float, default=48.0)
    parser.add_argument("--only-tickers", help="Comma-separated smoke-test allowlist")
    parser.add_argument("--generated-at", help="RFC3339 UTC timestamp; defaults to now")
    args = parser.parse_args(argv)

    if not (0 <= args.max_admitted_drift <= 1):
        parser.error("--max-admitted-drift must be in [0, 1]")
    if args.max_source_age_hours <= 0:
        parser.error("--max-source-age-hours must be positive")

    generated_at = parse_utc(args.generated_at) if args.generated_at else utcnow()
    out_path = Path(args.out).resolve()
    metadata_path = (
        Path(args.metadata_out).resolve()
        if args.metadata_out
        else out_path.with_suffix(".metadata.json")
    )

    try:
        nasdaq_data = read_source(args.nasdaq_file, NASDAQ_URL)
        other_data = read_source(args.other_listed_file, OTHER_LISTED_URL)
        nasdaq_rows, nasdaq_created_at = parse_pipe_feed(
            nasdaq_data, "nasdaqlisted.txt"
        )
        other_rows, other_created_at = parse_pipe_feed(other_data, "otherlisted.txt")
        nasdaq_sha = sha256_bytes(nasdaq_data)
        other_sha = sha256_bytes(other_data)
        security_metadata = load_security_metadata(
            Path(args.security_metadata).resolve()
        )
        rows = build_rows(
            nasdaq_rows,
            other_rows,
            nasdaq_created_at=nasdaq_created_at,
            other_created_at=other_created_at,
            nasdaq_sha=nasdaq_sha,
            other_sha=other_sha,
            generated_at=generated_at,
            security_metadata=security_metadata,
        )

        selected_tickers: set[str] | None = None
        if args.only_tickers:
            selected_tickers = {
                value.strip().upper()
                for value in args.only_tickers.split(",")
                if value.strip()
            }
            rows = [row for row in rows if row.ticker in selected_tickers]
            found_tickers = {row.ticker for row in rows}
            missing = sorted(selected_tickers - found_tickers)
            if missing:
                raise UniverseError(f"Smoke-test tickers are absent from feeds: {missing}")

        overrides = load_overrides(Path(args.overrides).resolve())
        if selected_tickers is not None:
            overrides = {
                key: value
                for key, value in overrides.items()
                if key[1] in selected_tickers
            }
        rows = apply_overrides(rows, overrides)
        validate_identifiers(rows)

        errors: list[str] = []
        warnings: list[str] = []
        for row in rows:
            if row.universe_admission_status == "ADMITTED" and row.security_type == "ADR" and not row.cik:
                errors.append(f"Admitted ADR lacks CIK: {row.security_id}")
            if row.universe_admission_status == "ADMITTED_ETF":
                if not row.expense_ratio:
                    warnings.append(f"ETF expense ratio unavailable: {row.security_id}")
                if not row.fund_provider:
                    warnings.append(f"ETF fund provider unavailable: {row.security_id}")
            if row.security_type == "ADR" and not row.home_exchange:
                warnings.append(f"ADR home exchange unavailable: {row.security_id}")
        source_snapshots = [
            SourceSnapshot(
                "nasdaqlisted.txt",
                NASDAQ_URL,
                nasdaq_sha,
                format_utc(nasdaq_created_at),
                len(nasdaq_rows),
            ),
            SourceSnapshot(
                "otherlisted.txt",
                OTHER_LISTED_URL,
                other_sha,
                format_utc(other_created_at),
                len(other_rows),
            ),
        ]
        for snapshot, created_at in zip(
            source_snapshots, (nasdaq_created_at, other_created_at), strict=True
        ):
            age_hours = (generated_at - created_at).total_seconds() / 3600
            if age_hours < -1:
                errors.append(f"{snapshot.name} creation time is in the future")
            elif age_hours > args.max_source_age_hours:
                errors.append(
                    f"{snapshot.name} is {age_hours:.1f} hours old; maximum is "
                    f"{args.max_source_age_hours:.1f}"
                )

        admitted_ids = {
            row.security_id
            for row in rows
            if row.universe_admission_status in ADMITTED_STATUSES
        }
        if not admitted_ids:
            errors.append("No securities were admitted")

        previous_count: int | None = None
        admitted_count_drift: float | None = None
        if args.previous_universe:
            previous_ids = admitted_ids_from_csv(Path(args.previous_universe).resolve())
            previous_count = len(previous_ids)
            if previous_count == 0:
                errors.append("Previous universe has no admitted securities")
            else:
                admitted_count_drift = abs(len(admitted_ids) - previous_count) / previous_count
                if admitted_count_drift > args.max_admitted_drift:
                    message = (
                        f"Admitted-count drift {admitted_count_drift:.2%} exceeds "
                        f"{args.max_admitted_drift:.2%}"
                    )
                    if args.allow_universe_drift:
                        warnings.append(message + " (manual override recorded)")
                    else:
                        errors.append(message)

        write_universe(out_path, rows)
        status_counts = Counter(row.universe_admission_status for row in rows)
        type_counts = Counter(row.security_type for row in rows)
        metadata = {
            "schema_version": SCHEMA_VERSION,
            "status": "READY" if not errors else "VALIDATION_FAILED",
            "generated_at_utc": format_utc(generated_at),
            "file": out_path.name,
            "sha256": hashlib.sha256(out_path.read_bytes()).hexdigest(),
            "bytes": out_path.stat().st_size,
            "rows": len(rows),
            "admitted_count": len(admitted_ids),
            "status_counts": dict(sorted(status_counts.items())),
            "security_type_counts": dict(sorted(type_counts.items())),
            "sources": [asdict(source) for source in source_snapshots],
            "drift": {
                "previous_admitted_count": previous_count,
                "current_admitted_count": len(admitted_ids),
                "admitted_count_drift": admitted_count_drift,
                "maximum": args.max_admitted_drift,
                "override_used": bool(
                    args.allow_universe_drift
                    and admitted_count_drift is not None
                    and admitted_count_drift > args.max_admitted_drift
                ),
            },
            "filters": {
                "exchange_mics": ["XNAS", "XNYS", "ARCX", "BATS", "CBOE"],
                "only_tickers": sorted(selected_tickers) if selected_tickers else None,
            },
            "errors": errors,
            "warnings": warnings,
        }
        atomic_json(metadata_path, metadata)
        print(json.dumps(metadata, indent=2, sort_keys=True))
        return 0 if not errors else 2
    except UniverseError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
