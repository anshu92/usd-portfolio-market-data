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
    "universe_admission_status",
    "admission_reason",
    "source_file",
    "source_file_created_at_utc",
    "source_file_sha256",
    "generated_at_utc",
)
ADMITTED_TYPES = {
    "COMMON_EQUITY",
    "ORDINARY_EQUITY",
    "ADR_ADS",
    "REIT_BENEFICIAL_INTEREST",
    "COMMON_PARTNERSHIP_UNIT",
}


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
        return "NEXTSHARES"
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
        return "PREFERRED_EQUITY"
    if re.search(r"\bNOTES?\b|\bBONDS?\b|\bDEBENTURES?\b", text):
        return "DEBT"
    if re.search(r"\bAMERICAN DEPOSIT(?:A|O)RY SHARES?\b|\bADRS?\b|\bADS\b", text):
        return "ADR_ADS"
    if "SHARES OF BENEFICIAL INTEREST" in text:
        return "REIT_BENEFICIAL_INTEREST"
    if re.search(r"\b(?:COMMON|LIMITED PARTNERSHIP) UNITS?\b", text):
        return "COMMON_PARTNERSHIP_UNIT"
    if re.search(r"\bUNITS?\b", text):
        return "ACQUISITION_UNIT"
    if re.search(r"\bORDINARY SHARES?\b", text):
        return "ORDINARY_EQUITY"
    if re.search(r"\bCOMMON STOCK\b|\bCOMMON SHARES?\b", text):
        return "COMMON_EQUITY"
    return "UNCLASSIFIED"


def admission(
    *,
    security_type: str,
    is_test: bool,
    financial_status: str | None,
) -> tuple[str, str]:
    if is_test:
        return "EXCLUDED", "TEST_ISSUE"
    if security_type in {"ETF", "NEXTSHARES"}:
        return "EXCLUDED", security_type
    if financial_status is not None and financial_status != "N":
        return "EXCLUDED", f"NASDAQ_FINANCIAL_STATUS_{financial_status or 'MISSING'}"
    if security_type in ADMITTED_TYPES:
        return "ADMITTED", f"ADMITTED_{security_type}"
    if security_type == "UNCLASSIFIED":
        return "REVIEW_REQUIRED", "UNCLASSIFIED_INSTRUMENT"
    return "EXCLUDED", security_type


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
) -> SecurityRow:
    ticker = ticker.strip().upper()
    if not ticker:
        raise UniverseError(f"Blank ticker in {source_name}")
    security_type = classify_security(legal_name, is_etf, is_nextshares)
    status, reason = admission(
        security_type=security_type,
        is_test=is_test,
        financial_status=financial_status,
    )
    return SecurityRow(
        security_id=f"{exchange_mic}:{ticker}",
        ticker=ticker,
        source_symbol=ticker,
        legal_name=legal_name.strip(),
        exchange_mic=exchange_mic,
        security_type=security_type,
        universe_admission_status=status,
        admission_reason=reason,
        source_file=source_name,
        source_file_created_at_utc=format_utc(source_created_at),
        source_file_sha256=source_sha,
        generated_at_utc=format_utc(generated_at),
    )


def build_rows(
    nasdaq_rows: Iterable[dict[str, str]],
    other_rows: Iterable[dict[str, str]],
    *,
    nasdaq_created_at: datetime,
    other_created_at: datetime,
    nasdaq_sha: str,
    other_sha: str,
    generated_at: datetime,
) -> list[SecurityRow]:
    output: list[SecurityRow] = []
    for row in nasdaq_rows:
        output.append(
            make_row(
                ticker=row.get("Symbol", ""),
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
            )
        )
    for row in other_rows:
        if row.get("Exchange") != "N":
            continue
        output.append(
            make_row(
                ticker=row.get("ACT Symbol", ""),
                legal_name=row.get("Security Name", ""),
                exchange_mic="XNYS",
                source_name="otherlisted.txt",
                source_created_at=other_created_at,
                source_sha=other_sha,
                generated_at=generated_at,
                is_test=row.get("Test Issue") != "N",
                is_etf=row.get("ETF") == "Y",
                is_nextshares=False,
                financial_status=None,
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
        status = "ADMITTED" if override["action"] == "ADMIT" else "EXCLUDED"
        output.append(
            replace(
                row,
                security_id=override["security_id"] or row.security_id,
                ticker=(override["canonical_ticker"] or row.ticker).upper(),
                security_type=override["security_type"] or row.security_type,
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
        if row.universe_admission_status == "ADMITTED":
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
            == "ADMITTED"
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
        rows = build_rows(
            nasdaq_rows,
            other_rows,
            nasdaq_created_at=nasdaq_created_at,
            other_created_at=other_created_at,
            nasdaq_sha=nasdaq_sha,
            other_sha=other_sha,
            generated_at=generated_at,
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
            if row.universe_admission_status == "ADMITTED"
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
                "exchange_mics": ["XNAS", "XNYS"],
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
