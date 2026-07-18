#!/usr/bin/env python3
"""Canonical portfolio-security reconciliation against a producer universe."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping


ADMITTED_STATUSES = {"ADMITTED", "ADMITTED_ETF"}
IDENTITY_ORDER = ("security_id", "cik", "cusip", "isin", "ticker_exchange")


class ReconciliationError(RuntimeError):
    """Raised when a portfolio position does not map uniquely."""


@dataclass(frozen=True)
class SecurityIdentity:
    security_id: str
    ticker: str
    exchange_mic: str
    cik: str = ""
    cusip: str = ""
    isin: str = ""


def _value(record: Mapping[str, object], name: str) -> str:
    return str(record.get(name) or "").strip().upper()


def load_active_universe(path: Path) -> list[SecurityIdentity]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        rows = list(csv.DictReader(handle))
    required = {"security_id", "ticker", "exchange_mic", "universe_admission_status"}
    if not rows or not required <= set(rows[0]):
        raise ReconciliationError("Universe lacks canonical identity columns")
    output = []
    for row in rows:
        if _value(row, "universe_admission_status") not in ADMITTED_STATUSES:
            continue
        identity = SecurityIdentity(
            _value(row, "security_id"), _value(row, "ticker"), _value(row, "exchange_mic"),
            _value(row, "cik"), _value(row, "cusip"), _value(row, "isin"),
        )
        if not all((identity.security_id, identity.ticker, identity.exchange_mic)):
            raise ReconciliationError("Admitted universe identity is incomplete")
        output.append(identity)
    if len({item.security_id for item in output}) != len(output):
        raise ReconciliationError("Two active securities share one security_id")
    if len({(item.ticker, item.exchange_mic) for item in output}) != len(output):
        raise ReconciliationError("Duplicate active (ticker, exchange) pair")
    return output


def reconcile(position: Mapping[str, object], universe: Iterable[SecurityIdentity]) -> SecurityIdentity:
    candidates = list(universe)
    checks = (
        ("security_id", lambda item: item.security_id == _value(position, "security_id")),
        ("cik", lambda item: bool(_value(position, "cik")) and item.cik == _value(position, "cik")),
        ("cusip", lambda item: bool(_value(position, "cusip")) and item.cusip == _value(position, "cusip")),
        ("isin", lambda item: bool(_value(position, "isin")) and item.isin == _value(position, "isin")),
        ("ticker_exchange", lambda item: item.ticker == _value(position, "ticker") and item.exchange_mic == _value(position, "exchange_mic")),
    )
    for name, predicate in checks:
        matches = [item for item in candidates if predicate(item)]
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            raise ReconciliationError(f"Ambiguous {name} mapping")
    ticker = _value(position, "ticker")
    ticker_matches = [item for item in candidates if item.ticker == ticker]
    if len(ticker_matches) == 1:
        return ticker_matches[0]
    raise ReconciliationError("Portfolio security cannot map uniquely")
