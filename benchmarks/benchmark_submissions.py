#!/usr/bin/env python3
"""Locally benchmark SEC submissions parsing with a scalable offline archive."""

from __future__ import annotations

import argparse
import importlib.util
import json
import tempfile
import time
import zipfile
from datetime import date, datetime
from pathlib import Path


def load_builder():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("enrichment_benchmark", root / "build-enrichment-data.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def submission(cik: str, ticker: str, filings: int) -> dict[str, object]:
    accessions = [f"{cik}-26-{index:06d}" for index in range(filings)]
    return {
        "cik": cik,
        "name": f"Benchmark Registrant {ticker}",
        "sic": "3571",
        "tickers": [ticker],
        "filings": {
            "recent": {
                "accessionNumber": accessions,
                "filingDate": ["2026-07-17"] * filings,
                "reportDate": ["2026-06-30"] * filings,
                "acceptanceDateTime": ["2026-07-17T20:00:00Z"] * filings,
                "form": ["10-Q"] * filings,
                "primaryDocument": ["form10q.htm"] * filings,
                "items": [""] * filings,
                "fileNumber": ["001-00001"] * filings,
                "filmNumber": ["26000001"] * filings,
            }
        },
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--documents", type=int, default=1_000)
    parser.add_argument("--filings-per-document", type=int, default=100)
    args = parser.parse_args(argv)
    if args.documents < 1 or args.filings_per_document < 1:
        parser.error("benchmark dimensions must be positive")
    builder = load_builder()
    with tempfile.TemporaryDirectory(prefix="submissions-benchmark-") as directory:
        archive_path = Path(directory) / "submissions.zip"
        ticker_to_id = {f"B{index}": f"XNAS:B{index}" for index in range(args.documents)}
        universe = {
            security_id: {"ticker": ticker, "exchange_mic": "XNAS"}
            for ticker, security_id in ticker_to_id.items()
        }
        with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for index in range(args.documents):
                cik = f"{index + 1:010d}"
                archive.writestr(
                    f"CIK{cik}.json",
                    json.dumps(submission(cik, f"B{index}", args.filings_per_document)),
                )
        started = time.monotonic()
        masters, filings, all_filings, cik_mapping = builder.parse_submissions(
            archive_path,
            ticker_to_id,
            universe,
            date(2026, 7, 17),
            datetime(2026, 7, 18),
        )
        elapsed = time.monotonic() - started
        total_filings = args.documents * args.filings_per_document
        print(json.dumps({"documents": args.documents, "filings_per_second": round(total_filings / elapsed, 1), "filings_requested": total_filings, "filings_retained": len(all_filings), "masters": len(masters), "mapped_ciks": len(cik_mapping), "released_filings": len(filings), "seconds": round(elapsed, 3)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
