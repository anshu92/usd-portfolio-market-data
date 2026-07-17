#!/usr/bin/env python3
"""Locally benchmark the exact SEC company-facts Parquet write contract."""

from __future__ import annotations

import argparse
import json
import tempfile
import time
from datetime import date, datetime
from pathlib import Path

import duckdb

from enrichment_contract import CONTRACTS, write_parquet


def rows(count: int):
    base = {
        "security_id": "XNAS:AAPL", "cik": "0000320193", "taxonomy": "us-gaap",
        "label": "Revenue", "description": "Official SEC benchmark description, with a quoted value \"x\".",
        "unit": "USD", "unit_status": "SUPPORTED", "fiscal_year": 2026,
        "fiscal_period": "Q2", "form": "10-Q", "filed_date": date(2026, 7, 17),
        "period_start": date(2026, 4, 1), "period_end": date(2026, 6, 30),
        "frame": None, "source_url": "https://data.sec.gov/api/xbrl/companyfacts/CIK0000320193.json",
        "source_revision": "a" * 64, "source_event_date": date(2026, 6, 30),
        "source_filing_date": date(2026, 7, 17),
        "source_acceptance_datetime_utc": datetime(2026, 7, 17, 20, 0),
        "source_publication_date": date(2026, 7, 18),
        "source_retrieved_at_utc": datetime(2026, 7, 18, 1, 0),
    }
    for index in range(count):
        yield {
            **base, "concept": f"RevenueBenchmark{index}",
            "accession_number": f"0000320193-26-{index % 1_000_000:06d}",
            "value": float(index),
        }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=1_000_000)
    args = parser.parse_args(argv)
    if args.rows < 1:
        parser.error("--rows must be positive")
    contract = CONTRACTS["sec-company-facts.parquet"]
    with tempfile.TemporaryDirectory(prefix="company-facts-benchmark-") as directory:
        output = Path(directory) / contract.filename
        started = time.monotonic()
        written = write_parquet(output, contract, rows(args.rows))
        elapsed = time.monotonic() - started
        con = duckdb.connect()
        try:
            verified = int(con.execute("SELECT count(*) FROM read_parquet(?)", [str(output)]).fetchone()[0])
        finally:
            con.close()
        print(json.dumps({"bytes": output.stat().st_size, "rows_per_second": round(written / elapsed, 1), "rows_requested": args.rows, "rows_verified": verified, "seconds": round(elapsed, 3)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
