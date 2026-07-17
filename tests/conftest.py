from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
from pathlib import Path

import duckdb
import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]


def load_script(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, REPO_ROOT / filename)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="session")
def universe_module():
    return load_script("build_security_universe", "build-security-universe.py")


@pytest.fixture(scope="session")
def aggregate_module():
    return load_script("build_yahoo_aggregate", "build-yahoo-aggregate-v2.py")


@pytest.fixture(scope="session")
def verify_module():
    return load_script("verify_release", "verify-release.py")


@pytest.fixture(scope="session")
def github_release_module():
    return load_script("verify_github_release", "verify-github-release.py")


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture
def aggregate_inputs(tmp_path: Path):
    universe = tmp_path / "security-universe.csv"
    universe.write_text(
        "security_id,ticker,universe_admission_status\n"
        "XNAS:AAPL,AAPL,ADMITTED\n"
        "XNYS:BRK.B,BRK.B,ADMITTED\n",
        encoding="utf-8",
    )
    metadata = tmp_path / "security-universe.metadata.json"
    metadata.write_text(
        json.dumps(
            {
                "schema_version": "1.0.0",
                "status": "READY",
                "file": universe.name,
                "sha256": sha256(universe),
                "rows": 2,
                "sources": [],
                "drift": {
                    "previous_admitted_count": None,
                    "current_admitted_count": 2,
                    "admitted_count_drift": None,
                    "maximum": 0.05,
                    "override_used": False,
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    prices = tmp_path / "prices.parquet"
    splits = tmp_path / "splits.parquet"
    con = duckdb.connect()
    try:
        con.execute(
            """
            CREATE TABLE prices(
              symbol VARCHAR, report_date VARCHAR, open DOUBLE, high DOUBLE,
              low DOUBLE, close DOUBLE, volume BIGINT
            )
            """
        )
        con.executemany(
            "INSERT INTO prices VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                ("AAPL", "2024-01-08", 100.0, 105.0, 99.0, 104.0, 1000),
                ("AAPL", "2024-01-09", 104.0, 107.0, 103.0, 106.0, 1100),
                ("AAPL", "2024-01-10", 106.0, 109.0, 105.0, 108.0, 1200),
                ("AAPL", "2024-01-10", 106.0, 109.0, 105.0, 108.0, 1200),
                ("BRK-B", "2024-01-08", 350.0, 352.0, 348.0, 351.0, 500),
                ("BRK-B", "2024-01-09", 351.0, 354.0, 350.0, 353.0, 550),
                ("BRK-B", "2024-01-10", 353.0, 356.0, 352.0, 355.0, 600),
                ("AAPL", "2024-01-11", 108.0, 110.0, 107.0, 109.0, 1300),
                ("AAPL", "1990-01-02", 0.0, 10.0, 9.0, 9.5, 100),
            ],
        )
        con.execute("COPY prices TO ? (FORMAT PARQUET)", [str(prices)])
        con.execute(
            "CREATE TABLE splits(symbol VARCHAR, report_date VARCHAR, split_factor VARCHAR)"
        )
        con.executemany(
            "INSERT INTO splits VALUES (?, ?, ?)",
            [
                ("AAPL", "2020-08-31", "4:1"),
                ("AAPL", "2020-08-31", "4:1"),
            ],
        )
        con.execute("COPY splits TO ? (FORMAT PARQUET)", [str(splits)])
    finally:
        con.close()
    return {
        "root": tmp_path,
        "universe": universe,
        "metadata": metadata,
        "prices": prices,
        "splits": splits,
    }
