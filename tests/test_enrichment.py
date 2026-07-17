from __future__ import annotations

import csv
import hashlib
import io
import json
import shutil
import zipfile
from datetime import date, datetime
from pathlib import Path

import duckdb
import pytest

from enrichment_contract import (
    ANALYST_ESTIMATES_CONTRACT,
    CONTRACTS,
    POINT_IN_TIME_COLUMNS,
)


ACCESSIONS = {
    "Q1": "0000320193-25-000001",
    "Q2": "0000320193-25-000002",
    "Q3": "0000320193-25-000003",
    "FY": "0000320193-26-000004",
    "8K": "0000320193-26-000005",
    "FORM4": "0000320193-26-000006",
}


def write_tsv_zip(path: Path, tables: dict[str, tuple[list[str], list[list[object]]]]) -> None:
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for filename, (fields, rows) in tables.items():
            buffer = io.StringIO(newline="")
            writer = csv.writer(buffer, delimiter="\t", lineterminator="\n")
            writer.writerow(fields)
            writer.writerows(rows)
            archive.writestr(filename, buffer.getvalue())


def fact_observations(
    values: list[float], *, unit: str = "USD", instant: bool = False
) -> list[dict[str, object]]:
    periods = [
        ("Q1", "2025-01-01", "2025-03-31", "2025-05-01", "10-Q"),
        ("Q2", "2025-04-01", "2025-06-30", "2025-08-01", "10-Q"),
        ("Q3", "2025-07-01", "2025-09-30", "2025-11-01", "10-Q"),
        ("FY", "2025-01-01", "2025-12-31", "2026-02-01", "10-K"),
    ]
    output = []
    for value, (period, start, end, filed, form) in zip(values, periods):
        row: dict[str, object] = {
            "end": end,
            "val": value,
            "accn": ACCESSIONS[period],
            "fy": 2025,
            "fp": period,
            "form": form,
            "filed": filed,
        }
        if not instant:
            row["start"] = start
        output.append(row)
    return output


def build_companyfacts(path: Path) -> None:
    concepts: dict[str, dict[str, object]] = {}

    def add(
        name: str,
        values: list[float],
        *,
        unit: str = "USD",
        instant: bool = False,
        cumulative: bool = False,
    ) -> None:
        observations = fact_observations(values, unit=unit, instant=instant)
        if cumulative:
            observations[1]["start"] = "2025-01-01"
            observations[2]["start"] = "2025-01-01"
        concepts[name] = {
            "label": name,
            "description": f"fixture {name}",
            "units": {unit: observations},
        }

    add("RevenueFromContractWithCustomerExcludingAssessedTax", [100, 110, 120, 460])
    add("GrossProfit", [45, 48, 50, 190])
    add("OperatingIncomeLoss", [20, 21, 22, 86])
    add("IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest", [18, 19, 20, 78])
    add("NetIncomeLoss", [15, 16, 17, 66])
    add(
        "NetCashProvidedByUsedInOperatingActivities",
        [10, 25, 45, 70],
        cumulative=True,
    )
    add(
        "PaymentsToAcquirePropertyPlantAndEquipment",
        [2, 5, 9, 14],
        cumulative=True,
    )
    add("CashAndCashEquivalentsAtCarryingValue", [40, 42, 44, 46], instant=True)
    add("AssetsCurrent", [90, 92, 94, 96], instant=True)
    add("LiabilitiesCurrent", [45, 46, 47, 48], instant=True)
    add("Assets", [300, 305, 310, 320], instant=True)
    add("Liabilities", [160, 162, 164, 166], instant=True)
    add("LongTermDebtCurrent", [5, 5, 5, 5], instant=True)
    add("LongTermDebtNoncurrent", [30, 29, 28, 27], instant=True)
    add("StockholdersEquity", [140, 143, 146, 154], instant=True)
    add("Goodwill", [10, 10, 10, 10], instant=True)
    add("FiniteLivedIntangibleAssetsNet", [4, 4, 3, 3], instant=True)
    add("WeightedAverageNumberOfSharesOutstandingBasic", [10, 10, 10, 10], unit="shares")
    add("WeightedAverageNumberOfDilutedSharesOutstanding", [11, 11, 11, 11], unit="shares")
    add("ShareBasedCompensation", [1, 1, 1, 4])
    add("InterestExpenseNonOperating", [1, 1, 1, 4])
    add("AccountsReceivableNetCurrent", [20, 21, 22, 23], instant=True)
    add("InventoryNet", [8, 8, 9, 9], instant=True)
    path.write_text(
        json.dumps(
            {
                "cik": 320193,
                "entityName": "Apple Inc.",
                "facts": {"us-gaap": concepts},
            }
        ),
        encoding="utf-8",
    )


def build_submissions(path: Path) -> None:
    keys = ["Q1", "Q2", "Q3", "FY", "8K", "FORM4"]
    forms = ["10-Q", "10-Q", "10-Q", "10-K", "8-K", "4"]
    filing_dates = [
        "2025-05-01",
        "2025-08-01",
        "2025-11-01",
        "2026-02-01",
        "2026-02-01",
        "2026-06-10",
    ]
    report_dates = [
        "2025-03-31",
        "2025-06-30",
        "2025-09-30",
        "2025-12-31",
        "2025-12-31",
        "2026-06-08",
    ]
    path.write_text(
        json.dumps(
            {
                "cik": "320193",
                "name": "Apple Inc.",
                "sic": "3571",
                "tickers": ["AAPL"],
                "filings": {
                    "recent": {
                        "accessionNumber": [ACCESSIONS[key] for key in keys],
                        "filingDate": filing_dates,
                        "reportDate": report_dates,
                        "acceptanceDateTime": [
                            f"{value}T21:00:00Z" for value in filing_dates
                        ],
                        "form": forms,
                        "primaryDocument": ["document.htm"] * len(keys),
                        "items": ["", "", "", "", "2.02", ""],
                        "fileNumber": ["001-36743"] * len(keys),
                        "filmNumber": ["26000001"] * len(keys),
                    }
                },
            }
        ),
        encoding="utf-8",
    )


@pytest.fixture
def enrichment_inputs(tmp_path: Path) -> dict[str, Path]:
    universe = tmp_path / "security-universe.csv"
    universe.write_text(
        "security_id,ticker,exchange_mic,universe_admission_status\n"
        "XNAS:AAPL,AAPL,XNAS,ADMITTED\n",
        encoding="utf-8",
    )
    companyfacts = tmp_path / "CIK0000320193.json"
    submissions = tmp_path / "submissions.json"
    build_companyfacts(companyfacts)
    build_submissions(submissions)

    insider = tmp_path / "insider.zip"
    write_tsv_zip(
        insider,
        {
            "SUBMISSION.tsv": (
                ["ACCESSION_NUMBER", "FILING_DATE", "DOCUMENT_TYPE", "ISSUERCIK", "ISSUERTRADINGSYMBOL", "AFF10B5ONE"],
                [[ACCESSIONS["FORM4"], "2026-06-10", "4", "320193", "AAPL", "0"]],
            ),
            "REPORTINGOWNER.tsv": (
                ["ACCESSION_NUMBER", "RPTOWNERCIK", "RPTOWNERNAME", "RPTOWNER_RELATIONSHIP", "RPTOWNER_TITLE"],
                [[ACCESSIONS["FORM4"], "1234567", "Fixture Owner", "Director Officer", "Chief Executive Officer"]],
            ),
            "NONDERIV_TRANS.tsv": (
                ["ACCESSION_NUMBER", "NONDERIV_TRANS_SK", "TRANS_DATE", "TRANS_FORM_TYPE", "SECURITY_TITLE", "TRANS_CODE", "TRANS_ACQUIRED_DISP_CD", "TRANS_SHARES", "TRANS_PRICEPERSHARE", "DIRECT_INDIRECT_OWNERSHIP", "SHRS_OWND_FOLWNG_TRANS"],
                [[ACCESSIONS["FORM4"], "1", "2026-06-08", "4", "Common Stock", "P", "A", "100", "200", "D", "1000"]],
            ),
            "DERIV_TRANS.tsv": (
                ["ACCESSION_NUMBER", "DERIV_TRANS_SK", "TRANS_DATE"],
                [],
            ),
        },
    )

    form13f = tmp_path / "form13f.zip"
    accession13f = "0001234567-26-000001"
    write_tsv_zip(
        form13f,
        {
            "SUBMISSION.tsv": (
                ["ACCESSION_NUMBER", "FILING_DATE", "SUBMISSIONTYPE", "CIK", "PERIODOFREPORT"],
                [[accession13f, "2026-05-15", "13F-HR", "7654321", "2026-03-31"]],
            ),
            "COVERPAGE.tsv": (
                ["ACCESSION_NUMBER", "FILINGMANAGER_NAME"],
                [[accession13f, "Fixture Capital"]],
            ),
            "INFOTABLE.tsv": (
                ["ACCESSION_NUMBER", "NAMEOFISSUER", "TITLEOFCLASS", "CUSIP", "VALUE", "SSHPRNAMT", "SSHPRNAMTTYPE", "PUTCALL", "INVESTMENTDISCRETION", "VOTING_AUTH_SOLE", "VOTING_AUTH_SHARED", "VOTING_AUTH_NONE"],
                [[accession13f, "APPLE INC", "COM", "037833100", "5000", "25000", "SH", "", "SOLE", "25000", "0", "0"]],
            ),
        },
    )

    finra = tmp_path / "finra.json"
    finra.write_text(
        json.dumps(
            [
                {
                    "symbolCode": "AAPL",
                    "issueName": "Apple Inc.",
                    "marketClassCode": "NMS",
                    "settlementDate": "2026-06-30",
                    "currentShortPositionQuantity": 1000,
                    "previousShortPositionQuantity": 900,
                    "changePreviousNumber": 100,
                    "changePercent": 11.11,
                    "averageDailyVolumeQuantity": 500,
                    "daysToCoverQuantity": 2,
                    "revisionFlag": "N",
                }
            ]
        ),
        encoding="utf-8",
    )

    prices = tmp_path / "yahoo-ohlcv-320.parquet"
    con = duckdb.connect()
    try:
        con.execute(
            "CREATE TABLE prices(security_id VARCHAR, session_date DATE, close DOUBLE)"
        )
        con.execute("INSERT INTO prices VALUES ('XNAS:AAPL', DATE '2026-07-16', 210.0)")
        con.execute("COPY prices TO ? (FORMAT PARQUET)", [str(prices)])
    finally:
        con.close()
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "schema_version": "1.0.0",
                "status": "READY",
                "source": {},
                "validation": {"warnings": [], "errors": []},
                "release_files": [],
            }
        ),
        encoding="utf-8",
    )
    return {
        "root": tmp_path,
        "universe": universe,
        "companyfacts": companyfacts,
        "submissions": submissions,
        "insider": insider,
        "form13f": form13f,
        "finra": finra,
        "prices": prices,
        "manifest": manifest,
    }


def test_offline_enrichment_build_is_point_in_time_safe(
    enrichment_module, verify_module, enrichment_inputs: dict[str, Path]
) -> None:
    root = enrichment_inputs["root"]
    args = [
        "--universe", str(enrichment_inputs["universe"]),
        "--prices", str(enrichment_inputs["prices"]),
        "--manifest", str(enrichment_inputs["manifest"]),
        "--companyfacts", str(enrichment_inputs["companyfacts"]),
        "--submissions", str(enrichment_inputs["submissions"]),
        "--insider-archive", str(enrichment_inputs["insider"]),
        "--form13f-archive", str(enrichment_inputs["form13f"]),
        "--finra-file", str(enrichment_inputs["finra"]),
        "--out-dir", str(root),
        "--cutoff-date", "2026-07-17",
        "--retrieved-at", "2026-07-17T12:00:00Z",
    ]
    assert enrichment_module.main(args) == 0

    con = duckdb.connect()
    try:
        for filename, contract in CONTRACTS.items():
            path = root / filename
            assert path.is_file()
            description = con.execute(
                "SELECT * FROM read_parquet(?) LIMIT 0", [str(path)]
            ).description
            assert tuple(column[0] for column in description) == tuple(
                name for name, _ in contract.columns
            )
            assert con.execute(
                "SELECT count(*) FROM read_parquet(?)", [str(path)]
            ).fetchone()[0] > 0
            actual_columns = {column[0] for column in description}
            assert {name for name, _ in POINT_IN_TIME_COLUMNS} <= actual_columns

        q4 = con.execute(
            """
            SELECT revenue, operating_cash_flow, capital_expenditure, free_cash_flow
              FROM read_parquet(?)
             WHERE fiscal_year = 2025 AND fiscal_period = 'Q4'
            """,
            [str(root / "normalized-fundamentals-quarterly.parquet")],
        ).fetchone()
        assert q4 == pytest.approx((130.0, 25.0, 5.0, 20.0))
        unavailable = con.execute(
            """
            SELECT count(*) FROM read_parquet(?)
             WHERE filing_available_date > factor_as_of_date
            """,
            [str(root / "fundamental-factors.parquet")],
        ).fetchone()[0]
        assert unavailable == 0
        finra_dates = con.execute(
            "SELECT settlement_date, publication_date FROM read_parquet(?)",
            [str(root / "finra-short-interest.parquet")],
        ).fetchone()
        assert finra_dates == (date(2026, 6, 30), date(2026, 7, 10))
    finally:
        con.close()

    manifest = json.loads(enrichment_inputs["manifest"].read_text())
    assert len(manifest["datasets"]) == len(CONTRACTS)
    assert manifest["coverage"]["analyst_estimates"]["status"] == "NOT_CONFIGURED"
    assert {record["file"] for record in manifest["release_files"]} == (
        set(CONTRACTS) | {"NOTICE.md"}
    )
    verify_module.verify(root, require_ready=True, require_production=False)


def test_finra_rejects_publication_before_settlement(
    enrichment_module, tmp_path: Path
) -> None:
    schedule = tmp_path / "schedule.csv"
    schedule.write_text(
        "settlement_date,publication_date,source_url\n"
        "2026-06-30,2026-06-29,https://www.finra.org/\n",
        encoding="utf-8",
    )
    with pytest.raises(enrichment_module.EnrichmentError, match="precedes settlement"):
        enrichment_module.load_finra_schedule(schedule, date(2026, 7, 17))


def test_conservative_availability_rolls_after_close(enrichment_module) -> None:
    assert enrichment_module.conservative_available_date(
        date(2026, 6, 10), datetime(2026, 6, 10, 21, 0)
    ) == date(2026, 6, 11)


def test_sec_date_parser_fast_path_preserves_legacy_formats(enrichment_module) -> None:
    assert enrichment_module.parse_date("2026-07-17") == date(2026, 7, 17)
    assert enrichment_module.parse_date("17-Jul-2026") == date(2026, 7, 17)


def test_optional_analyst_schema_exists_but_is_not_required() -> None:
    assert ANALYST_ESTIMATES_CONTRACT.filename == "analyst-estimates.parquet"
    assert ANALYST_ESTIMATES_CONTRACT.filename not in CONTRACTS
    names = {name for name, _ in ANALYST_ESTIMATES_CONTRACT.columns}
    assert {
        "security_id",
        "estimate_as_of_utc",
        "consensus_value",
        "provider",
        "provider_license",
        "source_retrieved_at_utc",
    } <= names


def test_official_archive_registry_and_workflow_headers() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    registry = json.loads(
        (repo_root / "config/official-source-archives.json").read_text()
    )
    for key in ("insider_archives", "form13f_archives"):
        urls = registry[key]
        assert len(urls) >= 2
        assert len(urls) == len(set(urls))
        assert all(url.startswith("https://www.sec.gov/") for url in urls)
    workflow = (repo_root / ".github/workflows/build-market-data.yml").read_text()
    assert "secrets.SEC_USER_AGENT" in workflow
    assert "--user-agent \"$SEC_USER_AGENT\"" in workflow
    assert "Accept-Encoding: gzip, deflate" in workflow
    assert "--compressed" in workflow
    assert "refresh_enrichment" in workflow
    assert 'cron: "17 8 * * 6"' in workflow
    assert 'EVENT_SCHEDULE: ${{ github.event.schedule }}' in workflow
    assert 'if [[ "$EVENT_SCHEDULE" == "17 8 * * 6" ]]' in workflow
    assert "github.event.schedule == '17 20 * * 1-5' && 24 || 360" in workflow
    assert "github.event.schedule == '17 20 * * 1-5' && 5 || 15" in workflow
    assert "reuse-enrichment-snapshot.py" in workflow
    assert (
        "steps.inputs.outputs.mode == 'full' && "
        "steps.inputs.outputs.refresh_enrichment == 'true'"
    ) in workflow


def test_daily_build_reuses_validated_snapshot_and_adds_new_admissions(
    enrichment_module,
    reuse_module,
    enrichment_inputs: dict[str, Path],
) -> None:
    previous = enrichment_inputs["root"]
    args = [
        "--universe", str(enrichment_inputs["universe"]),
        "--prices", str(enrichment_inputs["prices"]),
        "--manifest", str(enrichment_inputs["manifest"]),
        "--companyfacts", str(enrichment_inputs["companyfacts"]),
        "--submissions", str(enrichment_inputs["submissions"]),
        "--insider-archive", str(enrichment_inputs["insider"]),
        "--form13f-archive", str(enrichment_inputs["form13f"]),
        "--finra-file", str(enrichment_inputs["finra"]),
        "--out-dir", str(previous),
        "--cutoff-date", "2026-07-17",
        "--retrieved-at", "2026-07-17T12:00:00Z",
    ]
    assert enrichment_module.main(args) == 0

    daily = previous / "daily"
    daily.mkdir()
    universe = daily / "security-universe.csv"
    universe.write_text(
        "security_id,ticker,exchange_mic,universe_admission_status\n"
        "XNYS:NEW,NEW,XNYS,ADMITTED\n",
        encoding="utf-8",
    )
    unmatched = daily / "unmatched-tickers.csv"
    unmatched.write_text("ticker,reason\nNEW,NO_SOURCE_SYMBOL\n", encoding="utf-8")
    prices = daily / "yahoo-ohlcv-320.parquet"
    splits = daily / "yahoo-splits.parquet"
    shutil.copyfile(enrichment_inputs["prices"], prices)
    shutil.copyfile(enrichment_inputs["prices"], splits)

    def release_record(path: Path, rows: int) -> dict[str, object]:
        return {
            "file": path.name,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "bytes": path.stat().st_size,
            "rows": rows,
        }

    daily_manifest = {
        "schema_version": "1.0.0",
        "status": "READY",
        "created_at_utc": "2026-07-18T00:30:00Z",
        "aggregate": {"min_date": "2025-01-01", "max_date": "2026-07-17"},
        "source": {"revision": "fixture-market-revision"},
        "validation": {"warnings": [], "errors": []},
        "release_files": [
            release_record(prices, 1),
            release_record(splits, 0),
            release_record(universe, 1),
            release_record(unmatched, 1),
        ],
    }
    current_manifest = daily / "manifest.json"
    current_manifest.write_text(json.dumps(daily_manifest), encoding="utf-8")

    assert reuse_module.main(
        [
            "--current-manifest", str(current_manifest),
            "--previous-release", str(previous),
            "--out-dir", str(daily),
            "--source-tag", "market-data-20260717T120000Z",
        ]
    ) == 0
    result = json.loads(current_manifest.read_text())
    assert result["enrichment_snapshot"]["source_release_tag"] == (
        "market-data-20260717T120000Z"
    )
    assert result["enrichment_snapshot"]["new_unmapped_admissions_count"] == 1
    assert {record["file"] for record in result["release_files"]} == (
        {"NOTICE.md", *CONTRACTS}
        | {
            "security-universe.csv",
            "unmatched-tickers.csv",
            "yahoo-ohlcv-320.parquet",
            "yahoo-splits.parquet",
        }
    )
    assert len(result["datasets"]) == len(CONTRACTS) + 4
    assert result["coverage"]["security_master"] == {
        "status": "READY",
        "security_count": 2,
        "sec_cik_mapped_count": 1,
        "current_admitted_count": 1,
        "historical_security_count": 1,
        "new_unmapped_admissions_count": 1,
        "unmapped_daily_admission_count": 1,
    }
    con = duckdb.connect()
    try:
        new_master = con.execute(
            "SELECT cik, mapping_status FROM read_parquet(?) WHERE security_id = ?",
            [str(daily / "security-master.parquet"), "XNYS:NEW"],
        ).fetchone()
    finally:
        con.close()
    assert new_master == (None, "UNMAPPED_DAILY_ADMISSION")
    assert (
        hashlib.sha256((daily / "sec-filings.parquet").read_bytes()).hexdigest()
        == hashlib.sha256((previous / "sec-filings.parquet").read_bytes()).hexdigest()
    )


def test_sec_ticker_collision_uses_unique_latest_filer(
    enrichment_module, tmp_path: Path
) -> None:
    def submission(cik: int, accession: str, filing_date: str) -> dict[str, object]:
        return {
            "cik": cik,
            "name": f"Registrant {cik}",
            "sic": "2834",
            "tickers": ["AAPL"],
            "filings": {
                "recent": {
                    "accessionNumber": [accession],
                    "filingDate": [filing_date],
                    "reportDate": ["2025-12-31"],
                    "acceptanceDateTime": [f"{filing_date}T20:00:00Z"],
                    "form": ["10-K"],
                    "primaryDocument": ["form10k.htm"],
                    "items": [""],
                    "fileNumber": ["001-00001"],
                    "filmNumber": ["26000001"],
                }
            },
        }

    archive = tmp_path / "submissions.zip"
    with zipfile.ZipFile(archive, "w") as output:
        output.writestr(
            "CIK0000000001.json",
            json.dumps(submission(1, "0000000001-25-000001", "2025-02-01")),
        )
        output.writestr(
            "CIK0000000002.json",
            json.dumps(submission(2, "0000000002-26-000001", "2026-02-01")),
        )
    masters, filings, _, cik_mapping = enrichment_module.parse_submissions(
        archive,
        {"AAPL": "XNAS:AAPL"},
        {"XNAS:AAPL": {"ticker": "AAPL", "exchange_mic": "XNAS"}},
        date(2026, 7, 17),
        datetime(2026, 7, 17, 12, 0),
    )
    assert masters[0]["cik"] == "0000000002"
    assert masters[0]["mapping_status"] == "LATEST_SEC_FILING_TICKER_COLLISION"
    assert [row["accession_number"] for row in filings] == [
        "0000000002-26-000001"
    ]
    assert cik_mapping == {"0000000002": "XNAS:AAPL"}


def test_ownership_schedule_attaches_to_subject_not_filer(
    enrichment_module, tmp_path: Path
) -> None:
    accession = "0000000001-26-000199"

    def document(cik: int, ticker: str) -> dict[str, object]:
        return {
            "cik": cik,
            "name": f"Registrant {cik}",
            "sic": "2834",
            "tickers": [ticker],
            "filings": {
                "recent": {
                    "accessionNumber": [accession],
                    "filingDate": ["2026-05-12"],
                    "reportDate": [""],
                    "acceptanceDateTime": ["2026-05-12T13:17:28Z"],
                    "form": ["SCHEDULE 13G/A"],
                    "primaryDocument": ["primary_doc.xml"],
                    "items": [""],
                    "fileNumber": [""],
                    "filmNumber": [""],
                }
            },
        }

    archive = tmp_path / "submissions.zip"
    with zipfile.ZipFile(archive, "w") as output:
        output.writestr("CIK0000000001.json", json.dumps(document(1, "JPM")))
        output.writestr("CIK0000000002.json", json.dumps(document(2, "AAPL")))
        output.writestr("CIK0000000003.json", json.dumps(document(3, "MSFT")))
    _, filings, all_filings, _ = enrichment_module.parse_submissions(
        archive,
        {
            "JPM": "XNYS:JPM",
            "AAPL": "XNAS:AAPL",
            "MSFT": "XNAS:MSFT",
        },
        {
            "XNYS:JPM": {"ticker": "JPM", "exchange_mic": "XNYS"},
            "XNAS:AAPL": {"ticker": "AAPL", "exchange_mic": "XNAS"},
            "XNAS:MSFT": {"ticker": "MSFT", "exchange_mic": "XNAS"},
        },
        date(2026, 7, 17),
        datetime(2026, 7, 17, 12, 0),
    )
    assert set(all_filings) == {
        ("0000000002", accession),
        ("0000000003", accession),
    }
    assert {
        row["security_id"] for row in all_filings.values()
    } == {"XNAS:AAPL", "XNAS:MSFT"}
    assert [row["form"] for row in filings] == [
        "SCHEDULE 13G/A",
        "SCHEDULE 13G/A",
    ]
