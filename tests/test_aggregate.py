from __future__ import annotations

import csv
import json
from pathlib import Path

import duckdb


def build_args(inputs, out_dir: Path, *extra: str):
    return [
        "--universe",
        str(inputs["universe"]),
        "--universe-metadata",
        str(inputs["metadata"]),
        "--out-dir",
        str(out_dir),
        "--prices-file",
        str(inputs["prices"]),
        "--splits-file",
        str(inputs["splits"]),
        "--sessions",
        "3",
        "--minimum-history",
        "2",
        "--minimum-coverage",
        "1.0",
        "--minimum-adequate-history-coverage",
        "1.0",
        "--cutoff-date",
        "2024-01-10",
        "--observed-at",
        "2024-01-11T01:00:00Z",
        *extra,
    ]


def test_builds_ready_aggregate_and_resolves_class_share_alias(
    aggregate_module, verify_module, aggregate_inputs, tmp_path
):
    out_dir = tmp_path / "dist"
    exit_code = aggregate_module.main(build_args(aggregate_inputs, out_dir))
    assert exit_code == 0
    manifest = json.loads((out_dir / "manifest.json").read_text())
    assert manifest["status"] == "READY"
    assert manifest["aggregate"]["rows"] == 6
    assert manifest["universe"]["matched_coverage"] == 1.0
    assert any("byte-identical price" in item for item in manifest["validation"]["warnings"])
    assert any("byte-identical split" in item for item in manifest["validation"]["warnings"])

    con = duckdb.connect()
    try:
        rows = con.execute(
            "SELECT security_id, ticker, source_symbol, count(*) "
            "FROM read_parquet(?) GROUP BY ALL ORDER BY security_id",
            [str(out_dir / "yahoo-ohlcv-3.parquet")],
        ).fetchall()
        split_rows = con.execute(
            "SELECT security_id, ticker, source_symbol, event_date, split_factor, "
            "source_revision FROM read_parquet(?)",
            [str(out_dir / "yahoo-splits.parquet")],
        ).fetchall()
    finally:
        con.close()
    assert rows == [
        ("XNAS:AAPL", "AAPL", "AAPL", 3),
        ("XNYS:BRK.B", "BRK.B", "BRK-B", 3),
    ]
    assert verify_module.verify(out_dir, require_ready=True, require_production=False)
    assert split_rows == [
        (
            "XNAS:AAPL",
            "AAPL",
            "AAPL",
            aggregate_module.parse_date("2020-08-31"),
            "4:1",
            "LOCAL_OVERRIDE",
        )
    ]
    assert manifest["universe"]["history_distribution"] == {
        "minimum": 3,
        "median": 3,
        "maximum": 3,
        "buckets": {
            "0_to_29": 2,
            "30_to_251": 0,
            "252_to_319": 0,
            "320_or_more": 0,
        },
    }


def test_conflicting_source_rows_fail(aggregate_module, aggregate_inputs, tmp_path):
    con = duckdb.connect()
    try:
        con.execute(
            "INSERT INTO read_parquet(?) VALUES ('AAPL', '2024-01-10', 106, 109, 105, 107, 1200)",
            [str(aggregate_inputs["prices"])],
        )
    except duckdb.Error:
        # Parquet is immutable; rewrite a conflicting fixture through a table.
        con.execute(
            "CREATE TABLE conflicting AS SELECT * FROM read_parquet(?)",
            [str(aggregate_inputs["prices"])],
        )
        con.execute(
            "INSERT INTO conflicting VALUES ('AAPL', '2024-01-10', 106, 109, 105, 107, 1200)"
        )
        replacement = tmp_path / "conflicting.parquet"
        con.execute("COPY conflicting TO ? (FORMAT PARQUET)", [str(replacement)])
        aggregate_inputs["prices"] = replacement
    finally:
        con.close()
    exit_code = aggregate_module.main(
        build_args(aggregate_inputs, tmp_path / "dist-conflict")
    )
    assert exit_code == 2


def test_alias_order_and_ambiguous_aliases(aggregate_module, aggregate_inputs, tmp_path):
    assert aggregate_module.yahoo_aliases("BRK.B") == ["BRK.B", "BRK-B"]
    assert aggregate_module.yahoo_aliases("ABC/D") == ["ABC/D", "ABC-D"]

    universe = aggregate_inputs["universe"]
    universe.write_text(
        universe.read_text() + "XNYS:BRK-B,BRK-B,ADMITTED\n", encoding="utf-8"
    )
    metadata = json.loads(aggregate_inputs["metadata"].read_text())
    metadata["rows"] = 3
    metadata["sha256"] = aggregate_module.sha256_file(universe)
    aggregate_inputs["metadata"].write_text(json.dumps(metadata) + "\n")
    assert (
        aggregate_module.main(
            build_args(aggregate_inputs, tmp_path / "dist-ambiguous")
        )
        == 2
    )


def test_invalid_matched_ohlcv_fails(aggregate_module, aggregate_inputs, tmp_path):
    con = duckdb.connect()
    invalid = tmp_path / "invalid.parquet"
    try:
        con.execute(
            "CREATE TABLE invalid AS SELECT * FROM read_parquet(?)",
            [str(aggregate_inputs["prices"])],
        )
        con.execute("UPDATE invalid SET high = low - 1 WHERE symbol = 'AAPL'")
        con.execute("COPY invalid TO ? (FORMAT PARQUET)", [str(invalid)])
    finally:
        con.close()
    aggregate_inputs["prices"] = invalid
    assert (
        aggregate_module.main(build_args(aggregate_inputs, tmp_path / "dist-invalid"))
        == 2
    )


def test_systemic_invalid_session_is_quarantined(
    aggregate_module, aggregate_inputs, tmp_path
):
    con = duckdb.connect()
    systemic = tmp_path / "systemic.parquet"
    try:
        con.execute(
            "CREATE TABLE systemic AS SELECT * FROM read_parquet(?)",
            [str(aggregate_inputs["prices"])],
        )
        con.execute(
            "UPDATE systemic SET high = low - 1 WHERE report_date = '2024-01-10'"
        )
        con.execute("COPY systemic TO ? (FORMAT PARQUET)", [str(systemic)])
    finally:
        con.close()
    aggregate_inputs["prices"] = systemic
    out_dir = tmp_path / "dist-systemic"
    exit_code = aggregate_module.main(
        build_args(
            aggregate_inputs,
            out_dir,
            "--minimum-systemic-session-securities",
            "2",
        )
    )
    assert exit_code == 0
    manifest = json.loads((out_dir / "manifest.json").read_text())
    assert manifest["status"] == "READY"
    assert manifest["aggregate"]["rows"] == 4
    assert manifest["aggregate"]["max_date"] == "2024-01-09"
    assert manifest["source"]["quality"]["quarantined_sessions"] == [
        {
            "session_date": "2024-01-10",
            "invalid_rows": 2,
            "total_rows": 2,
            "invalid_rate": 1.0,
        }
    ]


def test_stale_source_history_is_unmatched(
    aggregate_module, aggregate_inputs, tmp_path
):
    universe = aggregate_inputs["universe"]
    universe.write_text(
        universe.read_text() + "XNAS:MSFT,MSFT,ADMITTED\n", encoding="utf-8"
    )
    metadata = json.loads(aggregate_inputs["metadata"].read_text())
    metadata["rows"] = 3
    metadata["sha256"] = aggregate_module.sha256_file(universe)
    aggregate_inputs["metadata"].write_text(json.dumps(metadata) + "\n")

    con = duckdb.connect()
    stale = tmp_path / "stale-symbol.parquet"
    try:
        con.execute(
            "CREATE TABLE stale_symbol AS SELECT * FROM read_parquet(?)",
            [str(aggregate_inputs["prices"])],
        )
        con.execute(
            "INSERT INTO stale_symbol VALUES "
            "('MSFT','2023-12-01',100,101,99,100.5,1000)"
        )
        con.execute("COPY stale_symbol TO ? (FORMAT PARQUET)", [str(stale)])
    finally:
        con.close()
    aggregate_inputs["prices"] = stale
    out_dir = tmp_path / "dist-stale-symbol"
    exit_code = aggregate_module.main(
        build_args(
            aggregate_inputs,
            out_dir,
            "--minimum-coverage",
            "0.60",
        )
    )
    assert exit_code == 0
    manifest = json.loads((out_dir / "manifest.json").read_text())
    assert manifest["source"]["quality"]["stale_source_symbols"][0][
        "security_id"
    ] == "XNAS:MSFT"
    with (out_dir / "unmatched-tickers.csv").open(newline="") as handle:
        unmatched = list(csv.DictReader(handle))
    assert unmatched[0]["reason"].startswith("SOURCE_HISTORY_STALE:")


def test_invalid_price_discontinuity_truncates_reused_symbol_history(
    aggregate_module, aggregate_inputs, tmp_path
):
    universe = aggregate_inputs["universe"]
    universe.write_text(
        universe.read_text() + "XNAS:SPCX,SPCX,ADMITTED\n", encoding="utf-8"
    )
    metadata = json.loads(aggregate_inputs["metadata"].read_text())
    metadata["rows"] = 3
    metadata["sha256"] = aggregate_module.sha256_file(universe)
    aggregate_inputs["metadata"].write_text(json.dumps(metadata) + "\n")

    con = duckdb.connect()
    reused = tmp_path / "reused-symbol.parquet"
    try:
        con.execute(
            "CREATE TABLE reused AS SELECT * FROM read_parquet(?)",
            [str(aggregate_inputs["prices"])],
        )
        con.execute(
            "INSERT INTO reused VALUES "
            "('SPCX','2024-01-07',20,21,19,20,1000),"
            "('SPCX','2024-01-08',0,100,100,100,0),"
            "('SPCX','2024-01-09',100,102,99,101,2000),"
            "('SPCX','2024-01-10',101,103,100,102,2100)"
        )
        con.execute("COPY reused TO ? (FORMAT PARQUET)", [str(reused)])
    finally:
        con.close()
    aggregate_inputs["prices"] = reused
    out_dir = tmp_path / "dist-reused-symbol"
    assert aggregate_module.main(build_args(aggregate_inputs, out_dir)) == 0
    manifest = json.loads((out_dir / "manifest.json").read_text())
    spcx = next(
        item
        for item in manifest["source"]["quality"]["source_history_truncations"]
        if item["security_id"] == "XNAS:SPCX"
    )
    assert spcx == {
        "security_id": "XNAS:SPCX",
        "previous_segment_latest_date": "2024-01-07",
        "latest_segment_start_date": "2024-01-09",
        "reason": "INVALID_PRICE_DISCONTINUITY",
    }


def test_unmatched_ticker_is_reported(aggregate_module, aggregate_inputs, tmp_path):
    universe = aggregate_inputs["universe"]
    universe.write_text(
        universe.read_text() + "XNAS:MSFT,MSFT,ADMITTED\n", encoding="utf-8"
    )
    metadata = json.loads(aggregate_inputs["metadata"].read_text())
    metadata["rows"] = 3
    metadata["sha256"] = aggregate_module.sha256_file(universe)
    aggregate_inputs["metadata"].write_text(json.dumps(metadata) + "\n")
    out_dir = tmp_path / "dist-unmatched"
    args = build_args(
        aggregate_inputs,
        out_dir,
        "--minimum-coverage",
        "0.60",
    )
    exit_code = aggregate_module.main(args)
    assert exit_code == 0
    with (out_dir / "unmatched-tickers.csv").open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert rows == [
        {
            "security_id": "XNAS:MSFT",
            "ticker": "MSFT",
            "aliases_tried": "MSFT",
            "reason": "NO_SOURCE_SYMBOL_MATCH",
        }
    ]


def test_freshness_warning_and_holiday_calendar(
    aggregate_module, aggregate_inputs, tmp_path
):
    assert aggregate_module.expected_latest_session(
        aggregate_module.parse_date("2024-01-15")
    ).isoformat() == "2024-01-12"

    con = duckdb.connect()
    stale_prices = tmp_path / "stale.parquet"
    try:
        con.execute(
            "CREATE TABLE stale AS SELECT * FROM read_parquet(?) WHERE report_date = '2024-01-05'",
            [str(aggregate_inputs["prices"])],
        )
        # The base fixture has no Jan 5 rows, so create one row per admitted symbol.
        con.execute(
            "INSERT INTO stale VALUES "
            "('AAPL','2024-01-05',100,102,99,101,1000),"
            "('BRK-B','2024-01-05',350,352,348,351,500)"
        )
        con.execute("COPY stale TO ? (FORMAT PARQUET)", [str(stale_prices)])
    finally:
        con.close()
    aggregate_inputs["prices"] = stale_prices
    out_dir = tmp_path / "dist-stale"
    exit_code = aggregate_module.main(
        build_args(
            aggregate_inputs,
            out_dir,
            "--minimum-history",
            "1",
        )
    )
    assert exit_code == 2
    manifest = json.loads((out_dir / "manifest.json").read_text())
    assert manifest["status"] == "STALE_WARNING"
    assert manifest["validation"]["missing_eligible_sessions"] == 3
