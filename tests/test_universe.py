from __future__ import annotations

import csv
import json
from pathlib import Path


FIXTURES = Path(__file__).parent / "fixtures"
GENERATED_AT = "2026-07-17T01:45:00Z"


def run_builder(module, tmp_path: Path, *extra: str):
    out = tmp_path / "security-universe.csv"
    metadata = tmp_path / "universe-metadata.json"
    overrides = tmp_path / "overrides.csv"
    if not overrides.exists():
        overrides.write_text(
            "exchange_mic,source_symbol,action,security_id,canonical_ticker,security_type,reason\n",
            encoding="utf-8",
        )
    exit_code = module.main(
        [
            "--out",
            str(out),
            "--metadata-out",
            str(metadata),
            "--overrides",
            str(overrides),
            "--nasdaq-file",
            str(FIXTURES / "nasdaqlisted.txt"),
            "--other-listed-file",
            str(FIXTURES / "otherlisted.txt"),
            "--generated-at",
            GENERATED_AT,
            *extra,
        ]
    )
    return exit_code, out, metadata, overrides


def read_rows(path: Path):
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def test_builds_selected_venues_and_instrument_statuses(universe_module, tmp_path):
    exit_code, out, metadata, _ = run_builder(universe_module, tmp_path)
    assert exit_code == 0
    rows = {row["ticker"]: row for row in read_rows(out)}
    assert "AMEX" not in rows
    assert rows["AAPL"]["security_id"] == "XNAS:AAPL"
    assert rows["BRK.B"]["security_id"] == "XNYS:BRK.B"
    assert rows["BRK.B"]["universe_admission_status"] == "ADMITTED"
    assert rows["ADRX"]["security_type"] == "ADR_ADS"
    assert rows["ORDY"]["security_type"] == "ORDINARY_EQUITY"
    assert rows["LPCO"]["security_type"] == "COMMON_PARTNERSHIP_UNIT"
    assert rows["REIT"]["security_type"] == "REIT_BENEFICIAL_INTEREST"
    assert rows["BADF"]["universe_admission_status"] == "EXCLUDED"
    assert rows["ETFQ"]["admission_reason"] == "ETF"
    assert rows["NEXT"]["security_type"] == "NEXTSHARES"
    assert rows["PREF"]["security_type"] == "PREFERRED_EQUITY"
    assert rows["PADS"]["security_type"] == "PREFERRED_EQUITY"
    assert rows["DEBT"]["security_type"] == "DEBT"
    assert rows["RIGHTR"]["security_type"] == "RIGHT"
    assert rows["UNITU"]["security_type"] == "ACQUISITION_UNIT"
    assert rows["WARRW"]["security_type"] == "WARRANT"
    assert rows["WCUW"]["security_type"] == "WARRANT"
    assert rows["TEST"]["admission_reason"] == "TEST_ISSUE"
    assert rows["UNKN"]["universe_admission_status"] == "REVIEW_REQUIRED"
    assert rows["AAPL"]["source_file_created_at_utc"] == "2026-07-17T01:31:00Z"
    assert len(rows["AAPL"]["source_file_sha256"]) == 64
    manifest = json.loads(metadata.read_text())
    assert manifest["status"] == "READY"
    assert manifest["admitted_count"] == 7


def test_output_is_deterministic_for_fixed_inputs(universe_module, tmp_path):
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.mkdir()
    second.mkdir()
    _, first_out, _, _ = run_builder(universe_module, first)
    _, second_out, _, _ = run_builder(universe_module, second)
    assert first_out.read_bytes() == second_out.read_bytes()


def test_override_is_applied_and_collisions_fail(universe_module, tmp_path):
    overrides = tmp_path / "overrides.csv"
    overrides.write_text(
        "exchange_mic,source_symbol,action,security_id,canonical_ticker,security_type,reason\n"
        "XNAS,UNKN,ADMIT,XNAS:UNKN,UNKN,COMMON_EQUITY,reviewed common equity\n",
        encoding="utf-8",
    )
    exit_code, out, _, _ = run_builder(universe_module, tmp_path)
    assert exit_code == 0
    rows = {row["ticker"]: row for row in read_rows(out)}
    assert rows["UNKN"]["universe_admission_status"] == "ADMITTED"
    assert rows["UNKN"]["admission_reason"].startswith("OVERRIDE_ADMIT:")

    overrides.write_text(
        "exchange_mic,source_symbol,action,security_id,canonical_ticker,security_type,reason\n"
        "XNAS,UNKN,ADMIT,XNAS:AAPL,UNKN,COMMON_EQUITY,collision test\n",
        encoding="utf-8",
    )
    exit_code, _, _, _ = run_builder(universe_module, tmp_path)
    assert exit_code == 2


def test_duplicate_overrides_fail(universe_module, tmp_path):
    overrides = tmp_path / "overrides.csv"
    header = (
        "exchange_mic,source_symbol,action,security_id,canonical_ticker,"
        "security_type,reason\n"
    )
    overrides.write_text(
        header
        + "XNAS,UNKN,ADMIT,XNAS:UNKN,UNKN,COMMON_EQUITY,first review\n"
        + "XNAS,UNKN,EXCLUDE,,,UNCLASSIFIED,second review\n",
        encoding="utf-8",
    )
    exit_code, _, _, _ = run_builder(universe_module, tmp_path)
    assert exit_code == 2


def test_stale_source_timestamp_fails(universe_module, tmp_path):
    exit_code, _, metadata, _ = run_builder(
        universe_module,
        tmp_path,
        "--generated-at",
        "2026-07-20T01:45:00Z",
    )
    assert exit_code == 2
    result = json.loads(metadata.read_text())
    assert result["status"] == "VALIDATION_FAILED"
    assert any("hours old" in error for error in result["errors"])


def test_drift_gate_and_manual_override(universe_module, tmp_path):
    previous = tmp_path / "previous.csv"
    previous.write_text(
        "security_id,universe_admission_status\nXNAS:AAPL,ADMITTED\n",
        encoding="utf-8",
    )
    exit_code, _, metadata, _ = run_builder(
        universe_module, tmp_path, "--previous-universe", str(previous)
    )
    assert exit_code == 2
    assert json.loads(metadata.read_text())["status"] == "VALIDATION_FAILED"

    exit_code, _, metadata, _ = run_builder(
        universe_module,
        tmp_path,
        "--previous-universe",
        str(previous),
        "--allow-universe-drift",
    )
    assert exit_code == 0
    drift = json.loads(metadata.read_text())["drift"]
    assert drift["override_used"] is True
