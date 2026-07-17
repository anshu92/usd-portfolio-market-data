from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import duckdb
import pytest

from enrichment_contract import (
    CSV_NULL_MARKER,
    ContractError,
    DatasetContract,
    write_parquet,
)


CONTRACT = DatasetContract(
    "contract-fixture.parquet",
    (
        ("id", "VARCHAR"),
        ("event_date", "DATE"),
        ("value", "DOUBLE"),
        ("quantity", "BIGINT"),
        ("enabled", "BOOLEAN"),
        ("description", "VARCHAR"),
        ("optional", "VARCHAR"),
        ("observed_at", "TIMESTAMP"),
    ),
    ("id",),
    "offline fixture",
)


def test_bulk_writer_preserves_types_nulls_and_quoted_text(tmp_path: Path) -> None:
    path = tmp_path / CONTRACT.filename
    rows = [
        {
            "id": "B",
            "event_date": date(2026, 7, 17),
            "value": 1.25,
            "quantity": 2,
            "enabled": True,
            "description": 'comma, quote " and newline\nremain exact',
            "optional": None,
            "observed_at": datetime(2026, 7, 17, 12, 30, 45),
        },
        {
            "id": "A",
            "event_date": date(2026, 7, 16),
            "value": -3.5,
            "quantity": 0,
            "enabled": False,
            "description": "",
            "optional": "present",
            "observed_at": datetime(2026, 7, 16, 8, 0),
        },
    ]
    assert write_parquet(path, CONTRACT, rows) == 2
    con = duckdb.connect()
    try:
        actual = con.execute(
            "SELECT * FROM read_parquet(?) ORDER BY id", [str(path)]
        ).fetchall()
    finally:
        con.close()
    assert actual == [
        (
            "A",
            date(2026, 7, 16),
            -3.5,
            0,
            False,
            "",
            "present",
            datetime(2026, 7, 16, 8, 0),
        ),
        (
            "B",
            date(2026, 7, 17),
            1.25,
            2,
            True,
            'comma, quote " and newline\nremain exact',
            None,
            datetime(2026, 7, 17, 12, 30, 45),
        ),
    ]
    assert not path.with_suffix(".parquet.csv.tmp").exists()


def test_bulk_writer_rejects_duplicate_keys_and_null_marker(tmp_path: Path) -> None:
    path = tmp_path / CONTRACT.filename
    base = {
        "id": "A",
        "event_date": date(2026, 7, 17),
        "value": 1.0,
        "quantity": 1,
        "enabled": True,
        "description": "fixture",
        "optional": None,
        "observed_at": datetime(2026, 7, 17, 12, 0),
    }
    with pytest.raises(ContractError, match="duplicate primary keys"):
        write_parquet(path, CONTRACT, [base, dict(base)])
    with pytest.raises(ContractError, match="collides with null marker"):
        write_parquet(path, CONTRACT, [{**base, "description": CSV_NULL_MARKER}])
    assert not path.with_suffix(".parquet.csv.tmp").exists()


def test_bulk_writer_preserves_schema_for_empty_output(tmp_path: Path) -> None:
    path = tmp_path / CONTRACT.filename
    assert write_parquet(path, CONTRACT, []) == 0
    con = duckdb.connect()
    try:
        description = con.execute(
            "SELECT * FROM read_parquet(?) LIMIT 0", [str(path)]
        ).description
        count = con.execute(
            "SELECT count(*) FROM read_parquet(?)", [str(path)]
        ).fetchone()[0]
    finally:
        con.close()
    assert tuple((str(column[0]), str(column[1])) for column in description) == (
        CONTRACT.columns
    )
    assert count == 0
