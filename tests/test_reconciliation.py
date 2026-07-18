from __future__ import annotations

import pytest


def test_reconciliation_prefers_canonical_ids_and_rejects_ambiguous_tickers(tmp_path):
    from conftest import load_script

    module = load_script("security_reconciliation", "security-reconciliation.py")
    universe = tmp_path / "universe.csv"
    universe.write_text(
        "security_id,ticker,exchange_mic,cik,universe_admission_status\n"
        "XNYS:TSM,TSM,XNYS,0001046179,ADMITTED\n"
        "BATS:MAGS,MAGS,BATS,,ADMITTED_ETF\n"
        "XNAS:MAGS,MAGS,XNAS,,ADMITTED\n",
        encoding="utf-8",
    )
    active = module.load_active_universe(universe)
    assert module.reconcile({"cik": "0001046179", "ticker": "TSM"}, active).security_id == "XNYS:TSM"
    assert module.reconcile({"security_id": "BATS:MAGS"}, active).security_id == "BATS:MAGS"
    with pytest.raises(module.ReconciliationError, match="cannot map uniquely"):
        module.reconcile({"ticker": "MAGS"}, active)
