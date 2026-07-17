# USD Portfolio Market Data

This repository builds a compact, validated historical OHLCV and split-event dataset
for normal-status Nasdaq- and NYSE-listed equities. It contains public reference data
and producer code only—never portfolio holdings, cash, transactions, credentials, or
other private state.

The output is historical research data, not a quote feed. Raw `close` is deliberately
not described as adjusted close.

## Outputs

A production release contains:

- `yahoo-ohlcv-320.parquet`
- `yahoo-splits.parquet`
- `security-universe.csv`
- `manifest.json`
- `unmatched-tickers.csv`

Consumers must download `manifest.json` first, require schema `1.0.0` and status
`READY`, then validate every file's SHA-256, size, row count, and schema before use.
The manifest records source revisions and hashes, universe provenance and drift,
coverage, history eligibility, XNYS-session freshness, and all warnings.

## Universe policy

`build-security-universe.py` reads the Nasdaq Trader symbol directories and admits:

- normal-status Nasdaq securities or NYSE exchange-code `N` securities;
- common or ordinary equities, ADRs/ADSs, REIT beneficial interests, and common
  partnership units.

It excludes test issues, ETFs/NextShares, preferreds, debt, warrants, rights,
acquisition units, and non-normal Nasdaq financial status. Unknown descriptions are
`REVIEW_REQUIRED`, not silently admitted. IDs are deterministic MIC/ticker pairs such
as `XNAS:AAPL` and `XNYS:BRK.B`.

Reviewed exceptions belong in `config/universe-overrides.csv`. Every override requires
an action, security type, and reason. Use an explicit `security_id` override to preserve
continuity across a known ticker rename or venue move.

## Local development

Use Python 3.13.14 and install the reviewed hash lock:

```bash
python3.13 -m venv .venv
. .venv/bin/activate
python -m pip install --require-hashes -r requirements-dev.txt
python -m pytest
```

Build a universe from local snapshots:

```bash
python build-security-universe.py \
  --nasdaq-file tests/fixtures/nasdaqlisted.txt \
  --other-listed-file tests/fixtures/otherlisted.txt \
  --out dist/security-universe.csv \
  --metadata-out dist/security-universe.metadata.json
```

For a fully offline market build, pass local Parquet inputs:

```bash
python build-yahoo-aggregate-v2.py \
  --universe dist/security-universe.csv \
  --universe-metadata dist/security-universe.metadata.json \
  --prices-file /path/to/stock_prices.parquet \
  --splits-file /path/to/stock_split_events.parquet \
  --out-dir dist
python verify-release.py --dist dist --require-ready
```

Without local overrides, the producer downloads the resolved Hugging Face dataset
revision into `.hf-cache` and records the exact revision and input hashes.

## Automation and publication

Pull requests run only offline fixtures with read-only permissions. The production
workflow runs at 20:17 America/New_York on weekdays and can also be dispatched:

- `smoke` is fixed to AAPL, MSFT, BRK.B, NVDA, and TSM with 30 sessions and never
  publishes;
- `full` defaults to 320 sessions and may publish only with current cutoff, explicit
  `publish=true`, a `READY` manifest, and repository variable
  `PRODUCTION_RELEASES_ENABLED=true`;
- scheduled builds publish only when the same variable is true.

The build job has read-only repository access. A separate publish job receives
`contents: write`, verifies the transferred artifact again, creates a draft release
with every asset attached, then publishes it as latest. Enable GitHub release
immutability before the first production release.

## Failure policy

Any schema, hash, identifier, OHLCV, coverage, history, freshness, or source-revision
failure stops publication. A previous release remains latest. Identical source rows are
collapsed and disclosed; conflicting rows fail. Unmatched securities remain in the
universe but are excluded from price-dependent analysis and listed diagnostically.

See [NOTICE.md](NOTICE.md) for attribution and upstream-rights caveats.
