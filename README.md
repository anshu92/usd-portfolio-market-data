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

Consumers without direct GitHub Release download access can dispatch
`export-release-for-consumer.yml`. The read-only workflow resolves the immutable latest
tag, verifies GitHub's SHA-256 digest for all five assets, runs the production manifest
verifier, and uploads `validated-market-data-{tag}` as a 30-day workflow artifact. The
artifact also contains `github-release.json` and `resolved-tag.txt` so the receiving
consumer can independently revalidate the pinned release before atomic promotion.

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

Validation applies to rows eligible for the requested per-security history window;
legacy source rows that cannot enter an output do not fail a build. Source-symbol
histories more than five eligible XNYS sessions stale are treated as unmatched to limit
recycled-ticker contamination. Histories are also truncated at gaps longer than 14
calendar days or an invalid row with a fourfold price discontinuity, preventing
pre-boundary rows from a recycled symbol from being attached to a current listing. A
broad source session with invalid OHLC bounds is
quarantined only when at least 100 securities are present and more than 1% are invalid.
Every quarantine is recorded in the manifest, and the normal market-freshness gate then
evaluates the resulting maximum date. Individual invalid output rows still fail.

See [NOTICE.md](NOTICE.md) for attribution and upstream-rights caveats.
