# USD Portfolio Market Data

This repository builds a compact, validated historical market, SEC, ownership, event,
and FINRA short-interest package for normal-status Nasdaq- and NYSE-listed equities. It
contains public reference data and producer code only—never portfolio holdings, cash,
transactions, credentials, or other private state.

The output is historical research data, not a quote feed. Raw `close` is deliberately
not described as adjusted close.

## Outputs

A production release contains:

- `NOTICE.md`
- `corporate-events.parquet`
- `earnings-and-guidance-events.parquet`
- `finra-short-interest.parquet`
- `fundamental-factors.parquet`
- `insider-signals.parquet`
- `insider-transactions.parquet`
- `institutional-holdings-13f.parquet`
- `institutional-ownership-signals.parquet`
- `manifest.json`
- `normalized-fundamentals-quarterly.parquet`
- `sec-company-facts.parquet`
- `sec-filings.parquet`
- `security-master.parquet`
- `security-universe.csv`
- `unmatched-tickers.csv`
- `yahoo-ohlcv-320.parquet`
- `yahoo-splits.parquet`

Consumers must download `manifest.json` first, require schema `1.0.0` and status
`READY`, then validate every file's SHA-256, size, row count, and schema before use.
The manifest records source revisions and hashes, universe provenance and drift,
coverage, history eligibility, XNYS-session freshness, and all warnings.

Consumers without direct GitHub Release download access can dispatch
`export-release-for-consumer.yml`. The read-only workflow resolves the immutable latest
tag, verifies GitHub's SHA-256 digest for every asset, runs the production manifest
verifier, and uploads `validated-market-data-{tag}` as a 30-day workflow artifact. The
artifact also contains `github-release.json` and `resolved-tag.txt` so the receiving
consumer can independently revalidate the pinned release before atomic promotion.
After upload, a separate write-scoped job commits
`consumer/latest-production-artifact.json` with the exact run, artifact, release,
producer-commit, expiry, size, and digest values. Consumers must compare that pointer
against the Actions artifact API and then revalidate the downloaded contents; the
pointer is discovery metadata, not a substitute for validation. Successful production
publication automatically dispatches a fresh consumer export.

The manifest reports each enrichment domain independently. Analyst estimates remain
absent until a licensed provider is configured and are explicitly reported as
`NOT_CONFIGURED`; this disables estimate-only components, not unrelated archetypes.
Daily releases retain each enrichment dataset's original retrieval timestamp and name
the immutable source release in `enrichment_snapshot`; consumers must never interpret
the package creation time as SEC or FINRA freshness.
See [consumer/README.md](consumer/README.md) for the import, freshness, point-in-time,
and factor-disable contract.

## Universe policy

`build-security-universe.py` reads the Nasdaq Trader symbol directories and admits:

- active USD listings on `XNAS`, `XNYS`, `ARCX`, `BATS`, and `CBOE` whose exchange
  country is the USA;
- `COMMON`, `ADR`, and non-leveraged, non-inverse `ETF` securities. ETFs receive
  `ADMITTED_ETF`; equities and ADRs receive `ADMITTED`.

It explicitly rejects inactive, non-USD/non-US, leveraged/inverse ETF, warrant, right,
unit, and unknown-security listings. Every non-admitted row has a deterministic
rejection state; unknowns are never emitted as `UNCLASSIFIED`. IDs are deterministic
MIC/ticker pairs such as `XNAS:AAPL`, `XNYS:TSM`, and `BATS:MAGS`.

`config/security-metadata.csv` records reviewed ADR identity and ETF metadata (CIK,
home market, fund provider, index, expense ratio, asset class, leverage, and inverse
flags). `config/universe-overrides.csv` remains the continuity registry for listing
renames or venue moves. ETFs are included in price, split, and security-master assets,
but are not stock-archetype candidates; consumers should treat them as diversification
and benchmark instruments.

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

Build enrichment files from local official-source snapshots:

```bash
python build-enrichment-data.py \
  --universe dist/security-universe.csv \
  --prices dist/yahoo-ohlcv-320.parquet \
  --manifest dist/manifest.json \
  --companyfacts /path/to/companyfacts.zip \
  --submissions /path/to/submissions.zip \
  --insider-archive /path/to/2026q2_form345.zip \
  --form13f-archive /path/to/01mar2026-31may2026_form13f.zip \
  --finra-file /path/to/finra-short-interest.json \
  --cutoff-date 2026-07-17 \
  --out-dir dist
python verify-release.py --dist dist --require-ready --require-production
```

SEC inputs use the nightly company-facts and submissions bulk archives plus the
version-controlled quarterly archive list in
`config/official-source-archives.json`. FINRA observations are queried only from its
official Equity API and receive the publication date from the reviewed official
schedule. Every output schema is defined in `enrichment_contract.py`; the optional
analyst-estimates schema exists there but is not a release requirement.
Raw company facts retain all eligible observations. Quarterly normalization retains a
rolling six-fiscal-year window, which covers the three-year stability/growth factors
while bounding full-universe memory use; the floor year is disclosed in the manifest.

Before a full GitHub Actions refresh, run the local writer benchmark and offline
enrichment fixture with phase logging:

```bash
PYTHONPATH=. uv run --python 3.13 --with-requirements requirements-dev.txt \
  python benchmarks/benchmark_company_facts.py --rows 1000000
PYTHONPATH=. uv run --python 3.13 --with-requirements requirements-dev.txt \
  python benchmarks/benchmark_submissions.py --documents 1000 --filings-per-document 100
PYTHONPATH=. uv run --python 3.13 --with-requirements requirements-dev.txt \
  pytest -q -s tests/test_enrichment.py::test_offline_enrichment_build_is_point_in_time_safe
```

The benchmark uses the exact `sec-company-facts.parquet` schema. The fixture prints
`enrichment_stage` records for source hashes, submissions, company facts, derived
datasets, and manifest generation; large writes also emit million-row progress records.

## Automation and publication

Pull requests run only offline fixtures with read-only permissions. The production
workflow runs the bounded market path at 20:17 America/New_York on weekdays, refreshes
official enrichment sources at 08:17 America/New_York each Saturday, and can also be
dispatched:

- `smoke` is fixed to AAPL, MSFT, BRK.B, NVDA, and TSM with 30 sessions and never
  publishes;
- `full` defaults to 320 sessions and may publish only with current cutoff, explicit
  `publish=true`, a `READY` manifest, and repository variable
  `PRODUCTION_RELEASES_ENABLED=true`;
- scheduled builds publish only when the same variable is true;
- weekday scheduled builds reuse the latest fully validated immutable enrichment
  snapshot and Yahoo Chart history baseline, then replace the prior 14 calendar days
  of Chart-backed observations with a fresh Yahoo delta. This keeps ETF/ADR history
  complete while retaining current-session freshness within the 24-minute build
  budget plus a 5-minute publish budget;
- Saturday scheduled builds set `refresh_enrichment=true` and perform the slower
  official SEC/FINRA rebuild with a six-hour ceiling; this weekly cadence captures SEC
  filing/fundamental changes and FINRA's semi-monthly updates without paying the
  roughly one-hour cost every day;
- a manual `refresh_enrichment=true` run remains available for bootstrapping, recovery,
  or an out-of-cycle source update.

The normal daily path therefore has at most 29 minutes of job execution. It downloads
the previous release by its immutable tag, verifies GitHub asset digests and the full
production contract, copies the enrichment assets without changing their source
clocks, and then verifies the newly assembled release again. If the fresh symbol
directory contains a new admission, `security-master.parquet` adds an explicit
`UNMAPPED_DAILY_ADMISSION` row; the next Saturday or manual enrichment refresh resolves
its SEC mapping. Historical master rows are retained so older point-in-time enrichment
rows remain referentially valid.

Before enabling scheduled publication, run one manual `full` workflow with
`refresh_enrichment=true`, validate its artifact, and publish it as the immutable
baseline. Until that baseline exists, snapshot reuse fails closed because the prior
five-asset release does not satisfy the expanded contract.

The build job has read-only repository access. A separate publish job receives
`contents: write`, verifies the transferred artifact again, creates a draft release
with every asset attached, then publishes it as latest. Enable GitHub release
immutability before the first production release.

Full refresh builds require the `SEC_USER_AGENT` GitHub secret. Every SEC request sends
that identification plus `Accept-Encoding: gzip, deflate`; bulk downloads are
sequential and remain below the SEC's ten-requests-per-second ceiling. Daily snapshot
reuse does not contact SEC or FINRA. Update the reviewed quarterly archive configuration
when the SEC publishes a new insider or 13F archive.

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
