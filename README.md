# USD Portfolio Market Data

This repository builds a compact, validated historical market, SEC, ownership, event,
and FINRA short-interest package for normal-status Nasdaq- and NYSE-listed equities. It
contains public reference data and producer code only—never portfolio holdings, cash,
transactions, credentials, or other private state.

The output is historical research data, not a quote feed. Raw `close` is deliberately
not described as adjusted close.

Published production data is governed by the
[producer reliability contract](PRODUCER_RELIABILITY_CONTRACT.md). The contract is
enforced by the builders, group composer, release verifier, production workflow, and
consumer export workflow; it is not documentation-only.

## Outputs

A production release contains:

- `NOTICE.md`
- `benchmark-certification.json` when the all-or-none benchmark lane certifies
- `benchmark-distributions.parquet` when the all-or-none benchmark lane certifies
- `benchmark-total-returns.parquet` when the all-or-none benchmark lane certifies
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
`READY`, then validate every file's SHA-256, size, row count, schema, and deterministic
`dataset_groups` identity before use.
The manifest records source revisions and hashes, universe provenance and drift,
coverage, history eligibility, XNYS-session freshness, and all warnings.

Consumers without direct GitHub Release download access can dispatch
`export-release-for-consumer.yml`. The read-only workflow resolves the immutable latest
tag, verifies GitHub's SHA-256 digest for every asset, runs the production manifest
verifier, and uploads `validated-market-data-{tag}` as a 30-day workflow artifact. The
artifact also contains `github-release.json` and `resolved-tag.txt` so the receiving
consumer can independently revalidate the pinned release before atomic promotion.
Every producer-side workflow upload is compressed at level 9 and first builds a
temporary level-9 ZIP preview. A preview at or above 511 MiB is rejected, reserving
1 MiB of packaging headroom so the resulting artifact remains strictly below 512 MiB.
After upload, a separate write-scoped job commits
`consumer/latest-production-artifact.json` with the exact run, artifact, release,
producer-commit, expiry, size, and digest values. Consumers must compare that pointer
against the Actions artifact API and then revalidate the downloaded contents; the
pointer is discovery metadata, not a substitute for validation. Successful production
publication automatically dispatches a fresh consumer export.

## Compact decision-support stream

Routine V5 phases must not download or import the complete historical release. After a
production tag is published, `build-decision-support.yml` downloads a bounded source
set pinned to that immutable tag and produces:

- `decision-support.sqlite.zst`, a pre-indexed read-only SQLite database;
- `decision-support-manifest.json`, which binds every byte to the source release and
  dataset-group identities; and
- `candidate-funnel.parquet`, `actionability-matrix.json`, and compressed evidence
  packets with stable evidence IDs and direct primary-document locators; and
- seven canonical JSON phase packs under `phase-packs/` for Sunday, pre-open,
  execution research, exception monitoring, terminal review, accounting, and Saturday
  replay.

The compact database contains recent raw-close research history and current derived
evidence, not executable quotes or dividend-adjusted prices. Phase packs expose missing
or stale capabilities as `BLOCKED` or `DEGRADED`; they never invent an input. Live
quotes, halt/LULD state, broker eligibility, portfolio state, cash, lots,
confirmations, and final decisions remain consumer-owned.

Phase packs separately expose artifact, benchmark-only, challenger, and live-snapshot
operating modes. They bind explicit data/session cutoffs, validity windows, source
watermarks, deterministic rejection codes, and both the Toronto task timezone and New
York exchange timezone. The producer workflow runs after certified releases and on
timezone-aware cadence backstops; consumers still enforce timestamps because GitHub
cron is not an execution-time SLO.
A pack whose static status is `READY` is not necessarily usable now. The consumer must
capture an actual `as_of_utc` and pass the phase gate, which requires
`not_before_utc <= as_of_utc <= expires_at_utc`. `as_of_utc` is a validation instant;
`phase_decision_cutoff_utc` is the scheduled decision reference and is not renamed or
replaced by a later validation time.

See [DECISION_SUPPORT_CONTRACT.md](DECISION_SUPPORT_CONTRACT.md) for the exact source
identity, phase-state, validation, live-snapshot, and failure contract.

`publish-decision-support-pointer.yml` validates the completed Actions artifact and
commits `consumer/latest-decision-support-artifact.json`. The full historical release
and its existing pointer remain unchanged for replay and audit consumers.
Routine consumers resolve this pointer once per attempted promotion, download its
numeric `artifact_id`, verify every pinned identity, and validate the requested phase
at the actual as-of time. They never hard-code an artifact ID or re-resolve the pointer
during the same validation attempt.

### Accepted accounting benchmark audit record

The first accounting benchmark proof used validation `as_of_utc`
`2026-08-13T02:45:00Z`; that value was not the phase cutoff. Its accounting pack
recorded `phase_decision_cutoff_utc` `2026-08-12T20:45:00Z` and an inclusive validity
window ending at `2026-08-13T11:45:00Z`, which has expired. The proof's decision
artifact `9165995215` is therefore an audit/replay identity only and must not be used
as a routine download target.

The discovery pointer subsequently advanced. The artifact selected by the pointer at
the time of this correction reported `broad_market_current=READY`, so the older 88.65%
broad-market result belongs only to audit artifact `9165995215`. Challenger processing
nevertheless remains blocked because current catalysts and primary evidence are stale,
while licensed news, point-in-time expectations, and candidate-funnel output are not
configured. Sunday has not been accepted; it requires a separate proof during the
actual Sunday window, using the then-current pointer and actual as-of time.

This acceptance enables only the public VTI/SPY/BIL benchmark leg. Complete portfolio
accounting remains consumer-owned and must validate positions, cash, lots,
transactions, confirmations, and arithmetic state.

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
workflow runs a pre-open backstop at 07:15 and a terminal-close build at 16:16
America/New_York on weekdays, starts the full official-source refresh at 02:00
America/New_York each Saturday, and can also be dispatched. GitHub cron is a start
trigger, not an action-time SLO; consumers must inspect source watermarks and phase
windows rather than assuming a scheduled trigger ran on time:

- `smoke` is fixed to AAPL, MSFT, BRK.B, NVDA, and TSM with 30 sessions and never
  publishes;
- `full` defaults to 320 sessions and may publish only with current cutoff, explicit
  `publish=true`, a `READY` manifest, and repository variable
  `PRODUCTION_RELEASES_ENABLED=true`;
- scheduled builds publish only when the same variable is true;
- weekday scheduled builds reuse the latest fully validated immutable enrichment
  snapshot and Yahoo Chart history baseline, then replace the prior 14 calendar days
  for every admitted symbol with a fresh Yahoo Chart delta. Fresh Chart observations
  deterministically win overlapping bulk-source rows. This keeps ETF/ADR history
  complete and lets the current-session gate evaluate the whole universe within the
  35-minute build budget plus a 10-minute publish budget. The pre-open run requires
  the prior completed session; the terminal-close run requires the same-day completed
  session, including the 13:00 close on XNYS early-close days;
- full builds attempt an independent, source-backed VTI/SPY/BIL benchmark lane. It is
  published only when all three securities reach the expected XNYS session with 100%
  coverage, adjusted-close returns reconcile to raw closes, splits, and cash events,
  and each supplies at least 140 sessions and 26 weekly observations. Failure
  quarantines the attempt, omits all three optional benchmark assets, and leaves
  accounting and benchmark-only gates blocked;
- Saturday scheduled builds set `refresh_enrichment=true` and perform the slower
  official SEC/FINRA rebuild with a six-hour ceiling; this weekly cadence captures SEC
  filing/fundamental changes and FINRA's semi-monthly updates without paying the
  roughly one-hour cost every day;
- a manual `refresh_enrichment=true` run remains available for bootstrapping, recovery,
  or an out-of-cycle source update.

The normal daily path therefore has at most 45 minutes of job execution. It downloads
the previous release by its immutable tag, verifies GitHub asset digests and the full
production contract, copies the enrichment assets without changing their source
clocks, and then verifies the newly assembled release again. If the fresh symbol
directory contains a new admission, `security-master.parquet` adds an explicit
`UNMAPPED_DAILY_ADMISSION` row; the next Saturday or manual enrichment refresh resolves
its SEC mapping. Historical master rows are retained so older point-in-time enrichment
rows remain referentially valid.

### Calendar-aware production dispatch

Run `dispatch-producer-phase.py` from an external scheduler. It derives deadlines from
the XNYS calendar (including holidays and early closes), emits stable idempotency keys,
and sends the `producer_phase_deadline` repository event. Plan accounting attempts with:

```bash
python dispatch-producer-phase.py plan --phase accounting --session 2026-08-12
```

At each planned time, invoke `dispatch` with exactly one `--attempt` and a token in
`GITHUB_TOKEN`. Correction attempts skip themselves when the latest immutable release
already contains a complete certification for that session. Workflow concurrency is
scoped to phase and expected session, preventing overlapping attempts. GitHub cron at
close +45, +75, and +120 is retained only as a backstop; its observed start time is
never treated as an action-time SLO.

Every full attempt publishes `market-coverage-diagnostic.json` in its seven-day
workflow artifact. The diagnostic freezes the eligible denominator before evaluation,
classifies every security as valid, missing, stale, invalid, or quarantined, and records
provider and targeted-retry outcomes without weakening the 95% broad-market gate.

After accounting or Sunday passes its exact cutoff gate and its pointer is published,
run `Publish phase readiness proof` with the decision and pointer workflow run IDs.
That workflow reruns holiday, early-close, stale-source, reconciliation, quarantine,
and rollback tests; measures discovery, download, decompression, and validation
separately; and publishes `readiness-proof.json` as a 90-day artifact.
After acceptance, the proof workflow requests `Publish durable readiness proof` for
that exact proof run. The durable workflow revalidates the proof and original decision
artifact, attaches the unchanged proof plus its SHA-256 sidecar to a durable
SHA-addressed Git path in one atomic content-addressed commit, then verifies those
exact commit bytes and confirms canonical release discovery is unchanged. The Actions
proof artifact is temporary and is not the permanent audit record.

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
