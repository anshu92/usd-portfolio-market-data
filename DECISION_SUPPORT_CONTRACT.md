# Decision-support producer contract

This contract is additive to `PRODUCER_RELIABILITY_CONTRACT.md`. The canonical market
data release remains the historical/replay source. The decision-support artifact is a
bounded, derived read model for routine V5 phases; it is not a quote feed and never
decides whether a portfolio should trade.

## Immutable source identity

Every decision-support build must name one published immutable `market-data-*` tag.
Every downloaded source asset is checked against both the GitHub Release digest and
the pinned source manifest. The output manifest records the source tag, source
manifest SHA-256, and every source file identity.

Publishing a compact artifact never modifies the canonical release. A failed build or
pointer update leaves the previous validated compact artifact discoverable.

## Artifact

A valid artifact contains exactly:

- `decision-support.sqlite.zst`;
- `decision-support-manifest.json`; and
- `candidate-funnel.parquet`, `actionability-matrix.json`, and
  `evidence-packets.jsonl.zst`; and
- one canonical JSON pack for each phase under `phase-packs/`.

The compressed database must not exceed 50 MiB. It contains a maximum of 64 recent
market sessions per security, the latest market snapshot and price-only returns,
current security identity, latest public factors/signals, and a bounded event window.
Raw company facts, detailed 13F positions, full history, and other cold replay data are
excluded.

The market tables are a `NON_EXECUTABLE_RESEARCH_PROXY`. Their raw close is explicitly
`RAW_CLOSE_NOT_DIVIDEND_ADJUSTED` and cannot satisfy a live execution requirement or a
certified total-return requirement.

Consumers must validate the compressed and uncompressed SHA-256 identities, SQLite
integrity, exact table set, row counts, foreign keys, recent-history bound, capability
snapshot, and phase packs before opening the database with `mode=ro&immutable=1`.
The manifest also binds the validator contract version, exact producer commit, every
validator-file digest, and a digest of that validator set.

## Capability and phase states

Capabilities use these operational states:

- `READY`: present and within its capability-specific freshness bound;
- `STALE` or `UNKNOWN_FRESHNESS`: present but unsafe for the required cutoff;
- `NOT_CONFIGURED`: no approved source lane exists; and
- `CONSUMER_REQUIRED`: must be fetched in the consumer environment at the cutoff.

A phase is `BLOCKED` when any producer-required capability is not `READY`, `DEGRADED`
when only optional capabilities are unavailable, and `READY` otherwise. External
consumer requirements do not make a producer pack structurally invalid, but the
consumer cannot complete that phase until it validates the named external snapshot.

The status is supplemented by ordered operating modes: `ARTIFACT_VALID`,
`BENCHMARK_ONLY_SAFE`, `CHALLENGER_RESEARCH_READY`, `LIVE_SNAPSHOT_REQUIRED`, and
`CHALLENGER_BLOCKED`. Missing challenger inputs do not suppress a separately certified
benchmark lane. Candidate-level actionability is empty and explicitly rejected until
an approved selector is configured.

Benchmark decisions use five separate capabilities: `benchmark_identity`,
`benchmark_market_current`, `certified_total_returns`, `funded_benchmark_inputs`, and
`broad_market_current`. Accounting requires the first four at 100% VTI/SPY/BIL
coverage but does not require the broad universe. Sunday uses the same four as required
inputs and treats broad-market and challenger lanes as optional, so it reports
`BENCHMARK_ONLY_SAFE` while remaining `DEGRADED` until those inputs exist. Screening
and candidate generation continue to require `broad_market_current`; broad-market
failure can never be promoted into full readiness.

Task delivery targets are expressed in `America/Toronto`; exchange sessions and closes
are calculated in `America/New_York`: pre-open by 08:10, exception
monitoring by 09:55 and 15:25, terminal review at XNYS close +20 minutes, accounting at
XNYS close +45 minutes, and Saturday replay by 08:30. GitHub's scheduled trigger time
is not evidence that a target was met. Consumers must enforce `built_at_utc`,
`data_cutoff_utc`, `valid_for_session`, `phase_decision_cutoff_utc`, `not_before_utc`,
`expires_at_utc`, and every source watermark.

## Live execution snapshot

The producer publishes schema `1.0.0`, contract `1.1.0` for provider-neutral live
snapshots and
validator. Provider and broker adapters run in the consumer environment with
consumer-owned credentials. A snapshot binds quotes, halt/LULD state, and broker
eligibility to one actual cutoff.

The strict allowlist binds canonical symbol, exchange, currency, provider/feed,
entitlement, request/response IDs, quote type, spread policy, and independent quote,
market-status, and broker ages per security. It rejects delayed/proprietary feeds,
identity mismatches, stale/crossed/non-positive/future quotes, unknown halt/LULD state,
invalid bands, broker ineligibility, response-time inconsistencies, duplicate
securities, and every undeclared field. A blocked security may not fall back to
producer OHLCV.

## Publication and promotion

The compact workflow is dispatched after each certified source release and has
timezone-aware cadence backstops for weekday 08:05, Sunday 17:15, Friday 15:05, and
Saturday 07:30 in `America/Toronto`. Scheduled triggers are not action-time SLOs.

Artifacts are named `decision-support-${SOURCE_TAG}-${RUN_ID}` and downloaded by
artifact ID. The publisher checks out and runs validators from the exact source-run
commit. Its pointer binds repository, workflow ID/path, branch, event, commit,
validator identity, artifact identity, and the collision-safe promotion key
`${SOURCE_TAG}/${ARTIFACT_ID}`. Pointer publication is monotonic by source tag and then
artifact ID; an older or identical completed run cannot replace the current pointer.

## Operational-readiness reporting

A successful producer/publisher run must be described as **“artifact build, integrity
validation, and pointer publication succeeded.”** It must not be described as a
successful phase, decision, or production-ready V5 workflow unless at least one phase
pack is independently usable at its actual cutoff.

Every reported byte count must name both the object and representation, for example
“compressed `decision-support.sqlite.zst` bytes” or “decompressed SQLite bytes.” Every
latency result must state whether artifact discovery, network download, decompression,
integrity/schema validation, phase evaluation, database open, and consumer query time
are included. A post-download verifier measurement includes Zstandard decompression
when `verify-decision-support.py` performs it, but excludes discovery and download.
The `--phase`/`--as-of` gate also recomputes required-capability age from `observed_at`
and `maximum_age_seconds`; a statically `READY` pack becomes unusable when an input is
future-dated at the decision instant or has expired.

Contract v1.1 is structurally production-grade but operationally integration-only
while every phase is `BLOCKED`. The first operational acceptance milestone is a
scheduled phase that passes its actual `--phase`/`--as-of` gate using current,
source-backed inputs.

Data-availability work proceeds in this order:

1. automated publication with market data current through the expected XNYS session;
2. certified distribution-adjusted benchmark returns with at least 140 sessions and
   26 weekly observations;
3. rapid SEC filings, current catalysts, and licensed news;
4. point-in-time expectations and revision history;
5. populated deterministic candidate, actionability, primary-document, and evidence
   packet lanes; and
6. effective-dated universe membership, delistings, and survivorship-aware replay.

For every phase that changes from unavailable to usable, the producer publishes a
proof bundle containing the producer run ID, artifact ID and promotion key, source
watermarks, old and new phase states, the exact `--phase`/`--as-of` result, coverage
counts, end-to-end consumer latency with scope, and rollback plus failure-injection
results. Holiday and XNYS early-close cases are mandatory before production-readiness
is declared.

## Private-state boundary

Portfolio holdings, positions, cash, tax lots, account identifiers, orders,
confirmations, and arbitration are forbidden in public producer schemas. Funded
benchmark results remain consumer-owned because they require private cash flows. The
producer may eventually supply only certified public benchmark total-return and
distribution inputs.

## Failure behavior

- Source identity, compression, schema, SQLite integrity, row-count, or phase-pack
  inconsistency stops publication.
- Missing provider-backed lanes remain explicit and block dependent phases.
- The consumer never downloads the full release as an automatic routine fallback.
- Replay and audit continue to use the separately validated canonical release.
