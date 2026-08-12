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

Delivery targets are expressed in `America/New_York`: pre-open by 08:10, exception
monitoring by 09:55 and 15:25, terminal review at XNYS close +20 minutes, accounting at
XNYS close +45 minutes, and Saturday replay by 08:30. GitHub's scheduled trigger time
is not evidence that a target was met; consumers use the pack's actual generation,
cutoff, source observation, and freshness values.

## Live execution snapshot

The producer publishes only the provider-neutral `1.0.0` live-snapshot contract and
validator. Provider and broker adapters run in the consumer environment with
consumer-owned credentials. A snapshot binds quotes, halt/LULD state, and broker
eligibility to one actual cutoff.

The validator fails stale, crossed, non-positive, or future quotes; unknown or stale
halt/LULD state; invalid bands; broker ineligibility; response-time inconsistencies;
duplicate securities; and private account or portfolio keys. A blocked snapshot may
not fall back to producer OHLCV.

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
