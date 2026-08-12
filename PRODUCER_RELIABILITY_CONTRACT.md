# USD Portfolio Market-Data Producer Reliability Contract

Proposed repository destination:
`PRODUCER_RELIABILITY_CONTRACT.md` in the root of
`anshu92/usd-portfolio-market-data`, linked from `README.md` and
`consumer/README.md`.

This is a producer-to-consumer data contract. It improves availability without
weakening identity, point-in-time, or release-integrity guarantees. The producer
supplies research data; it never decides whether a portfolio should trade.

## 1. Release objective

Every published production release must be an immutable, internally coherent
composite containing one active version of every dataset group. A bad refresh of
one group must not freeze unrelated groups.

The producer must:

1. Build each candidate group in a release-tag-scoped staging directory.
2. Validate each group and its cross-group dependencies before promotion.
3. Promote newly valid group bytes.
4. If a candidate group fails, never place those bytes in the production
   release. Reuse the exact bytes of the most recent validated immutable group,
   pinned by source release tag, source manifest SHA-256, and group SHA-256.
5. Record the failed candidate and its exact diagnostics in the new manifest.
6. Assemble and validate the complete composite before publishing it.
7. Publish nothing if the core identity or market group has neither a valid
   candidate nor an allowed validated fallback.

Reusing a pinned group is explicit composition, not implicit release mixing.
Files inside a group must always come from one group identity. No file may be
copied independently from a different release.

## 2. Dataset groups and dependencies

| Group | Files | Dependencies |
|---|---|---|
| `identity` | `security-universe.csv`, `security-master.parquet`, `unmatched-tickers.csv` | none |
| `market` | `yahoo-ohlcv-320.parquet`, `yahoo-splits.parquet` | `identity` |
| `fundamentals` | `sec-company-facts.parquet`, `normalized-fundamentals-quarterly.parquet`, `fundamental-factors.parquet` | `identity`, filing availability metadata |
| `filings_events` | `sec-filings.parquet`, `corporate-events.parquet`, `earnings-and-guidance-events.parquet` | `identity` |
| `insiders` | `insider-transactions.parquet`, `insider-signals.parquet` | `identity`, `filings_events` |
| `institutional` | `institutional-holdings-13f.parquet`, `institutional-ownership-signals.parquet` | `identity` |
| `short_interest` | `finra-short-interest.parquet` | `identity` |
| `analyst_estimates` | `analyst-estimates.parquet`, when licensed | `identity`; optional |
| `total_returns` | `distributions.parquet`, `benchmark-total-returns.parquet` | `identity`, `market`; optional and all-or-none |

`NOTICE.md` is release-level metadata.

The `total_returns` group is `READY_NEW` only when its VTI source response reaches the
market group's expected completed XNYS session, raw closes reconcile to the canonical
market bytes, cash distributions reconcile to adjusted-close returns, and coverage is
at least 140 sessions and 26 weekly observations. Otherwise both optional files are
omitted and benchmark/accounting capabilities remain blocked.

Every referenced `security_id` must exist in the active `security-master`
contained by the same composite. Historical master rows may be retained to
preserve point-in-time foreign keys.

## 3. Group states

Active groups may have only these states:

- `READY_NEW`: freshly built and fully validated.
- `READY_REUSED`: byte-identical reuse of a validated immutable group within
  its freshness allowance.
- `READY_WITH_EXCLUSIONS`: valid after bounded, explicitly enumerated
  security-level exclusions; thresholds below must pass.
- `STALE_DISABLED`: structurally valid historical bytes retained for audit, but
  consumers must disable factors that require the group.
- `NOT_CONFIGURED`: permitted only for declared optional groups such as licensed
  analyst estimates.

`QUARANTINED` describes an attempted candidate that failed validation and can
appear only as `attempt_state` under `candidate_attempt_failures`. Each attempt
record names the separately validated `released_state`. `candidate_group_failures`
must be empty in production manifests; quarantined candidate bytes belong only in
a short-lived diagnostic artifact, never the production artifact.

Every dataset selected from a prior immutable release records the source release
tag, immutable-release assertion, source-manifest SHA-256, and source-group
SHA-256. The final group digest always describes the bytes actually released.

The top-level manifest may remain `status: READY` for backward compatibility
when `identity` and `market` are usable and all active bytes pass structural
validation. Consumers must use group states to enable or disable individual
factors and archetypes. A missing, stale, or quarantined non-core group must not
disable unrelated groups.

## 4. Canonical identity and CIK rules

CIK values must be normalized to exactly ten decimal digits before comparison.
Ticker is a routing hint, not an issuer identity.

The producer must maintain:

- `security_id -> canonical_cik | null`;
- `canonical_cik -> set[security_id]`;
- `(canonical_cik, exchange_mic, ticker) -> security_id` for class-level
  resolution.

For every insider transaction:

1. Resolve `ISSUERCIK` first.
2. If that CIK maps to one security, use it.
3. If it maps to multiple share classes, use `ISSUERTRADINGSYMBOL` only to
   select among security IDs already carrying the same CIK.
4. A ticker fallback is allowed only when the selected security's non-null
   canonical CIK equals `ISSUERCIK`.
5. Never emit a transaction against a security with a null or conflicting
   canonical CIK.
6. Quarantine unresolved rows by reason:
   `UNKNOWN_ISSUER_CIK`, `AMBIGUOUS_SHARE_CLASS`,
   `TICKER_CIK_CONFLICT`, or `MISSING_CANONICAL_CIK`.
7. Derive `insider-signals.parquet` only from accepted transactions.

The current expression in `parse_insider_archives`,
`cik_to_security.get(issuer_cik) or resolve_ticker(...)`, violates this
contract because the ticker fallback can override missing canonical CIK
evidence. Replace it with a CIK-constrained resolver.

`verify-release.py` must fail the `insiders` candidate unless this invariant has
zero violations:

```text
insider_transactions.security_id exists in security_master
AND security_master.cik is non-null
AND normalize(insider_transactions.issuer_cik)
    == normalize(security_master.cik)
```

The manifest must report accepted and rejected insider-row counts by reason,
plus a bounded sample of accession numbers for diagnostics. The current failed
release baseline is 538 CIK conflicts and 13 rows mapped to a master entry
without a canonical CIK; the replacement release must report zero emitted
violations.

Ticker collisions must not be resolved solely by "latest filer" when two CIKs
compete for one listing. Use reviewed effective-dated overrides or mark the
security unresolved. A recommended reviewed registry is
`config/security-cik-overrides.csv` with:
`security_id,cik,effective_from,effective_to,source_url,reviewed_at_utc`.

## 5. OHLC session integrity

Each OHLC row must have a unique `(security_id, session_date)`, finite positive
open/high/low/close, non-negative volume, and:

```text
high >= max(open, close, low)
low  <= min(open, close, high)
```

An apparent discontinuity must be checked against split events before being
classified as recycled-symbol contamination.

Invalid-row handling is repair-first and security-local:

1. Identify invalid security/session rows.
2. Re-fetch those exact symbols and dates using the fresh Yahoo Chart path.
3. Accept repaired rows only after the same validation, recording distinct
   source revision and observation time.
4. Exclude unresolved rows individually and enumerate them in manifest quality
   diagnostics.
5. Do not discard an entire session merely because a fixed invalid-row
   percentage was exceeded.

After repair, measure latest-session coverage against active matched
`security_id`s:

- `READY_NEW`: at least 99% valid coverage, and configured benchmark `VTI` is
  valid.
- `READY_WITH_EXCLUSIONS`: at least 95% valid coverage, all unresolved rows are
  enumerated, and `VTI` is valid.
- Candidate session `QUARANTINED`: below 95% coverage or affirmative evidence
  of common-mode corruption affecting the trustworthiness of otherwise
  apparently valid rows.

If a latest session is quarantined, reuse the last validated `market` group
within the freshness allowance instead of blocking valid enrichment updates.
The July 24 behavior—discarding 10,000 valid rows because 208 of 10,208 rows
failed bounds—is not permitted by this contract.

## 6. Freshness service levels

Use the XNYS calendar. `expected_latest_completed_session` means the latest
regular session completed before the build cutoff.

| Group | Ready requirement | Reuse/degradation limit |
|---|---|---|
| `market` | maximum session equals `expected_latest_completed_session`; production pointer published by 22:00 America/New_York on trading days | one eligible session behind may be `READY_REUSED` with penalty; two or more behind is `STALE_DISABLED` |
| `identity` | symbol-directory retrieval no older than 24 hours | reuse up to 3 calendar days with explicit age; then disabled |
| SEC fundamentals, filings/events, insiders | source retrieval no older than 8 calendar days | days 9–14 may be reused with a stale confidence penalty; over 14 is disabled |
| `institutional` | retrieval within 8 days and latest official archive ingested | evaluate expected filing/publication schedule; never infer freshness from report period |
| `short_interest` | retrieval within 8 days and latest officially published FINRA settlement present | evaluate official publication schedule, not settlement date alone |
| consumer export pointer | generated within 60 minutes of successful export; artifact unexpired with at least 7 days remaining | otherwise pointer is invalid |

Every group manifest entry must state expected timestamp/date, observed
timestamp/date, lag in hours/calendar days/eligible sessions as applicable, and
the derived freshness state. Package creation time must never substitute for a
source clock.

## 7. Manifest contract

Keep existing `schema_version: 1.0.0`, `release_files`, and `datasets` fields
until producer and consumer are upgraded together. Add `dataset_groups`
additively.

Each dataset record must include:

- path, group ID, dataset schema version;
- SHA-256, byte size, row count;
- ordered logical columns and physical types;
- primary key and nullability policy;
- source name, immutable source revision and source retrieval time;
- minimum/maximum event or session date;
- point-in-time-safe flag;
- validation result.

Each group record must include:

- group ID, group contract version, state and mode;
- sorted file list;
- deterministic `group_sha256`;
- source release tag, source manifest SHA-256 and source group SHA-256 when
  reused;
- freshness object;
- validation errors, warnings and exclusions;
- dependency group identities.

Compute `group_sha256` from canonical JSON of the sorted file identity records
`{path, sha256, bytes, rows, physical_schema, primary_key}`. A reused group's
digest and bytes must exactly equal its source group.

The manifest must be written only after all final file bytes exist. The
published tag and release assets are immutable. Never repair an existing tag;
publish a new tag.

## 8. Exact consumer artifact identity

`consumer/latest-production-artifact.json` must retain these fields and
semantics exactly:

- `release_tag`
- `workflow_run_id`
- `artifact_id`
- `artifact_name`
- `artifact_sha256`
- `artifact_size_bytes`
- `producer_commit`
- `manifest_sha256`

Before committing the pointer, cross-check:

- export workflow run completed successfully;
- artifact API ID, name, digest, byte count and expiry;
- artifact `workflow_run.id == workflow_run_id`;
- artifact `workflow_run.head_sha == producer_commit`;
- artifact is not expired;
- artifact name is exactly `validated-market-data-{release_tag}`;
- `resolved-tag.txt`, GitHub release metadata and pointer use the same tag;
- downloaded `manifest.json` hash equals `manifest_sha256`;
- all group/file identities revalidate from downloaded bytes.

The pointer is discovery metadata only and must never be used to waive archive
or manifest validation.

## 9. Workflow and validator changes

Implement this contract in:

- `build-enrichment-data.py`: CIK-constrained issuer resolver and per-group
  candidate outputs.
- `build-yahoo-aggregate-v2.py`: repair-first, security-level OHLC exclusions
  and latest-session coverage.
- `verify-release.py`: group validation, physical schema, canonical CIK join and
  dependency checks.
- `reuse-enrichment-snapshot.py`: generalize to exact immutable group reuse, or
  replace with `compose-release.py`.
- `.github/workflows/build-market-data.yml`: stage, validate, choose
  new-versus-reused group, compose, revalidate, then publish.
- `.github/workflows/export-release-for-consumer.yml`: retain exact pointer
  fields and validate group identities before upload.
- `consumer/README.md`: define group-level enable/disable behavior.

The publication workflow must always run the same validator version from the
producer commit that created the release.

## 10. Required positive and negative tests

Add tests proving:

1. Single-class CIK mapping succeeds.
2. Multi-class CIK mapping selects only a ticker within the same CIK set.
3. Conflicting ticker/CIK rows are quarantined, never emitted.
4. Null-canonical-CIK insider rows are quarantined.
5. Producer validation catches the historical 538/13 failure pattern.
6. A failed insiders refresh reuses only the previous validated `insiders`
   group while a fresh market group is promoted.
7. Reused group bytes and group digest match the pinned source release.
8. A 2% invalid OHLC subset is retried and then excluded per security without
   discarding the remaining valid session.
9. A session below 95% valid coverage or with common-mode corruption is
   quarantined.
10. XNYS holidays and weekends produce the correct expected-session freshness.
11. Any pointer/run/artifact/tag/commit/hash/byte-count mismatch fails.
12. A non-core stale or unavailable group disables only its dependent
    factors/archetypes; unrelated ready groups remain usable.

## 11. Recovery procedure

Do not mutate or relabel the July 25 immutable release.

1. Implement the CIK resolver and producer-side canonical join.
2. Implement per-group composition and OHLC repair/exclusion.
3. Run offline tests and a full non-publishing build.
4. Require zero emitted insider CIK conflicts and zero emitted rows against a
   null/missing canonical CIK.
5. Run a manual full enrichment refresh, validate every group, and publish a
   new immutable tag.
6. Export it and update the same consumer pointer only after exact artifact
   reconciliation.
7. Have the consumer download, clean-stage, validate, and atomically promote
   the new composite.
