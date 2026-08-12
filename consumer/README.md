# Consumer contract

This operational contract implements the repository's
[producer reliability contract](../PRODUCER_RELIABILITY_CONTRACT.md).

Consume only a validated, immutable tag. Resolve
`latest-production-artifact.json`, confirm its run/artifact/release identity through the
GitHub Actions API, download into a tag-scoped staging directory, and run both
validators before changing any production database:

```bash
python verify-github-release.py --metadata github-release.json --dist staging/release
python verify-release.py --dist staging/release --require-ready --require-production
```

Compare every release file with both its `release_files` record and its `datasets`
entry. Validate SHA-256, bytes, rows, ordered columns, physical types, primary keys,
and `security_id` membership. Promote the staged directory atomically only after all
checks pass. On any failure, leave the previous validated tag and database untouched.

Use `dataset_groups`, not the package-wide `status`, to enable data-dependent behavior:

- `READY_NEW`, `READY_REUSED`, and `READY_WITH_EXCLUSIONS` are usable, subject to the
  group's freshness and exclusions.
- `STALE_DISABLED` remains importable for audit, but every factor or archetype that
  depends on that group must be disabled.
- `NOT_CONFIGURED` is valid only for declared optional groups.
- `candidate_attempt_failures` is diagnostic evidence about rejected build attempts;
  its `released_state` must match the final group. `candidate_group_failures` must be
  empty. Never import quarantined candidate bytes.

Resolve dependencies transitively. A disabled `filings_events` group disables
`insiders`, while ready `market`, `fundamentals`, `institutional`, and
`short_interest` groups remain independently usable. Persist the group ID, state,
digest, source release provenance, freshness lag, and disable reason with each run.

Import each Parquet file into a same-named normalized SQLite table and record its
manifest `source_revision`, `source_retrieved_at_utc`, minimum/maximum event dates,
row count, digest, and coverage status. Do not assign one package-wide freshness value:
market history, fundamentals, filings/events, insiders, 13F ownership, and FINRA short
interest each have independent clocks.

When `enrichment_snapshot.mode` is `REUSED_VALIDATED_IMMUTABLE_RELEASE`, retain the
source release tag and manifest digest in import metadata. The current package time
applies to the market/universe build only; enrichment freshness still comes from each
dataset and row. `security-master.parquet` may include historical securities that are
not in the current admitted universe so older point-in-time rows preserve valid foreign
keys. A current admission with `mapping_status=UNMAPPED_DAILY_ADMISSION` has no SEC
mapping yet and must not be ticker-guessed by the consumer.

Apply availability using the five `source_*` columns on every enrichment row:

- SEC filings and insider transactions: acceptance time when present; otherwise the
  producer's conservative publication date.
- FINRA short interest: publication date, never settlement date.
- 13F holdings: filing/publication date, never report period.
- Derived signals and factors: their source publication date must be on or before the
  portfolio cutoff; `filing_available_date <= factor_as_of_date` is mandatory.

Never replace nulls with zero. Never silently redistribute a missing component's
weight. Persist the disabled factor, required dataset, and reason with each run.

Archetype readiness is evaluated by subset:

- Deep Value: normalized fundamentals, fundamental factors, and SEC filings.
- Quality Compounder: normalized fundamentals, factors, and insider signals.
- GARP: factors, earnings/guidance events, and market history. With analyst estimates
  `NOT_CONFIGURED`, use `GARP_FUNDAMENTAL_AND_PRICE_MODE`.
- Contrarian Value: FINRA short interest, insider signals, institutional signals, and
  factors, with their publication lags.
- Special Situations and Activism: corporate events, SEC filings, and insider
  transactions.
- Expectations, Revisions and Catalyst: earnings/guidance events, corporate events,
  and market history. With analyst estimates `NOT_CONFIGURED`, use
  `EXPECTATIONS_EVENT_AND_PRICE_MODE`.

Missing analyst estimates may lower conviction only for estimate-specific components.
They must not erase confirmed guidance, earnings or filing events, price drift, growth,
valuation, or other ready inputs.

## Routine decision-support import

Routine V5 work should resolve `latest-decision-support-artifact.json`, verify its
Actions run/artifact and immutable source-release identity with
`verify-decision-support-pointer.py`, and then run:

```bash
python verify-decision-support.py --dist staging/decision-support
zstd -d staging/decision-support/decision-support.sqlite.zst \
  -o staging/decision-support/decision-support.sqlite
```

Download by the pointer's numeric `artifact_id`, not by artifact name. Stage the
validated bytes under the collision-safe promotion key
`${source_release_tag}/${artifact_id}`. Never overwrite a prior build from the same
source tag.

Before a phase is used, enforce its active UTC window and status programmatically:

```bash
python verify-decision-support.py \
  --dist staging/decision-support \
  --phase pre_open \
  --as-of "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
```

This command checks all bytes first, then requires `as-of` to be in one of the pack's
`[not_before_utc, expires_at_utc)` windows. It fails for `BLOCKED`; `DEGRADED` also
fails unless the caller explicitly supplies `--allow-degraded`. Independently compare
the task session to `valid_for_session` and treat `data_cutoff_utc` and each source
watermark as the maximum facts known—not as build time.

Open the database read-only and immutable:

```text
file:/absolute/path/decision-support.sqlite?mode=ro&immutable=1
```

Read the phase pack matching the current V5 phase before querying evidence. `BLOCKED`
means at least one producer-required capability is unavailable or too stale;
`DEGRADED` means only optional capabilities are unavailable. A listed
`CONSUMER_REQUIRED` capability, such as `execution_snapshot`, must be fetched at the
actual cutoff and is never satisfied by the historical OHLCV tables.

Use `operating_modes` rather than one aggregate boolean: `BENCHMARK_ONLY_SAFE` permits
only the certified benchmark path, `CHALLENGER_RESEARCH_READY` permits challenger
research, `LIVE_SNAPSHOT_REQUIRED` still gates execution, and `CHALLENGER_BLOCKED`
requires candidate-specific rejection. Empty `candidate-funnel.parquet` and empty
security actionability are intentional while the selector capability is
`NOT_CONFIGURED`.

Quote, halt/LULD, and broker integrations must emit the provider-neutral live-snapshot
schema and pass `python verify-live-snapshot.py --snapshot live-snapshot.json`. The
validator rejects stale or crossed quotes, future timestamps, unknown halt/LULD state,
broker ineligibility, invalid bands, and private account or portfolio fields. A blocked
live snapshot must never fall back to producer OHLCV for execution.

The live contract is a strict allowlist. Adapters must supply canonical symbol,
exchange MIC, currency, SIP/direct NBBO feed identity, entitlement scope,
request/response IDs, broker observation time, and spread thresholds. Undeclared
fields—including account, cash, lot, order, fill, or position variants—fail validation.

After validation, promote with an atomic same-filesystem rename. For example, create a
temporary symlink to the immutable promotion directory and use `os.replace`:

```python
import os
from pathlib import Path

cache = Path("/var/lib/v5/decision-support")
target = cache / source_release_tag / str(artifact_id)
temporary = cache / f".current.{os.getpid()}"
temporary.symlink_to(target, target_is_directory=True)
os.replace(temporary, cache / "current")
```

Do not use a multi-step unlink/relink sequence; readers must see either the previous
validated artifact or the new one.

The compact stream is not a replacement for the canonical release. Saturday replay,
audit, raw SEC facts, detailed 13F holdings, and history beyond the recent hot window
must continue to use the pinned full release.
