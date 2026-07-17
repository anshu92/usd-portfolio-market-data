# Consumer contract

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

Import each Parquet file into a same-named normalized SQLite table and record its
manifest `source_revision`, `source_retrieved_at_utc`, minimum/maximum event dates,
row count, digest, and coverage status. Do not assign one package-wide freshness value:
market history, fundamentals, filings/events, insiders, 13F ownership, and FINRA short
interest each have independent clocks.

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
