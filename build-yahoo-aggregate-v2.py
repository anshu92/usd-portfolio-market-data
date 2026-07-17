#!/usr/bin/env python3
"""Build and validate a compact Yahoo-derived OHLCV research aggregate."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import shutil
import sys
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable

import duckdb
import exchange_calendars as xcals
from huggingface_hub import HfApi, hf_hub_download


DEFAULT_REPO = "defeatbeta/yahoo-finance-data"
PRICES_PATH = "data/stock_prices.parquet"
SPLITS_PATH = "data/stock_split_events.parquet"
SCHEMA_VERSION = "1.0.0"
OHLCV_SCHEMA = (
    "security_id",
    "ticker",
    "source_symbol",
    "session_date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "source_dataset",
    "source_revision",
    "observed_at_utc",
)
SPLIT_SCHEMA = (
    "security_id",
    "ticker",
    "source_symbol",
    "event_date",
    "split_factor",
    "source_dataset",
    "source_revision",
    "observed_at_utc",
)
PRICE_INVALID_SQL = """
    open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL OR volume IS NULL
    OR NOT isfinite(open) OR NOT isfinite(high) OR NOT isfinite(low) OR NOT isfinite(close)
    OR open <= 0 OR high <= 0 OR low <= 0 OR close <= 0 OR volume < 0
    OR low > high OR low > least(open, close) OR high < greatest(open, close)
"""


class AggregateError(RuntimeError):
    """Raised when an input cannot produce a trustworthy aggregate."""


@dataclass(frozen=True)
class Security:
    security_id: str
    ticker: str


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def format_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise AggregateError(f"Timestamp must include a timezone: {value}")
    return parsed.astimezone(timezone.utc)


def parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise AggregateError(f"Invalid YYYY-MM-DD date: {value}") from exc


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_json(path: Path, value: object) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def atomic_copy_to_parquet(
    con: duckdb.DuckDBPyConnection,
    query: str,
    params: list[object],
    target: Path,
) -> None:
    tmp = target.with_suffix(target.suffix + ".tmp")
    if tmp.exists():
        tmp.unlink()
    # DuckDB binds COPY's destination placeholder before placeholders in the
    # source query. Quote the trusted, locally constructed path so the caller's
    # query parameters retain their natural order.
    quoted_tmp = str(tmp).replace("'", "''")
    con.execute(
        f"COPY ({query}) TO '{quoted_tmp}' (FORMAT PARQUET, COMPRESSION ZSTD)",
        params,
    )
    os.replace(tmp, target)


def parquet_columns(con: duckdb.DuckDBPyConnection, path: Path) -> tuple[str, ...]:
    cursor = con.execute("SELECT * FROM read_parquet(?) LIMIT 0", [str(path)])
    return tuple(str(column[0]) for column in cursor.description)


def load_universe(path: Path) -> list[Security]:
    required = {"security_id", "ticker", "universe_admission_status"}
    securities: list[Security] = []
    seen_ids: set[str] = set()
    seen_tickers: set[str] = set()
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        missing = required - set(reader.fieldnames or ())
        if missing:
            raise AggregateError(f"Universe lacks required columns: {sorted(missing)}")
        for line_number, row in enumerate(reader, start=2):
            if str(row.get("universe_admission_status") or "").strip().upper() != "ADMITTED":
                continue
            security_id = str(row.get("security_id") or "").strip()
            ticker = str(row.get("ticker") or "").strip().upper()
            if not security_id or not ticker:
                raise AggregateError(f"Blank admitted identifier at {path}:{line_number}")
            if security_id in seen_ids:
                raise AggregateError(f"Duplicate admitted security_id: {security_id}")
            if ticker in seen_tickers:
                raise AggregateError(f"Duplicate admitted ticker: {ticker}")
            seen_ids.add(security_id)
            seen_tickers.add(ticker)
            securities.append(Security(security_id, ticker))
    if not securities:
        raise AggregateError(f"No admitted securities in {path}")
    return securities


def load_universe_metadata(path: Path, universe_path: Path) -> dict[str, object]:
    try:
        metadata = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AggregateError(f"Invalid universe metadata {path}: {exc}") from exc
    if metadata.get("schema_version") != SCHEMA_VERSION:
        raise AggregateError("Unsupported universe metadata schema_version")
    if metadata.get("status") != "READY":
        raise AggregateError("Universe metadata status is not READY")
    actual_sha = sha256_file(universe_path)
    if metadata.get("sha256") != actual_sha:
        raise AggregateError("Universe CSV hash does not match its metadata")
    return metadata


def yahoo_aliases(ticker: str) -> list[str]:
    candidates = [ticker, ticker.replace(".", "-"), ticker.replace("/", "-")]
    output: list[str] = []
    for candidate in candidates:
        candidate = candidate.strip().upper()
        if candidate and candidate not in output:
            output.append(candidate)
    return output


def build_alias_table(
    con: duckdb.DuckDBPyConnection, securities: Iterable[Security]
) -> None:
    con.execute(
        "CREATE TEMP TABLE aliases(security_id VARCHAR, ticker VARCHAR, "
        "source_symbol VARCHAR, priority INTEGER)"
    )
    records: list[tuple[str, str, str, int]] = []
    owners: dict[str, str] = {}
    for security in securities:
        for priority, alias in enumerate(yahoo_aliases(security.ticker)):
            previous = owners.get(alias)
            if previous is not None and previous != security.security_id:
                raise AggregateError(
                    f"Yahoo alias {alias!r} is ambiguous between {previous} and "
                    f"{security.security_id}"
                )
            owners[alias] = security.security_id
            records.append((security.security_id, security.ticker, alias, priority))
    con.executemany("INSERT INTO aliases VALUES (?, ?, ?, ?)", records)


def resolve_source_file(
    *,
    repo_id: str,
    revision: str,
    filename: str,
    local_override: str | None,
    cache_dir: Path,
) -> tuple[Path, str]:
    if local_override:
        path = Path(local_override).resolve()
        if not path.is_file():
            raise AggregateError(f"Source file does not exist: {path}")
        return path, "LOCAL_OVERRIDE"
    info = HfApi().dataset_info(repo_id=repo_id, revision=revision)
    resolved_revision = str(info.sha or revision)
    path = Path(
        hf_hub_download(
            repo_id=repo_id,
            repo_type="dataset",
            filename=filename,
            revision=resolved_revision,
            cache_dir=str(cache_dir),
        )
    )
    return path, resolved_revision


def validate_source_schema(
    con: duckdb.DuckDBPyConnection,
    path: Path,
    required: set[str],
    label: str,
) -> None:
    columns = set(parquet_columns(con, path))
    missing = required - columns
    if missing:
        raise AggregateError(f"{label} source lacks columns: {sorted(missing)}")


def prepare_price_source(
    con: duckdb.DuckDBPyConnection, path: Path, cutoff: date
) -> int:
    validate_source_schema(
        con,
        path,
        {"symbol", "report_date", "open", "high", "low", "close", "volume"},
        "Price",
    )
    con.execute(
        """
        CREATE TEMP TABLE price_source AS
        SELECT upper(trim(p.symbol)) AS source_symbol,
               try_cast(p.report_date AS DATE) AS session_date,
               try_cast(p.open AS DOUBLE) AS open,
               try_cast(p.high AS DOUBLE) AS high,
               try_cast(p.low AS DOUBLE) AS low,
               try_cast(p.close AS DOUBLE) AS close,
               try_cast(p.volume AS BIGINT) AS volume
        FROM read_parquet(?) p
        JOIN (SELECT DISTINCT source_symbol FROM aliases) a
          ON upper(trim(p.symbol)) = a.source_symbol
        WHERE try_cast(p.report_date AS DATE) <= cast(? AS DATE)
        """,
        [str(path), cutoff.isoformat()],
    )
    conflicts = int(
        con.execute(
            """
            SELECT count(*) FROM (
              SELECT source_symbol, session_date
              FROM price_source
              GROUP BY source_symbol, session_date
              HAVING count(DISTINCT open) > 1 OR count(DISTINCT high) > 1
                  OR count(DISTINCT low) > 1 OR count(DISTINCT close) > 1
                  OR count(DISTINCT volume) > 1
            )
            """
        ).fetchone()[0]
    )
    if conflicts:
        raise AggregateError(
            f"Price source contains {conflicts} conflicting symbol/date groups"
        )
    identical_duplicates = int(
        con.execute(
            """
            SELECT coalesce(sum(row_count - 1), 0) FROM (
              SELECT count(*) AS row_count
              FROM price_source
              GROUP BY source_symbol, session_date, open, high, low, close, volume
              HAVING count(*) > 1
            )
            """
        ).fetchone()[0]
    )
    con.execute("CREATE TEMP TABLE price_dedup AS SELECT DISTINCT * FROM price_source")
    return identical_duplicates


def prepare_price_candidates(
    con: duckdb.DuckDBPyConnection,
    *,
    sessions: int,
    expected_session: date,
    maximum_security_staleness_sessions: int,
    maximum_source_gap_days: int,
    source_reuse_price_ratio: float,
    systemic_invalid_session_rate: float,
    minimum_systemic_session_securities: int,
) -> tuple[
    date,
    list[tuple[str, date]],
    list[tuple[str, date, date, str]],
    list[tuple[date, int, int, float]],
]:
    """Select releasable history and isolate systemic upstream session failures.

    Invalid rows outside the requested per-security history cannot enter an output and
    therefore do not invalidate a build. A recent source symbol must have observations
    within the configured XNYS-session window; this prevents histories from a recycled
    ticker from being attached to a current listing. If a sufficiently broad source
    session has a systemic invalid-OHLC rate, the entire session is quarantined and the
    normal market-freshness gate evaluates the resulting output date.
    """
    con.execute(
        f"""
        CREATE TEMP TABLE price_candidate AS
        WITH matched AS (
          SELECT a.security_id, a.ticker, p.source_symbol, p.session_date,
                 p.open, p.high, p.low, p.close, p.volume,
                 row_number() OVER (
                   PARTITION BY a.security_id, p.session_date
                   ORDER BY a.priority, p.source_symbol
                 ) AS alias_rank
          FROM price_dedup p
          JOIN aliases a ON p.source_symbol = a.source_symbol
        ), best_alias AS (
          SELECT * EXCLUDE(alias_rank) FROM matched WHERE alias_rank = 1
        ), with_previous AS (
          SELECT *,
                 lag(session_date) OVER (
                   PARTITION BY security_id ORDER BY session_date
                 ) AS previous_session,
                 lag(close) OVER (
                   PARTITION BY security_id ORDER BY session_date
                 ) AS previous_close
          FROM best_alias
        ), with_break AS (
          SELECT *,
                 CASE
                   WHEN previous_session IS NOT NULL
                        AND datediff('day', previous_session, session_date) > ?
                     THEN 'SESSION_GAP'
                   WHEN previous_close > 0 AND close > 0
                        AND ({PRICE_INVALID_SQL})
                        AND greatest(close / previous_close, previous_close / close) >= ?
                     THEN 'INVALID_PRICE_DISCONTINUITY'
                   ELSE NULL
                 END AS segment_break_reason
          FROM with_previous
        ), segmented AS (
          SELECT *, sum(
                   CASE WHEN segment_break_reason IS NOT NULL THEN 1 ELSE 0 END
                 ) OVER (
                   PARTITION BY security_id ORDER BY session_date
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                 ) AS segment_id
          FROM with_break
        ), annotated AS (
          SELECT *, max(segment_id) OVER (
                   PARTITION BY security_id
                 ) AS latest_segment_id,
                 min(CASE WHEN NOT ({PRICE_INVALID_SQL}) THEN session_date END) OVER (
                   PARTITION BY security_id, segment_id
                 ) AS valid_segment_start_date,
                 max(
                   CASE WHEN segment_break_reason IS NOT NULL
                        THEN previous_session END
                 ) OVER (
                   PARTITION BY security_id, segment_id
                 ) AS previous_segment_latest_date,
                 max(segment_break_reason) OVER (
                   PARTITION BY security_id, segment_id
                 ) AS latest_segment_break_reason
          FROM segmented
        ), latest_segment AS (
          SELECT * FROM annotated
          WHERE segment_id = latest_segment_id
            AND valid_segment_start_date IS NOT NULL
            AND session_date >= valid_segment_start_date
        ), ranked AS (
          SELECT *,
                 row_number() OVER (
                   PARTITION BY security_id ORDER BY session_date DESC
                 ) AS history_rank
          FROM latest_segment
        )
        SELECT * FROM ranked WHERE history_rank <= ?
        """,
        [maximum_source_gap_days, source_reuse_price_ratio, sessions],
    )

    calendar = xcals.get_calendar("XNYS")
    expected_label = calendar.date_to_session(
        expected_session.isoformat(), direction="previous"
    )
    oldest_active_session = calendar.session_offset(
        expected_label, -maximum_security_staleness_sessions
    ).date()
    con.execute(
        """
        CREATE TEMP TABLE security_source_latest AS
        SELECT security_id, max(session_date) AS latest_session
        FROM price_candidate GROUP BY security_id
        """
    )
    stale_rows = [
        (str(security_id), latest_session)
        for security_id, latest_session in con.execute(
            """
            SELECT security_id, latest_session FROM security_source_latest
            WHERE latest_session < cast(? AS DATE)
            ORDER BY security_id
            """,
            [oldest_active_session.isoformat()],
        ).fetchall()
    ]
    con.execute(
        """
        CREATE TEMP TABLE active_security_ids AS
        SELECT security_id FROM security_source_latest
        WHERE latest_session >= cast(? AS DATE)
        """,
        [oldest_active_session.isoformat()],
    )

    history_truncation_rows = [
        (str(security_id), previous_latest, segment_start, str(reason))
        for security_id, previous_latest, segment_start, reason in con.execute(
            """
            SELECT security_id,
                   max(previous_segment_latest_date) AS previous_latest,
                   min(valid_segment_start_date) AS segment_start,
                   max(latest_segment_break_reason) AS reason
            FROM price_candidate
            WHERE previous_segment_latest_date IS NOT NULL
            GROUP BY security_id
            ORDER BY security_id
            """
        ).fetchall()
    ]

    con.execute(
        f"""
        CREATE TEMP TABLE quarantined_price_sessions AS
        WITH quality AS (
          SELECT session_date,
                 count(*) AS total_rows,
                 count(*) FILTER (WHERE {PRICE_INVALID_SQL}) AS invalid_rows
          FROM price_candidate
          JOIN active_security_ids USING (security_id)
          GROUP BY session_date
        )
        SELECT session_date, invalid_rows, total_rows,
               cast(invalid_rows AS DOUBLE) / total_rows AS invalid_rate
        FROM quality
        WHERE total_rows >= ?
          AND cast(invalid_rows AS DOUBLE) / total_rows > ?
        """,
        [minimum_systemic_session_securities, systemic_invalid_session_rate],
    )
    quarantined_rows = [
        (session_date, int(invalid_rows), int(total_rows), float(invalid_rate))
        for session_date, invalid_rows, total_rows, invalid_rate in con.execute(
            """
            SELECT session_date, invalid_rows, total_rows, invalid_rate
            FROM quarantined_price_sessions ORDER BY session_date
            """
        ).fetchall()
    ]
    return (
        oldest_active_session,
        stale_rows,
        history_truncation_rows,
        quarantined_rows,
    )


def prepare_split_source(
    con: duckdb.DuckDBPyConnection, path: Path, cutoff: date
) -> int:
    validate_source_schema(
        con,
        path,
        {"symbol", "report_date", "split_factor"},
        "Split",
    )
    con.execute(
        """
        CREATE TEMP TABLE split_source AS
        SELECT upper(trim(p.symbol)) AS source_symbol,
               try_cast(p.report_date AS DATE) AS event_date,
               trim(cast(p.split_factor AS VARCHAR)) AS split_factor
        FROM read_parquet(?) p
        JOIN (SELECT DISTINCT source_symbol FROM aliases) a
          ON upper(trim(p.symbol)) = a.source_symbol
        WHERE try_cast(p.report_date AS DATE) <= cast(? AS DATE)
        """,
        [str(path), cutoff.isoformat()],
    )
    invalid = int(
        con.execute(
            """
            SELECT count(*) FROM split_source
            WHERE source_symbol IS NULL OR source_symbol = '' OR event_date IS NULL
               OR split_factor IS NULL OR split_factor = ''
            """
        ).fetchone()[0]
    )
    if invalid:
        raise AggregateError(f"Split source contains {invalid} invalid matched rows")
    conflicts = int(
        con.execute(
            """
            SELECT count(*) FROM (
              SELECT source_symbol, event_date
              FROM split_source
              GROUP BY source_symbol, event_date
              HAVING count(DISTINCT split_factor) > 1
            )
            """
        ).fetchone()[0]
    )
    if conflicts:
        raise AggregateError(
            f"Split source contains {conflicts} conflicting symbol/date groups"
        )
    identical_duplicates = int(
        con.execute(
            """
            SELECT coalesce(sum(row_count - 1), 0) FROM (
              SELECT count(*) AS row_count
              FROM split_source
              GROUP BY source_symbol, event_date, split_factor
              HAVING count(*) > 1
            )
            """
        ).fetchone()[0]
    )
    con.execute("CREATE TEMP TABLE split_dedup AS SELECT DISTINCT * FROM split_source")
    return identical_duplicates


def write_unmatched(
    path: Path,
    securities: list[Security],
    matched_ids: set[str],
    reasons: dict[str, str] | None = None,
) -> None:
    reasons = reasons or {}
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(["security_id", "ticker", "aliases_tried", "reason"])
        for security in securities:
            if security.security_id not in matched_ids:
                writer.writerow(
                    [
                        security.security_id,
                        security.ticker,
                        ";".join(yahoo_aliases(security.ticker)),
                        reasons.get(security.security_id, "NO_SOURCE_SYMBOL_MATCH"),
                    ]
                )


def expected_latest_session(cutoff: date) -> date:
    calendar = xcals.get_calendar("XNYS")
    return calendar.date_to_session(cutoff.isoformat(), direction="previous").date()


def stale_sessions(max_date: date, expected: date) -> int:
    if max_date > expected:
        raise AggregateError(
            f"Aggregate maximum date {max_date} is after expected session {expected}"
        )
    calendar = xcals.get_calendar("XNYS")
    sessions = calendar.sessions_in_range(max_date.isoformat(), expected.isoformat())
    return max(0, len(sessions) - 1)


def file_record(
    path: Path,
    *,
    rows: int,
    schema: Iterable[str] | None = None,
) -> dict[str, object]:
    record: dict[str, object] = {
        "file": path.name,
        "sha256": sha256_file(path),
        "bytes": path.stat().st_size,
        "rows": rows,
    }
    if schema is not None:
        record["schema"] = list(schema)
    return record


def build(args: argparse.Namespace) -> tuple[dict[str, object], int]:
    cutoff = parse_date(args.cutoff_date)
    observed_at = parse_utc(args.observed_at) if args.observed_at else utcnow()
    universe_path = Path(args.universe).resolve()
    universe_metadata_path = Path(args.universe_metadata).resolve()
    out_dir = Path(args.out_dir).resolve()
    cache_dir = Path(args.cache_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)

    securities = load_universe(universe_path)
    universe_metadata = load_universe_metadata(universe_metadata_path, universe_path)
    prices_path, prices_revision = resolve_source_file(
        repo_id=args.source_repo,
        revision=args.source_revision,
        filename=PRICES_PATH,
        local_override=args.prices_file,
        cache_dir=cache_dir,
    )
    splits_path, splits_revision = resolve_source_file(
        repo_id=args.source_repo,
        revision=prices_revision if prices_revision != "LOCAL_OVERRIDE" else args.source_revision,
        filename=SPLITS_PATH,
        local_override=args.splits_file,
        cache_dir=cache_dir,
    )
    if prices_revision != splits_revision:
        raise AggregateError(
            f"Source revision mismatch: prices={prices_revision}, splits={splits_revision}"
        )

    aggregate_path = out_dir / f"yahoo-ohlcv-{args.sessions}.parquet"
    splits_out = out_dir / "yahoo-splits.parquet"
    unmatched_path = out_dir / "unmatched-tickers.csv"
    manifest_path = out_dir / "manifest.json"
    packaged_universe_path = out_dir / "security-universe.csv"
    if universe_path != packaged_universe_path:
        tmp_universe = packaged_universe_path.with_suffix(".csv.tmp")
        shutil.copyfile(universe_path, tmp_universe)
        os.replace(tmp_universe, packaged_universe_path)

    con = duckdb.connect()
    try:
        build_alias_table(con, securities)
        price_duplicate_count = prepare_price_source(con, prices_path, cutoff)
        split_duplicate_count = prepare_split_source(con, splits_path, cutoff)
        expected_session = expected_latest_session(cutoff)
        (
            oldest_active_session,
            stale_source_rows,
            source_history_truncation_rows,
            quarantined_session_rows,
        ) = prepare_price_candidates(
            con,
            sessions=args.sessions,
            expected_session=expected_session,
            maximum_security_staleness_sessions=(
                args.maximum_security_staleness_sessions
            ),
            maximum_source_gap_days=args.maximum_source_gap_days,
            source_reuse_price_ratio=args.source_reuse_price_ratio,
            systemic_invalid_session_rate=args.systemic_invalid_session_rate,
            minimum_systemic_session_securities=(
                args.minimum_systemic_session_securities
            ),
        )

        aggregate_query = """
          SELECT p.security_id, p.ticker, p.source_symbol, p.session_date,
                 p.open, p.high, p.low, p.close, p.volume,
                 cast(? AS VARCHAR) AS source_dataset,
                 cast(? AS VARCHAR) AS source_revision,
                 cast(? AS VARCHAR) AS observed_at_utc
          FROM price_candidate p
          JOIN active_security_ids a USING (security_id)
          LEFT JOIN quarantined_price_sessions q USING (session_date)
          WHERE q.session_date IS NULL
          ORDER BY p.ticker, p.session_date
        """
        atomic_copy_to_parquet(
            con,
            aggregate_query,
            [
                args.source_repo,
                prices_revision,
                format_utc(observed_at),
            ],
            aggregate_path,
        )

        split_query = """
          WITH matched AS (
            SELECT a.security_id, a.ticker, s.source_symbol, s.event_date,
                   s.split_factor,
                   row_number() OVER (
                     PARTITION BY a.security_id, s.event_date
                     ORDER BY a.priority, s.source_symbol
                   ) AS alias_rank
            FROM split_dedup s
            JOIN aliases a ON s.source_symbol = a.source_symbol
            JOIN active_security_ids active ON a.security_id = active.security_id
          )
          SELECT security_id, ticker, source_symbol, event_date, split_factor,
                 cast(? AS VARCHAR) AS source_dataset,
                 cast(? AS VARCHAR) AS source_revision,
                 cast(? AS VARCHAR) AS observed_at_utc
          FROM matched WHERE alias_rank = 1
          ORDER BY ticker, event_date
        """
        atomic_copy_to_parquet(
            con,
            split_query,
            [args.source_repo, splits_revision, format_utc(observed_at)],
            splits_out,
        )

        if parquet_columns(con, aggregate_path) != OHLCV_SCHEMA:
            raise AggregateError("Generated OHLCV schema does not match contract")
        if parquet_columns(con, splits_out) != SPLIT_SCHEMA:
            raise AggregateError("Generated split schema does not match contract")

        stats = con.execute(
            f"""
            SELECT count(*) AS rows,
                   count(DISTINCT security_id) AS securities,
                   min(session_date) AS min_date,
                   max(session_date) AS max_date,
                   count(*) FILTER (
                     WHERE security_id IS NULL OR ticker IS NULL OR source_symbol IS NULL
                        OR session_date IS NULL OR open IS NULL OR high IS NULL OR low IS NULL
                        OR close IS NULL OR volume IS NULL OR source_dataset IS NULL
                        OR source_revision IS NULL OR observed_at_utc IS NULL
                        OR {PRICE_INVALID_SQL}
                   ) AS invalid_rows,
                   count(*) - count(DISTINCT (security_id, session_date)) AS duplicate_rows,
                   count(*) FILTER (WHERE session_date > cast(? AS DATE)) AS after_cutoff
            FROM read_parquet(?)
            """,
            [cutoff.isoformat(), str(aggregate_path)],
        ).fetchone()
        history_rows = con.execute(
            """
            SELECT security_id, max(ticker) AS ticker, count(*) AS sessions
            FROM read_parquet(?) GROUP BY security_id
            """,
            [str(aggregate_path)],
        ).fetchall()
        split_stats = con.execute(
            """
            SELECT count(*) AS rows,
                   count(*) - count(DISTINCT (security_id, event_date)) AS duplicate_rows,
                   count(*) FILTER (
                     WHERE security_id IS NULL OR ticker IS NULL OR source_symbol IS NULL
                        OR event_date IS NULL OR split_factor IS NULL OR split_factor = ''
                        OR source_dataset IS NULL OR source_revision IS NULL
                        OR observed_at_utc IS NULL
                   ) AS invalid_rows
            FROM read_parquet(?)
            """,
            [str(splits_out)],
        ).fetchone()

        matched_ids = {str(row[0]) for row in history_rows}
        stale_reasons = {
            security_id: f"SOURCE_HISTORY_STALE:latest_session={latest_session}"
            for security_id, latest_session in stale_source_rows
        }
        write_unmatched(
            unmatched_path,
            securities,
            matched_ids,
            reasons=stale_reasons,
        )
        admitted_count = len(securities)
        matched_count = len(matched_ids)
        coverage = matched_count / admitted_count
        adequate_count = sum(
            1 for _, _, sessions in history_rows if int(sessions) >= args.minimum_history
        )
        adequate_coverage = adequate_count / matched_count if matched_count else 0.0
        session_counts = sorted(int(row[2]) for row in history_rows)
        history_distribution = {
            "minimum": session_counts[0] if session_counts else None,
            "median": (
                session_counts[(len(session_counts) - 1) // 2]
                if session_counts
                else None
            ),
            "maximum": session_counts[-1] if session_counts else None,
            "buckets": {
                "0_to_29": sum(count < 30 for count in session_counts),
                "30_to_251": sum(30 <= count < 252 for count in session_counts),
                "252_to_319": sum(252 <= count < 320 for count in session_counts),
                "320_or_more": sum(count >= 320 for count in session_counts),
            },
        }

        errors: list[str] = []
        warnings: list[str] = []
        if price_duplicate_count:
            warnings.append(
                f"Collapsed {price_duplicate_count} byte-identical price rows"
            )
        if split_duplicate_count:
            warnings.append(
                f"Collapsed {split_duplicate_count} byte-identical split rows"
            )
        if stale_source_rows:
            warnings.append(
                f"Excluded {len(stale_source_rows)} source-symbol histories older than "
                f"{oldest_active_session}"
            )
        if source_history_truncation_rows:
            warnings.append(
                f"Truncated {len(source_history_truncation_rows)} source-symbol "
                "histories at gap or invalid-discontinuity boundaries"
            )
        for session_date, invalid_rows, total_rows, invalid_rate in (
            quarantined_session_rows
        ):
            warnings.append(
                f"Quarantined systemic invalid OHLC session {session_date}: "
                f"{invalid_rows}/{total_rows} rows ({invalid_rate:.2%})"
            )
        if coverage < args.minimum_coverage:
            errors.append(
                f"Matched coverage {coverage:.2%} is below {args.minimum_coverage:.2%}"
            )
        if adequate_coverage < args.minimum_adequate_history_coverage:
            errors.append(
                f"Adequate-history coverage {adequate_coverage:.2%} is below "
                f"{args.minimum_adequate_history_coverage:.2%}"
            )
        if int(stats[0] or 0) == 0:
            errors.append("Aggregate contains no rows")
        if int(stats[4] or 0):
            errors.append(f"Aggregate contains {int(stats[4])} invalid rows")
        if int(stats[5] or 0):
            errors.append(f"Aggregate contains {int(stats[5])} duplicate keys")
        if int(stats[6] or 0):
            errors.append(f"Aggregate contains {int(stats[6])} rows after cutoff")
        if int(split_stats[1] or 0):
            errors.append(f"Split output contains {int(split_stats[1])} duplicate keys")
        if int(split_stats[2] or 0):
            errors.append(f"Split output contains {int(split_stats[2])} invalid rows")
        too_long = [row for row in history_rows if int(row[2]) > args.sessions]
        if too_long:
            errors.append("One or more securities exceed the requested session limit")

        observed_max = stats[3]
        missing_sessions: int | None = None
        if observed_max is None:
            errors.append("Aggregate has no maximum session date")
        else:
            missing_sessions = stale_sessions(observed_max, expected_session)
            if 3 <= missing_sessions <= 5:
                warnings.append(
                    f"Market data is {missing_sessions} eligible sessions stale"
                )
            elif missing_sessions > 5:
                errors.append(
                    f"Market data is {missing_sessions} eligible sessions stale"
                )

        if errors:
            status = "VALIDATION_FAILED"
        elif missing_sessions is not None and missing_sessions >= 3:
            status = "STALE_WARNING"
        else:
            status = "READY"

        aggregate_record = file_record(
            aggregate_path, rows=int(stats[0] or 0), schema=OHLCV_SCHEMA
        )
        aggregate_record.update(
            {
                "securities": int(stats[1] or 0),
                "min_date": str(stats[2]) if stats[2] else None,
                "max_date": str(stats[3]) if stats[3] else None,
            }
        )
        splits_record = file_record(
            splits_out, rows=int(split_stats[0] or 0), schema=SPLIT_SCHEMA
        )
        with unmatched_path.open(newline="", encoding="utf-8") as handle:
            unmatched_rows = max(sum(1 for _ in handle) - 1, 0)
        unmatched_record = file_record(unmatched_path, rows=unmatched_rows)
        universe_record = file_record(
            packaged_universe_path,
            rows=int(universe_metadata.get("rows", 0)),
        )

        manifest: dict[str, object] = {
            "schema_version": SCHEMA_VERSION,
            "status": status,
            "created_at_utc": format_utc(observed_at),
            "cutoff_date": cutoff.isoformat(),
            "sessions_requested": args.sessions,
            "thresholds": {
                "minimum_history_sessions": args.minimum_history,
                "minimum_matched_coverage": args.minimum_coverage,
                "minimum_adequate_history_coverage": args.minimum_adequate_history_coverage,
                "maximum_security_staleness_sessions": (
                    args.maximum_security_staleness_sessions
                ),
                "maximum_source_gap_days": args.maximum_source_gap_days,
                "source_reuse_price_ratio": args.source_reuse_price_ratio,
                "systemic_invalid_session_rate": args.systemic_invalid_session_rate,
                "minimum_systemic_session_securities": (
                    args.minimum_systemic_session_securities
                ),
                "ready_maximum_stale_sessions": 2,
                "warning_maximum_stale_sessions": 5,
            },
            "validation": {
                "expected_latest_xnys_session": expected_session.isoformat(),
                "missing_eligible_sessions": missing_sessions,
                "errors": errors,
                "warnings": warnings,
            },
            "universe": {
                **universe_record,
                "admitted_count": admitted_count,
                "matched_count": matched_count,
                "matched_coverage": coverage,
                "adequate_history_count": adequate_count,
                "adequate_history_coverage": adequate_coverage,
                "history_distribution": history_distribution,
                "sources": universe_metadata.get("sources", []),
                "drift": universe_metadata.get("drift", {}),
            },
            "aggregate": aggregate_record,
            "splits": splits_record,
            "unmatched": unmatched_record,
            "release_files": [
                aggregate_record,
                splits_record,
                universe_record,
                unmatched_record,
            ],
            "source": {
                "dataset": args.source_repo,
                "revision": prices_revision,
                "prices_path": PRICES_PATH,
                "prices_sha256": sha256_file(prices_path),
                "splits_path": SPLITS_PATH,
                "splits_sha256": sha256_file(splits_path),
                "license": "ODC-By-1.0 (per dataset card); preserve attribution",
                "usage": "historical research only; raw close is not adjusted close or a live quote",
                "quality": {
                    "oldest_active_source_session": oldest_active_session.isoformat(),
                    "stale_source_symbols": [
                        {
                            "security_id": security_id,
                            "latest_session": latest_session.isoformat(),
                            "missing_eligible_sessions": stale_sessions(
                                latest_session, expected_session
                            ),
                        }
                        for security_id, latest_session in stale_source_rows
                    ],
                    "source_history_truncations": [
                        {
                            "security_id": security_id,
                            "previous_segment_latest_date": previous_latest.isoformat(),
                            "latest_segment_start_date": segment_start.isoformat(),
                            "reason": reason,
                        }
                        for security_id, previous_latest, segment_start, reason in (
                            source_history_truncation_rows
                        )
                    ],
                    "quarantined_sessions": [
                        {
                            "session_date": session_date.isoformat(),
                            "invalid_rows": invalid_rows,
                            "total_rows": total_rows,
                            "invalid_rate": invalid_rate,
                        }
                        for session_date, invalid_rows, total_rows, invalid_rate in (
                            quarantined_session_rows
                        )
                    ],
                },
            },
        }
        atomic_json(manifest_path, manifest)
        print(json.dumps(manifest, indent=2, sort_keys=True))
        return manifest, 0 if status == "READY" else 2
    finally:
        con.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--universe", required=True)
    parser.add_argument("--universe-metadata", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--sessions", type=int, default=320)
    parser.add_argument("--minimum-history", type=int, default=252)
    parser.add_argument("--minimum-coverage", type=float, default=0.80)
    parser.add_argument("--minimum-adequate-history-coverage", type=float, default=0.50)
    parser.add_argument("--maximum-security-staleness-sessions", type=int, default=5)
    parser.add_argument("--maximum-source-gap-days", type=int, default=14)
    parser.add_argument("--source-reuse-price-ratio", type=float, default=4.0)
    parser.add_argument("--systemic-invalid-session-rate", type=float, default=0.01)
    parser.add_argument("--minimum-systemic-session-securities", type=int, default=100)
    parser.add_argument("--cutoff-date", default=date.today().isoformat())
    parser.add_argument("--source-repo", default=DEFAULT_REPO)
    parser.add_argument("--source-revision", default="main")
    parser.add_argument("--prices-file")
    parser.add_argument("--splits-file")
    parser.add_argument("--cache-dir", default=".hf-cache")
    parser.add_argument("--observed-at", help="RFC3339 UTC timestamp; defaults to now")
    args = parser.parse_args(argv)

    if args.sessions <= 0:
        parser.error("--sessions must be positive")
    if args.minimum_history <= 0 or args.sessions < args.minimum_history:
        parser.error("--sessions must be >= --minimum-history > 0")
    if args.maximum_security_staleness_sessions < 0:
        parser.error("--maximum-security-staleness-sessions must be nonnegative")
    if args.maximum_source_gap_days <= 0:
        parser.error("--maximum-source-gap-days must be positive")
    if args.source_reuse_price_ratio <= 1:
        parser.error("--source-reuse-price-ratio must be greater than 1")
    if args.minimum_systemic_session_securities <= 0:
        parser.error("--minimum-systemic-session-securities must be positive")
    if not (0 < args.systemic_invalid_session_rate <= 1):
        parser.error("--systemic-invalid-session-rate must be in (0, 1]")
    for name in ("minimum_coverage", "minimum_adequate_history_coverage"):
        value = float(getattr(args, name))
        if not (0 < value <= 1):
            parser.error(f"--{name.replace('_', '-')} must be in (0, 1]")

    try:
        _, exit_code = build(args)
        return exit_code
    except (AggregateError, duckdb.Error, OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
