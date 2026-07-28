#!/usr/bin/env python3
"""Compose a coherent release from independently validated dataset groups."""

from __future__ import annotations

import argparse
import copy
import csv
import json
import os
import shutil
import sys
from datetime import date
from pathlib import Path
from typing import Mapping

import duckdb
import exchange_calendars as xcals

from enrichment_contract import CONTRACTS, SCHEMA_VERSION, sha256_file
from reliability_contract import (
    CORE_GROUPS,
    GROUPS,
    MARKET_READY_MAX_LAG_SESSIONS,
    apply_dataset_groups,
    freshness_state_for_reuse,
)


class CompositionError(RuntimeError):
    """Raised when no safe candidate/fallback composition can be assembled."""


COVERAGE_BY_GROUP = {
    "identity": "security_master",
    "fundamentals": "fundamentals",
    "filings_events": "filings_and_events",
    "insiders": "insider_transactions",
    "institutional": "institutional_ownership",
    "short_interest": "short_interest",
}


def merge_reused_coverage(
    candidate: dict[str, object],
    previous: Mapping[str, object],
    reused_groups: set[str],
) -> None:
    current = candidate.get("coverage")
    coverage = copy.deepcopy(current) if isinstance(current, dict) else {}
    previous_coverage = previous.get("coverage")
    if not isinstance(previous_coverage, dict):
        if reused_groups - {"market"}:
            raise CompositionError("Fallback release has no enrichment coverage")
        return
    for group_id in reused_groups:
        section = COVERAGE_BY_GROUP.get(group_id)
        if section is not None:
            if section not in previous_coverage:
                raise CompositionError(
                    f"Fallback release lacks coverage section: {section}"
                )
            coverage[section] = copy.deepcopy(previous_coverage[section])
    if "analyst_estimates" not in coverage and "analyst_estimates" in previous_coverage:
        coverage["analyst_estimates"] = copy.deepcopy(
            previous_coverage["analyst_estimates"]
        )
    if coverage:
        candidate["coverage"] = coverage


def load_manifest(path: Path, label: str) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CompositionError(f"Cannot read {label} manifest: {exc}") from exc
    if not isinstance(value, dict) or value.get("schema_version") != SCHEMA_VERSION:
        raise CompositionError(f"{label} manifest is not schema {SCHEMA_VERSION}")
    return value


def records_by_name(
    manifest: Mapping[str, object], key: str, section: str
) -> dict[str, dict[str, object]]:
    raw = manifest.get(section)
    if not isinstance(raw, list):
        return {}
    output: dict[str, dict[str, object]] = {}
    for record in raw:
        if not isinstance(record, dict):
            raise CompositionError(f"{section} contains a non-object")
        name = str(record.get(key) or "")
        if not name or Path(name).name != name or name in output:
            raise CompositionError(f"{section} contains an unsafe identity: {name!r}")
        output[name] = record
    return output


def validate_group_files(
    directory: Path,
    manifest: Mapping[str, object],
    group_id: str,
) -> list[str]:
    group = GROUPS[group_id]
    release = records_by_name(manifest, "file", "release_files")
    diagnostics: list[str] = []
    con = duckdb.connect()
    try:
        for filename in group.files:
            record = release.get(filename)
            path = directory / filename
            if record is None:
                diagnostics.append(f"missing manifest release identity: {filename}")
                continue
            if path.is_symlink() or not path.is_file():
                diagnostics.append(f"missing regular file: {filename}")
                continue
            if path.stat().st_size != int(record.get("bytes", -1)):
                diagnostics.append(f"byte-size mismatch: {filename}")
                continue
            if sha256_file(path) != str(record.get("sha256") or ""):
                diagnostics.append(f"sha256 mismatch: {filename}")
                continue
            if path.suffix == ".parquet":
                rows = int(
                    con.execute(
                        "SELECT count(*) FROM read_parquet(?)", [str(path)]
                    ).fetchone()[0]
                )
                if rows != int(record.get("rows", -1)):
                    diagnostics.append(f"row-count mismatch: {filename}")
                contract = CONTRACTS.get(filename)
                if contract is not None:
                    actual = tuple(
                        (str(column[0]), str(column[1]))
                        for column in con.execute(
                            "SELECT * FROM read_parquet(?) LIMIT 0", [str(path)]
                        ).description
                    )
                    if actual != contract.columns:
                        diagnostics.append(f"physical schema mismatch: {filename}")
        if group_id == "market" and not diagnostics:
            prices = directory / "yahoo-ohlcv-320.parquet"
            invalid = int(
                con.execute(
                    """
                    SELECT count(*) FROM read_parquet(?)
                    WHERE security_id IS NULL OR session_date IS NULL
                       OR open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL
                       OR volume IS NULL OR NOT isfinite(open) OR NOT isfinite(high)
                       OR NOT isfinite(low) OR NOT isfinite(close)
                       OR open <= 0 OR high <= 0 OR low <= 0 OR close <= 0
                       OR volume < 0 OR high < greatest(open, close, low)
                       OR low > least(open, close, high)
                    """,
                    [str(prices)],
                ).fetchone()[0]
            )
            duplicates = int(
                con.execute(
                    """
                    SELECT count(*) - count(DISTINCT (security_id, session_date))
                    FROM read_parquet(?)
                    """,
                    [str(prices)],
                ).fetchone()[0]
            )
            if invalid:
                diagnostics.append(f"invalid OHLC rows: {invalid}")
            if duplicates:
                diagnostics.append(f"duplicate OHLC keys: {duplicates}")
        if group_id == "insiders" and not diagnostics:
            master = directory / "security-master.parquet"
            if not master.is_file():
                diagnostics.append("candidate identity master is unavailable")
            else:
                violations = int(
                    con.execute(
                        """
                        SELECT count(*)
                        FROM read_parquet(?) insider
                        LEFT JOIN read_parquet(?) master USING (security_id)
                        WHERE master.security_id IS NULL OR master.cik IS NULL
                           OR NOT regexp_full_match(master.cik, '[0-9]{10}')
                           OR lpad(regexp_replace(cast(insider.issuer_cik AS VARCHAR),
                                                  '[^0-9]', '', 'g'), 10, '0')
                              <> master.cik
                        """,
                        [
                            str(directory / "insider-transactions.parquet"),
                            str(master),
                        ],
                    ).fetchone()[0]
                )
                if violations:
                    diagnostics.append(f"canonical insider CIK violations: {violations}")
    except duckdb.Error as exc:
        diagnostics.append(f"DuckDB validation error: {exc}")
    finally:
        con.close()
    return diagnostics


def identity_orphan_counts(directory: Path, filenames: list[str]) -> dict[str, int]:
    """Count dataset identities that are absent from the packaged security master."""
    master = directory / "security-master.parquet"
    if not master.is_file():
        raise CompositionError("Final security master is unavailable")
    counts: dict[str, int] = {}
    con = duckdb.connect()
    try:
        for filename in filenames:
            path = directory / filename
            if path.suffix != ".parquet" or filename == master.name:
                continue
            columns = {
                str(column[0])
                for column in con.execute(
                    "SELECT * FROM read_parquet(?) LIMIT 0", [str(path)]
                ).description
            }
            if "security_id" not in columns:
                continue
            count = int(
                con.execute(
                    """
                    SELECT count(*)
                    FROM read_parquet(?) dataset
                    LEFT JOIN read_parquet(?) master USING (security_id)
                    WHERE dataset.security_id IS NOT NULL
                      AND master.security_id IS NULL
                    """,
                    [str(path), str(master)],
                ).fetchone()[0]
            )
            if count:
                counts[filename] = count
    finally:
        con.close()
    return counts


def filter_market_orphans(directory: Path) -> dict[str, int]:
    """Remove fallback histories that cannot join the selected identity group."""
    filenames = ["yahoo-ohlcv-320.parquet", "yahoo-splits.parquet"]
    counts = identity_orphan_counts(directory, filenames)
    if not counts:
        return {}
    master = directory / "security-master.parquet"
    con = duckdb.connect()
    try:
        for filename in counts:
            source = directory / filename
            target = source.with_suffix(source.suffix + ".identity.tmp")
            source_sql = str(source).replace("'", "''")
            master_sql = str(master).replace("'", "''")
            target_sql = str(target).replace("'", "''")
            con.execute(
                f"""
                COPY (
                    SELECT dataset.*
                    FROM read_parquet('{source_sql}') dataset
                    SEMI JOIN read_parquet('{master_sql}') master USING (security_id)
                ) TO '{target_sql}' (FORMAT PARQUET)
                """,
            )
            os.replace(target, source)
    finally:
        con.close()
    return counts


def refreshed_release_record(
    path: Path, prior: Mapping[str, object]
) -> dict[str, object]:
    record = copy.deepcopy(dict(prior))
    con = duckdb.connect()
    try:
        rows = int(
            con.execute("SELECT count(*) FROM read_parquet(?)", [str(path)]).fetchone()[0]
        )
    finally:
        con.close()
    record.update({"sha256": sha256_file(path), "bytes": path.stat().st_size, "rows": rows})
    return record


def recompute_market_readiness(directory: Path, manifest: dict[str, object]) -> None:
    """Derive coverage from the final released identities and market bytes."""
    admitted: set[str] = set()
    tickers: dict[str, str] = {}
    with (directory / "security-universe.csv").open(
        newline="", encoding="utf-8-sig"
    ) as handle:
        for row in csv.DictReader(handle):
            if str(row.get("universe_admission_status") or "").upper() not in {
                "ADMITTED",
                "ADMITTED_ETF",
            }:
                continue
            security_id = str(row.get("security_id") or "")
            admitted.add(security_id)
            tickers[security_id] = str(row.get("ticker") or "").upper()
    prices = directory / "yahoo-ohlcv-320.parquet"
    con = duckdb.connect()
    try:
        observed = con.execute(
            "SELECT max(session_date) FROM read_parquet(?)", [str(prices)]
        ).fetchone()[0]
        covered = {
            str(row[0])
            for row in con.execute(
                "SELECT DISTINCT security_id FROM read_parquet(?) WHERE session_date = ?",
                [str(prices), observed],
            ).fetchall()
        }
    finally:
        con.close()
    coverage = len(covered & admitted) / len(admitted) if admitted else 0.0
    validation = manifest.setdefault("validation", {})
    if not isinstance(validation, dict):
        raise CompositionError("Candidate validation section is invalid")
    validation["latest_session_coverage"] = coverage
    validation["benchmark_valid"] = any(
        tickers.get(security_id) == "VTI" for security_id in covered & admitted
    )
    if coverage < 0.95:
        raise CompositionError(
            f"Final market latest-session coverage {coverage:.2%} is below 95.00%"
        )


def copy_atomic(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + ".compose.tmp")
    shutil.copyfile(source, temporary)
    os.replace(temporary, target)


def eligible_session_lag(observed: object, expected: object) -> int:
    try:
        observed_date = date.fromisoformat(str(observed))
        expected_date = date.fromisoformat(str(expected))
    except ValueError as exc:
        raise CompositionError("Market group has invalid freshness dates") from exc
    if observed_date > expected_date:
        raise CompositionError("Fallback market date is after expected session")
    calendar = xcals.get_calendar("XNYS")
    sessions = calendar.sessions_in_range(
        observed_date.isoformat(), expected_date.isoformat()
    )
    return max(0, len(sessions) - 1)


def compose_release(
    candidate_directory: Path,
    previous_directory: Path,
    output_directory: Path,
    source_tag: str,
) -> dict[str, object]:
    candidate_manifest_path = candidate_directory / "manifest.json"
    previous_manifest_path = previous_directory / "manifest.json"
    candidate = load_manifest(candidate_manifest_path, "candidate")
    previous = load_manifest(previous_manifest_path, "previous")
    if previous.get("status") != "READY":
        raise CompositionError("Previous fallback release is not READY")
    previous_manifest_sha = sha256_file(previous_manifest_path)
    candidate_release = records_by_name(candidate, "file", "release_files")
    previous_release = records_by_name(previous, "file", "release_files")
    candidate_datasets = records_by_name(candidate, "path", "datasets")
    previous_datasets = records_by_name(previous, "path", "datasets")
    previous_groups = {
        str(record.get("group_id")): record
        for record in previous.get("dataset_groups", [])
        if isinstance(record, dict)
    }

    selected_release: dict[str, dict[str, object]] = {}
    selected_datasets: dict[str, dict[str, object]] = {}
    overrides: dict[str, dict[str, object]] = {}
    failures: list[dict[str, object]] = []
    reused_groups: set[str] = set()

    for group_id, group in GROUPS.items():
        if group.optional:
            continue
        diagnostics = validate_group_files(candidate_directory, candidate, group_id)
        candidate_state = next(
            (
                str(record.get("state") or "")
                for record in candidate.get("dataset_groups", [])
                if isinstance(record, dict) and record.get("group_id") == group_id
            ),
            "READY_NEW",
        )
        if group_id == "market" and candidate.get("status") != "READY":
            diagnostics.extend(
                str(value)
                for value in (candidate.get("validation") or {}).get("errors", [])
            )
        if not diagnostics:
            for filename in group.files:
                candidate_path = candidate_directory / filename
                output_path = output_directory / filename
                if candidate_path.resolve() != output_path.resolve():
                    copy_atomic(candidate_path, output_path)
                selected_release[filename] = copy.deepcopy(candidate_release[filename])
                if filename in candidate_datasets:
                    selected_datasets[filename] = copy.deepcopy(
                        candidate_datasets[filename]
                    )
            overrides[group_id] = {
                "state": (
                    candidate_state
                    if candidate_state in {"READY_NEW", "READY_WITH_EXCLUSIONS"}
                    else "READY_NEW"
                ),
                "mode": "FRESH_CANDIDATE",
            }
            continue

        failures.append(
            {
                "group_id": group_id,
                "attempt_state": "QUARANTINED",
                "diagnostics": diagnostics,
            }
        )
        previous_diagnostics = validate_group_files(
            previous_directory, previous, group_id
        )
        if previous_diagnostics:
            if group_id in CORE_GROUPS:
                raise CompositionError(
                    f"Core group {group_id} has no valid candidate or fallback: "
                    + "; ".join(previous_diagnostics)
                )
            raise CompositionError(
                f"Group {group_id} fallback is invalid: "
                + "; ".join(previous_diagnostics)
            )
        for filename in group.files:
            copy_atomic(previous_directory / filename, output_directory / filename)
        orphan_counts = filter_market_orphans(output_directory) if group_id == "market" else {}
        source_group_sha = str(previous_groups.get(group_id, {}).get("group_sha256") or "")
        if not source_group_sha:
            raise CompositionError(f"Fallback group lacks immutable identity: {group_id}")
        for filename in group.files:
            selected_release[filename] = (
                refreshed_release_record(output_directory / filename, previous_release[filename])
                if filename in orphan_counts
                else copy.deepcopy(previous_release[filename])
            )
            if filename in previous_datasets:
                dataset = copy.deepcopy(previous_datasets[filename])
                dataset.update(
                    {
                        "source_release_tag": source_tag,
                        "source_release_immutable": True,
                        "source_manifest_sha256": previous_manifest_sha,
                        "source_group_sha256": source_group_sha,
                    }
                )
                selected_datasets[filename] = dataset
        reused_groups.add(group_id)
        overrides[group_id] = {
            "state": "READY_WITH_EXCLUSIONS" if orphan_counts else "READY_REUSED",
            "mode": (
                "FILTERED_PINNED_IMMUTABLE_GROUP_REUSE"
                if orphan_counts
                else "PINNED_IMMUTABLE_GROUP_REUSE"
            ),
            "source_release_tag": source_tag,
            "source_release_immutable": True,
            "source_manifest_sha256": previous_manifest_sha,
            "source_group_sha256": source_group_sha,
            "exclusions": [
                {"file": filename, "reason": "UNKNOWN_SECURITY_ID", "rows": rows}
                for filename, rows in sorted(orphan_counts.items())
            ],
        }
        failures[-1]["released_state"] = overrides[group_id]["state"]
        if group_id == "market":
            candidate["aggregate"] = copy.deepcopy(previous.get("aggregate"))
            candidate["splits"] = copy.deepcopy(previous.get("splits"))
            expected = (candidate.get("validation") or {}).get(
                "expected_latest_xnys_session"
            )
            observed = (candidate.get("aggregate") or {}).get("max_date")
            lag = eligible_session_lag(observed, expected)
            candidate.setdefault("validation", {})["missing_eligible_sessions"] = lag
            candidate["validation"]["errors"] = []
            if lag > MARKET_READY_MAX_LAG_SESSIONS:
                raise CompositionError(
                    f"Fallback market group is {lag} eligible sessions stale"
                )

    notice_source = (
        candidate_directory / "NOTICE.md"
        if (candidate_directory / "NOTICE.md").is_file()
        else previous_directory / "NOTICE.md"
    )
    copy_atomic(notice_source, output_directory / "NOTICE.md")
    notice_record = (
        candidate_release.get("NOTICE.md")
        if notice_source.parent == candidate_directory
        else previous_release.get("NOTICE.md")
    )
    if notice_record is None:
        raise CompositionError("No NOTICE.md release identity is available")
    candidate["release_files"] = [
        selected_release[filename] for filename in sorted(selected_release)
    ] + [copy.deepcopy(notice_record)]
    candidate["datasets"] = [
        selected_datasets[filename] for filename in sorted(selected_datasets)
    ]
    remaining_orphans = identity_orphan_counts(
        output_directory, sorted(selected_release)
    )
    if remaining_orphans:
        raise CompositionError(f"Final release contains unknown security IDs: {remaining_orphans}")
    recompute_market_readiness(output_directory, candidate)
    merge_reused_coverage(candidate, previous, reused_groups)
    candidate["status"] = "READY"
    apply_dataset_groups(
        candidate,
        output_directory,
        group_overrides=overrides,
        candidate_group_failures=[],
        candidate_attempt_failures=failures,
    )
    for record in candidate["dataset_groups"]:
        group_id = str(record.get("group_id") or "")
        if group_id not in reused_groups:
            continue
        source_digest = (
            previous_groups.get(group_id, {}).get("group_sha256")
            or record["group_sha256"]
        )
        if record["state"] == "READY_REUSED" and source_digest != record["group_sha256"]:
            raise CompositionError(f"Reused group digest changed: {group_id}")
        record["source_group_sha256"] = source_digest
        if record["state"] == "READY_REUSED":
            record["state"] = freshness_state_for_reuse(
                group_id, record["freshness"]
            )
        if group_id in CORE_GROUPS and record["state"] == "STALE_DISABLED":
            raise CompositionError(f"Core fallback group is stale-disabled: {group_id}")

    temporary = output_directory / "manifest.json.compose.tmp"
    temporary.write_text(
        json.dumps(candidate, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, output_directory / "manifest.json")
    return candidate


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--previous-release", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--source-tag", required=True)
    args = parser.parse_args(argv)
    try:
        manifest = compose_release(
            Path(args.candidate).resolve(),
            Path(args.previous_release).resolve(),
            Path(args.out_dir).resolve(),
            args.source_tag,
        )
    except (CompositionError, OSError, ValueError, duckdb.Error) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(
        json.dumps(
            {
                "status": manifest["status"],
                "candidate_group_failures": len(
                    manifest["candidate_attempt_failures"]
                ),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
