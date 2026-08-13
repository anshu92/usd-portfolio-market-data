#!/usr/bin/env python3
"""Build a machine-readable proof bundle for a newly usable producer phase."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping


class ProofError(RuntimeError):
    """Raised when readiness evidence is incomplete or does not prove usability."""


def load(path: str, label: str) -> dict[str, object]:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ProofError(f"Cannot read {label}: {exc}") from exc
    if not isinstance(value, dict):
        raise ProofError(f"{label} root is not an object")
    return value


def digest(path: str) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def numeric(value: str, label: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise ProofError(f"Invalid {label}") from exc
    if parsed < 0:
        raise ProofError(f"Negative {label}")
    return parsed


def phase_record(manifest: Mapping[str, object], phase: str) -> Mapping[str, object]:
    for raw in manifest.get("phase_packs") or []:
        if isinstance(raw, dict) and raw.get("phase_id") == phase:
            return raw
    raise ProofError(f"Decision manifest lacks phase {phase}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-manifest", required=True)
    parser.add_argument("--benchmark-certification", required=True)
    parser.add_argument("--decision-manifest", required=True)
    parser.add_argument("--decision-pointer", required=True)
    parser.add_argument("--phase-result", required=True)
    parser.add_argument("--source-release-metadata", required=True)
    parser.add_argument("--decision-run-metadata", required=True)
    parser.add_argument("--pointer-run-metadata", required=True)
    parser.add_argument("--artifact-metadata", required=True)
    parser.add_argument("--test-results", required=True)
    parser.add_argument("--phase", choices=("accounting", "sunday"), required=True)
    parser.add_argument("--previous-phase-status", default="BLOCKED")
    parser.add_argument("--discovery-seconds", required=True)
    parser.add_argument("--download-seconds", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args(argv)
    try:
        source = load(args.source_manifest, "source manifest")
        certification = load(args.benchmark_certification, "benchmark certification")
        decision = load(args.decision_manifest, "decision manifest")
        pointer = load(args.decision_pointer, "decision pointer")
        phase_result_document = load(args.phase_result, "phase result")
        release = load(args.source_release_metadata, "source release metadata")
        decision_run = load(args.decision_run_metadata, "decision run metadata")
        pointer_run = load(args.pointer_run_metadata, "pointer run metadata")
        artifact = load(args.artifact_metadata, "artifact metadata")
        tests = load(args.test_results, "test results")
        phase_evaluation = phase_result_document.get("phase_evaluation")
        if not isinstance(phase_evaluation, dict) or phase_evaluation.get("status") != "USABLE":
            raise ProofError("Exact phase evaluation is not USABLE")
        if certification.get("status") != "CERTIFIED" or certification.get(
            "coverage"
        ) != {"required": 3, "valid": 3, "ratio": 1.0}:
            raise ProofError("Benchmark certification does not prove 100% coverage")
        securities = certification.get("securities")
        if not isinstance(securities, list) or len(securities) != 3:
            raise ProofError("Benchmark certification lacks three securities")
        if min(int(item.get("sessions", 0)) for item in securities) < 140 or min(
            int(item.get("weekly_observations", 0)) for item in securities
        ) < 26:
            raise ProofError("Benchmark history coverage is insufficient")
        if any(value != "PASS" for value in tests.values()):
            raise ProofError("Mandatory readiness tests did not all pass")
        if pointer.get("artifact_id") != artifact.get("id"):
            raise ProofError("Published pointer does not identify the tested artifact")
        if pointer.get("workflow_run_id") != decision_run.get("id"):
            raise ProofError("Published pointer does not identify the tested run")
        phase = phase_record(decision, args.phase)
        capability_values = decision.get("capabilities")
        if not isinstance(capability_values, list):
            raise ProofError("Decision manifest capabilities are invalid")
        capabilities = {
            str(value.get("capability_id")): value
            for value in capability_values
            if isinstance(value, dict) and value.get("capability_id")
        }
        validation_latency = phase_result_document.get("latency")
        if not isinstance(validation_latency, dict):
            raise ProofError("Validator output lacks scoped latency")
        payload = {
            "schema_version": "1.0.0",
            "status": "PHASE_USABLE_PROOF",
            "generated_at_utc": datetime.now(timezone.utc).isoformat().replace(
                "+00:00", "Z"
            ),
            "phase": args.phase,
            "phase_transition": {
                "from": args.previous_phase_status,
                "to": phase.get("status"),
                "operating_mode": phase_evaluation.get("decision_mode"),
            },
            "canonical_release": {
                "tag": release.get("tag_name"),
                "release_id": release.get("id"),
                "manifest_sha256": digest(args.source_manifest),
                "producer_workflow_run_id": (source.get("production_run") or {}).get(
                    "workflow_run_id"
                ),
                "producer_commit": (source.get("production_run") or {}).get(
                    "producer_commit"
                ),
                "benchmark_asset_ids": {
                    str(item.get("name")): item.get("id")
                    for item in release.get("assets") or []
                    if isinstance(item, dict)
                    and item.get("name")
                    in {
                        "benchmark-total-returns.parquet",
                        "benchmark-distributions.parquet",
                        "benchmark-certification.json",
                    }
                },
            },
            "decision_support": {
                "workflow_run_id": decision_run.get("id"),
                "artifact_id": artifact.get("id"),
                "artifact_name": artifact.get("name"),
                "artifact_digest": artifact.get("digest"),
                "pointer_publication_run_id": pointer_run.get("id"),
                "pointer_promotion_key": pointer.get("promotion_key"),
                "producer_commit": decision_run.get("head_sha"),
                "validator_set_sha256": (
                    decision.get("validator_identity") or {}
                ).get("validator_set_sha256"),
            },
            "benchmark": {
                "required_tickers": certification.get("required_tickers"),
                "securities": securities,
                "coverage": certification.get("coverage"),
                "expected_session": certification.get(
                    "expected_latest_xnys_session"
                ),
                "observed_session": certification.get(
                    "observed_latest_xnys_session"
                ),
                "source_retrieved_at_utc": certification.get(
                    "source_retrieved_at_utc"
                ),
                "certified_at_utc": certification.get("certified_at_utc"),
                "reconciliation": certification.get("validation"),
            },
            "capabilities": {
                key: value
                for key, value in capabilities.items()
                if key
                in {
                    "benchmark_identity",
                    "benchmark_market_current",
                    "certified_total_returns",
                    "funded_benchmark_inputs",
                    "broad_market_current",
                }
            },
            "source_watermarks": decision.get("source_watermarks"),
            "exact_validator_result": phase_result_document,
            "consumer_latency": {
                "artifact_discovery_seconds": numeric(
                    args.discovery_seconds, "artifact discovery latency"
                ),
                "artifact_download_seconds": numeric(
                    args.download_seconds, "artifact download latency"
                ),
                **validation_latency,
            },
            "tests": tests,
        }
        target = Path(args.out)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    except (OSError, ProofError, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(json.dumps({"status": payload["status"], "phase": args.phase}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
