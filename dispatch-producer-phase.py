#!/usr/bin/env python3
"""Plan or send XNYS-aware producer deadline dispatches.

This command is intended to run from an external scheduler. GitHub cron remains a
backstop and is not used to calculate exchange deadlines.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import exchange_calendars as xcals


class DispatchError(RuntimeError):
    """Raised when a safe calendar-aware dispatch cannot be produced."""


def format_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def session_label(value: str):
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise DispatchError("Session must be YYYY-MM-DD") from exc
    calendar = xcals.get_calendar("XNYS")
    try:
        return calendar.date_to_session(parsed, direction="none")
    except ValueError as exc:
        raise DispatchError(f"{value} is not an XNYS session") from exc


def accounting_plan(session: str) -> list[dict[str, object]]:
    calendar = xcals.get_calendar("XNYS")
    label = session_label(session)
    close = calendar.session_close(label).to_pydatetime().astimezone(timezone.utc)
    decision_cutoff = close + timedelta(minutes=75)
    return [
        {
            "phase": "accounting",
            "expected_session": label.date().isoformat(),
            "scheduled_time": format_utc(close + timedelta(minutes=offset)),
            "decision_cutoff": format_utc(
                decision_cutoff if offset <= 75 else close + timedelta(minutes=135)
            ),
            "attempt": f"CLOSE_PLUS_{offset}",
            "idempotency_key": f"accounting:{label.date().isoformat()}:close-plus-{offset}",
            "early_close": close.hour < 20,
        }
        for offset in (45, 75, 120)
    ]


def sunday_plan(session: str) -> list[dict[str, object]]:
    label = session_label(session)
    session_day = label.date()
    days = (6 - session_day.weekday()) % 7 or 7
    sunday = session_day + timedelta(days=days)
    scheduled = datetime(
        sunday.year,
        sunday.month,
        sunday.day,
        17,
        15,
        tzinfo=ZoneInfo("America/Toronto"),
    ).astimezone(timezone.utc)
    cutoff = scheduled + timedelta(minutes=15)
    return [
        {
            "phase": "sunday",
            "expected_session": session_day.isoformat(),
            "scheduled_time": format_utc(scheduled),
            "decision_cutoff": format_utc(cutoff),
            "attempt": "SUNDAY_PREP",
            "idempotency_key": f"sunday:{session_day.isoformat()}:prep",
            "early_close": False,
        }
    ]


def dispatch(repository: str, token: str, payload: dict[str, object]) -> None:
    body = json.dumps(
        {"event_type": "producer_phase_deadline", "client_payload": payload},
        sort_keys=True,
    ).encode("utf-8")
    request = urllib.request.Request(
        f"https://api.github.com/repos/{repository}/dispatches",
        data=body,
        method="POST",
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "User-Agent": "usd-portfolio-external-dispatcher/1.0",
            "X-GitHub-Api-Version": "2026-03-10",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            if response.status != 204:
                raise DispatchError(f"GitHub dispatch returned HTTP {response.status}")
    except urllib.error.HTTPError as exc:
        raise DispatchError(f"GitHub dispatch returned HTTP {exc.code}") from exc
    except OSError as exc:
        raise DispatchError(f"GitHub dispatch failed: {exc}") from exc


def already_certified(repository: str, token: str, expected_session: str) -> bool:
    request = urllib.request.Request(
        "https://github.com/"
        + repository
        + "/releases/latest/download/benchmark-certification.json",
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
            "User-Agent": "usd-portfolio-external-dispatcher/1.0",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            value = json.loads(response.read())
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return False
        raise DispatchError(f"Certification lookup returned HTTP {exc.code}") from exc
    except (OSError, json.JSONDecodeError) as exc:
        raise DispatchError(f"Certification lookup failed: {exc}") from exc
    return bool(
        isinstance(value, dict)
        and value.get("status") == "CERTIFIED"
        and value.get("expected_latest_xnys_session") == expected_session
        and value.get("coverage") == {"required": 3, "valid": 3, "ratio": 1.0}
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("plan", "dispatch"))
    parser.add_argument("--session", required=True)
    parser.add_argument("--phase", choices=("accounting", "sunday", "all"), default="all")
    parser.add_argument("--attempt", help="Dispatch only the matching attempt label")
    parser.add_argument("--repository", default=os.environ.get("GITHUB_REPOSITORY", ""))
    parser.add_argument("--token-env", default="GITHUB_TOKEN")
    args = parser.parse_args(argv)
    try:
        events: list[dict[str, object]] = []
        if args.phase in {"accounting", "all"}:
            events.extend(accounting_plan(args.session))
        if args.phase in {"sunday", "all"}:
            events.extend(sunday_plan(args.session))
        if args.attempt:
            events = [event for event in events if event["attempt"] == args.attempt]
        if not events:
            raise DispatchError("No dispatch event matched the requested phase/attempt")
        if args.command == "dispatch":
            if not args.attempt:
                raise DispatchError("dispatch requires --attempt to avoid concurrent retries")
            token = os.environ.get(args.token_env, "")
            if not args.repository or not token:
                raise DispatchError("Dispatch requires --repository and a token")
            for event in events:
                if event["phase"] == "accounting" and already_certified(
                    args.repository, token, str(event["expected_session"])
                ):
                    event["dispatch_result"] = "SKIPPED_ALREADY_CERTIFIED"
                else:
                    dispatch(args.repository, token, event)
                    event["dispatch_result"] = "DISPATCHED"
        print(json.dumps({"events": events}, indent=2, sort_keys=True))
    except DispatchError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
