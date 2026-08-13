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


def decision_event(
    *,
    phase: str,
    expected_session: date,
    window_id: str,
    scheduled: datetime,
    cutoff: datetime,
    early_close: bool,
) -> dict[str, object]:
    return {
        "event_type": "decision_phase_deadline",
        "phase": phase,
        "expected_session": expected_session.isoformat(),
        "window_id": window_id,
        "scheduled_time": format_utc(scheduled),
        "decision_cutoff": format_utc(cutoff),
        "artifact_deadline": format_utc(cutoff),
        "attempt": window_id,
        "idempotency_key": f"decision:{phase}:{expected_session.isoformat()}:{window_id.lower()}",
        "early_close": early_close,
    }


def accounting_source_plan(session: str) -> list[dict[str, object]]:
    calendar = xcals.get_calendar("XNYS")
    label = session_label(session)
    close = calendar.session_close(label).to_pydatetime().astimezone(timezone.utc)
    return [
        {
            "event_type": "producer_phase_deadline",
            "phase": "accounting",
            "expected_session": label.date().isoformat(),
            "scheduled_time": format_utc(close + timedelta(minutes=offset)),
            "decision_cutoff": format_utc(close + timedelta(minutes=45)),
            "artifact_deadline": format_utc(close + timedelta(minutes=45)),
            "attempt": f"SOURCE_CLOSE_PLUS_{offset}",
            "idempotency_key": f"source:accounting:{label.date().isoformat()}:close-plus-{offset}",
            "early_close": close.hour < 20,
        }
        for offset in (45, 75, 120)
    ]


def decision_phase_plan(session: str) -> list[dict[str, object]]:
    calendar = xcals.get_calendar("XNYS")
    label = session_label(session)
    session_day = label.date()
    close = calendar.session_close(label).to_pydatetime().astimezone(timezone.utc)
    early_close = close.hour < 20
    next_session = calendar.next_session(label).date()
    task_zone = ZoneInfo("America/Toronto")

    def local(day: date, hour: int, minute: int) -> datetime:
        return datetime(day.year, day.month, day.day, hour, minute, tzinfo=task_zone)

    events = [
        decision_event(
            phase="pre_open",
            expected_session=session_day,
            window_id="PRE_OPEN",
            scheduled=local(next_session, 7, 55),
            cutoff=local(next_session, 8, 10),
            early_close=early_close,
        ),
        decision_event(
            phase="execution_research",
            expected_session=session_day,
            window_id="EXECUTION_RESEARCH",
            scheduled=local(next_session, 9, 23),
            cutoff=local(next_session, 9, 38),
            early_close=early_close,
        ),
        decision_event(
            phase="exception_monitoring",
            expected_session=session_day,
            window_id="OPEN_EXCEPTION",
            scheduled=local(next_session, 9, 40),
            cutoff=local(next_session, 9, 55),
            early_close=early_close,
        ),
        decision_event(
            phase="exception_monitoring",
            expected_session=session_day,
            window_id="CLOSE_EXCEPTION",
            scheduled=local(next_session, 15, 10),
            cutoff=local(next_session, 15, 25),
            early_close=early_close,
        ),
        decision_event(
            phase="terminal_review",
            expected_session=session_day,
            window_id="TERMINAL_REVIEW",
            scheduled=close + timedelta(minutes=5),
            cutoff=close + timedelta(minutes=20),
            early_close=early_close,
        ),
        decision_event(
            phase="accounting",
            expected_session=session_day,
            window_id="ACCOUNTING",
            scheduled=close + timedelta(minutes=40),
            cutoff=close + timedelta(minutes=45),
            early_close=early_close,
        ),
    ]
    days_to_saturday = (5 - session_day.weekday()) % 7 or 7
    saturday = session_day + timedelta(days=days_to_saturday)
    events.append(
        decision_event(
            phase="saturday_replay",
            expected_session=session_day,
            window_id="SATURDAY_REPLAY",
            scheduled=local(saturday, 8, 15),
            cutoff=local(saturday, 8, 30),
            early_close=early_close,
        )
    )
    days = (6 - session_day.weekday()) % 7 or 7
    sunday = session_day + timedelta(days=days)
    events.append(
        decision_event(
            phase="sunday",
            expected_session=session_day,
            window_id="SUNDAY",
            scheduled=local(sunday, 17, 15),
            cutoff=local(sunday, 17, 30),
            early_close=early_close,
        )
    )
    return events


def accounting_plan(session: str) -> list[dict[str, object]]:
    """Compatibility wrapper for source-refresh correction attempts."""
    return accounting_source_plan(session)


def sunday_plan(session: str) -> list[dict[str, object]]:
    """Compatibility wrapper for the Sunday decision artifact target."""
    return [event for event in decision_phase_plan(session) if event["phase"] == "sunday"]


def dispatch(repository: str, token: str, payload: dict[str, object]) -> None:
    event_type = str(payload.get("event_type") or "")
    if event_type not in {"producer_phase_deadline", "decision_phase_deadline"}:
        raise DispatchError(f"Unsupported repository event type: {event_type}")
    client_payload = {key: value for key, value in payload.items() if key != "event_type"}
    body = json.dumps(
        {"event_type": event_type, "client_payload": client_payload},
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
    parser.add_argument(
        "--phase",
        choices=(
            "pre_open",
            "execution_research",
            "exception_monitoring",
            "terminal_review",
            "accounting",
            "saturday_replay",
            "sunday",
            "source_accounting",
            "all",
        ),
        default="all",
    )
    parser.add_argument("--attempt", help="Dispatch only the matching attempt label")
    parser.add_argument("--repository", default=os.environ.get("GITHUB_REPOSITORY", ""))
    parser.add_argument("--token-env", default="GITHUB_TOKEN")
    args = parser.parse_args(argv)
    try:
        events: list[dict[str, object]] = []
        decision_events = decision_phase_plan(args.session)
        if args.phase == "all":
            events.extend(decision_events)
            events.extend(accounting_source_plan(args.session))
        elif args.phase == "source_accounting":
            events.extend(accounting_source_plan(args.session))
        else:
            events.extend(event for event in decision_events if event["phase"] == args.phase)
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
                if event["event_type"] == "producer_phase_deadline" and already_certified(
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
