"""schema_drift.watch — real-time schema monitoring"""
from __future__ import annotations
import threading
from datetime import datetime, timezone
from typing import Any, Callable

from .diff import diff_schemas, has_changes, is_breaking, print_diff, CYAN, DIM, YELLOW, RED, RESET


def watch(
    drift: Any,
    interval: int = 60,
    on_change: Callable | None = None,
    on_breaking: Callable | None = None,
    auto_snapshot: bool = True,
    message: str = "auto-snapshot",
) -> None:
    """
    Poll the database every `interval` seconds and report schema changes.

    Args:
        drift:         SchemaDrift instance to poll.
        interval:      Polling interval in seconds (default: 60).
        on_change:     Callback called with (diff) on any schema change.
        on_breaking:   Callback called with (diff) on breaking changes.
        auto_snapshot: Automatically save a snapshot on each change (default: True).
        message:       Message prefix for auto-snapshots.

    Press Ctrl+C to stop watching.
    """
    print(f"{CYAN}watching{RESET}  {DIM}polling every {interval}s — press Ctrl+C to stop{RESET}")

    history = drift._load()
    if not history:
        print(f"{DIM}no snapshots found — taking baseline...{RESET}")
        drift.snapshot("baseline (watch)")
        history = drift._load()

    last_schema = history[-1]["schema"]
    checks = 0
    stop_event = threading.Event()

    if threading.current_thread() is threading.main_thread():
        import signal
        def _stop(sig, frame):
            print(f"\n{DIM}stopped after {checks} checks{RESET}")
            stop_event.set()
        signal.signal(signal.SIGINT, _stop)

    while not stop_event.is_set():
        stop_event.wait(timeout=interval)
        if stop_event.is_set():
            break
        checks += 1

        try:
            current = drift._backend.extract()
        except Exception as e:
            print(f"{RED}error extracting schema: {e}{RESET}")
            continue

        diff = diff_schemas(last_schema, current)

        if not has_changes(diff):
            print(f"{DIM}check #{checks}  no changes{RESET}")
            continue

        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n{YELLOW}change detected{RESET}  {DIM}{ts}{RESET}")
        print_diff(diff)

        if auto_snapshot:
            drift.snapshot(f"{message} (check #{checks})")

        if on_change:
            on_change(diff)

        if is_breaking(diff) and on_breaking:
            on_breaking(diff)

        last_schema = current
        print()
