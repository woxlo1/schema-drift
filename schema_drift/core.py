"""
schema-drift: track why your schema changed, not just what changed.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from .backends import get_backend
from .diff import diff_schemas, has_changes, schema_hash, print_diff
from .diff import RESET, BOLD, GREEN, RED, YELLOW, CYAN, DIM

SNAPSHOTS_FILE = ".schema-drift.json"


class SchemaDrift:
    """
    Track schema changes with messages — like git log, but for your database.

    Supports: SQLite, PostgreSQL, MySQL, MariaDB, Oracle, OpenAPI, JSON Schema.
    """

    def __init__(
        self,
        connection: Any,
        db_type: str = "auto",
        storage_path: str | Path = SNAPSHOTS_FILE,
    ):
        self._connection = connection
        self._backend = get_backend(connection, db_type)
        self._storage = Path(storage_path)

    def snapshot(self, message: str = "") -> dict:
        """Capture the current schema and save it with a message."""
        current = self._backend.extract()
        history = self._load()

        diff: dict = {}
        if history:
            diff = diff_schemas(history[-1]["schema"], current)

        entry = {
            "id": schema_hash(current),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "message": message,
            "schema": current,
            "diff": diff,
            "stats": {
                "tables": len(current),
                "columns": sum(len(t.get("columns", {})) for t in current.values()),
            },
        }
        history.append(entry)
        self._save(history)

        n_tables = entry["stats"]["tables"]
        n_cols = entry["stats"]["columns"]
        print(f"{CYAN}snapshot{RESET}  {entry['id']}  {DIM}{entry['timestamp'][:10]}{RESET}")
        print(f"         {message or '(no message)'}")
        print(f"         {DIM}{n_tables} tables, {n_cols} columns{RESET}")

        if diff and has_changes(diff):
            print()
            print_diff(diff)

        return diff

    def diff(self, a: int = -2, b: int = -1) -> dict:
        """Show diff between two snapshots (default: last two)."""
        history = self._load()
        if len(history) < 2:
            print(f"{DIM}Need at least 2 snapshots to diff. Run snapshot() first.{RESET}")
            return {}

        snap_a = history[a]
        snap_b = history[b]
        diff = diff_schemas(snap_a["schema"], snap_b["schema"])
        print(f"{DIM}diff  {snap_a['id']} ({snap_a['timestamp'][:10]})  ->  {snap_b['id']} ({snap_b['timestamp'][:10]}){RESET}")
        print()
        print_diff(diff)
        return diff

    def log(self) -> None:
        """Print a compact history of all snapshots."""
        history = self._load()
        if not history:
            print(f"{DIM}No snapshots yet. Run snapshot() first.{RESET}")
            return
        print(f"{BOLD}{'date':<12} {'id':<14} {'tables':>7} {'cols':>6}  message{RESET}")
        print(DIM + "-" * 60 + RESET)
        for entry in history:
            date = entry["timestamp"][:10]
            tables = entry["stats"]["tables"]
            cols = entry["stats"]["columns"]
            msg = entry.get("message", "")
            print(f"{date:<12} {DIM}{entry['id']:<14}{RESET} {tables:>7} {cols:>6}  {msg}")

    def rollback(self, index: int) -> dict:
        """Return the schema at a given snapshot index."""
        history = self._load()
        if not history:
            raise ValueError("No snapshots found.")
        entry = history[index]
        print(f"{YELLOW}schema at snapshot {entry['id']} ({entry['timestamp'][:10]}){RESET}")
        print(f"{DIM}{entry.get('message', '')}{RESET}")
        return entry["schema"]

    def watch(self, interval: int = 60, on_change=None, on_breaking=None,
              auto_snapshot: bool = True, message: str = "auto-snapshot") -> None:
        """Poll for schema changes. Press Ctrl+C to stop."""
        from .watch import watch as _watch
        _watch(self, interval=interval, on_change=on_change,
               on_breaking=on_breaking, auto_snapshot=auto_snapshot, message=message)

    def export_json(self, index: int = -1) -> str:
        from .export import to_json
        return to_json(self._load()[index]["schema"])

    def export_csv(self, index: int = -1) -> str:
        from .export import to_csv
        return to_csv(self._load()[index]["schema"])

    def export_sql(self, index: int = -1, dialect: str = "generic") -> str:
        from .export import to_sql
        return to_sql(self._load()[index]["schema"], dialect=dialect)

    def _load(self) -> list:
        if not self._storage.exists():
            return []
        with self._storage.open() as f:
            return json.load(f)

    def _save(self, history: list) -> None:
        with self._storage.open("w") as f:
            json.dump(history, f, indent=2, default=str)
