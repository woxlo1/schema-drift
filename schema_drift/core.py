"""
schema-drift: track why your schema changed, not just what changed.
"""

from __future__ import annotations

import json
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import psycopg2
    import psycopg2.extras
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

try:
    import sqlite3
    HAS_SQLITE = True
except ImportError:
    HAS_SQLITE = False


SNAPSHOTS_FILE = ".schema-drift.json"


# ── Schema extractors ──────────────────────────────────────────────────────────

def _extract_postgres(conn_or_url: Any) -> dict:
    if not HAS_PSYCOPG2:
        raise ImportError("psycopg2 is required for PostgreSQL support: pip install psycopg2-binary")

    if isinstance(conn_or_url, str):
        conn = psycopg2.connect(conn_or_url)
        close_after = True
    else:
        conn = conn_or_url
        close_after = False

    schema: dict = {}
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        # Tables + columns
        cur.execute("""
            SELECT
                t.table_name,
                c.column_name,
                c.data_type,
                c.character_maximum_length,
                c.is_nullable,
                c.column_default,
                c.ordinal_position
            FROM information_schema.tables t
            JOIN information_schema.columns c
                ON t.table_name = c.table_name
                AND t.table_schema = c.table_schema
            WHERE t.table_schema = 'public'
              AND t.table_type = 'BASE TABLE'
            ORDER BY t.table_name, c.ordinal_position
        """)
        for row in cur.fetchall():
            table = row["table_name"]
            if table not in schema:
                schema[table] = {"columns": {}, "indexes": {}, "constraints": {}}
            col: dict = {
                "type": row["data_type"],
                "nullable": row["is_nullable"] == "YES",
                "default": row["column_default"],
            }
            if row["character_maximum_length"]:
                col["max_length"] = row["character_maximum_length"]
            schema[table]["columns"][row["column_name"]] = col

        # Indexes
        cur.execute("""
            SELECT
                t.relname AS table_name,
                i.relname AS index_name,
                ix.indisunique AS is_unique,
                ix.indisprimary AS is_primary,
                array_agg(a.attname ORDER BY a.attnum) AS columns
            FROM pg_class t
            JOIN pg_index ix ON t.oid = ix.indrelid
            JOIN pg_class i  ON i.oid = ix.indexrelid
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
            JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE n.nspname = 'public'
            GROUP BY t.relname, i.relname, ix.indisunique, ix.indisprimary
            ORDER BY t.relname, i.relname
        """)
        for row in cur.fetchall():
            table = row["table_name"]
            if table in schema:
                schema[table]["indexes"][row["index_name"]] = {
                    "columns": row["columns"],
                    "unique": row["is_unique"],
                    "primary": row["is_primary"],
                }

    if close_after:
        conn.close()

    return schema


def _extract_sqlite(conn_or_path: Any) -> dict:
    if isinstance(conn_or_path, str):
        conn = sqlite3.connect(conn_or_path)
        close_after = True
    else:
        conn = conn_or_path
        close_after = False

    schema: dict = {}
    cur = conn.cursor()

    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [row[0] for row in cur.fetchall() if not row[0].startswith("sqlite_")]

    for table in tables:
        schema[table] = {"columns": {}, "indexes": {}}
        cur.execute(f"PRAGMA table_info({table})")
        for row in cur.fetchall():
            schema[table]["columns"][row[1]] = {
                "type": row[2],
                "nullable": row[3] == 0,
                "default": row[4],
                "primary_key": row[5] > 0,
            }

        cur.execute(f"PRAGMA index_list({table})")
        for idx in cur.fetchall():
            cur.execute(f"PRAGMA index_info({idx[1]})")
            cols = [r[2] for r in cur.fetchall()]
            schema[table]["indexes"][idx[1]] = {
                "columns": cols,
                "unique": bool(idx[2]),
            }

    if close_after:
        conn.close()

    return schema


# ── Diff engine ────────────────────────────────────────────────────────────────

def _diff_schemas(old: dict, new: dict) -> dict:
    changes: dict = {
        "tables_added": [],
        "tables_removed": [],
        "columns_added": [],
        "columns_removed": [],
        "columns_modified": [],
        "indexes_added": [],
        "indexes_removed": [],
    }

    old_tables = set(old)
    new_tables = set(new)

    changes["tables_added"] = sorted(new_tables - old_tables)
    changes["tables_removed"] = sorted(old_tables - new_tables)

    for table in old_tables & new_tables:
        old_cols = old[table].get("columns", {})
        new_cols = new[table].get("columns", {})

        for col in sorted(set(new_cols) - set(old_cols)):
            changes["columns_added"].append({"table": table, "column": col, "definition": new_cols[col]})

        for col in sorted(set(old_cols) - set(new_cols)):
            changes["columns_removed"].append({"table": table, "column": col, "was": old_cols[col]})

        for col in sorted(set(old_cols) & set(new_cols)):
            if old_cols[col] != new_cols[col]:
                changes["columns_modified"].append({
                    "table": table,
                    "column": col,
                    "before": old_cols[col],
                    "after": new_cols[col],
                })

        old_idx = old[table].get("indexes", {})
        new_idx = new[table].get("indexes", {})

        for idx in sorted(set(new_idx) - set(old_idx)):
            changes["indexes_added"].append({"table": table, "index": idx, "definition": new_idx[idx]})

        for idx in sorted(set(old_idx) - set(new_idx)):
            changes["indexes_removed"].append({"table": table, "index": idx})

    return changes


def _has_changes(diff: dict) -> bool:
    return any(diff.values())


def _schema_hash(schema: dict) -> str:
    raw = json.dumps(schema, sort_keys=True)
    return hashlib.sha256(raw.encode()).hexdigest()[:12]


# ── Pretty printers ────────────────────────────────────────────────────────────

RESET  = "\033[0m"
BOLD   = "\033[1m"
GREEN  = "\033[32m"
RED    = "\033[31m"
YELLOW = "\033[33m"
CYAN   = "\033[36m"
DIM    = "\033[2m"


def _print_diff(diff: dict) -> None:
    if not _has_changes(diff):
        print(f"{DIM}No schema changes detected.{RESET}")
        return

    for table in diff["tables_added"]:
        print(f"{GREEN}+ table  {BOLD}{table}{RESET}")
    for table in diff["tables_removed"]:
        print(f"{RED}- table  {BOLD}{table}{RESET}")

    for c in diff["columns_added"]:
        t = c["definition"].get("type", "")
        print(f"{GREEN}+ {c['table']}.{BOLD}{c['column']}{RESET}{GREEN}  ({t}){RESET}")

    for c in diff["columns_removed"]:
        t = c["was"].get("type", "")
        print(f"{RED}- {c['table']}.{BOLD}{c['column']}{RESET}{RED}  ({t}){RESET}")

    for c in diff["columns_modified"]:
        before = c["before"].get("type", "")
        after  = c["after"].get("type", "")
        print(f"{YELLOW}~ {c['table']}.{BOLD}{c['column']}{RESET}{YELLOW}  {before} → {after}{RESET}")

    for i in diff["indexes_added"]:
        cols = ", ".join(i["definition"].get("columns", []))
        print(f"{GREEN}+ index  {BOLD}{i['index']}{RESET}{GREEN} on {i['table']} ({cols}){RESET}")

    for i in diff["indexes_removed"]:
        print(f"{RED}- index  {BOLD}{i['index']}{RESET}{RED} on {i['table']}{RESET}")


# ── Main class ─────────────────────────────────────────────────────────────────

class SchemaDrift:
    """
    Track schema changes with messages — like git log, but for your database.

    Usage::

        from schema_drift import SchemaDrift

        drift = SchemaDrift("postgresql://user:pass@localhost/mydb")
        drift.snapshot("initial schema")
        # ... later, after ALTER TABLE ...
        drift.snapshot("added users.email for auth feature")
        drift.diff()
        drift.log()
    """

    def __init__(
        self,
        connection: Any,
        db_type: str = "auto",
        storage_path: str | Path = SNAPSHOTS_FILE,
    ):
        """
        Args:
            connection:   DB connection object or connection string.
                          Supported: psycopg2 connection, sqlite3 connection,
                          PostgreSQL URL (postgresql://...), SQLite file path.
            db_type:      "postgres", "sqlite", or "auto" (default).
            storage_path: Where to store snapshot history. Defaults to
                          .schema-drift.json in the current directory.
        """
        self._connection = connection
        self._db_type = self._detect_type(connection, db_type)
        self._storage = Path(storage_path)

    # ── Public API ─────────────────────────────────────────────────────────────

    def snapshot(self, message: str = "") -> dict:
        """
        Capture the current schema and save it with an optional message.

        Returns the diff from the previous snapshot (empty dict if first snapshot).
        """
        current = self._extract()
        history = self._load()

        diff: dict = {}
        if history:
            diff = _diff_schemas(history[-1]["schema"], current)

        entry = {
            "id": _schema_hash(current),
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

        if diff and _has_changes(diff):
            print()
            _print_diff(diff)

        return diff

    def diff(self, a: int = -2, b: int = -1) -> dict:
        """
        Show diff between two snapshots (default: last two).

        Args:
            a: Index of the older snapshot.
            b: Index of the newer snapshot.
        """
        history = self._load()
        if len(history) < 2:
            print(f"{DIM}Need at least 2 snapshots to diff. Run snapshot() first.{RESET}")
            return {}

        snap_a = history[a]
        snap_b = history[b]
        diff = _diff_schemas(snap_a["schema"], snap_b["schema"])

        print(f"{DIM}diff  {snap_a['id']} ({snap_a['timestamp'][:10]})  →  {snap_b['id']} ({snap_b['timestamp'][:10]}){RESET}")
        print()
        _print_diff(diff)
        return diff

    def log(self) -> None:
        """Print a compact history of all snapshots."""
        history = self._load()
        if not history:
            print(f"{DIM}No snapshots yet. Run snapshot() first.{RESET}")
            return

        print(f"{BOLD}{'date':<12} {'id':<14} {'tables':>7} {'cols':>6}  message{RESET}")
        print(DIM + "─" * 60 + RESET)
        for entry in history:
            date = entry["timestamp"][:10]
            sid = entry["id"]
            tables = entry["stats"]["tables"]
            cols = entry["stats"]["columns"]
            msg = entry.get("message", "")
            print(f"{date:<12} {DIM}{sid:<14}{RESET} {tables:>7} {cols:>6}  {msg}")

    def rollback(self, index: int) -> dict:
        """
        Return the schema at a given snapshot index (does not modify the DB).

        Useful for inspecting what the schema looked like at a point in time.
        """
        history = self._load()
        if not history:
            raise ValueError("No snapshots found.")
        entry = history[index]
        print(f"{YELLOW}schema at snapshot {entry['id']} ({entry['timestamp'][:10]}){RESET}")
        print(f"{DIM}{entry.get('message', '')}{RESET}")
        return entry["schema"]

    # ── Internals ──────────────────────────────────────────────────────────────

    def _detect_type(self, conn: Any, hint: str) -> str:
        if hint != "auto":
            return hint
        if isinstance(conn, str):
            if conn.startswith("postgresql") or conn.startswith("postgres"):
                return "postgres"
            return "sqlite"
        if HAS_PSYCOPG2 and isinstance(conn, psycopg2.extensions.connection):
            return "postgres"
        return "sqlite"

    def _extract(self) -> dict:
        if self._db_type == "postgres":
            return _extract_postgres(self._connection)
        return _extract_sqlite(self._connection)

    def _load(self) -> list:
        if not self._storage.exists():
            return []
        with self._storage.open() as f:
            return json.load(f)

    def _save(self, history: list) -> None:
        with self._storage.open("w") as f:
            json.dump(history, f, indent=2, default=str)


# ── OpenAPI / JSON Schema support (injected at import time) ───────────────────
# Monkey-patch _detect_type and _extract to support openapi/jsonschema sources.

_original_detect_type = SchemaDrift._detect_type
_original_extract = SchemaDrift._extract


def _detect_type_v2(self, conn: Any, hint: str) -> str:
    if hint != "auto":
        return hint
    from schema_drift.openapi import detect_source_type
    t = detect_source_type(conn)
    if t in ("openapi", "jsonschema"):
        return t
    return _original_detect_type(self, conn, hint)


def _extract_v2(self) -> dict:
    if self._db_type == "openapi":
        from schema_drift.openapi import _extract_openapi
        return _extract_openapi(self._connection)
    if self._db_type == "jsonschema":
        from schema_drift.openapi import _extract_json_schema
        return _extract_json_schema(self._connection)
    return _original_extract(self)


SchemaDrift._detect_type = _detect_type_v2
SchemaDrift._extract = _extract_v2
