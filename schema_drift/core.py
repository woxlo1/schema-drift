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

    def watch(
        self,
        interval: int = 60,
        on_change: Any = None,
        on_breaking: Any = None,
        auto_snapshot: bool = True,
        message: str = "auto-snapshot",
    ) -> None:
        """
        Poll the database every `interval` seconds and report schema changes.

        Args:
            interval:      Polling interval in seconds (default: 60).
            on_change:     Callback called with (diff) on any schema change.
            on_breaking:   Callback called with (diff) on breaking changes.
            auto_snapshot: Automatically save a snapshot on each change (default: True).
            message:       Message prefix for auto-snapshots.

        Press Ctrl+C to stop watching.

        Example::

            def alert(diff):
                print("Schema changed!", diff)

            drift.watch(interval=30, on_change=alert)
        """
        import time
        import threading

        print(f"{CYAN}watching{RESET}  {DIM}polling every {interval}s — press Ctrl+C to stop{RESET}")

        # Take a baseline snapshot if none exists
        history = self._load()
        if not history:
            print(f"{DIM}no snapshots found — taking baseline...{RESET}")
            self.snapshot("baseline (watch)")
            history = self._load()

        last_schema = history[-1]["schema"]
        checks = 0
        stop_event = threading.Event()

        # Only register SIGINT handler if we're in the main thread
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
                current = self._extract()
            except Exception as e:
                print(f"{RED}error extracting schema: {e}{RESET}")
                continue

            diff = _diff_schemas(last_schema, current)

            if not _has_changes(diff):
                print(f"{DIM}check #{checks}  no changes{RESET}")
                continue

            # Changes detected
            ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            print(f"\n{YELLOW}change detected{RESET}  {DIM}{ts}{RESET}")
            _print_diff(diff)

            breaking = bool(
                diff.get("tables_removed")
                or diff.get("columns_removed")
                or diff.get("columns_modified")
            )

            if auto_snapshot:
                snap_msg = f"{message} (check #{checks})"
                self.snapshot(snap_msg)

            if on_change:
                on_change(diff)

            if breaking and on_breaking:
                on_breaking(diff)

            last_schema = current
            print()

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


# ── MySQL support ──────────────────────────────────────────────────────────────

try:
    import mysql.connector
    HAS_MYSQL = True
except ImportError:
    HAS_MYSQL = False


def _extract_mysql(conn_or_url: Any) -> dict:
    if not HAS_MYSQL:
        raise ImportError("mysql-connector-python is required: pip install schema-drift[mysql]")

    close_after = False
    if isinstance(conn_or_url, str):
        # Parse mysql://user:pass@host:port/dbname
        from urllib.parse import urlparse
        p = urlparse(conn_or_url)
        conn = mysql.connector.connect(
            host=p.hostname or "localhost",
            port=p.port or 3306,
            user=p.username,
            password=p.password or "",
            database=p.path.lstrip("/"),
        )
        close_after = True
    else:
        conn = conn_or_url

    schema: dict = {}
    cur = conn.cursor(dictionary=True)
    db_name = conn.database

    # Tables + columns
    cur.execute("""
        SELECT
            c.TABLE_NAME   AS table_name,
            c.COLUMN_NAME  AS column_name,
            c.DATA_TYPE    AS data_type,
            c.COLUMN_TYPE  AS column_type,
            c.IS_NULLABLE  AS is_nullable,
            c.COLUMN_DEFAULT AS column_default,
            c.COLUMN_KEY   AS column_key,
            c.ORDINAL_POSITION AS ordinal_position
        FROM information_schema.COLUMNS c
        JOIN information_schema.TABLES t
          ON c.TABLE_NAME = t.TABLE_NAME
         AND c.TABLE_SCHEMA = t.TABLE_SCHEMA
        WHERE c.TABLE_SCHEMA = %s
          AND t.TABLE_TYPE = 'BASE TABLE'
        ORDER BY c.TABLE_NAME, c.ORDINAL_POSITION
    """, (db_name,))

    for row in cur.fetchall():
        table = row["table_name"]
        if table not in schema:
            schema[table] = {"columns": {}, "indexes": {}}
        schema[table]["columns"][row["column_name"]] = {
            "type": row["data_type"],
            "column_type": row["column_type"],
            "nullable": row["is_nullable"] == "YES",
            "default": row["column_default"],
            "primary_key": row["column_key"] == "PRI",
        }

    # Indexes
    cur.execute("""
        SELECT
            TABLE_NAME   AS table_name,
            INDEX_NAME   AS index_name,
            NON_UNIQUE   AS non_unique,
            COLUMN_NAME  AS column_name,
            SEQ_IN_INDEX AS seq
        FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA = %s
        ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX
    """, (db_name,))

    for row in cur.fetchall():
        table = row["table_name"]
        idx = row["index_name"]
        if table not in schema:
            continue
        if idx not in schema[table]["indexes"]:
            schema[table]["indexes"][idx] = {
                "columns": [],
                "unique": row["non_unique"] == 0,
                "primary": idx == "PRIMARY",
            }
        schema[table]["indexes"][idx]["columns"].append(row["column_name"])

    cur.close()
    if close_after:
        conn.close()

    return schema


# Extend detection and extraction to include MySQL

_detect_type_v2_prev = SchemaDrift._detect_type
_extract_v2_prev = SchemaDrift._extract


def _detect_type_v3(self, conn: Any, hint: str) -> str:
    if hint != "auto":
        return hint
    if isinstance(conn, str) and (conn.startswith("mysql://") or conn.startswith("mysql+")):
        return "mysql"
    if HAS_MYSQL and isinstance(conn, mysql.connector.connection.MySQLConnection):
        return "mysql"
    return _detect_type_v2_prev(self, conn, hint)


def _extract_v3(self) -> dict:
    if self._db_type == "mysql":
        return _extract_mysql(self._connection)
    return _extract_v2_prev(self)


SchemaDrift._detect_type = _detect_type_v3
SchemaDrift._extract = _extract_v3

# ── MariaDB support ────────────────────────────────────────────────────────────
# MariaDB is wire-compatible with MySQL — same extractor, just different URL scheme.

_detect_type_v3_prev = SchemaDrift._detect_type
_extract_v3_prev = SchemaDrift._extract


def _detect_type_v4(self, conn: Any, hint: str) -> str:
    if hint != "auto":
        return hint
    if isinstance(conn, str) and (
        conn.startswith("mariadb://") or conn.startswith("mariadb+")
    ):
        return "mariadb"
    return _detect_type_v3_prev(self, conn, hint)


def _extract_v4(self) -> dict:
    if self._db_type == "mariadb":
        # Reuse MySQL extractor — MariaDB is wire-compatible
        conn_or_url = self._connection
        if isinstance(conn_or_url, str):
            # Rewrite mariadb:// → mysql:// for connector
            conn_or_url = conn_or_url.replace("mariadb://", "mysql://", 1)
            conn_or_url = conn_or_url.replace("mariadb+", "mysql+", 1)
        return _extract_mysql(conn_or_url)
    return _extract_v3_prev(self)


SchemaDrift._detect_type = _detect_type_v4
SchemaDrift._extract = _extract_v4

# ── Oracle support ─────────────────────────────────────────────────────────────

try:
    import oracledb
    HAS_ORACLE = True
except ImportError:
    HAS_ORACLE = False


def _extract_oracle(conn_or_url: Any) -> dict:
    if not HAS_ORACLE:
        raise ImportError("oracledb is required: pip install schema-drift[oracle]")

    close_after = False
    if isinstance(conn_or_url, str):
        # oracle://user:pass@host:port/service  or  oracle://user:pass@host/service
        from urllib.parse import urlparse
        p = urlparse(conn_or_url)
        dsn = oracledb.makedsn(
            p.hostname or "localhost",
            p.port or 1521,
            service_name=p.path.lstrip("/"),
        )
        conn = oracledb.connect(user=p.username, password=p.password or "", dsn=dsn)
        close_after = True
    else:
        conn = conn_or_url

    schema: dict = {}
    cur = conn.cursor()

    # Tables + columns (current user's tables only)
    cur.execute("""
        SELECT
            t.TABLE_NAME,
            c.COLUMN_NAME,
            c.DATA_TYPE,
            c.DATA_LENGTH,
            c.DATA_PRECISION,
            c.DATA_SCALE,
            c.NULLABLE,
            c.DATA_DEFAULT,
            c.COLUMN_ID
        FROM USER_TABLES t
        JOIN USER_TAB_COLUMNS c ON t.TABLE_NAME = c.TABLE_NAME
        ORDER BY t.TABLE_NAME, c.COLUMN_ID
    """)

    for row in cur.fetchall():
        table = row[0]
        if table not in schema:
            schema[table] = {"columns": {}, "indexes": {}}

        col_type = row[2]
        if row[3] and col_type in ("VARCHAR2", "CHAR", "NVARCHAR2", "NCHAR"):
            col_type = f"{col_type}({row[3]})"
        elif row[4] is not None and row[5] is not None:
            col_type = f"{col_type}({row[4]},{row[5]})"

        schema[table]["columns"][row[1]] = {
            "type": col_type,
            "nullable": row[6] == "Y",
            "default": str(row[7]).strip() if row[7] else None,
        }

    # Indexes
    cur.execute("""
        SELECT
            i.TABLE_NAME,
            i.INDEX_NAME,
            i.UNIQUENESS,
            c.COLUMN_NAME,
            c.COLUMN_POSITION
        FROM USER_INDEXES i
        JOIN USER_IND_COLUMNS c ON i.INDEX_NAME = c.INDEX_NAME
        ORDER BY i.TABLE_NAME, i.INDEX_NAME, c.COLUMN_POSITION
    """)

    for row in cur.fetchall():
        table = row[0]
        idx = row[1]
        if table not in schema:
            continue
        if idx not in schema[table]["indexes"]:
            schema[table]["indexes"][idx] = {
                "columns": [],
                "unique": row[2] == "UNIQUE",
                "primary": idx.endswith("_PK") or idx.startswith("SYS_C"),
            }
        schema[table]["indexes"][idx]["columns"].append(row[3])

    cur.close()
    if close_after:
        conn.close()

    return schema


# Extend detection and extraction to include Oracle

_detect_type_v4_prev = SchemaDrift._detect_type
_extract_v4_prev = SchemaDrift._extract


def _detect_type_v5(self, conn: Any, hint: str) -> str:
    if hint != "auto":
        return hint
    if isinstance(conn, str) and (
        conn.startswith("oracle://") or conn.startswith("oracle+")
    ):
        return "oracle"
    if HAS_ORACLE and isinstance(conn, oracledb.Connection):
        return "oracle"
    return _detect_type_v4_prev(self, conn, hint)


def _extract_v5(self) -> dict:
    if self._db_type == "oracle":
        return _extract_oracle(self._connection)
    return _extract_v4_prev(self)


SchemaDrift._detect_type = _detect_type_v5
SchemaDrift._extract = _extract_v5
