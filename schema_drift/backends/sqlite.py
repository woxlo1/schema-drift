"""schema_drift.backends.sqlite"""
from __future__ import annotations
from typing import Any
from .base import BaseBackend

try:
    import sqlite3
    HAS_SQLITE = True
except ImportError:
    HAS_SQLITE = False


class SQLiteBackend(BaseBackend):
    def __init__(self, connection: Any):
        self._conn = connection

    @classmethod
    def accepts(cls, connection: Any) -> bool:
        if isinstance(connection, str):
            return not any(connection.startswith(p) for p in (
                "postgresql", "postgres", "mysql", "mariadb", "oracle"
            ))
        if HAS_SQLITE and isinstance(connection, sqlite3.Connection):
            return True
        return False

    def extract(self) -> dict:
        if isinstance(self._conn, str):
            conn = sqlite3.connect(self._conn)
            close_after = True
        else:
            conn = self._conn
            close_after = False

        schema: dict = {}
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = [r[0] for r in cur.fetchall() if not r[0].startswith("sqlite_")]

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
