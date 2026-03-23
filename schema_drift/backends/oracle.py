"""schema_drift.backends.oracle"""
from __future__ import annotations
from typing import Any
from .base import BaseBackend

try:
    import oracledb
    HAS_ORACLE = True
except ImportError:
    HAS_ORACLE = False


class OracleBackend(BaseBackend):
    def __init__(self, connection: Any):
        self._conn = connection

    @classmethod
    def accepts(cls, connection: Any) -> bool:
        if isinstance(connection, str):
            return connection.startswith(("oracle://", "oracle+"))
        if HAS_ORACLE and isinstance(connection, oracledb.Connection):
            return True
        return False

    def extract(self) -> dict:
        if not HAS_ORACLE:
            raise ImportError("oracledb is required: pip install schema-drift[oracle] @ git+https://github.com/woxlo1/schema-drift.git")

        if isinstance(self._conn, str):
            from urllib.parse import urlparse
            p = urlparse(self._conn)
            dsn = oracledb.makedsn(p.hostname or "localhost", p.port or 1521, service_name=p.path.lstrip("/"))
            conn = oracledb.connect(user=p.username, password=p.password or "", dsn=dsn)
            close_after = True
        else:
            conn = self._conn
            close_after = False

        schema: dict = {}
        cur = conn.cursor()

        cur.execute("""
            SELECT t.TABLE_NAME, c.COLUMN_NAME, c.DATA_TYPE,
                   c.DATA_LENGTH, c.DATA_PRECISION, c.DATA_SCALE,
                   c.NULLABLE, c.DATA_DEFAULT, c.COLUMN_ID
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

        cur.execute("""
            SELECT i.TABLE_NAME, i.INDEX_NAME, i.UNIQUENESS,
                   c.COLUMN_NAME, c.COLUMN_POSITION
            FROM USER_INDEXES i
            JOIN USER_IND_COLUMNS c ON i.INDEX_NAME = c.INDEX_NAME
            ORDER BY i.TABLE_NAME, i.INDEX_NAME, c.COLUMN_POSITION
        """)
        for row in cur.fetchall():
            table, idx = row[0], row[1]
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
