"""schema_drift.backends.postgres"""
from __future__ import annotations
from typing import Any
from .base import BaseBackend

try:
    import psycopg2
    import psycopg2.extras
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False


class PostgreSQLBackend(BaseBackend):
    def __init__(self, connection: Any):
        self._conn = connection

    @classmethod
    def accepts(cls, connection: Any) -> bool:
        if isinstance(connection, str):
            return connection.startswith(("postgresql://", "postgres://"))
        if HAS_PSYCOPG2 and isinstance(connection, psycopg2.extensions.connection):
            return True
        return False

    def extract(self) -> dict:
        if not HAS_PSYCOPG2:
            raise ImportError("psycopg2 is required: pip install schema-drift[postgres]")

        if isinstance(self._conn, str):
            conn = psycopg2.connect(self._conn)
            close_after = True
        else:
            conn = self._conn
            close_after = False

        schema: dict = {}
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT t.table_name, c.column_name, c.data_type,
                       c.character_maximum_length, c.is_nullable,
                       c.column_default, c.ordinal_position
                FROM information_schema.tables t
                JOIN information_schema.columns c
                  ON t.table_name = c.table_name
                 AND t.table_schema = c.table_schema
                WHERE t.table_schema = 'public' AND t.table_type = 'BASE TABLE'
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

            cur.execute("""
                SELECT t.relname AS table_name, i.relname AS index_name,
                       ix.indisunique AS is_unique, ix.indisprimary AS is_primary,
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
