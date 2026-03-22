"""schema_drift.backends.mysql"""
from __future__ import annotations
from typing import Any
from .base import BaseBackend

try:
    import mysql.connector
    HAS_MYSQL = True
except ImportError:
    HAS_MYSQL = False


class MySQLBackend(BaseBackend):
    def __init__(self, connection: Any):
        self._conn = connection

    @classmethod
    def accepts(cls, connection: Any) -> bool:
        if isinstance(connection, str):
            return connection.startswith(("mysql://", "mysql+"))
        if HAS_MYSQL and isinstance(connection, mysql.connector.connection.MySQLConnection):
            return True
        return False

    def extract(self) -> dict:
        if not HAS_MYSQL:
            raise ImportError("mysql-connector-python is required: pip install schema-drift[mysql]")

        if isinstance(self._conn, str):
            from urllib.parse import urlparse
            p = urlparse(self._conn)
            conn = mysql.connector.connect(
                host=p.hostname or "localhost",
                port=p.port or 3306,
                user=p.username,
                password=p.password or "",
                database=p.path.lstrip("/"),
            )
            close_after = True
        else:
            conn = self._conn
            close_after = False

        schema: dict = {}
        cur = conn.cursor(dictionary=True)
        db_name = conn.database

        cur.execute("""
            SELECT c.TABLE_NAME AS table_name, c.COLUMN_NAME AS column_name,
                   c.DATA_TYPE AS data_type, c.COLUMN_TYPE AS column_type,
                   c.IS_NULLABLE AS is_nullable, c.COLUMN_DEFAULT AS column_default,
                   c.COLUMN_KEY AS column_key, c.ORDINAL_POSITION AS ordinal_position
            FROM information_schema.COLUMNS c
            JOIN information_schema.TABLES t
              ON c.TABLE_NAME = t.TABLE_NAME AND c.TABLE_SCHEMA = t.TABLE_SCHEMA
            WHERE c.TABLE_SCHEMA = %s AND t.TABLE_TYPE = 'BASE TABLE'
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

        cur.execute("""
            SELECT TABLE_NAME AS table_name, INDEX_NAME AS index_name,
                   NON_UNIQUE AS non_unique, COLUMN_NAME AS column_name,
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
