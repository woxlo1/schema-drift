"""Tests for MySQL support (using mocks — no real DB needed)."""
import json
import pytest
from unittest.mock import MagicMock
from schema_drift import SchemaDrift
from schema_drift.backends import get_backend
from schema_drift.backends.mysql import MySQLBackend


def make_mock_conn(tables_rows, columns_rows, indexes_rows, db_name="testdb"):
    conn = MagicMock()
    conn.database = db_name
    cur = MagicMock()
    conn.cursor.return_value = cur
    cur.fetchall.side_effect = [columns_rows, indexes_rows]
    return conn, cur


COLUMNS_ROWS = [
    {"table_name": "users", "column_name": "id",    "data_type": "int",     "column_type": "int(11)",      "is_nullable": "NO",  "column_default": None, "column_key": "PRI", "ordinal_position": 1},
    {"table_name": "users", "column_name": "name",  "data_type": "varchar", "column_type": "varchar(255)", "is_nullable": "NO",  "column_default": None, "column_key": "",    "ordinal_position": 2},
    {"table_name": "users", "column_name": "email", "data_type": "varchar", "column_type": "varchar(255)", "is_nullable": "YES", "column_default": None, "column_key": "UNI", "ordinal_position": 3},
    {"table_name": "posts", "column_name": "id",    "data_type": "int",     "column_type": "int(11)",      "is_nullable": "NO",  "column_default": None, "column_key": "PRI", "ordinal_position": 1},
    {"table_name": "posts", "column_name": "title", "data_type": "varchar", "column_type": "varchar(255)", "is_nullable": "NO",  "column_default": None, "column_key": "",    "ordinal_position": 2},
]

INDEXES_ROWS = [
    {"table_name": "users", "index_name": "PRIMARY",   "non_unique": 0, "column_name": "id",    "seq": 1},
    {"table_name": "users", "index_name": "email_idx", "non_unique": 0, "column_name": "email", "seq": 1},
    {"table_name": "posts", "index_name": "PRIMARY",   "non_unique": 0, "column_name": "id",    "seq": 1},
]


def _extract(conn):
    import schema_drift.backends.mysql as m
    orig = m.HAS_MYSQL
    m.HAS_MYSQL = True
    backend = MySQLBackend.__new__(MySQLBackend)
    backend._conn = conn
    result = backend.extract()
    m.HAS_MYSQL = orig
    return result


def test_detect_mysql_url():
    assert isinstance(get_backend("mysql://user:pass@localhost/mydb"), MySQLBackend)

def test_detect_mysql_plus_url():
    assert isinstance(get_backend("mysql+connector://user:pass@localhost/mydb"), MySQLBackend)

def test_mysql_extracts_tables():
    conn, _ = make_mock_conn([], COLUMNS_ROWS, INDEXES_ROWS)
    schema = _extract(conn)
    assert "users" in schema
    assert "posts" in schema

def test_mysql_extracts_columns():
    conn, _ = make_mock_conn([], COLUMNS_ROWS, INDEXES_ROWS)
    schema = _extract(conn)
    assert "id" in schema["users"]["columns"]
    assert "name" in schema["users"]["columns"]
    assert "email" in schema["users"]["columns"]

def test_mysql_column_nullable():
    conn, _ = make_mock_conn([], COLUMNS_ROWS, INDEXES_ROWS)
    schema = _extract(conn)
    assert schema["users"]["columns"]["email"]["nullable"] is True
    assert schema["users"]["columns"]["name"]["nullable"] is False

def test_mysql_primary_key():
    conn, _ = make_mock_conn([], COLUMNS_ROWS, INDEXES_ROWS)
    schema = _extract(conn)
    assert schema["users"]["columns"]["id"]["primary_key"] is True
    assert schema["users"]["columns"]["name"]["primary_key"] is False

def test_mysql_extracts_indexes():
    conn, _ = make_mock_conn([], COLUMNS_ROWS, INDEXES_ROWS)
    schema = _extract(conn)
    assert "PRIMARY" in schema["users"]["indexes"]
    assert "email_idx" in schema["users"]["indexes"]
    assert schema["users"]["indexes"]["email_idx"]["unique"] is True

def test_mysql_index_is_primary():
    conn, _ = make_mock_conn([], COLUMNS_ROWS, INDEXES_ROWS)
    schema = _extract(conn)
    assert schema["users"]["indexes"]["PRIMARY"]["primary"] is True
    assert schema["users"]["indexes"]["email_idx"]["primary"] is False

def test_mysql_raises_without_driver():
    import schema_drift.backends.mysql as m
    orig = m.HAS_MYSQL
    m.HAS_MYSQL = False
    with pytest.raises(ImportError, match="mysql-connector-python"):
        m.MySQLBackend("mysql://localhost/test").extract()
    m.HAS_MYSQL = orig

def test_mysql_diff_integration(tmp_path):
    conn, _ = make_mock_conn([], COLUMNS_ROWS, INDEXES_ROWS)
    storage = tmp_path / "drift.json"
    import schema_drift.backends.mysql as m; m.HAS_MYSQL = True
    SchemaDrift(conn, db_type="mysql", storage_path=storage).snapshot("initial")

    updated_cols = COLUMNS_ROWS + [
        {"table_name": "users", "column_name": "bio", "data_type": "text",
         "column_type": "text", "is_nullable": "YES", "column_default": None,
         "column_key": "", "ordinal_position": 4},
    ]
    conn2, _ = make_mock_conn([], updated_cols, INDEXES_ROWS)
    diff = SchemaDrift(conn2, db_type="mysql", storage_path=storage).snapshot("add bio")
    m.HAS_MYSQL = False
    assert "bio" in [c["column"] for c in diff["columns_added"]]
