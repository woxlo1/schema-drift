"""Tests for MariaDB support."""
import json
import pytest
from unittest.mock import patch
from schema_drift import SchemaDrift
from schema_drift.backends import get_backend
from schema_drift.backends.mariadb import MariaDBBackend
from tests.test_mysql import make_mock_conn, COLUMNS_ROWS, INDEXES_ROWS


def test_detect_mariadb_url():
    assert isinstance(get_backend("mariadb://user:pass@localhost/mydb"), MariaDBBackend)

def test_detect_mariadb_plus_url():
    assert isinstance(get_backend("mariadb+connector://user:pass@localhost/mydb"), MariaDBBackend)

def test_mariadb_extracts_tables(tmp_path):
    conn, _ = make_mock_conn([], COLUMNS_ROWS, INDEXES_ROWS)
    import schema_drift.backends.mysql as m; m.HAS_MYSQL = True
    drift = SchemaDrift(conn, db_type="mariadb", storage_path=tmp_path / "drift.json")
    drift.snapshot("initial")
    m.HAS_MYSQL = False
    history = json.loads((tmp_path / "drift.json").read_text())
    assert "users" in history[0]["schema"]
    assert "posts" in history[0]["schema"]

def test_mariadb_diff_detects_new_column(tmp_path):
    conn, _ = make_mock_conn([], COLUMNS_ROWS, INDEXES_ROWS)
    storage = tmp_path / "drift.json"
    import schema_drift.backends.mysql as m; m.HAS_MYSQL = True
    SchemaDrift(conn, db_type="mariadb", storage_path=storage).snapshot("initial")
    updated_cols = COLUMNS_ROWS + [
        {"table_name": "users", "column_name": "bio", "data_type": "text",
         "column_type": "text", "is_nullable": "YES", "column_default": None,
         "column_key": "", "ordinal_position": 4},
    ]
    conn2, _ = make_mock_conn([], updated_cols, INDEXES_ROWS)
    diff = SchemaDrift(conn2, db_type="mariadb", storage_path=storage).snapshot("add bio")
    m.HAS_MYSQL = False
    assert "bio" in [c["column"] for c in diff["columns_added"]]

def test_mariadb_url_rewritten_to_mysql():
    backend = MariaDBBackend("mariadb://user:pass@localhost/mydb")
    assert backend._backend._conn.startswith("mysql://")
