"""Tests for Oracle support (using mocks — no real DB needed)."""
import json
import pytest
from unittest.mock import MagicMock
from schema_drift import SchemaDrift
from schema_drift.backends import get_backend
oracledb = pytest.importorskip("oracledb", reason="oracledb not installed")
from schema_drift.backends.oracle import OracleBackend

COLUMNS_ROWS = [
    ("USERS", "ID",    "NUMBER",   None, 10, 0, "N", None, 1),
    ("USERS", "NAME",  "VARCHAR2", 255,  None, None, "N", None, 2),
    ("USERS", "EMAIL", "VARCHAR2", 255,  None, None, "Y", None, 3),
    ("POSTS", "ID",    "NUMBER",   None, 10, 0, "N", None, 1),
    ("POSTS", "TITLE", "VARCHAR2", 255,  None, None, "N", None, 2),
]

INDEXES_ROWS = [
    ("USERS", "USERS_PK",    "UNIQUE", "ID",    1),
    ("USERS", "USERS_EMAIL", "UNIQUE", "EMAIL", 1),
    ("POSTS", "POSTS_PK",    "UNIQUE", "ID",    1),
]


def make_mock_conn():
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur
    cur.fetchall.side_effect = [COLUMNS_ROWS, INDEXES_ROWS]
    return conn, cur


def _extract(conn):
    backend = OracleBackend.__new__(OracleBackend)
    backend._conn = conn
    return backend.extract()


def test_detect_oracle_url():
    assert isinstance(get_backend("oracle://user:pass@localhost/ORCL"), OracleBackend)

def test_detect_oracle_plus_url():
    assert isinstance(get_backend("oracle+oracledb://user:pass@localhost/ORCL"), OracleBackend)

def test_oracle_extracts_tables():
    conn, _ = make_mock_conn()
    assert "USERS" in _extract(conn)
    conn, _ = make_mock_conn()
    assert "POSTS" in _extract(conn)

def test_oracle_extracts_columns():
    conn, _ = make_mock_conn()
    cols = _extract(conn)["USERS"]["columns"]
    assert "ID" in cols and "NAME" in cols and "EMAIL" in cols

def test_oracle_column_type_with_length():
    conn, _ = make_mock_conn()
    assert _extract(conn)["USERS"]["columns"]["NAME"]["type"] == "VARCHAR2(255)"

def test_oracle_column_nullable():
    conn, _ = make_mock_conn()
    schema = _extract(conn)
    assert schema["USERS"]["columns"]["EMAIL"]["nullable"] is True
    assert schema["USERS"]["columns"]["NAME"]["nullable"] is False

def test_oracle_extracts_indexes():
    conn, _ = make_mock_conn()
    schema = _extract(conn)
    assert "USERS_PK" in schema["USERS"]["indexes"]
    assert schema["USERS"]["indexes"]["USERS_EMAIL"]["unique"] is True

def test_oracle_raises_without_driver():
    import schema_drift.backends.oracle as m
    orig = m.HAS_ORACLE
    m.HAS_ORACLE = False
    with pytest.raises(ImportError, match="oracledb"):
        m.OracleBackend("oracle://localhost/ORCL").extract()
    m.HAS_ORACLE = orig

def test_oracle_diff_integration(tmp_path):
    conn, _ = make_mock_conn()
    storage = tmp_path / "drift.json"
    SchemaDrift(conn, db_type="oracle", storage_path=storage).snapshot("initial")

    updated = COLUMNS_ROWS + [("USERS", "BIO", "CLOB", None, None, None, "Y", None, 4)]
    conn2, cur2 = make_mock_conn()
    cur2.fetchall.side_effect = [updated, INDEXES_ROWS]
    diff = SchemaDrift(conn2, db_type="oracle", storage_path=storage).snapshot("add BIO")
    assert "BIO" in [c["column"] for c in diff["columns_added"]]
