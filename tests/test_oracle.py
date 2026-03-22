"""Tests for Oracle support (using mocks — no real DB needed)."""
import pytest
import json
from unittest.mock import MagicMock, patch


COLUMNS_ROWS = [
    ("USERS", "ID",    "NUMBER",   None, 10, 0, "N", None, 1),
    ("USERS", "NAME",  "VARCHAR2", 255,  None, None, "N", None, 2),
    ("USERS", "EMAIL", "VARCHAR2", 255,  None, None, "Y", None, 3),
    ("POSTS", "ID",    "NUMBER",   None, 10, 0, "N", None, 1),
    ("POSTS", "TITLE", "VARCHAR2", 255,  None, None, "N", None, 2),
]

INDEXES_ROWS = [
    ("USERS", "USERS_PK",    "UNIQUE", "ID",    1),
    ("USERS", "USERS_EMAIL",  "UNIQUE", "EMAIL", 1),
    ("POSTS", "POSTS_PK",    "UNIQUE", "ID",    1),
]


def make_mock_conn():
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur
    cur.fetchall.side_effect = [COLUMNS_ROWS, INDEXES_ROWS]
    return conn, cur


@pytest.fixture
def mock_oracle_conn():
    return make_mock_conn()


def test_detect_oracle_url():
    from schema_drift import SchemaDrift
    drift = SchemaDrift.__new__(SchemaDrift)
    result = drift._detect_type("oracle://user:pass@localhost/ORCL", "auto")
    assert result == "oracle"


def test_detect_oracle_plus_url():
    from schema_drift import SchemaDrift
    drift = SchemaDrift.__new__(SchemaDrift)
    result = drift._detect_type("oracle+oracledb://user:pass@localhost/ORCL", "auto")
    assert result == "oracle"


def test_oracle_extracts_tables(mock_oracle_conn):
    conn, _ = mock_oracle_conn
    with patch("schema_drift.core.HAS_ORACLE", True):
        from schema_drift.core import _extract_oracle
        schema = _extract_oracle(conn)

    assert "USERS" in schema
    assert "POSTS" in schema


def test_oracle_extracts_columns(mock_oracle_conn):
    conn, _ = mock_oracle_conn
    with patch("schema_drift.core.HAS_ORACLE", True):
        from schema_drift.core import _extract_oracle
        schema = _extract_oracle(conn)

    assert "ID" in schema["USERS"]["columns"]
    assert "NAME" in schema["USERS"]["columns"]
    assert "EMAIL" in schema["USERS"]["columns"]


def test_oracle_column_type_with_length(mock_oracle_conn):
    conn, _ = mock_oracle_conn
    with patch("schema_drift.core.HAS_ORACLE", True):
        from schema_drift.core import _extract_oracle
        schema = _extract_oracle(conn)

    assert schema["USERS"]["columns"]["NAME"]["type"] == "VARCHAR2(255)"


def test_oracle_column_nullable(mock_oracle_conn):
    conn, _ = mock_oracle_conn
    with patch("schema_drift.core.HAS_ORACLE", True):
        from schema_drift.core import _extract_oracle
        schema = _extract_oracle(conn)

    assert schema["USERS"]["columns"]["EMAIL"]["nullable"] is True
    assert schema["USERS"]["columns"]["NAME"]["nullable"] is False


def test_oracle_extracts_indexes(mock_oracle_conn):
    conn, _ = mock_oracle_conn
    with patch("schema_drift.core.HAS_ORACLE", True):
        from schema_drift.core import _extract_oracle
        schema = _extract_oracle(conn)

    assert "USERS_PK" in schema["USERS"]["indexes"]
    assert "USERS_EMAIL" in schema["USERS"]["indexes"]
    assert schema["USERS"]["indexes"]["USERS_EMAIL"]["unique"] is True


def test_oracle_raises_without_driver():
    with patch("schema_drift.core.HAS_ORACLE", False):
        from schema_drift.core import _extract_oracle
        with pytest.raises(ImportError, match="oracledb"):
            _extract_oracle("oracle://localhost/ORCL")


def test_oracle_diff_integration(tmp_path):
    conn, _ = make_mock_conn()
    storage = tmp_path / "drift.json"

    with patch("schema_drift.core.HAS_ORACLE", True):
        from schema_drift import SchemaDrift
        drift = SchemaDrift(conn, db_type="oracle", storage_path=storage)
        drift.snapshot("initial")

    # Second snapshot: add a column
    updated_cols = COLUMNS_ROWS + [
        ("USERS", "BIO", "CLOB", None, None, None, "Y", None, 4),
    ]
    conn2, cur2 = make_mock_conn()
    cur2.fetchall.side_effect = [updated_cols, INDEXES_ROWS]

    with patch("schema_drift.core.HAS_ORACLE", True):
        drift2 = SchemaDrift(conn2, db_type="oracle", storage_path=storage)
        diff = drift2.snapshot("add USERS.BIO")

    added = [c["column"] for c in diff["columns_added"]]
    assert "BIO" in added
