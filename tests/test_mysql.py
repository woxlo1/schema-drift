"""Tests for MySQL support (using mocks — no real DB needed)."""
import json
import pytest
from unittest.mock import MagicMock, patch, PropertyMock


def make_mock_conn(tables_rows, columns_rows, indexes_rows, db_name="testdb"):
    """Build a mock mysql.connector connection."""
    conn = MagicMock()
    conn.database = db_name

    cur = MagicMock()
    conn.cursor.return_value = cur

    # cursor().fetchall() returns different data per query call
    cur.fetchall.side_effect = [columns_rows, indexes_rows]

    return conn, cur


COLUMNS_ROWS = [
    {"table_name": "users", "column_name": "id",    "data_type": "int",     "column_type": "int(11)",     "is_nullable": "NO",  "column_default": None, "column_key": "PRI", "ordinal_position": 1},
    {"table_name": "users", "column_name": "name",  "data_type": "varchar", "column_type": "varchar(255)","is_nullable": "NO",  "column_default": None, "column_key": "",    "ordinal_position": 2},
    {"table_name": "users", "column_name": "email", "data_type": "varchar", "column_type": "varchar(255)","is_nullable": "YES", "column_default": None, "column_key": "UNI", "ordinal_position": 3},
    {"table_name": "posts", "column_name": "id",    "data_type": "int",     "column_type": "int(11)",     "is_nullable": "NO",  "column_default": None, "column_key": "PRI", "ordinal_position": 1},
    {"table_name": "posts", "column_name": "title", "data_type": "varchar", "column_type": "varchar(255)","is_nullable": "NO",  "column_default": None, "column_key": "",    "ordinal_position": 2},
]

INDEXES_ROWS = [
    {"table_name": "users", "index_name": "PRIMARY",   "non_unique": 0, "column_name": "id",    "seq": 1},
    {"table_name": "users", "index_name": "email_idx", "non_unique": 0, "column_name": "email", "seq": 1},
    {"table_name": "posts", "index_name": "PRIMARY",   "non_unique": 0, "column_name": "id",    "seq": 1},
]


@pytest.fixture
def mock_mysql_conn():
    conn, cur = make_mock_conn([], COLUMNS_ROWS, INDEXES_ROWS)
    return conn, cur


def test_mysql_extracts_tables(mock_mysql_conn):
    conn, _ = mock_mysql_conn
    with patch("schema_drift.core.HAS_MYSQL", True):
        from schema_drift.core import _extract_mysql
        schema = _extract_mysql(conn)

    assert "users" in schema
    assert "posts" in schema


def test_mysql_extracts_columns(mock_mysql_conn):
    conn, _ = mock_mysql_conn
    with patch("schema_drift.core.HAS_MYSQL", True):
        from schema_drift.core import _extract_mysql
        schema = _extract_mysql(conn)

    assert "id" in schema["users"]["columns"]
    assert "name" in schema["users"]["columns"]
    assert "email" in schema["users"]["columns"]


def test_mysql_column_nullable(mock_mysql_conn):
    conn, _ = mock_mysql_conn
    with patch("schema_drift.core.HAS_MYSQL", True):
        from schema_drift.core import _extract_mysql
        schema = _extract_mysql(conn)

    assert schema["users"]["columns"]["email"]["nullable"] is True
    assert schema["users"]["columns"]["name"]["nullable"] is False


def test_mysql_primary_key(mock_mysql_conn):
    conn, _ = mock_mysql_conn
    with patch("schema_drift.core.HAS_MYSQL", True):
        from schema_drift.core import _extract_mysql
        schema = _extract_mysql(conn)

    assert schema["users"]["columns"]["id"]["primary_key"] is True
    assert schema["users"]["columns"]["name"]["primary_key"] is False


def test_mysql_extracts_indexes(mock_mysql_conn):
    conn, _ = mock_mysql_conn
    with patch("schema_drift.core.HAS_MYSQL", True):
        from schema_drift.core import _extract_mysql
        schema = _extract_mysql(conn)

    assert "PRIMARY" in schema["users"]["indexes"]
    assert "email_idx" in schema["users"]["indexes"]
    assert schema["users"]["indexes"]["email_idx"]["unique"] is True


def test_mysql_index_is_primary(mock_mysql_conn):
    conn, _ = mock_mysql_conn
    with patch("schema_drift.core.HAS_MYSQL", True):
        from schema_drift.core import _extract_mysql
        schema = _extract_mysql(conn)

    assert schema["users"]["indexes"]["PRIMARY"]["primary"] is True
    assert schema["users"]["indexes"]["email_idx"]["primary"] is False


def test_mysql_raises_without_driver():
    with patch("schema_drift.core.HAS_MYSQL", False):
        from schema_drift.core import _extract_mysql
        with pytest.raises(ImportError, match="mysql-connector-python"):
            _extract_mysql("mysql://localhost/test")


def test_detect_mysql_url():
    from schema_drift import SchemaDrift
    drift = SchemaDrift.__new__(SchemaDrift)
    result = drift._detect_type("mysql://user:pass@localhost/mydb", "auto")
    assert result == "mysql"


def test_detect_mysql_plus_url():
    from schema_drift import SchemaDrift
    drift = SchemaDrift.__new__(SchemaDrift)
    result = drift._detect_type("mysql+connector://user:pass@localhost/mydb", "auto")
    assert result == "mysql"


def test_mysql_diff_integration(tmp_path, mock_mysql_conn):
    """Full snapshot → diff flow with mock MySQL connection."""
    conn, cur = mock_mysql_conn
    storage = tmp_path / "drift.json"

    with patch("schema_drift.core.HAS_MYSQL", True):
        from schema_drift import SchemaDrift
        drift = SchemaDrift(conn, db_type="mysql", storage_path=storage)
        drift.snapshot("initial")

    # Second snapshot: add a column
    updated_cols = COLUMNS_ROWS + [
        {"table_name": "users", "column_name": "bio", "data_type": "text",
         "column_type": "text", "is_nullable": "YES", "column_default": None,
         "column_key": "", "ordinal_position": 4},
    ]
    conn2, cur2 = make_mock_conn([], updated_cols, INDEXES_ROWS)

    with patch("schema_drift.core.HAS_MYSQL", True):
        drift2 = SchemaDrift(conn2, db_type="mysql", storage_path=storage)
        diff = drift2.snapshot("add users.bio")

    added = [c["column"] for c in diff["columns_added"]]
    assert "bio" in added
