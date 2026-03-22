"""Tests for MariaDB support (reuses MySQL mock — wire-compatible)."""
import json
import pytest
from unittest.mock import MagicMock, patch
from tests.test_mysql import make_mock_conn, COLUMNS_ROWS, INDEXES_ROWS


def test_detect_mariadb_url():
    from schema_drift import SchemaDrift
    drift = SchemaDrift.__new__(SchemaDrift)
    result = drift._detect_type("mariadb://user:pass@localhost/mydb", "auto")
    assert result == "mariadb"


def test_detect_mariadb_plus_url():
    from schema_drift import SchemaDrift
    drift = SchemaDrift.__new__(SchemaDrift)
    result = drift._detect_type("mariadb+connector://user:pass@localhost/mydb", "auto")
    assert result == "mariadb"


def test_mariadb_extracts_tables(tmp_path):
    conn, _ = make_mock_conn([], COLUMNS_ROWS, INDEXES_ROWS)
    storage = tmp_path / "drift.json"

    with patch("schema_drift.core.HAS_MYSQL", True):
        from schema_drift import SchemaDrift
        drift = SchemaDrift(conn, db_type="mariadb", storage_path=storage)
        drift.snapshot("initial")

    history = json.loads(storage.read_text())
    assert "users" in history[0]["schema"]
    assert "posts" in history[0]["schema"]


def test_mariadb_diff_detects_new_column(tmp_path):
    conn, _ = make_mock_conn([], COLUMNS_ROWS, INDEXES_ROWS)
    storage = tmp_path / "drift.json"

    with patch("schema_drift.core.HAS_MYSQL", True):
        from schema_drift import SchemaDrift
        drift = SchemaDrift(conn, db_type="mariadb", storage_path=storage)
        drift.snapshot("initial")

    updated_cols = COLUMNS_ROWS + [
        {"table_name": "users", "column_name": "bio", "data_type": "text",
         "column_type": "text", "is_nullable": "YES", "column_default": None,
         "column_key": "", "ordinal_position": 4},
    ]
    conn2, _ = make_mock_conn([], updated_cols, INDEXES_ROWS)

    with patch("schema_drift.core.HAS_MYSQL", True):
        drift2 = SchemaDrift(conn2, db_type="mariadb", storage_path=storage)
        diff = drift2.snapshot("add users.bio")

    added = [c["column"] for c in diff["columns_added"]]
    assert "bio" in added


def test_mariadb_url_rewritten_to_mysql(tmp_path):
    """mariadb:// URL should be rewritten to mysql:// before connecting."""
    with patch("schema_drift.core.HAS_MYSQL", True), \
         patch("schema_drift.core._extract_mysql") as mock_extract:
        mock_extract.return_value = {}
        from schema_drift import SchemaDrift
        drift = SchemaDrift.__new__(SchemaDrift)
        drift._connection = "mariadb://user:pass@localhost/mydb"
        drift._db_type = "mariadb"
        drift._extract()

        called_url = mock_extract.call_args[0][0]
        assert called_url.startswith("mysql://")
