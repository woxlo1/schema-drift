"""
Tests for schema-drift v0.1
Uses SQLite so no external DB is needed.
"""
import json
import sqlite3
import tempfile
from pathlib import Path

import pytest
from schema_drift import SchemaDrift


@pytest.fixture
def db_and_drift(tmp_path):
    """Return a fresh SQLite connection + SchemaDrift instance."""
    conn = sqlite3.connect(":memory:")
    storage = tmp_path / "drift.json"
    drift = SchemaDrift(conn, db_type="sqlite", storage_path=storage)
    return conn, drift


def create_initial_schema(conn):
    conn.execute("""
        CREATE TABLE users (
            id   INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            age  INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE posts (
            id      INTEGER PRIMARY KEY,
            title   TEXT NOT NULL,
            user_id INTEGER
        )
    """)
    conn.commit()


# ── snapshot ──────────────────────────────────────────────────────────────────

def test_first_snapshot_creates_file(db_and_drift, tmp_path):
    conn, drift = db_and_drift
    create_initial_schema(conn)
    drift.snapshot("initial schema")

    assert drift._storage.exists()
    history = json.loads(drift._storage.read_text())
    assert len(history) == 1
    assert history[0]["message"] == "initial schema"


def test_snapshot_records_tables(db_and_drift):
    conn, drift = db_and_drift
    create_initial_schema(conn)
    drift.snapshot("initial")

    history = json.loads(drift._storage.read_text())
    schema = history[0]["schema"]
    assert "users" in schema
    assert "posts" in schema


def test_snapshot_records_columns(db_and_drift):
    conn, drift = db_and_drift
    create_initial_schema(conn)
    drift.snapshot("initial")

    history = json.loads(drift._storage.read_text())
    cols = history[0]["schema"]["users"]["columns"]
    assert "name" in cols
    assert "age" in cols


def test_snapshot_stats(db_and_drift):
    conn, drift = db_and_drift
    create_initial_schema(conn)
    drift.snapshot("initial")

    history = json.loads(drift._storage.read_text())
    assert history[0]["stats"]["tables"] == 2
    assert history[0]["stats"]["columns"] == 6  # 3 + 3


# ── diff ──────────────────────────────────────────────────────────────────────

def test_diff_detects_new_column(db_and_drift):
    conn, drift = db_and_drift
    create_initial_schema(conn)
    drift.snapshot("initial")

    conn.execute("ALTER TABLE users ADD COLUMN email TEXT")
    conn.commit()
    diff = drift.snapshot("add users.email")

    added = diff["columns_added"]
    assert any(c["table"] == "users" and c["column"] == "email" for c in added)


def test_diff_detects_new_table(db_and_drift):
    conn, drift = db_and_drift
    create_initial_schema(conn)
    drift.snapshot("initial")

    conn.execute("CREATE TABLE tags (id INTEGER PRIMARY KEY, name TEXT)")
    conn.commit()
    diff = drift.snapshot("add tags table")

    assert "tags" in diff["tables_added"]


def test_diff_detects_dropped_table(db_and_drift):
    conn, drift = db_and_drift
    create_initial_schema(conn)
    drift.snapshot("initial")

    conn.execute("DROP TABLE posts")
    conn.commit()
    diff = drift.snapshot("drop posts")

    assert "posts" in diff["tables_removed"]


def test_no_changes_empty_diff(db_and_drift):
    conn, drift = db_and_drift
    create_initial_schema(conn)
    drift.snapshot("initial")
    diff = drift.snapshot("no change")

    from schema_drift.diff import has_changes as _has_changes
    assert not _has_changes(diff)


# ── log ───────────────────────────────────────────────────────────────────────

def test_log_shows_all_snapshots(db_and_drift, capsys):
    conn, drift = db_and_drift
    create_initial_schema(conn)
    drift.snapshot("initial")

    conn.execute("ALTER TABLE users ADD COLUMN email TEXT")
    conn.commit()
    drift.snapshot("add email")

    drift.log()
    out = capsys.readouterr().out
    assert "initial" in out
    assert "add email" in out


# ── rollback ─────────────────────────────────────────────────────────────────

def test_rollback_returns_old_schema(db_and_drift):
    conn, drift = db_and_drift
    create_initial_schema(conn)
    drift.snapshot("initial")

    conn.execute("ALTER TABLE users ADD COLUMN email TEXT")
    conn.commit()
    drift.snapshot("add email")

    old_schema = drift.rollback(0)
    assert "email" not in old_schema["users"]["columns"]


def test_rollback_new_schema_has_email(db_and_drift):
    conn, drift = db_and_drift
    create_initial_schema(conn)
    drift.snapshot("initial")

    conn.execute("ALTER TABLE users ADD COLUMN email TEXT")
    conn.commit()
    drift.snapshot("add email")

    new_schema = drift.rollback(-1)
    assert "email" in new_schema["users"]["columns"]
