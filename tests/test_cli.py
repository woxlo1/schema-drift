"""Tests for the CLI."""
import json
import sqlite3
from pathlib import Path

import pytest
from schema_drift import SchemaDrift


@pytest.fixture
def db_path(tmp_path):
    db = tmp_path / "test.db"
    conn = sqlite3.connect(str(db))
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
    conn.commit()
    conn.close()
    return db


@pytest.fixture
def storage(tmp_path):
    return tmp_path / "drift.json"


def run_cli(args: list[str], monkeypatch, tmp_path, db_path, storage):
    """Helper: run CLI with given args."""
    from schema_drift.cli import build_parser
    parser = build_parser()
    parsed = parser.parse_args(
        ["--db", str(db_path), "--storage", str(storage)] + args
    )
    parsed.func(parsed)


def test_cli_snapshot(monkeypatch, tmp_path, db_path, storage):
    run_cli(["snapshot", "initial"], monkeypatch, tmp_path, db_path, storage)
    history = json.loads(storage.read_text())
    assert len(history) == 1
    assert history[0]["message"] == "initial"


def test_cli_snapshot_no_message(monkeypatch, tmp_path, db_path, storage):
    run_cli(["snapshot"], monkeypatch, tmp_path, db_path, storage)
    history = json.loads(storage.read_text())
    assert history[0]["message"] == ""


def test_cli_log(monkeypatch, tmp_path, db_path, storage, capsys):
    run_cli(["snapshot", "first"], monkeypatch, tmp_path, db_path, storage)
    run_cli(["log"], monkeypatch, tmp_path, db_path, storage)
    out = capsys.readouterr().out
    assert "first" in out


def test_cli_diff(monkeypatch, tmp_path, db_path, storage, capsys):
    run_cli(["snapshot", "initial"], monkeypatch, tmp_path, db_path, storage)

    conn = sqlite3.connect(str(db_path))
    conn.execute("ALTER TABLE users ADD COLUMN email TEXT")
    conn.commit()
    conn.close()

    run_cli(["snapshot", "add email"], monkeypatch, tmp_path, db_path, storage)
    run_cli(["diff"], monkeypatch, tmp_path, db_path, storage)
    out = capsys.readouterr().out
    assert "email" in out


def test_cli_rollback(monkeypatch, tmp_path, db_path, storage, capsys):
    run_cli(["snapshot", "initial"], monkeypatch, tmp_path, db_path, storage)
    run_cli(["rollback", "0"], monkeypatch, tmp_path, db_path, storage)
    out = capsys.readouterr().out
    assert "initial" in out


def test_cli_missing_db_exits(monkeypatch, tmp_path):
    """Should exit with error when no DB is provided."""
    import os
    monkeypatch.delenv("SCHEMA_DRIFT_DB", raising=False)
    from schema_drift.cli import build_parser
    parser = build_parser()
    parsed = parser.parse_args(["snapshot", "test"])
    with pytest.raises(SystemExit):
        parsed.func(parsed)
