"""Tests for drift.watch()"""
import json
import sqlite3
import threading
import time
import pytest
from schema_drift import SchemaDrift

@pytest.fixture
def watched_db(tmp_path):
    db = tmp_path / "watch.db"
    conn = sqlite3.connect(str(db))
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
    conn.commit()
    conn.close()
    storage = tmp_path / "drift.json"
    return str(db), storage


def run_watch_thread(drift, **kwargs):
    t = threading.Thread(target=drift.watch, kwargs=kwargs, daemon=True)
    t.start()
    return t


def test_watch_calls_on_change(watched_db):
    db_path, storage = watched_db
    drift = SchemaDrift(db_path, storage_path=storage)
    drift.snapshot("baseline")

    changes = []
    ready = threading.Event()

    def on_change(diff):
        changes.append(diff)
        ready.set()

    run_watch_thread(drift, interval=0, on_change=on_change, auto_snapshot=False)
    time.sleep(0.1)

    conn = sqlite3.connect(db_path)
    conn.execute("ALTER TABLE users ADD COLUMN email TEXT")
    conn.commit()
    conn.close()

    ready.wait(timeout=5.0)
    assert len(changes) >= 1
    assert "email" in [c["column"] for c in changes[0]["columns_added"]]


def test_watch_calls_on_breaking(watched_db):
    db_path, storage = watched_db
    drift = SchemaDrift(db_path, storage_path=storage)
    drift.snapshot("baseline")

    breaking_diffs = []
    ready = threading.Event()

    def on_breaking(diff):
        breaking_diffs.append(diff)
        ready.set()

    run_watch_thread(drift, interval=0, on_breaking=on_breaking, auto_snapshot=False)
    time.sleep(0.1)

    conn = sqlite3.connect(db_path)
    conn.execute("DROP TABLE users")
    conn.commit()
    conn.close()

    ready.wait(timeout=5.0)
    assert len(breaking_diffs) >= 1
    assert "users" in breaking_diffs[0]["tables_removed"]


def test_watch_auto_snapshot(watched_db):
    db_path, storage = watched_db
    drift = SchemaDrift(db_path, storage_path=storage)
    drift.snapshot("baseline")

    ready = threading.Event()
    original_snapshot = drift.snapshot

    def patched_snapshot(msg=""):
        result = original_snapshot(msg)
        if "auto" in msg:
            ready.set()
        return result

    drift.snapshot = patched_snapshot
    run_watch_thread(drift, interval=0, auto_snapshot=True)
    time.sleep(0.1)

    conn = sqlite3.connect(db_path)
    conn.execute("ALTER TABLE users ADD COLUMN email TEXT")
    conn.commit()
    conn.close()

    ready.wait(timeout=5.0)
    history = json.loads(storage.read_text())
    assert len(history) >= 2


def test_watch_no_change_no_callback(watched_db):
    db_path, storage = watched_db
    drift = SchemaDrift(db_path, storage_path=storage)
    drift.snapshot("baseline")

    calls = []
    run_watch_thread(drift, interval=0, on_change=lambda d: calls.append(d), auto_snapshot=False)
    time.sleep(0.3)

    assert len(calls) == 0


def test_watch_baseline_created_if_no_snapshots(watched_db):
    db_path, storage = watched_db
    drift = SchemaDrift(db_path, storage_path=storage)

    assert not storage.exists()

    run_watch_thread(drift, interval=999, auto_snapshot=False)
    time.sleep(0.2)

    assert storage.exists()
    history = json.loads(storage.read_text())
    assert history[0]["message"] == "baseline (watch)"