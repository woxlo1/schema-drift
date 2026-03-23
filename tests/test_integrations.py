"""Tests for team features and integrations."""
import json
import sqlite3
import pytest
from schema_drift import SchemaDrift
from schema_drift.integrations.team import (
    approve, annotate, audit_log, pending_approvals, require_approval
)
from schema_drift.integrations.slack import _build_blocks, notify, make_notifier, SlackNotifier
from schema_drift.diff import has_changes


@pytest.fixture
def drift_with_changes(tmp_path):
    db = tmp_path / "test.db"
    conn = sqlite3.connect(str(db))
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
    conn.commit()
    storage = tmp_path / "drift.json"
    drift = SchemaDrift(str(db), storage_path=storage)
    drift.snapshot("initial")
    conn.execute("ALTER TABLE users ADD COLUMN email TEXT")
    conn.commit()
    conn.close()
    drift.snapshot("add email")
    return drift, storage


def test_approve_adds_approval(drift_with_changes):
    drift, storage = drift_with_changes
    approve(drift, snapshot_index=-1, approver="alice", note="looks good")
    history = json.loads(storage.read_text())
    assert history[-1]["approvals"][0]["approver"] == "alice"
    assert history[-1]["approvals"][0]["note"] == "looks good"


def test_approve_multiple(drift_with_changes):
    drift, storage = drift_with_changes
    approve(drift, approver="alice")
    approve(drift, approver="bob", note="LGTM")
    history = json.loads(storage.read_text())
    assert len(history[-1]["approvals"]) == 2


def test_annotate_adds_note(drift_with_changes):
    drift, storage = drift_with_changes
    annotate(drift, note="related to PROJ-123", author="alice")
    history = json.loads(storage.read_text())
    assert history[-1]["annotations"][0]["note"] == "related to PROJ-123"
    assert history[-1]["annotations"][0]["author"] == "alice"


def test_pending_approvals_returns_unapproved(drift_with_changes):
    drift, _ = drift_with_changes
    pending = pending_approvals(drift)
    assert len(pending) >= 1


def test_pending_approvals_empty_after_approve(drift_with_changes):
    drift, _ = drift_with_changes
    approve(drift, approver="alice")
    pending = pending_approvals(drift)
    assert len(pending) == 0


def test_require_approval_returns_true_when_pending(drift_with_changes):
    drift, _ = drift_with_changes
    assert require_approval(drift) is True


def test_require_approval_returns_false_when_approved(drift_with_changes):
    drift, _ = drift_with_changes
    approve(drift, approver="alice")
    assert require_approval(drift) is False


def test_audit_log_runs(drift_with_changes, capsys):
    drift, _ = drift_with_changes
    approve(drift, approver="alice", note="ok")
    annotate(drift, note="ticket PROJ-1")
    audit_log(drift)
    out = capsys.readouterr().out
    assert "alice" in out
    assert "PROJ-1" in out


def make_diff(added=None, removed=None, modified=None):
    return {
        "tables_added": [],
        "tables_removed": removed or [],
        "columns_added": added or [],
        "columns_removed": [],
        "columns_modified": modified or [],
        "indexes_added": [],
        "indexes_removed": [],
    }


def test_slack_build_blocks_safe():
    diff = make_diff(added=[{"table": "users", "column": "email", "definition": {"type": "TEXT"}}])
    payload = _build_blocks(diff, "Test")
    assert payload["attachments"][0]["color"] == "#36C5F0"


def test_slack_build_blocks_breaking():
    diff = make_diff(removed=["orders"])
    payload = _build_blocks(diff, "Test")
    assert payload["attachments"][0]["color"] == "#E01E5A"


def test_slack_build_blocks_mention():
    diff = make_diff(removed=["orders"])
    payload = _build_blocks(diff, "Test", mention="@channel")
    text = payload["attachments"][0]["blocks"][1]["text"]["text"]
    assert "@channel" in text


def test_slack_notify_skips_no_changes():
    diff = make_diff()
    result = notify("https://example.com", diff)
    assert result is False


def test_slack_notify_skips_when_only_breaking_and_safe():
    diff = make_diff(added=[{"table": "users", "column": "email", "definition": {"type": "TEXT"}}])
    result = notify("https://example.com", diff, only_breaking=True)
    assert result is False


def test_slack_notifier_class():
    notifier = SlackNotifier("https://example.com", mention_on_breaking="@channel")
    assert notifier.webhook_url == "https://example.com"
    assert notifier.mention_on_breaking == "@channel"


def test_make_notifier_returns_callable():
    notifier = make_notifier("https://example.com")
    assert callable(notifier)
