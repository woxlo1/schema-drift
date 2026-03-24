"""Tests for REST API and webhook."""
import json
import sqlite3
import pytest
from unittest.mock import patch, MagicMock
from schema_drift import SchemaDrift
from schema_drift.integrations.webhook import (
    _build_payload, _sign, notify, make_notifier, WebhookNotifier
)

try:
    from fastapi.testclient import TestClient
    from schema_drift.api import create_app
    HAS_FASTAPI = True
except ImportError:
    HAS_FASTAPI = False


# ── Webhook tests ─────────────────────────────────────────────────────────────

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


def test_build_payload_event():
    diff = make_diff(added=[{"table": "users", "column": "email", "definition": {"type": "TEXT"}}])
    payload = _build_payload(diff, event="schema.changed")
    assert payload["event"] == "schema.changed"
    assert payload["breaking"] is False
    assert payload["diff"]["columns_added"][0]["column"] == "email"


def test_build_payload_breaking():
    diff = make_diff(removed=["orders"])
    payload = _build_payload(diff)
    assert payload["breaking"] is True
    assert "orders" in payload["diff"]["tables_removed"]


def test_build_payload_metadata():
    diff = make_diff(added=[{"table": "users", "column": "email", "definition": {"type": "TEXT"}}])
    payload = _build_payload(diff, metadata={"env": "production"})
    assert payload["metadata"]["env"] == "production"


def test_sign_produces_sha256():
    sig = _sign(b"hello", "secret")
    assert sig.startswith("sha256=")
    assert len(sig) == 71  # sha256= + 64 hex chars


def test_notify_skips_no_changes():
    diff = make_diff()
    result = notify("https://example.com", diff)
    assert result is False


def test_notify_skips_when_only_breaking_and_safe():
    diff = make_diff(added=[{"table": "users", "column": "email", "definition": {"type": "TEXT"}}])
    result = notify("https://example.com", diff, only_breaking=True)
    assert result is False


def test_notify_sends_request(tmp_path):
    diff = make_diff(added=[{"table": "users", "column": "email", "definition": {"type": "TEXT"}}])
    with patch("urllib.request.urlopen") as mock_open:
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_open.return_value = mock_resp
        result = notify("https://example.com/webhook", diff)
    assert result is True


def test_notify_breaking_changes_event():
    diff = make_diff(removed=["orders"])
    with patch("urllib.request.urlopen") as mock_open:
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_open.return_value = mock_resp
        notify("https://example.com/webhook", diff)
        req = mock_open.call_args[0][0]
        body = json.loads(req.data)
        assert body["event"] == "schema.breaking"


def test_notify_includes_signature():
    diff = make_diff(added=[{"table": "users", "column": "email", "definition": {"type": "TEXT"}}])
    with patch("urllib.request.urlopen") as mock_open:
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_open.return_value = mock_resp
        notify("https://example.com/webhook", diff, secret="mysecret")
        req = mock_open.call_args[0][0]
        assert "X-schema-drift-signature" in req.headers


def test_make_notifier_returns_callable():
    notifier = make_notifier("https://example.com")
    assert callable(notifier)


def test_webhook_notifier_class():
    notifier = WebhookNotifier("https://example.com", secret="s3cr3t")
    assert notifier.url == "https://example.com"
    assert notifier.secret == "s3cr3t"


# ── REST API tests ────────────────────────────────────────────────────────────

@pytest.mark.skipif(not HAS_FASTAPI, reason="fastapi not installed")
class TestAPI:
    @pytest.fixture
    def client(self, tmp_path):
        db = tmp_path / "test.db"
        conn = sqlite3.connect(str(db))
        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        conn.commit()
        conn.close()
        storage = tmp_path / "drift.json"
        drift = SchemaDrift(str(db), storage_path=storage)
        drift.snapshot("initial")
        app = create_app(storage_path=storage, db_url=str(db))
        return TestClient(app), str(db), storage

    def test_health(self, client):
        c, db, storage = client
        resp = c.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"
        assert resp.json()["snapshots"] == 1

    def test_list_snapshots(self, client):
        c, db, storage = client
        resp = c.get("/snapshots")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["message"] == "initial"

    def test_get_snapshot(self, client):
        c, db, storage = client
        resp = c.get("/snapshots/0")
        assert resp.status_code == 200
        assert resp.json()["message"] == "initial"

    def test_get_snapshot_not_found(self, client):
        c, db, storage = client
        resp = c.get("/snapshots/99")
        assert resp.status_code == 404

    def test_take_snapshot(self, client):
        c, db, storage = client
        conn = sqlite3.connect(db)
        conn.execute("ALTER TABLE users ADD COLUMN email TEXT")
        conn.commit()
        conn.close()
        resp = c.post("/snapshots", json={"message": "add email"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["message"] == "add email"
        assert data["has_changes"] is True

    def test_diff(self, client):
        c, db, storage = client
        conn = sqlite3.connect(db)
        conn.execute("ALTER TABLE users ADD COLUMN email TEXT")
        conn.commit()
        conn.close()
        c.post("/snapshots", json={"message": "add email"})
        resp = c.get("/diff")
        assert resp.status_code == 200
        data = resp.json()
        assert data["has_changes"] is True
        assert any(col["column"] == "email" for col in data["diff"]["columns_added"])

    def test_diff_not_enough_snapshots(self, client):
        c, db, storage = client
        resp = c.get("/diff")
        assert resp.status_code == 400

    def test_get_schema(self, client):
        c, db, storage = client
        resp = c.get("/schema")
        assert resp.status_code == 200
        assert "users" in resp.json()["schema"]
