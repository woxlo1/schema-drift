"""
schema_drift.integrations.webhook

Send HTTP POST notifications when schema changes are detected.

Usage::

    from schema_drift import SchemaDrift
    from schema_drift.integrations.webhook import notify, make_notifier, WebhookNotifier

    drift = SchemaDrift("postgresql://localhost/mydb")

    # One-off notification
    diff = drift.snapshot("add users.email")
    notify("https://your-service.com/webhook", diff)

    # Use with watch
    notifier = make_notifier("https://your-service.com/webhook")
    drift.watch(on_change=notifier)

    # Advanced
    notifier = WebhookNotifier(
        url="https://your-service.com/webhook",
        secret="my-secret",
        only_breaking=False,
    )
    drift.watch(on_change=notifier.on_change, on_breaking=notifier.on_breaking)
"""
from __future__ import annotations

import hashlib
import hmac
import json
import urllib.request
from datetime import datetime, timezone
from typing import Any

from ..diff import has_changes, is_breaking


def _build_payload(diff: dict, event: str = "schema.changed", metadata: dict | None = None) -> dict:
    """Build the webhook payload."""
    return {
        "event": event,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "breaking": is_breaking(diff),
        "diff": {
            "tables_added": diff.get("tables_added", []),
            "tables_removed": diff.get("tables_removed", []),
            "columns_added": [
                {"table": c["table"], "column": c["column"], "type": c["definition"].get("type", "")}
                for c in diff.get("columns_added", [])
            ],
            "columns_removed": [
                {"table": c["table"], "column": c["column"], "was": c["was"].get("type", "")}
                for c in diff.get("columns_removed", [])
            ],
            "columns_modified": [
                {"table": c["table"], "column": c["column"],
                 "before": c["before"].get("type", ""), "after": c["after"].get("type", "")}
                for c in diff.get("columns_modified", [])
            ],
            "indexes_added": [
                {"table": i["table"], "index": i["index"]}
                for i in diff.get("indexes_added", [])
            ],
            "indexes_removed": [
                {"table": i["table"], "index": i["index"]}
                for i in diff.get("indexes_removed", [])
            ],
        },
        "metadata": metadata or {},
    }


def _sign(payload: bytes, secret: str) -> str:
    """Generate HMAC-SHA256 signature for the payload."""
    return "sha256=" + hmac.new(
        secret.encode("utf-8"),
        payload,
        hashlib.sha256,
    ).hexdigest()


def notify(
    url: str,
    diff: dict,
    event: str = "schema.changed",
    secret: str = "",
    only_breaking: bool = False,
    metadata: dict | None = None,
    timeout: int = 10,
) -> bool:
    """
    Send a webhook notification for a schema diff.

    Args:
        url:           Webhook endpoint URL.
        diff:          Diff dict from drift.snapshot() or drift.diff().
        event:         Event name (default: "schema.changed").
        secret:        Optional secret for HMAC-SHA256 signature verification.
        only_breaking: Only send on breaking changes.
        metadata:      Optional extra data to include in the payload.
        timeout:       Request timeout in seconds.

    Returns:
        True if the webhook was delivered successfully (2xx response).

    Payload example::

        {
            "event": "schema.changed",
            "timestamp": "2026-03-24T10:00:00Z",
            "breaking": true,
            "diff": {
                "tables_added": [],
                "tables_removed": ["orders"],
                "columns_added": [{"table": "users", "column": "email", "type": "TEXT"}],
                "columns_removed": [],
                "columns_modified": [],
                "indexes_added": [],
                "indexes_removed": []
            },
            "metadata": {}
        }
    """
    if not has_changes(diff):
        return False
    if only_breaking and not is_breaking(diff):
        return False

    if is_breaking(diff):
        event = "schema.breaking"

    payload = _build_payload(diff, event=event, metadata=metadata)
    data = json.dumps(payload).encode("utf-8")

    headers = {"Content-Type": "application/json"}
    if secret:
        headers["X-Schema-Drift-Signature"] = _sign(data, secret)
        headers["X-Schema-Drift-Event"] = event

    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return 200 <= resp.status < 300
    except Exception as e:
        print(f"schema-drift: webhook failed: {e}")
        return False


def make_notifier(
    url: str,
    secret: str = "",
    only_breaking: bool = False,
    metadata: dict | None = None,
):
    """
    Return a callback suitable for drift.watch(on_change=...).

    Usage::

        notifier = make_notifier("https://your-service.com/webhook")
        drift.watch(on_change=notifier)
    """
    def _notify(diff: dict) -> None:
        notify(url, diff, secret=secret, only_breaking=only_breaking, metadata=metadata)
    return _notify


class WebhookNotifier:
    """
    Full-featured webhook notifier with separate on_change and on_breaking callbacks.

    Usage::

        notifier = WebhookNotifier(
            url="https://your-service.com/webhook",
            secret="my-secret",
        )
        drift.watch(
            on_change=notifier.on_change,
            on_breaking=notifier.on_breaking,
        )
    """

    def __init__(
        self,
        url: str,
        secret: str = "",
        only_breaking: bool = False,
        metadata: dict | None = None,
        timeout: int = 10,
    ):
        self.url = url
        self.secret = secret
        self.only_breaking = only_breaking
        self.metadata = metadata or {}
        self.timeout = timeout

    def on_change(self, diff: dict) -> None:
        """Call from drift.watch(on_change=...)."""
        if not is_breaking(diff):
            notify(self.url, diff, event="schema.changed",
                   secret=self.secret, metadata=self.metadata, timeout=self.timeout)

    def on_breaking(self, diff: dict) -> None:
        """Call from drift.watch(on_breaking=...)."""
        notify(self.url, diff, event="schema.breaking",
               secret=self.secret, metadata=self.metadata, timeout=self.timeout)
