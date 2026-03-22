"""schema_drift.integrations.slack — Slack notifications for schema changes"""
from __future__ import annotations
import json
import urllib.request
from typing import Any

from ..diff import has_changes, is_breaking


def _build_message(diff: dict, title: str = "Schema drift detected") -> dict:
    """Build a Slack Block Kit message from a diff."""
    breaking = is_breaking(diff)
    emoji = "🚨" if breaking else "📋"
    color = "#E01E5A" if breaking else "#36C5F0"

    lines: list[str] = []
    for table in diff.get("tables_added", []):
        lines.append(f"✅ table added: `{table}`")
    for table in diff.get("tables_removed", []):
        lines.append(f"❌ table dropped: `{table}`")
    for c in diff.get("columns_added", []):
        lines.append(f"✅ column added: `{c['table']}.{c['column']}` ({c['definition'].get('type','')})")
    for c in diff.get("columns_removed", []):
        lines.append(f"❌ column dropped: `{c['table']}.{c['column']}` (was {c['was'].get('type','')})")
    for c in diff.get("columns_modified", []):
        lines.append(f"⚠️ column changed: `{c['table']}.{c['column']}` {c['before'].get('type','')} → {c['after'].get('type','')}")

    text = "\n".join(lines) or "No details available."

    return {
        "attachments": [
            {
                "color": color,
                "blocks": [
                    {
                        "type": "header",
                        "text": {"type": "plain_text", "text": f"{emoji} {title}"},
                    },
                    {
                        "type": "section",
                        "text": {"type": "mrkdwn", "text": text},
                    },
                    {
                        "type": "context",
                        "elements": [
                            {
                                "type": "mrkdwn",
                                "text": f"{'⚠️ Breaking changes detected' if breaking else 'No breaking changes'} · schema-drift",
                            }
                        ],
                    },
                ],
            }
        ]
    }


def notify(
    webhook_url: str,
    diff: dict,
    title: str = "Schema drift detected",
    only_breaking: bool = False,
) -> bool:
    """
    Send a Slack notification for a schema diff.

    Args:
        webhook_url:   Slack incoming webhook URL.
        diff:          Diff dict from drift.snapshot() or drift.diff().
        title:         Notification title.
        only_breaking: If True, only send notifications for breaking changes.

    Returns:
        True if the notification was sent, False if skipped.

    Usage::

        from schema_drift.integrations.slack import notify

        diff = drift.snapshot("add users.email")
        notify("https://hooks.slack.com/services/...", diff)
    """
    if not has_changes(diff):
        return False
    if only_breaking and not is_breaking(diff):
        return False

    payload = _build_message(diff, title)
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        webhook_url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status == 200
    except Exception as e:
        print(f"schema-drift: Slack notification failed: {e}")
        return False


def make_notifier(webhook_url: str, only_breaking: bool = False, title: str = "Schema drift detected"):
    """
    Return a callback suitable for drift.watch(on_change=...).

    Usage::

        notifier = make_notifier("https://hooks.slack.com/services/...")
        drift.watch(on_change=notifier)
    """
    def _notify(diff: dict) -> None:
        notify(webhook_url, diff, title=title, only_breaking=only_breaking)

    return _notify
