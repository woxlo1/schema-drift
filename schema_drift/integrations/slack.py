"""
schema_drift.integrations.slack

Slack notifications for schema changes.

Usage::

    from schema_drift.integrations.slack import notify, make_notifier, SlackNotifier

    # Simple one-off notification
    diff = drift.snapshot("add users.email")
    notify("https://hooks.slack.com/services/...", diff)

    # Use as watch callback
    notifier = make_notifier("https://hooks.slack.com/services/...")
    drift.watch(on_change=notifier)

    # Advanced: full control with SlackNotifier
    notifier = SlackNotifier(
        webhook_url="https://hooks.slack.com/services/...",
        channel="#db-changes",
        username="schema-drift",
        only_breaking=False,
        mention_on_breaking="@channel",
    )
    drift.watch(on_change=notifier.on_change, on_breaking=notifier.on_breaking)
"""
from __future__ import annotations
import json
import urllib.request
from typing import Any

from ..diff import has_changes, is_breaking


def _build_blocks(diff: dict, title: str, mention: str = "") -> dict:
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
    for i in diff.get("indexes_added", []):
        lines.append(f"✅ index added: `{i['index']}` on `{i['table']}`")
    for i in diff.get("indexes_removed", []):
        lines.append(f"⚠️ index dropped: `{i['index']}` on `{i['table']}`")

    text = "\n".join(lines) or "No details available."
    if mention:
        text = f"{mention}\n{text}"

    footer = "⚠️ Breaking changes detected" if breaking else "✅ No breaking changes"

    return {
        "attachments": [{
            "color": color,
            "blocks": [
                {"type": "header", "text": {"type": "plain_text", "text": f"{emoji} {title}"}},
                {"type": "section", "text": {"type": "mrkdwn", "text": text}},
                {"type": "context", "elements": [{"type": "mrkdwn", "text": f"{footer} · schema-drift"}]},
            ],
        }]
    }


def notify(
    webhook_url: str,
    diff: dict,
    title: str = "Schema drift detected",
    only_breaking: bool = False,
    mention_on_breaking: str = "",
) -> bool:
    """
    Send a Slack notification for a schema diff.

    Args:
        webhook_url:          Slack incoming webhook URL.
        diff:                 Diff dict from drift.snapshot() or drift.diff().
        title:                Notification title.
        only_breaking:        Only notify on breaking changes.
        mention_on_breaking:  Slack mention when breaking (e.g. "@channel", "@alice").

    Returns:
        True if notification was sent.
    """
    if not has_changes(diff):
        return False
    if only_breaking and not is_breaking(diff):
        return False

    mention = mention_on_breaking if is_breaking(diff) else ""
    payload = _build_blocks(diff, title, mention)
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


def make_notifier(
    webhook_url: str,
    title: str = "Schema drift detected",
    only_breaking: bool = False,
    mention_on_breaking: str = "",
):
    """Return a callback for drift.watch(on_change=...)."""
    def _notify(diff: dict) -> None:
        notify(webhook_url, diff, title=title,
               only_breaking=only_breaking,
               mention_on_breaking=mention_on_breaking)
    return _notify


class SlackNotifier:
    """
    Full-featured Slack notifier with separate on_change and on_breaking callbacks.

    Usage::

        notifier = SlackNotifier(
            webhook_url="https://hooks.slack.com/services/...",
            mention_on_breaking="@channel",
        )
        drift.watch(
            on_change=notifier.on_change,
            on_breaking=notifier.on_breaking,
        )
    """

    def __init__(
        self,
        webhook_url: str,
        title: str = "Schema drift detected",
        mention_on_breaking: str = "",
    ):
        self.webhook_url = webhook_url
        self.title = title
        self.mention_on_breaking = mention_on_breaking

    def on_change(self, diff: dict) -> None:
        """Call this from drift.watch(on_change=...)."""
        if not is_breaking(diff):
            notify(self.webhook_url, diff, title=self.title)

    def on_breaking(self, diff: dict) -> None:
        """Call this from drift.watch(on_breaking=...)."""
        notify(
            self.webhook_url, diff,
            title=f"🚨 {self.title}",
            mention_on_breaking=self.mention_on_breaking,
        )
