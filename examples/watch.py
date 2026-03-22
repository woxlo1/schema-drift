"""Real-time schema monitoring with drift.watch()."""
from schema_drift import SchemaDrift
from schema_drift.integrations.slack import make_notifier

drift = SchemaDrift("postgresql://user:pass@localhost/mydb")

# Optional: notify Slack on every change
# notifier = make_notifier("https://hooks.slack.com/services/...")

def on_change(diff):
    added = [c["column"] for c in diff.get("columns_added", [])]
    removed = [c["column"] for c in diff.get("columns_removed", [])]
    if added:
        print(f"New columns: {added}")
    if removed:
        print(f"Dropped columns: {removed}")

def on_breaking(diff):
    print("WARNING: breaking schema change detected!")
    # send alert, page on-call, etc.

drift.watch(
    interval=60,
    on_change=on_change,
    on_breaking=on_breaking,
    auto_snapshot=True,
)
