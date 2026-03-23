# Integrations

## Alembic

Automatically snapshot after each migration.

```python
from schema_drift import SchemaDrift
from schema_drift.integrations.alembic import register

drift = SchemaDrift("postgresql://localhost/mydb")
register(drift)
```

## Django

Automatically snapshot after each `manage.py migrate`.

```python
# myapp/apps.py
from django.apps import AppConfig

class MyAppConfig(AppConfig):
    name = "myapp"

    def ready(self):
        from schema_drift import SchemaDrift
        from schema_drift.integrations.django import register

        drift = SchemaDrift("postgresql://localhost/mydb")
        register(drift)
```

## Slack

Send notifications when schema changes are detected.

```python
from schema_drift.integrations.slack import notify, make_notifier

# One-off notification
diff = drift.snapshot("add users.email")
notify("https://hooks.slack.com/services/...", diff)

# Only notify on breaking changes
notify("https://hooks.slack.com/services/...", diff, only_breaking=True)

# Use as a watch callback
notifier = make_notifier("https://hooks.slack.com/services/...")
drift.watch(on_change=notifier)
```

### Slack message format

| change | appearance |
| ------ | ---------- |
| table / column added | ✅ green |
| table / column dropped | ❌ red |
| column type changed | ⚠️ yellow |

---

## Team features

Track approvals and annotations on schema snapshots.

```python
from schema_drift.integrations.team import approve, annotate, audit_log, pending_approvals, require_approval

drift = SchemaDrift("postgresql://localhost/mydb")

# Approve latest snapshot
approve(drift, approver="alice", note="reviewed in standup")

# Approve specific snapshot by index
approve(drift, snapshot_index=2, approver="bob")

# Add an annotation
annotate(drift, note="related to PROJ-123", author="alice")

# Print full audit trail
audit_log(drift)

# Check for unapproved changes (useful in CI)
if require_approval(drift):
    print("Schema changes need approval!")
    exit(1)
```

### Slack — advanced usage

```python
from schema_drift.integrations.slack import SlackNotifier

notifier = SlackNotifier(
    webhook_url="https://hooks.slack.com/services/...",
    mention_on_breaking="@channel",
)

drift.watch(
    on_change=notifier.on_change,
    on_breaking=notifier.on_breaking,
)
```
