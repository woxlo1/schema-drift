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
