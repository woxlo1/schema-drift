# Watching for changes

`drift.watch()` polls the database at a configurable interval and fires callbacks when schema changes are detected.

## Basic usage

```python
from schema_drift import SchemaDrift

drift = SchemaDrift("postgresql://localhost/mydb")
drift.watch()  # polls every 60 seconds
```

## Callbacks

```python
def on_change(diff):
    print("Schema changed!", diff)

def on_breaking(diff):
    print("BREAKING CHANGE!", diff)

drift.watch(
    interval=30,
    on_change=on_change,
    on_breaking=on_breaking,
)
```

## Auto-snapshots

By default, a snapshot is saved on every detected change.

```python
drift.watch(auto_snapshot=True, message="auto-snapshot")

# Disable auto-snapshots
drift.watch(auto_snapshot=False)
```

## CLI

```bash
schema-drift --db ./mydb.sqlite watch
schema-drift --db ./mydb.sqlite watch --interval 30
schema-drift --db ./mydb.sqlite watch --no-snapshot
```

## Slack integration

```python
from schema_drift.integrations.slack import make_notifier

notifier = make_notifier("https://hooks.slack.com/services/...")
drift.watch(on_change=notifier)
```

Press `Ctrl+C` to stop watching.
