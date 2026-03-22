"""Slack notifications for schema changes."""
from schema_drift import SchemaDrift
from schema_drift.integrations.slack import notify, make_notifier

WEBHOOK_URL = "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"

drift = SchemaDrift("postgresql://user:pass@localhost/mydb")

# --- Option 1: manual notification after snapshot ---
diff = drift.snapshot("add users.email")
notify(WEBHOOK_URL, diff)

# --- Option 2: only notify on breaking changes ---
notify(WEBHOOK_URL, diff, only_breaking=True)

# --- Option 3: use as watch callback ---
notifier = make_notifier(WEBHOOK_URL)
drift.watch(on_change=notifier, interval=60)
