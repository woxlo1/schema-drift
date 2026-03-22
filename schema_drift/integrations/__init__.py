"""schema_drift.integrations"""
from .slack import notify as slack_notify, make_notifier as slack_notifier

__all__ = ["slack_notify", "slack_notifier"]
