"""schema_drift.integrations.alembic — Alembic event hooks"""
from __future__ import annotations
from typing import Any


def register(drift: Any, message_template: str = "alembic migration: {revision}") -> None:
    """
    Register schema-drift hooks into Alembic's event system.

    After each successful migration, a snapshot is automatically taken.

    Usage::

        from alembic import context
        from schema_drift import SchemaDrift
        from schema_drift.integrations.alembic import register

        drift = SchemaDrift("postgresql://localhost/mydb")
        register(drift)

    Args:
        drift:            SchemaDrift instance to use for snapshots.
        message_template: Message format. {revision} is replaced with the migration revision.
    """
    try:
        from alembic.runtime.migration import MigrationContext
        from alembic import events as alembic_events
    except ImportError:
        raise ImportError("Alembic is required: pip install alembic")

    def after_cursor_execute(context: MigrationContext, **kw: Any) -> None:
        revision = getattr(context, "get_current_revision", lambda: "unknown")()
        message = message_template.format(revision=revision or "unknown")
        try:
            drift.snapshot(message)
        except Exception as e:
            print(f"schema-drift: snapshot failed after migration: {e}")

    alembic_events.register(after_cursor_execute, "after_cursor_execute")
