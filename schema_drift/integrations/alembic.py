"""
schema_drift.integrations.alembic

Auto-snapshot after every Alembic migration.

Usage::

    # In env.py (same file where you call context.run_migrations())
    from schema_drift import SchemaDrift
    from schema_drift.integrations.alembic import register

    drift = SchemaDrift("postgresql://localhost/mydb")
    register(drift)

    # That's it — every migration will now auto-snapshot.
"""
from __future__ import annotations
from typing import Any


def register(
    drift: Any,
    message_template: str = "alembic: {revision}",
    only_on_upgrade: bool = True,
) -> None:
    """
    Register schema-drift hooks into Alembic's migration context.

    Args:
        drift:            SchemaDrift instance.
        message_template: Snapshot message. {revision} is replaced with the
                          migration revision ID.
        only_on_upgrade:  If True (default), only snapshot on upgrades,
                          not downgrades.
    """
    try:
        from alembic.runtime.environment import EnvironmentContext
    except ImportError:
        raise ImportError("Alembic is required: pip install alembic")

    original_run = EnvironmentContext.run_migrations

    def patched_run(self, **kw: Any) -> None:
        is_upgrade = getattr(self, "_update_kwargs", {}).get("is_upgrade", True)
        if only_on_upgrade and not is_upgrade:
            return original_run(self, **kw)

        original_run(self, **kw)

        try:
            ctx = self.get_context()
            revision = ctx.get_current_revision() or "unknown"
            message = message_template.format(revision=revision)
            drift.snapshot(message)
        except Exception as e:
            print(f"schema-drift: snapshot failed after migration: {e}")

    EnvironmentContext.run_migrations = patched_run


def listener(drift: Any, message_template: str = "alembic: {revision}"):
    """
    Return an after_cursor_execute listener for manual registration.

    Usage::

        from sqlalchemy import event
        from schema_drift.integrations.alembic import listener

        engine = create_engine(...)
        event.listen(engine, "after_cursor_execute", listener(drift))
    """
    def _after_execute(conn, cursor, statement, parameters, context, executemany):
        if not statement.strip().upper().startswith(("ALTER", "CREATE", "DROP")):
            return
        try:
            revision = getattr(context, "migration_context", None)
            rev_id = getattr(revision, "get_current_revision", lambda: "unknown")()
            message = message_template.format(revision=rev_id or "unknown")
            drift.snapshot(message)
        except Exception as e:
            print(f"schema-drift: snapshot failed: {e}")

    return _after_execute
