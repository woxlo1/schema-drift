"""schema_drift.integrations.django — Django migrations signal hooks"""
from __future__ import annotations
from typing import Any


def register(drift: Any, message_template: str = "django migration: {app}.{migration}") -> None:
    """
    Register schema-drift hooks into Django's post_migrate signal.

    After each `manage.py migrate`, a snapshot is automatically taken.

    Usage (in AppConfig.ready())::

        from schema_drift import SchemaDrift
        from schema_drift.integrations.django import register

        drift = SchemaDrift("postgresql://localhost/mydb")
        register(drift)

    Args:
        drift:            SchemaDrift instance to use for snapshots.
        message_template: Message format. {app} and {migration} are replaced.
    """
    try:
        from django.db.models.signals import post_migrate
    except ImportError:
        raise ImportError("Django is required: pip install django")

    def on_post_migrate(sender: Any, **kwargs: Any) -> None:
        app = getattr(sender, "label", "unknown")
        migration = kwargs.get("plan", [("unknown", False)])[0][0] if kwargs.get("plan") else "unknown"
        if hasattr(migration, "name"):
            migration = migration.name
        message = message_template.format(app=app, migration=str(migration))
        try:
            drift.snapshot(message)
        except Exception as e:
            print(f"schema-drift: snapshot failed after migration: {e}")

    post_migrate.connect(on_post_migrate)
