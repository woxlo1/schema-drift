"""
schema_drift.integrations.django

Auto-snapshot after Django migrations.

Usage::

    # In your AppConfig.ready():
    from schema_drift import SchemaDrift
    from schema_drift.integrations.django import register

    drift = SchemaDrift("postgresql://localhost/mydb")
    register(drift)
"""
from __future__ import annotations
from typing import Any


def register(
    drift: Any,
    message_template: str = "django: {app}.{migration}",
    only_on_upgrade: bool = True,
) -> None:
    """
    Register schema-drift hooks into Django's post_migrate signal.

    Args:
        drift:            SchemaDrift instance.
        message_template: Snapshot message. {app} and {migration} are replaced.
        only_on_upgrade:  If True (default), only snapshot on forward migrations.
    """
    try:
        from django.db.models.signals import post_migrate
    except ImportError:
        raise ImportError("Django is required: pip install django")

    def on_post_migrate(sender: Any, **kwargs: Any) -> None:
        plan = kwargs.get("plan", [])
        if not plan:
            return

        migration, is_backward = plan[-1]
        if only_on_upgrade and is_backward:
            return

        app = getattr(sender, "label", "unknown")
        migration_name = getattr(migration, "name", str(migration))
        message = message_template.format(app=app, migration=migration_name)

        try:
            drift.snapshot(message)
        except Exception as e:
            print(f"schema-drift: snapshot failed after migration: {e}")

    post_migrate.connect(on_post_migrate, weak=False)


def register_pre_migrate(
    drift: Any,
    message_template: str = "django pre-migrate: {app}",
) -> None:
    """
    Also snapshot before migrations run (useful for before/after comparison).
    """
    try:
        from django.db.models.signals import pre_migrate
    except ImportError:
        raise ImportError("Django is required: pip install django")

    def on_pre_migrate(sender: Any, **kwargs: Any) -> None:
        app = getattr(sender, "label", "unknown")
        message = message_template.format(app=app)
        try:
            drift.snapshot(message)
        except Exception as e:
            print(f"schema-drift: pre-migrate snapshot failed: {e}")

    pre_migrate.connect(on_pre_migrate, weak=False)
