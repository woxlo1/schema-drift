"""schema_drift.backends — auto-detection registry"""
from __future__ import annotations
from typing import Any
from .base import BaseBackend
from .sqlite import SQLiteBackend
from .postgres import PostgreSQLBackend
from .mysql import MySQLBackend
from .mariadb import MariaDBBackend
from .oracle import OracleBackend
from .openapi import OpenAPIBackend, JSONSchemaBackend

# Order matters — more specific backends first
BACKENDS: list[type[BaseBackend]] = [
    MariaDBBackend,
    MySQLBackend,
    PostgreSQLBackend,
    OracleBackend,
    OpenAPIBackend,
    JSONSchemaBackend,
    SQLiteBackend,  # fallback
]


def get_backend(connection: Any, db_type: str = "auto") -> BaseBackend:
    """Return the appropriate backend for the given connection."""
    if db_type != "auto":
        mapping = {
            "sqlite": SQLiteBackend,
            "postgres": PostgreSQLBackend,
            "postgresql": PostgreSQLBackend,
            "mysql": MySQLBackend,
            "mariadb": MariaDBBackend,
            "oracle": OracleBackend,
            "openapi": OpenAPIBackend,
            "jsonschema": JSONSchemaBackend,
        }
        cls = mapping.get(db_type)
        if cls:
            return cls(connection)
        raise ValueError(f"Unknown db_type: {db_type!r}")

    for cls in BACKENDS:
        if cls.accepts(connection):
            return cls(connection)

    raise ValueError(f"Could not detect backend for connection: {connection!r}")


__all__ = [
    "BaseBackend", "SQLiteBackend", "PostgreSQLBackend", "MySQLBackend",
    "MariaDBBackend", "OracleBackend", "OpenAPIBackend", "JSONSchemaBackend",
    "get_backend",
]
