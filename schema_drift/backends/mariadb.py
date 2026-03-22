"""schema_drift.backends.mariadb — reuses MySQL extractor (wire-compatible)"""
from __future__ import annotations
from typing import Any
from .base import BaseBackend
from .mysql import MySQLBackend


class MariaDBBackend(BaseBackend):
    def __init__(self, connection: Any):
        # Rewrite mariadb:// → mysql:// for the connector
        if isinstance(connection, str):
            connection = connection.replace("mariadb://", "mysql://", 1)
            connection = connection.replace("mariadb+", "mysql+", 1)
        self._backend = MySQLBackend(connection)

    @classmethod
    def accepts(cls, connection: Any) -> bool:
        if isinstance(connection, str):
            return connection.startswith(("mariadb://", "mariadb+"))
        return False

    def extract(self) -> dict:
        return self._backend.extract()
