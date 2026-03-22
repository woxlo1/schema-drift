"""
schema_drift.backends.base

Abstract base class for all database/schema backends.
"""
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any


class BaseBackend(ABC):
    """All backends must implement extract()."""

    @abstractmethod
    def extract(self) -> dict:
        """Extract the current schema and return a normalized dict."""
        ...

    @classmethod
    def accepts(cls, connection: Any) -> bool:
        """Return True if this backend can handle the given connection."""
        return False
