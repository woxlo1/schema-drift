"""Tests for schema_drift.ci"""
import json
import pytest
from schema_drift.ci import _diff_to_markdown, MARKER


def make_diff(
    tables_added=None,
    tables_removed=None,
    columns_added=None,
    columns_removed=None,
    columns_modified=None,
    indexes_added=None,
    indexes_removed=None,
):
    return {
        "tables_added": tables_added or [],
        "tables_removed": tables_removed or [],
        "columns_added": columns_added or [],
        "columns_removed": columns_removed or [],
        "columns_modified": columns_modified or [],
        "indexes_added": indexes_added or [],
        "indexes_removed": indexes_removed or [],
    }


def test_no_changes_returns_empty():
    diff = make_diff()
    assert _diff_to_markdown(diff) == ""


def test_column_added_shows_checkmark():
    diff = make_diff(columns_added=[{"table": "users", "column": "email", "definition": {"type": "TEXT"}}])
    report = _diff_to_markdown(diff)
    assert "✅" in report
    assert "users.email" in report
    assert MARKER in report


def test_column_removed_shows_breaking():
    diff = make_diff(columns_removed=[{"table": "users", "column": "email", "was": {"type": "TEXT"}}])
    report = _diff_to_markdown(diff)
    assert "❌" in report
    assert "users.email" in report
    assert "CAUTION" in report


def test_table_dropped_is_breaking():
    diff = make_diff(tables_removed=["orders"])
    report = _diff_to_markdown(diff)
    assert "❌" in report
    assert "orders" in report
    assert "CAUTION" in report


def test_table_added_is_safe():
    diff = make_diff(tables_added=["tags"])
    report = _diff_to_markdown(diff)
    assert "✅" in report
    assert "tags" in report
    assert "NOTE" in report


def test_column_modified_is_breaking():
    diff = make_diff(columns_modified=[{
        "table": "users", "column": "age",
        "before": {"type": "INTEGER"}, "after": {"type": "TEXT"}
    }])
    report = _diff_to_markdown(diff)
    assert "⚠️" in report
    assert "INTEGER" in report
    assert "TEXT" in report
    assert "CAUTION" in report


def test_report_contains_marker():
    diff = make_diff(tables_added=["tags"])
    report = _diff_to_markdown(diff)
    assert MARKER in report


def test_mixed_breaking_and_safe():
    diff = make_diff(
        tables_added=["tags"],
        columns_removed=[{"table": "users", "column": "legacy_id", "was": {"type": "INTEGER"}}],
    )
    report = _diff_to_markdown(diff)
    assert "✅" in report
    assert "❌" in report
    assert "CAUTION" in report
