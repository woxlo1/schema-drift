"""schema_drift.export — export schema snapshots to SQL / CSV / JSON"""
from __future__ import annotations
import csv
import io
import json
from pathlib import Path
from typing import Any


def to_json(schema: dict, indent: int = 2) -> str:
    """Export schema as pretty-printed JSON."""
    return json.dumps(schema, indent=indent, default=str)


def to_csv(schema: dict) -> str:
    """Export schema as CSV with columns: table, column, type, nullable, default."""
    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(["table", "column", "type", "nullable", "default"])
    for table, info in sorted(schema.items()):
        if table.startswith("__"):
            continue
        for col, defn in sorted(info.get("columns", {}).items()):
            writer.writerow([
                table,
                col,
                defn.get("type", ""),
                defn.get("nullable", ""),
                defn.get("default", ""),
            ])
    return out.getvalue()


def to_sql(schema: dict, dialect: str = "generic") -> str:
    """
    Export schema as CREATE TABLE SQL statements.

    Args:
        schema:  Normalized schema dict from a snapshot.
        dialect: "generic", "postgres", "mysql", or "sqlite".
    """
    lines: list[str] = []

    type_map: dict = {}
    if dialect == "sqlite":
        type_map = {"character varying": "TEXT", "integer": "INTEGER", "boolean": "INTEGER"}
    elif dialect in ("postgres", "postgresql"):
        type_map = {"varchar": "VARCHAR", "int": "INTEGER"}

    for table, info in sorted(schema.items()):
        if table.startswith("__") or table.startswith("#/"):
            continue
        cols = info.get("columns", {})
        if not cols:
            continue

        lines.append(f"CREATE TABLE {table} (")
        col_lines = []
        for col, defn in cols.items():
            col_type = type_map.get(defn.get("type", "").lower(), defn.get("type", "TEXT").upper())
            nullable = "" if defn.get("nullable", True) else " NOT NULL"
            default = f" DEFAULT {defn['default']}" if defn.get("default") else ""
            pk = " PRIMARY KEY" if defn.get("primary_key") else ""
            col_lines.append(f"  {col} {col_type}{pk}{nullable}{default}")

        lines.append(",\n".join(col_lines))
        lines.append(");\n")

    return "\n".join(lines)


def save(content: str, path: str | Path) -> None:
    """Write exported content to a file."""
    Path(path).write_text(content, encoding="utf-8")
    print(f"exported to {path}")
