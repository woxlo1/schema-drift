"""
schema_drift.openapi

OpenAPI 3.x and JSON Schema support for schema-drift.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


# ── OpenAPI extractor ──────────────────────────────────────────────────────────

def _extract_openapi(source: str | dict) -> dict:
    """
    Parse an OpenAPI 3.x spec (file path, URL string, or dict) into a
    normalized schema dict that _diff_schemas can compare.
    """
    if isinstance(source, dict):
        spec = source
    elif isinstance(source, str):
        path = Path(source)
        if not path.exists():
            raise FileNotFoundError(f"OpenAPI file not found: {source}")
        with path.open() as f:
            if source.endswith(".json"):
                spec = json.load(f)
            else:
                try:
                    import yaml
                    spec = yaml.safe_load(f)
                except ImportError:
                    raise ImportError("PyYAML is required for YAML support: pip install pyyaml")
    else:
        raise TypeError(f"Expected file path or dict, got {type(source)}")

    schema: dict = {
        "__meta__": {
            "title": spec.get("info", {}).get("title", ""),
            "version": spec.get("info", {}).get("version", ""),
            "openapi": spec.get("openapi", ""),
        }
    }

    # Paths → endpoints
    for path, path_item in (spec.get("paths") or {}).items():
        for method, operation in path_item.items():
            if method.startswith("x-") or method == "parameters":
                continue
            key = f"{method.upper()} {path}"
            schema[key] = {
                "columns": _extract_operation(operation, spec),
                "indexes": {},
            }

    # Components → schemas (as "tables")
    for name, json_schema in (spec.get("components", {}).get("schemas") or {}).items():
        key = f"#/components/schemas/{name}"
        schema[key] = {
            "columns": _extract_json_schema_properties(json_schema, spec),
            "indexes": {"required": {"columns": json_schema.get("required", []), "unique": False}},
        }

    return schema


def _resolve_ref(ref: str, spec: dict) -> dict:
    """Resolve a $ref like '#/components/schemas/User'."""
    if not ref.startswith("#/"):
        return {}
    parts = ref.lstrip("#/").split("/")
    node = spec
    for part in parts:
        node = node.get(part, {})
    return node


def _extract_operation(operation: dict, spec: dict) -> dict:
    """Extract parameters + request body + responses as pseudo-columns."""
    cols: dict = {}

    # Parameters
    for param in operation.get("parameters", []):
        if "$ref" in param:
            param = _resolve_ref(param["$ref"], spec)
        name = f"param:{param.get('in', '?')}:{param.get('name', '?')}"
        cols[name] = {
            "type": _schema_type(param.get("schema", {})),
            "nullable": not param.get("required", False),
            "default": param.get("schema", {}).get("default"),
        }

    # Request body
    rb = operation.get("requestBody", {})
    if rb:
        content = rb.get("content", {})
        for media_type, media in content.items():
            schema = media.get("schema", {})
            if "$ref" in schema:
                schema = _resolve_ref(schema["$ref"], spec)
            cols[f"requestBody:{media_type}"] = {
                "type": _schema_type(schema),
                "nullable": not rb.get("required", False),
                "default": None,
            }

    # Responses
    for status, response in operation.get("responses", {}).items():
        if "$ref" in response:
            response = _resolve_ref(response["$ref"], spec)
        content = response.get("content", {})
        for media_type, media in content.items():
            schema = media.get("schema", {})
            if "$ref" in schema:
                schema = _resolve_ref(schema["$ref"], spec)
            cols[f"response:{status}:{media_type}"] = {
                "type": _schema_type(schema),
                "nullable": True,
                "default": None,
            }

    return cols


def _extract_json_schema_properties(schema: dict, spec: dict) -> dict:
    """Extract properties from a JSON Schema object."""
    cols: dict = {}
    required = set(schema.get("required", []))

    for prop, definition in (schema.get("properties") or {}).items():
        if "$ref" in definition:
            definition = _resolve_ref(definition["$ref"], spec)
        cols[prop] = {
            "type": _schema_type(definition),
            "nullable": prop not in required,
            "default": definition.get("default"),
        }

    return cols


def _schema_type(schema: dict) -> str:
    """Produce a compact type string from a JSON Schema fragment."""
    if not schema:
        return "any"
    if "$ref" in schema:
        return schema["$ref"].split("/")[-1]
    t = schema.get("type", "")
    fmt = schema.get("format", "")
    items = schema.get("items", {})
    if t == "array" and items:
        return f"array<{_schema_type(items)}>"
    if fmt:
        return f"{t}({fmt})"
    return t or "any"


# ── JSON Schema extractor ──────────────────────────────────────────────────────

def _extract_json_schema(source: str | dict) -> dict:
    """
    Parse a standalone JSON Schema file or dict into the normalized schema dict.
    """
    if isinstance(source, dict):
        root = source
    elif isinstance(source, str):
        path = Path(source)
        if not path.exists():
            raise FileNotFoundError(f"JSON Schema file not found: {source}")
        with path.open() as f:
            root = json.load(f)
    else:
        raise TypeError(f"Expected file path or dict, got {type(source)}")

    title = root.get("title", "root")
    schema: dict = {
        title: {
            "columns": _extract_json_schema_properties(root, root),
            "indexes": {"required": {"columns": root.get("required", []), "unique": False}},
        }
    }

    # Also extract $defs / definitions
    for section in ("$defs", "definitions"):
        for name, defn in (root.get(section) or {}).items():
            schema[name] = {
                "columns": _extract_json_schema_properties(defn, root),
                "indexes": {"required": {"columns": defn.get("required", []), "unique": False}},
            }

    return schema


# ── Type detection ─────────────────────────────────────────────────────────────

def detect_source_type(source: Any) -> str:
    """Return 'openapi', 'jsonschema', 'postgres', or 'sqlite'."""
    if isinstance(source, dict):
        if "openapi" in source or "paths" in source:
            return "openapi"
        if "$schema" in source or "properties" in source:
            return "jsonschema"
        return "jsonschema"

    if isinstance(source, str):
        low = source.lower()
        if low.endswith((".yaml", ".yml")):
            return "openapi"
        if low.endswith(".json"):
            # Peek at the file to distinguish OpenAPI vs JSON Schema
            try:
                with open(source) as f:
                    data = json.load(f)
                if "openapi" in data or "paths" in data:
                    return "openapi"
                return "jsonschema"
            except Exception:
                return "jsonschema"
        if low.startswith(("postgresql://", "postgres://")):
            return "postgres"
        # SQLite file or :memory:
        return "sqlite"

    return "sqlite"
