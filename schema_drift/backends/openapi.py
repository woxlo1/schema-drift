"""schema_drift.backends.openapi — OpenAPI 3.x and JSON Schema backends"""
from __future__ import annotations
import json
from pathlib import Path
from typing import Any
from .base import BaseBackend


def _resolve_ref(ref: str, spec: dict) -> dict:
    if not ref.startswith("#/"):
        return {}
    parts = ref.lstrip("#/").split("/")
    node = spec
    for part in parts:
        node = node.get(part, {})
    return node


def _schema_type(schema: dict) -> str:
    if not schema:
        return "any"
    if "$ref" in schema:
        return schema["$ref"].split("/")[-1]
    t = schema.get("type", "")
    fmt = schema.get("format", "")
    items = schema.get("items", {})
    if t == "array" and items:
        return f"array<{_schema_type(items)}>"
    return f"{t}({fmt})" if fmt else t or "any"


def _extract_operation(operation: dict, spec: dict) -> dict:
    cols: dict = {}
    for param in operation.get("parameters", []):
        if "$ref" in param:
            param = _resolve_ref(param["$ref"], spec)
        name = f"param:{param.get('in','?')}:{param.get('name','?')}"
        cols[name] = {
            "type": _schema_type(param.get("schema", {})),
            "nullable": not param.get("required", False),
            "default": param.get("schema", {}).get("default"),
        }
    rb = operation.get("requestBody", {})
    if rb:
        for media_type, media in rb.get("content", {}).items():
            s = media.get("schema", {})
            if "$ref" in s:
                s = _resolve_ref(s["$ref"], spec)
            cols[f"requestBody:{media_type}"] = {
                "type": _schema_type(s), "nullable": not rb.get("required", False), "default": None,
            }
    for status, response in operation.get("responses", {}).items():
        if "$ref" in response:
            response = _resolve_ref(response["$ref"], spec)
        for media_type, media in response.get("content", {}).items():
            s = media.get("schema", {})
            if "$ref" in s:
                s = _resolve_ref(s["$ref"], spec)
            cols[f"response:{status}:{media_type}"] = {
                "type": _schema_type(s), "nullable": True, "default": None,
            }
    return cols


def _extract_json_schema_properties(schema: dict, spec: dict) -> dict:
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


class OpenAPIBackend(BaseBackend):
    def __init__(self, connection: Any):
        self._source = connection

    @classmethod
    def accepts(cls, connection: Any) -> bool:
        if isinstance(connection, dict):
            return "openapi" in connection or "paths" in connection
        if isinstance(connection, str):
            if connection.lower().endswith((".yaml", ".yml")):
                return True
            if connection.lower().endswith(".json"):
                try:
                    with open(connection) as f:
                        data = json.load(f)
                    return "openapi" in data or "paths" in data
                except Exception:
                    pass
        return False

    def extract(self) -> dict:
        if isinstance(self._source, dict):
            spec = self._source
        else:
            path = Path(self._source)
            if not path.exists():
                raise FileNotFoundError(f"OpenAPI file not found: {self._source}")
            with path.open() as f:
                if self._source.endswith(".json"):
                    spec = json.load(f)
                else:
                    try:
                        import yaml
                        spec = yaml.safe_load(f)
                    except ImportError:
                        raise ImportError("PyYAML is required: pip install pyyaml")

        schema: dict = {
            "__meta__": {
                "title": spec.get("info", {}).get("title", ""),
                "version": spec.get("info", {}).get("version", ""),
                "openapi": spec.get("openapi", ""),
            }
        }
        for path, path_item in (spec.get("paths") or {}).items():
            for method, operation in path_item.items():
                if method.startswith("x-") or method == "parameters":
                    continue
                schema[f"{method.upper()} {path}"] = {
                    "columns": _extract_operation(operation, spec),
                    "indexes": {},
                }
        for name, json_schema in (spec.get("components", {}).get("schemas") or {}).items():
            schema[f"#/components/schemas/{name}"] = {
                "columns": _extract_json_schema_properties(json_schema, spec),
                "indexes": {"required": {"columns": json_schema.get("required", []), "unique": False}},
            }
        return schema


class JSONSchemaBackend(BaseBackend):
    def __init__(self, connection: Any):
        self._source = connection

    @classmethod
    def accepts(cls, connection: Any) -> bool:
        if isinstance(connection, dict):
            return "$schema" in connection or "properties" in connection
        if isinstance(connection, str) and connection.lower().endswith(".json"):
            try:
                with open(connection) as f:
                    data = json.load(f)
                return "$schema" in data or ("properties" in data and "openapi" not in data)
            except Exception:
                pass
        return False

    def extract(self) -> dict:
        if isinstance(self._source, dict):
            root = self._source
        else:
            path = Path(self._source)
            if not path.exists():
                raise FileNotFoundError(f"JSON Schema file not found: {self._source}")
            with path.open() as f:
                root = json.load(f)

        title = root.get("title", "root")
        schema: dict = {
            title: {
                "columns": _extract_json_schema_properties(root, root),
                "indexes": {"required": {"columns": root.get("required", []), "unique": False}},
            }
        }
        for section in ("$defs", "definitions"):
            for name, defn in (root.get(section) or {}).items():
                schema[name] = {
                    "columns": _extract_json_schema_properties(defn, root),
                    "indexes": {"required": {"columns": defn.get("required", []), "unique": False}},
                }
        return schema
