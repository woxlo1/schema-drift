"""Tests for OpenAPI and JSON Schema support."""
import json
import pytest
from pathlib import Path
from schema_drift import SchemaDrift
from schema_drift.openapi import (
    _extract_openapi,
    _extract_json_schema,
    detect_source_type,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

OPENAPI_SPEC = {
    "openapi": "3.0.0",
    "info": {"title": "Test API", "version": "1.0.0"},
    "paths": {
        "/users": {
            "get": {
                "parameters": [{"in": "query", "name": "limit", "schema": {"type": "integer"}}],
                "responses": {"200": {"content": {"application/json": {"schema": {"type": "array"}}}}},
            },
            "post": {
                "requestBody": {
                    "required": True,
                    "content": {"application/json": {"schema": {"$ref": "#/components/schemas/User"}}},
                },
                "responses": {"201": {"content": {"application/json": {"schema": {"$ref": "#/components/schemas/User"}}}}},
            },
        },
        "/users/{id}": {
            "get": {
                "parameters": [{"in": "path", "name": "id", "required": True, "schema": {"type": "string"}}],
                "responses": {"200": {"content": {"application/json": {"schema": {"$ref": "#/components/schemas/User"}}}}},
            },
            "delete": {
                "responses": {"204": {}},
            },
        },
    },
    "components": {
        "schemas": {
            "User": {
                "type": "object",
                "required": ["id", "name"],
                "properties": {
                    "id": {"type": "string", "format": "uuid"},
                    "name": {"type": "string"},
                    "email": {"type": "string", "format": "email"},
                },
            }
        }
    },
}

JSON_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "Product",
    "type": "object",
    "required": ["id", "name", "price"],
    "properties": {
        "id": {"type": "integer"},
        "name": {"type": "string"},
        "price": {"type": "number"},
        "description": {"type": "string"},
    },
    "$defs": {
        "Category": {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "label": {"type": "string"},
            },
        }
    },
}


# ── detect_source_type ────────────────────────────────────────────────────────

def test_detect_openapi_dict():
    assert detect_source_type({"openapi": "3.0.0", "paths": {}}) == "openapi"

def test_detect_jsonschema_dict():
    assert detect_source_type({"$schema": "...", "properties": {}}) == "jsonschema"

def test_detect_postgres_url():
    assert detect_source_type("postgresql://localhost/mydb") == "postgres"

def test_detect_sqlite():
    assert detect_source_type("mydb.sqlite") == "sqlite"

def test_detect_yaml_extension():
    assert detect_source_type("openapi.yaml") == "openapi"

def test_detect_yml_extension():
    assert detect_source_type("spec.yml") == "openapi"


# ── OpenAPI extraction ────────────────────────────────────────────────────────

def test_openapi_extracts_endpoints():
    schema = _extract_openapi(OPENAPI_SPEC)
    assert "GET /users" in schema
    assert "POST /users" in schema
    assert "GET /users/{id}" in schema
    assert "DELETE /users/{id}" in schema

def test_openapi_extracts_components():
    schema = _extract_openapi(OPENAPI_SPEC)
    assert "#/components/schemas/User" in schema

def test_openapi_extracts_parameters():
    schema = _extract_openapi(OPENAPI_SPEC)
    cols = schema["GET /users"]["columns"]
    assert any("limit" in k for k in cols)

def test_openapi_extracts_component_properties():
    schema = _extract_openapi(OPENAPI_SPEC)
    cols = schema["#/components/schemas/User"]["columns"]
    assert "id" in cols
    assert "name" in cols
    assert "email" in cols

def test_openapi_from_file(tmp_path):
    spec_file = tmp_path / "openapi.json"
    spec_file.write_text(json.dumps(OPENAPI_SPEC))
    schema = _extract_openapi(str(spec_file))
    assert "GET /users" in schema

def test_openapi_missing_file_raises():
    with pytest.raises(FileNotFoundError):
        _extract_openapi("/nonexistent/openapi.json")


# ── JSON Schema extraction ────────────────────────────────────────────────────

def test_jsonschema_extracts_root():
    schema = _extract_json_schema(JSON_SCHEMA)
    assert "Product" in schema

def test_jsonschema_extracts_properties():
    schema = _extract_json_schema(JSON_SCHEMA)
    cols = schema["Product"]["columns"]
    assert "id" in cols
    assert "name" in cols
    assert "price" in cols
    assert "description" in cols

def test_jsonschema_extracts_defs():
    schema = _extract_json_schema(JSON_SCHEMA)
    assert "Category" in schema

def test_jsonschema_from_file(tmp_path):
    schema_file = tmp_path / "schema.json"
    schema_file.write_text(json.dumps(JSON_SCHEMA))
    schema = _extract_json_schema(str(schema_file))
    assert "Product" in schema


# ── SchemaDrift integration ───────────────────────────────────────────────────

def test_schemadrift_openapi_snapshot(tmp_path):
    storage = tmp_path / "drift.json"
    drift = SchemaDrift(OPENAPI_SPEC, storage_path=storage)
    drift.snapshot("initial API")
    history = json.loads(storage.read_text())
    assert len(history) == 1
    assert "GET /users" in history[0]["schema"]

def test_schemadrift_openapi_diff_detects_new_endpoint(tmp_path):
    storage = tmp_path / "drift.json"
    drift = SchemaDrift(OPENAPI_SPEC, storage_path=storage)
    drift.snapshot("initial")

    # Add a new endpoint
    updated = json.loads(json.dumps(OPENAPI_SPEC))
    updated["paths"]["/posts"] = {
        "get": {"responses": {"200": {}}}
    }
    drift2 = SchemaDrift(updated, storage_path=storage)
    diff = drift2.snapshot("add /posts")
    assert "GET /posts" in diff["tables_added"]

def test_schemadrift_openapi_diff_detects_removed_endpoint(tmp_path):
    storage = tmp_path / "drift.json"
    drift = SchemaDrift(OPENAPI_SPEC, storage_path=storage)
    drift.snapshot("initial")

    updated = json.loads(json.dumps(OPENAPI_SPEC))
    del updated["paths"]["/users/{id}"]
    drift2 = SchemaDrift(updated, storage_path=storage)
    diff = drift2.snapshot("remove /users/{id}")
    assert any("/users/{id}" in t for t in diff["tables_removed"])

def test_schemadrift_jsonschema_snapshot(tmp_path):
    storage = tmp_path / "drift.json"
    drift = SchemaDrift(JSON_SCHEMA, storage_path=storage)
    drift.snapshot("initial schema")
    history = json.loads(storage.read_text())
    assert "Product" in history[0]["schema"]

def test_schemadrift_jsonschema_diff_detects_new_field(tmp_path):
    storage = tmp_path / "drift.json"
    drift = SchemaDrift(JSON_SCHEMA, storage_path=storage)
    drift.snapshot("initial")

    updated = json.loads(json.dumps(JSON_SCHEMA))
    updated["properties"]["tags"] = {"type": "array", "items": {"type": "string"}}
    drift2 = SchemaDrift(updated, storage_path=storage)
    diff = drift2.snapshot("add tags field")
    added_cols = [c["column"] for c in diff["columns_added"]]
    assert "tags" in added_cols
