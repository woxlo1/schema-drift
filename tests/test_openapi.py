"""Tests for OpenAPI and JSON Schema backends."""
import json
import pytest
from schema_drift import SchemaDrift
from schema_drift.backends import get_backend
from schema_drift.backends.openapi import OpenAPIBackend, JSONSchemaBackend
from schema_drift.backends.sqlite import SQLiteBackend

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
                "requestBody": {"required": True, "content": {"application/json": {"schema": {"$ref": "#/components/schemas/User"}}}},
                "responses": {"201": {"content": {"application/json": {"schema": {"$ref": "#/components/schemas/User"}}}}},
            },
        },
        "/users/{id}": {
            "get": {
                "parameters": [{"in": "path", "name": "id", "required": True, "schema": {"type": "string"}}],
                "responses": {"200": {"content": {"application/json": {"schema": {"$ref": "#/components/schemas/User"}}}}},
            },
            "delete": {"responses": {"204": {}}},
        },
    },
    "components": {"schemas": {"User": {"type": "object", "required": ["id", "name"], "properties": {"id": {"type": "string", "format": "uuid"}, "name": {"type": "string"}, "email": {"type": "string", "format": "email"}}}}},
}

JSON_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "Product",
    "type": "object",
    "required": ["id", "name", "price"],
    "properties": {"id": {"type": "integer"}, "name": {"type": "string"}, "price": {"type": "number"}, "description": {"type": "string"}},
    "$defs": {"Category": {"type": "object", "properties": {"id": {"type": "integer"}, "label": {"type": "string"}}}},
}

def test_detect_openapi_dict():
    assert isinstance(get_backend({"openapi": "3.0.0", "paths": {}}), OpenAPIBackend)

def test_detect_jsonschema_dict():
    assert isinstance(get_backend({"$schema": "...", "properties": {}}), JSONSchemaBackend)

def test_detect_sqlite():
    assert isinstance(get_backend("mydb.sqlite"), SQLiteBackend)

def test_detect_yaml_extension():
    assert isinstance(get_backend("openapi.yaml"), OpenAPIBackend)

def test_detect_yml_extension():
    assert isinstance(get_backend("spec.yml"), OpenAPIBackend)

def test_openapi_extracts_endpoints():
    schema = OpenAPIBackend(OPENAPI_SPEC).extract()
    assert "GET /users" in schema
    assert "POST /users" in schema
    assert "GET /users/{id}" in schema
    assert "DELETE /users/{id}" in schema

def test_openapi_extracts_components():
    assert "#/components/schemas/User" in OpenAPIBackend(OPENAPI_SPEC).extract()

def test_openapi_extracts_parameters():
    assert any("limit" in k for k in OpenAPIBackend(OPENAPI_SPEC).extract()["GET /users"]["columns"])

def test_openapi_extracts_component_properties():
    cols = OpenAPIBackend(OPENAPI_SPEC).extract()["#/components/schemas/User"]["columns"]
    assert "id" in cols and "name" in cols and "email" in cols

def test_openapi_from_file(tmp_path):
    f = tmp_path / "openapi.json"
    f.write_text(json.dumps(OPENAPI_SPEC))
    assert "GET /users" in OpenAPIBackend(str(f)).extract()

def test_openapi_missing_file_raises():
    with pytest.raises(FileNotFoundError):
        OpenAPIBackend("/nonexistent/openapi.json").extract()

def test_jsonschema_extracts_root():
    assert "Product" in JSONSchemaBackend(JSON_SCHEMA).extract()

def test_jsonschema_extracts_properties():
    cols = JSONSchemaBackend(JSON_SCHEMA).extract()["Product"]["columns"]
    assert "id" in cols and "name" in cols and "price" in cols

def test_jsonschema_extracts_defs():
    assert "Category" in JSONSchemaBackend(JSON_SCHEMA).extract()

def test_jsonschema_from_file(tmp_path):
    f = tmp_path / "schema.json"
    f.write_text(json.dumps(JSON_SCHEMA))
    assert "Product" in JSONSchemaBackend(str(f)).extract()

def test_schemadrift_openapi_snapshot(tmp_path):
    drift = SchemaDrift(OPENAPI_SPEC, storage_path=tmp_path / "drift.json")
    drift.snapshot("initial API")
    assert "GET /users" in json.loads((tmp_path / "drift.json").read_text())[0]["schema"]

def test_schemadrift_openapi_diff_new_endpoint(tmp_path):
    storage = tmp_path / "drift.json"
    SchemaDrift(OPENAPI_SPEC, storage_path=storage).snapshot("initial")
    updated = json.loads(json.dumps(OPENAPI_SPEC))
    updated["paths"]["/posts"] = {"get": {"responses": {"200": {}}}}
    diff = SchemaDrift(updated, storage_path=storage).snapshot("add /posts")
    assert "GET /posts" in diff["tables_added"]

def test_schemadrift_openapi_diff_removed_endpoint(tmp_path):
    storage = tmp_path / "drift.json"
    SchemaDrift(OPENAPI_SPEC, storage_path=storage).snapshot("initial")
    updated = json.loads(json.dumps(OPENAPI_SPEC))
    del updated["paths"]["/users/{id}"]
    diff = SchemaDrift(updated, storage_path=storage).snapshot("remove")
    assert any("/users/{id}" in t for t in diff["tables_removed"])

def test_schemadrift_jsonschema_snapshot(tmp_path):
    drift = SchemaDrift(JSON_SCHEMA, storage_path=tmp_path / "drift.json")
    drift.snapshot("initial")
    assert "Product" in json.loads((tmp_path / "drift.json").read_text())[0]["schema"]

def test_schemadrift_jsonschema_diff_new_field(tmp_path):
    storage = tmp_path / "drift.json"
    SchemaDrift(JSON_SCHEMA, storage_path=storage).snapshot("initial")
    updated = json.loads(json.dumps(JSON_SCHEMA))
    updated["properties"]["tags"] = {"type": "array", "items": {"type": "string"}}
    diff = SchemaDrift(updated, storage_path=storage).snapshot("add tags")
    assert "tags" in [c["column"] for c in diff["columns_added"]]
