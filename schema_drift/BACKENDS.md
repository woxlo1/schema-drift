# Backends

schema-drift supports the following backends. Detection is automatic based on the connection string.

## SQLite

Built-in, no extra dependencies required.

```python
drift = SchemaDrift("mydb.sqlite")
drift = SchemaDrift(":memory:")

import sqlite3
conn = sqlite3.connect("mydb.sqlite")
drift = SchemaDrift(conn)
```

## PostgreSQL

```bash
pip install "schema-drift[postgres]"
```

```python
drift = SchemaDrift("postgresql://user:pass@localhost/mydb")
```

## MySQL

```bash
pip install "schema-drift[mysql]"
```

```python
drift = SchemaDrift("mysql://user:pass@localhost/mydb")
```

## MariaDB

Uses the same driver as MySQL.

```bash
pip install "schema-drift[mysql]"
```

```python
drift = SchemaDrift("mariadb://user:pass@localhost/mydb")
```

## Oracle

```bash
pip install "schema-drift[oracle]"
```

```python
drift = SchemaDrift("oracle://user:pass@localhost/ORCL")
```

## OpenAPI

Tracks API endpoints and component schemas.

```python
drift = SchemaDrift("openapi.json")
drift = SchemaDrift("openapi.yaml")  # requires pip install pyyaml
drift = SchemaDrift({"openapi": "3.0.0", "paths": {...}})
```

## JSON Schema

Tracks properties and definitions.

```python
drift = SchemaDrift("schema.json")
drift = SchemaDrift({"title": "User", "properties": {...}})
```

## Explicit db_type

```python
drift = SchemaDrift(conn, db_type="postgres")
drift = SchemaDrift(conn, db_type="mysql")
drift = SchemaDrift(conn, db_type="sqlite")
drift = SchemaDrift(conn, db_type="oracle")
drift = SchemaDrift(conn, db_type="openapi")
drift = SchemaDrift(conn, db_type="jsonschema")
```
