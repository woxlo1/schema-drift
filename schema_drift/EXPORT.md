# Exporting schemas

Export any snapshot to JSON, CSV, or SQL.

## Python API

```python
from schema_drift import SchemaDrift

drift = SchemaDrift("mydb.sqlite")
drift.snapshot("initial schema")

# Export latest snapshot
print(drift.export_json())
print(drift.export_csv())
print(drift.export_sql())

# Export specific snapshot by index
print(drift.export_sql(index=0, dialect="postgres"))
```

## SQL dialects

| dialect | description |
| ------- | ----------- |
| `generic` | standard SQL (default) |
| `postgres` | PostgreSQL |
| `mysql` | MySQL / MariaDB |
| `sqlite` | SQLite |

## CLI

```bash
schema-drift --db ./mydb.sqlite export --format json
schema-drift --db ./mydb.sqlite export --format csv
schema-drift --db ./mydb.sqlite export --format sql
schema-drift --db ./mydb.sqlite export --format sql --dialect postgres
```

## Save to file

```python
from schema_drift.export import save

save(drift.export_csv(), "schema.csv")
save(drift.export_sql(dialect="postgres"), "schema.sql")
```
