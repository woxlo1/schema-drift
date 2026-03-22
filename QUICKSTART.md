# Quickstart

## Installation

```bash
pip install git+https://github.com/woxlo1/schema-drift.git
```

## Basic usage

```python
from schema_drift import SchemaDrift

drift = SchemaDrift("mydb.sqlite")
drift.snapshot("initial schema")

# After ALTER TABLE...
drift.snapshot("add users.email")

drift.log()
drift.diff()
```

## CLI

```bash
schema-drift --db ./mydb.sqlite snapshot "initial schema"
schema-drift --db ./mydb.sqlite log
schema-drift --db ./mydb.sqlite diff
```

Set `SCHEMA_DRIFT_DB` to avoid repeating `--db`:

```bash
export SCHEMA_DRIFT_DB=postgresql://user:pass@localhost/mydb
schema-drift snapshot "initial schema"
```
