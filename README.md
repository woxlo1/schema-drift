# schema-drift

> Track **why** your schema changed, not just what changed.

`schema-drift` is a lightweight Python library that records your database schema over time — with messages, diffs, and history — like `git log` but for your tables.

Migration tools run SQL. `schema-drift` remembers the story.

```
2024-03-01  a3f9b2  initial schema            8 tables, 42 columns
2024-03-15  b71cd4  added users.email         + users.email (TEXT)
2024-04-02  cc82a1  dropped legacy_id         - orders.legacy_id
```

---

## Install

```bash
pip install schema-drift

# PostgreSQL support
pip install "schema-drift[postgres]"
```

---

## Quickstart

```python
from schema_drift import SchemaDrift

# SQLite
drift = SchemaDrift("mydb.sqlite")

# PostgreSQL
drift = SchemaDrift("postgresql://user:pass@localhost/mydb")

# Or pass an existing connection
import sqlite3
conn = sqlite3.connect("mydb.sqlite")
drift = SchemaDrift(conn)
```

### Take a snapshot

```python
drift.snapshot("initial schema")
```

### See what changed

```python
# After an ALTER TABLE...
drift.snapshot("added users.email for auth feature #42")

# Output:
# + users.email  (TEXT)
```

### View history

```python
drift.log()

# date         id             tables   cols  message
# ─────────────────────────────────────────────────────────────
# 2024-03-01   a3f9b2c1d4e5      8      42  initial schema
# 2024-03-15   b71cd4f2a3b1      8      43  added users.email for auth feature #42
```

### Diff two snapshots

```python
drift.diff()        # last two snapshots
drift.diff(-3, -1)  # between any two
```

### Inspect an old schema

```python
old = drift.rollback(0)   # schema at first snapshot
old["users"]["columns"]   # {"id": ..., "name": ..., "age": ...}
```

---

## Why not just use git?

Git tracks your migration *files*. `schema-drift` tracks the actual **state of your database**. These drift apart constantly — applied hotfixes, out-of-band changes, environment differences. `schema-drift` catches all of that.

## Why not Alembic / Flyway?

Those tools *apply* migrations. `schema-drift` *observes and records* schema state. They work great together — run `drift.snapshot()` after each migration to build a human-readable audit trail.

---

## Roadmap

- [x] v0.1 — SQLite + PostgreSQL, snapshot / diff / log / rollback
- [ ] v0.2 — CLI (`schema-drift log`, `schema-drift diff`)
- [ ] v0.3 — GitHub Actions integration (auto-comment schema diffs on PRs)
- [ ] v1.0 — OpenAPI / JSON Schema support

---

## Contributing

PRs welcome!

```bash
git clone https://github.com/woxlo1/schema-drift
cd schema-drift
pip install -e ".[dev]"
pytest
```

## License

MIT
