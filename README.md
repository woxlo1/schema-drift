# schema-drift

> Track **why** your schema changed, not just what changed.

`schema-drift` is a lightweight Python library that records your database schema over time — with messages, diffs, and history — like `git log` but for your tables.

Migration tools run SQL. `schema-drift` remembers the story.

![demo](assets/demo.png)

---

## Install

```bash
pip install git+https://github.com/woxlo1/schema-drift.git

# With PostgreSQL support
pip install "schema-drift[postgres] @ git+https://github.com/woxlo1/schema-drift.git"

# With MySQL / MariaDB support
pip install "schema-drift[mysql] @ git+https://github.com/woxlo1/schema-drift.git"

# With Oracle support
pip install "schema-drift[oracle] @ git+https://github.com/woxlo1/schema-drift.git"
```

---

## Supported databases

| database | install |
| -------- | ------- |
| SQLite | built-in |
| PostgreSQL | `pip install "schema-drift[postgres]"` |
| MySQL | `pip install "schema-drift[mysql]"` |
| MariaDB | `pip install "schema-drift[mysql]"` |
| Oracle | `pip install "schema-drift[oracle]"` |
| OpenAPI 3.x | built-in (`pip install pyyaml` for YAML) |
| JSON Schema | built-in |

---

## Quickstart

```python
from schema_drift import SchemaDrift

drift = SchemaDrift("mydb.sqlite")
drift.snapshot("initial schema")

# After ALTER TABLE...
drift.snapshot("add users.email")

drift.log()
drift.diff()
```

See [QUICKSTART.md](schema_drift/QUICKSTART.md) for more examples.

---

## CLI

```bash
schema-drift --db ./mydb.sqlite snapshot "initial schema"
schema-drift --db ./mydb.sqlite log
schema-drift --db ./mydb.sqlite diff
schema-drift --db ./mydb.sqlite watch --interval 30
schema-drift --db ./mydb.sqlite export --format sql
schema-drift web
```

Set `SCHEMA_DRIFT_DB` to avoid repeating `--db`:

```bash
export SCHEMA_DRIFT_DB=postgresql://user:pass@localhost/mydb
schema-drift snapshot "initial schema"
```

---

## Watch for changes

```python
drift.watch(
    interval=60,
    on_change=lambda diff: print("changed!", diff),
    on_breaking=lambda diff: print("BREAKING!", diff),
)
```

See [WATCH.md](schema_drift/WATCH.md).

---

## Export

```python
print(drift.export_json())
print(drift.export_csv())
print(drift.export_sql(dialect="postgres"))
```

See [EXPORT.md](schema_drift/EXPORT.md).

---

## Integrations

- **Alembic** — auto-snapshot after migrations
- **Django** — auto-snapshot after `manage.py migrate`
- **Slack** — notifications for schema changes

See [INTEGRATIONS.md](schema_drift/INTEGRATIONS.md).

---

## Web UI

```bash
schema-drift web
```

Open `http://127.0.0.1:8080` to browse snapshot history in your browser.

See [WEB-UI.md](schema_drift/WEB-UI.md).

---

## GitHub Actions

Automatically comment schema diffs on PRs and fail CI on breaking changes.

See [.github/workflows/schema-drift.yml](.github/workflows/schema-drift.yml).

---

## Why not just use git?

Git tracks your migration *files*. `schema-drift` tracks the actual **state of your database**. These drift apart constantly — applied hotfixes, out-of-band changes, environment differences. `schema-drift` catches all of that.

## Why not Alembic / Flyway?

Those tools *apply* migrations. `schema-drift` *observes and records* schema state. They work great together.

---

## Roadmap

- [x] v0.1 — SQLite + PostgreSQL, snapshot / diff / log / rollback
- [x] v0.2 — CLI
- [x] v0.3 — GitHub Actions integration
- [x] v1.0 — OpenAPI / JSON Schema support
- [x] v1.1 — MySQL, MariaDB, Oracle support + drift.watch()
- [x] v2.0 — Modular backends, integrations, Web UI, export

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

```bash
git clone https://github.com/woxlo1/schema-drift
cd schema-drift
pip install -e ".[dev]"
pytest
```

## License

MIT
