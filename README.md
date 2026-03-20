# schema-drift

> Track **why** your schema changed, not just what changed.

`schema-drift` is a lightweight Python library that records your database schema over time — with messages, diffs, and history — like `git log` but for your tables.

Migration tools run SQL. `schema-drift` remembers the story.

![demo](assets/demo.png)

---

## Install

```bash
pip install schema-drift

# PostgreSQL support
pip install "schema-drift[postgres]"

# MySQL support
pip install "schema-drift[mysql]"
```

---

## CLI

```bash
# Take a snapshot
schema-drift --db ./mydb.sqlite snapshot "initial schema"

# After an ALTER TABLE...
schema-drift --db ./mydb.sqlite snapshot "add users.email"

# View history
schema-drift --db ./mydb.sqlite log

# Show what changed between last two snapshots
schema-drift --db ./mydb.sqlite diff

# Inspect schema at a given snapshot
schema-drift --db ./mydb.sqlite rollback 0
```

Set `SCHEMA_DRIFT_DB` to avoid repeating `--db` every time:

```bash
export SCHEMA_DRIFT_DB=postgresql://user:pass@localhost/mydb
schema-drift snapshot "initial schema"
schema-drift log
```

---

## Python API

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
- [x] v0.2 — CLI (`schema-drift snapshot/log/diff/rollback`)
- [x] v0.3 — GitHub Actions integration (auto-comment schema diffs on PRs)
- [x] v1.0 — OpenAPI / JSON Schema support

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

---

## GitHub Actions

Automatically comment schema diffs on PRs and fail CI on breaking changes.

### Setup

1. Add your DB connection string as a repository secret named `SCHEMA_DRIFT_DB`
2. Create `.github/workflows/schema-drift.yml`:

```yaml
name: Schema Drift

on:
  pull_request:
    types: [opened, synchronize, reopened]

permissions:
  contents: read
  pull-requests: write

jobs:
  schema-drift:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install schema-drift
        run: pip install schema-drift

      - name: Run schema drift check
        id: drift
        env:
          SCHEMA_DRIFT_DB: ${{ secrets.SCHEMA_DRIFT_DB }}
        run: |
          python -m schema_drift.ci \
            --base-ref ${{ github.event.pull_request.base.sha }} \
            --head-ref ${{ github.event.pull_request.head.sha }} \
            --output-file drift-report.md

      - name: Comment on PR
        if: always()
        uses: actions/github-script@v7
        with:
          script: |
            const fs = require('fs');
            if (!fs.existsSync('drift-report.md')) return;
            const body = fs.readFileSync('drift-report.md', 'utf8');
            if (!body.trim()) return;
            const marker = '<!-- schema-drift-report -->';
            const { data: comments } = await github.rest.issues.listComments({
              owner: context.repo.owner, repo: context.repo.repo,
              issue_number: context.issue.number,
            });
            const existing = comments.find(c => c.body.includes(marker));
            if (existing) {
              await github.rest.issues.updateComment({
                owner: context.repo.owner, repo: context.repo.repo,
                comment_id: existing.id, body,
              });
            } else {
              await github.rest.issues.createComment({
                owner: context.repo.owner, repo: context.repo.repo,
                issue_number: context.issue.number, body,
              });
            }

      - name: Fail on breaking changes
        if: steps.drift.outputs.breaking == 'true'
        run: exit 1
```

### What it does

| event | action |
| ----- | ------ |
| column / table added | ✅ posts comment, CI passes |
| column / table dropped | ❌ posts comment, **CI fails** |
| column type changed | ⚠️ posts comment, **CI fails** |
| no schema change | silent, CI passes |

---

## OpenAPI & JSON Schema support

`schema-drift` also tracks API schema changes — not just databases.

### OpenAPI 3.x

```python
from schema_drift import SchemaDrift

# From a file
drift = SchemaDrift("openapi.json")
drift.snapshot("initial API")

# Or pass a dict directly
import yaml
with open("openapi.yaml") as f:
    spec = yaml.safe_load(f)
drift = SchemaDrift(spec)
drift.snapshot("initial API")

# After updating the spec...
drift.snapshot("add /users/{id}/posts endpoint")
drift.diff()

# Output:
# + GET /users/{id}/posts
# + POST /users/{id}/posts
```

YAML support requires PyYAML:
```bash
pip install pyyaml
```

### JSON Schema

```python
drift = SchemaDrift("schema.json")
drift.snapshot("initial schema")

# After adding a field...
drift.snapshot("add tags field")
drift.diff()

# Output:
# + Product.tags  (array<string>)
```

### What counts as a breaking change

| change | verdict |
| ------ | ------- |
| endpoint added | ✅ safe |
| field / property added | ✅ safe |
| endpoint removed | ❌ breaking |
| field / property removed | ❌ breaking |
| parameter type changed | ⚠️ breaking |

---

## MySQL support

```python
from schema_drift import SchemaDrift

drift = SchemaDrift("mysql://user:pass@localhost/mydb")
drift.snapshot("initial schema")
```

Requires:
```bash
pip install "schema-drift[mysql]"
```
