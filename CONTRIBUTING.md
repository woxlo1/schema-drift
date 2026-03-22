# Contributing

Thank you for considering contributing to schema-drift!

## Getting started

```bash
git clone https://github.com/woxlo1/schema-drift
cd schema-drift
pip install -e ".[dev]"
pytest
```

## Running tests

```bash
pytest tests/ -v
```

## Project structure

```
schema_drift/
  backends/       # database extractors (SQLite, PostgreSQL, MySQL, etc.)
  integrations/   # third-party integrations (Alembic, Django, Slack)
  web/            # built-in web UI
  core.py         # SchemaDrift main class
  cli.py          # CLI entrypoint
  ci.py           # GitHub Actions integration
  diff.py         # diff engine and pretty printer
  watch.py        # real-time monitoring
  export.py       # JSON / CSV / SQL export
  QUICKSTART.md (root)
  BACKENDS.md
  INTEGRATIONS.md
  WATCH.md
  WEB-UI.md
  EXPORT.md
examples/         # runnable examples
tests/            # test suite
```

## Adding a new backend

1. Create `schema_drift/backends/yourdb.py`
2. Subclass `BaseBackend` and implement `extract()` and `accepts()`
3. Register it in `schema_drift/backends/__init__.py`
4. Add tests in `tests/test_yourdb.py`
5. Add docs in `schema_drift/BACKENDS.md`

## Pull requests

- Keep PRs focused — one feature or fix per PR
- Add tests for new functionality
- Update relevant `.md` files
- Follow existing code style
