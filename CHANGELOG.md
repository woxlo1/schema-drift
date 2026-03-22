# Changelog

## v2.0.0 (unreleased)

### Breaking changes
- Internals refactored: `core.py` now delegates to `backends/` — existing `SchemaDrift` API unchanged

### New features
- `backends/` — modular backend system (SQLite, PostgreSQL, MySQL, MariaDB, Oracle, OpenAPI, JSON Schema)
- `integrations/alembic.py` — auto-snapshot after Alembic migrations
- `integrations/django.py` — auto-snapshot after Django migrations
- `integrations/slack.py` — Slack notifications for schema changes
- `web/` — built-in web UI (`schema-drift web`)
- `export.py` — export to JSON, CSV, SQL (`schema-drift export`)
- `diff.py` — diff engine extracted as standalone module
- `watch.py` — watch module extracted as standalone module

---

## v1.1.5

- Added `drift.watch()` for real-time schema monitoring
- CLI: `schema-drift watch --interval 30`

## v1.1.4

- Added Oracle support (`oracle://`)

## v1.1.3

- Added MariaDB support (`mariadb://`)

## v1.1.2

- Added git install instructions

## v1.1.1

- Added LICENSE file

## v1.1.0

- Added MySQL support (`mysql://`)

## v1.0.0

- Added OpenAPI 3.x support
- Added JSON Schema support

## v0.3.0

- GitHub Actions integration (auto-comment schema diffs on PRs)

## v0.2.0

- CLI: `schema-drift snapshot/log/diff/rollback`

## v0.1.0

- Initial release
- SQLite + PostgreSQL support
- `snapshot()`, `diff()`, `log()`, `rollback()`
