# Web UI

schema-drift includes a built-in web UI for browsing snapshot history in the browser.

## Start the web UI

```python
from schema_drift.web import serve

serve(".schema-drift.json", port=8080)
```

## CLI

```bash
schema-drift web
schema-drift web --port 9000
schema-drift web --host 0.0.0.0 --port 8080
```

Open `http://127.0.0.1:8080` in your browser.

## Features

- Timeline view of all snapshots
- Color-coded diffs (green = added, red = removed, yellow = modified)
- Breaking change badges
- Table and column counts per snapshot
- Click any snapshot to expand the diff

## API

The web UI also exposes a JSON API:

```
GET /api/history  →  full snapshot history as JSON
```
