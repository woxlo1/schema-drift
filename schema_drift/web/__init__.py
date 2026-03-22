"""
schema_drift.web — lightweight web UI for browsing drift history.

Usage::

    from schema_drift.web import serve
    serve(".schema-drift.json", port=8080)

Or from the CLI::

    schema-drift web --port 8080
"""
from __future__ import annotations
import json
from pathlib import Path
from typing import Any


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>schema-drift</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: #0d1117; color: #e6edf3; min-height: 100vh; }
  header { background: #161b22; border-bottom: 1px solid #30363d; padding: 16px 24px; display: flex; align-items: center; gap: 12px; }
  header h1 { font-size: 18px; font-weight: 600; color: #f0f6fc; }
  header span { font-size: 12px; color: #8b949e; background: #21262d; padding: 2px 8px; border-radius: 12px; border: 1px solid #30363d; }
  .container { max-width: 1100px; margin: 0 auto; padding: 24px; }
  .timeline { display: flex; flex-direction: column; gap: 12px; }
  .entry { background: #161b22; border: 1px solid #30363d; border-radius: 8px; overflow: hidden; cursor: pointer; transition: border-color .15s; }
  .entry:hover { border-color: #58a6ff; }
  .entry-header { padding: 14px 18px; display: flex; align-items: center; gap: 12px; }
  .entry-id { font-family: monospace; font-size: 12px; color: #8b949e; min-width: 100px; }
  .entry-date { font-size: 12px; color: #8b949e; min-width: 90px; }
  .entry-msg { font-size: 14px; color: #f0f6fc; flex: 1; }
  .entry-stats { font-size: 12px; color: #8b949e; }
  .entry-body { display: none; padding: 0 18px 14px; border-top: 1px solid #21262d; }
  .entry-body.open { display: block; }
  .diff-line { font-family: monospace; font-size: 13px; padding: 3px 0; }
  .add { color: #3fb950; }
  .remove { color: #f85149; }
  .modify { color: #d29922; }
  .no-changes { color: #8b949e; font-size: 13px; padding: 8px 0; }
  .badge { font-size: 11px; padding: 1px 7px; border-radius: 10px; font-weight: 500; }
  .badge-break { background: #3d1a1a; color: #f85149; border: 1px solid #f85149; }
  .badge-safe  { background: #1a3d1a; color: #3fb950; border: 1px solid #3fb950; }
  .stats-bar { display: flex; gap: 16px; padding: 16px 0 8px; }
  .stat { background: #21262d; border-radius: 8px; padding: 12px 18px; flex: 1; text-align: center; }
  .stat-num { font-size: 24px; font-weight: 600; color: #f0f6fc; }
  .stat-lbl { font-size: 12px; color: #8b949e; margin-top: 2px; }
  h2 { font-size: 14px; font-weight: 500; color: #8b949e; margin: 20px 0 10px; text-transform: uppercase; letter-spacing: .05em; }
</style>
</head>
<body>
<header>
  <h1>schema-drift</h1>
  <span id="snapshot-count"></span>
</header>
<div class="container">
  <div class="stats-bar" id="stats-bar"></div>
  <h2>Snapshot history</h2>
  <div class="timeline" id="timeline"></div>
</div>
<script>
const data = __DATA__;

document.getElementById('snapshot-count').textContent = data.length + ' snapshots';

const latest = data[data.length - 1] || {};
const statsBar = document.getElementById('stats-bar');
statsBar.innerHTML = `
  <div class="stat"><div class="stat-num">${data.length}</div><div class="stat-lbl">Snapshots</div></div>
  <div class="stat"><div class="stat-num">${(latest.stats||{}).tables||0}</div><div class="stat-lbl">Tables</div></div>
  <div class="stat"><div class="stat-num">${(latest.stats||{}).columns||0}</div><div class="stat-lbl">Columns</div></div>
`;

const timeline = document.getElementById('timeline');
[...data].reverse().forEach((entry, i) => {
  const diff = entry.diff || {};
  const hasBreaking = (diff.tables_removed||[]).length > 0 || (diff.columns_removed||[]).length > 0 || (diff.columns_modified||[]).length > 0;
  const hasChanges = Object.values(diff).some(v => v && v.length > 0);
  const badge = i === 0 ? '' : hasChanges
    ? (hasBreaking ? '<span class="badge badge-break">breaking</span>' : '<span class="badge badge-safe">safe</span>')
    : '';

  const el = document.createElement('div');
  el.className = 'entry';
  el.innerHTML = `
    <div class="entry-header">
      <span class="entry-id">${entry.id}</span>
      <span class="entry-date">${entry.timestamp.slice(0,10)}</span>
      <span class="entry-msg">${entry.message || '(no message)'}</span>
      ${badge}
      <span class="entry-stats">${(entry.stats||{}).tables||0}t / ${(entry.stats||{}).columns||0}c</span>
    </div>
    <div class="entry-body" id="body-${i}"></div>
  `;
  el.querySelector('.entry-header').addEventListener('click', () => toggle(i, diff));
  timeline.appendChild(el);
});

function toggle(i, diff) {
  const body = document.getElementById('body-' + i);
  if (body.classList.contains('open')) { body.classList.remove('open'); return; }
  if (!body.innerHTML) { body.innerHTML = renderDiff(diff); }
  body.classList.add('open');
}

function renderDiff(diff) {
  const lines = [];
  (diff.tables_added||[]).forEach(t => lines.push(`<div class="diff-line add">+ table  ${t}</div>`));
  (diff.tables_removed||[]).forEach(t => lines.push(`<div class="diff-line remove">- table  ${t}</div>`));
  (diff.columns_added||[]).forEach(c => lines.push(`<div class="diff-line add">+ ${c.table}.${c.column}  (${(c.definition||{}).type||''})</div>`));
  (diff.columns_removed||[]).forEach(c => lines.push(`<div class="diff-line remove">- ${c.table}.${c.column}  (${(c.was||{}).type||''})</div>`));
  (diff.columns_modified||[]).forEach(c => lines.push(`<div class="diff-line modify">~ ${c.table}.${c.column}  ${(c.before||{}).type||''} → ${(c.after||{}).type||''}</div>`));
  (diff.indexes_added||[]).forEach(i => lines.push(`<div class="diff-line add">+ index  ${i.index} on ${i.table}</div>`));
  (diff.indexes_removed||[]).forEach(i => lines.push(`<div class="diff-line remove">- index  ${i.index} on ${i.table}</div>`));
  return lines.length ? lines.join('') : '<div class="no-changes">No schema changes in this snapshot.</div>';
}
</script>
</body>
</html>
"""


def _load_history(storage_path: str | Path) -> list:
    path = Path(storage_path)
    if not path.exists():
        return []
    with path.open() as f:
        return json.load(f)


def get_html(storage_path: str | Path = ".schema-drift.json") -> str:
    """Return the web UI HTML with embedded snapshot data."""
    history = _load_history(storage_path)
    data_json = json.dumps(history, default=str)
    return HTML_TEMPLATE.replace("__DATA__", data_json)


def serve(
    storage_path: str | Path = ".schema-drift.json",
    host: str = "127.0.0.1",
    port: int = 8080,
) -> None:
    """
    Start a local web server to browse drift history.

    Args:
        storage_path: Path to .schema-drift.json (default: current directory).
        host:         Host to bind to (default: 127.0.0.1).
        port:         Port to listen on (default: 8080).
    """
    from http.server import BaseHTTPRequestHandler, HTTPServer

    storage = Path(storage_path)

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path in ("/", "/index.html"):
                html = get_html(storage).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(html)))
                self.end_headers()
                self.wfile.write(html)
            elif self.path == "/api/history":
                history = _load_history(storage)
                body = json.dumps(history, default=str).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, fmt: str, *args: Any) -> None:
            pass  # suppress default access log

    server = HTTPServer((host, port), Handler)
    print(f"\033[36mschema-drift web UI\033[0m  →  http://{host}:{port}")
    print("\033[2mpress Ctrl+C to stop\033[0m")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nstopped.")
        server.server_close()
