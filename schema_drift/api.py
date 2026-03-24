"""
schema_drift.api

REST API for schema-drift using FastAPI.

Usage::

    # Start the API server
    schema-drift api --db ./mydb.sqlite --port 8000

    # Or from Python
    from schema_drift.api import create_app
    import uvicorn

    app = create_app(".schema-drift.json")
    uvicorn.run(app, host="127.0.0.1", port=8000)

Endpoints:
    GET  /health              — health check
    GET  /snapshots           — list all snapshots
    GET  /snapshots/{index}   — get snapshot by index
    POST /snapshots           — take a new snapshot
    GET  /diff                — diff between last two snapshots
    GET  /diff/{a}/{b}        — diff between two specific snapshots
    GET  /schema              — latest schema
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

try:
    from fastapi import FastAPI, HTTPException
    from fastapi.responses import JSONResponse
    from pydantic import BaseModel
    HAS_FASTAPI = True
except ImportError:
    HAS_FASTAPI = False


if HAS_FASTAPI:
    class SnapshotRequest(BaseModel):
        message: str = ""
        db: str = ""
else:
    class SnapshotRequest:
        message: str = ""
        db: str = ""


def create_app(
    storage_path: str | Path = ".schema-drift.json",
    db_url: str = "",
) -> Any:
    """
    Create and return the FastAPI app.

    Args:
        storage_path: Path to the snapshot storage file.
        db_url:       Database connection string. If empty, uses SCHEMA_DRIFT_DB env var.
    """
    if not HAS_FASTAPI:
        raise ImportError("FastAPI is required: pip install schema-drift[api]")

    import os
    from .core import SchemaDrift
    from .diff import diff_schemas, has_changes, is_breaking

    app = FastAPI(
        title="schema-drift API",
        description="REST API for schema-drift — track why your schema changed.",
        version="2.1.1",
    )

    def _get_drift(db: str = "") -> SchemaDrift:
        url = db or db_url or os.environ.get("SCHEMA_DRIFT_DB", "")
        if not url:
            raise HTTPException(status_code=400, detail="No database specified. Set SCHEMA_DRIFT_DB or pass db in request body.")
        return SchemaDrift(url, storage_path=storage_path)

    def _load_history() -> list:
        path = Path(storage_path)
        if not path.exists():
            return []
        import json
        with path.open() as f:
            return json.load(f)

    @app.get("/health")
    def health():
        """Health check."""
        history = _load_history()
        return {
            "status": "ok",
            "snapshots": len(history),
            "storage": str(storage_path),
        }

    @app.get("/snapshots")
    def list_snapshots():
        """List all snapshots."""
        history = _load_history()
        return [
            {
                "index": i,
                "id": entry["id"],
                "timestamp": entry["timestamp"],
                "message": entry.get("message", ""),
                "stats": entry["stats"],
                "has_changes": has_changes(entry.get("diff", {})),
                "is_breaking": is_breaking(entry.get("diff", {})),
            }
            for i, entry in enumerate(history)
        ]

    @app.get("/snapshots/{index}")
    def get_snapshot(index: int):
        """Get a specific snapshot by index."""
        history = _load_history()
        if not history:
            raise HTTPException(status_code=404, detail="No snapshots found.")
        try:
            entry = history[index]
        except IndexError:
            raise HTTPException(status_code=404, detail=f"Snapshot {index} not found.")
        return entry

    @app.post("/snapshots")
    def take_snapshot(req: SnapshotRequest):
        """Take a new snapshot of the current schema."""
        drift = _get_drift(req.db)
        diff = drift.snapshot(req.message)
        history = _load_history()
        latest = history[-1] if history else {}
        return {
            "id": latest.get("id"),
            "timestamp": latest.get("timestamp"),
            "message": req.message,
            "stats": latest.get("stats", {}),
            "diff": diff,
            "has_changes": has_changes(diff),
            "is_breaking": is_breaking(diff),
        }

    @app.get("/diff")
    def get_diff():
        """Get diff between the last two snapshots."""
        history = _load_history()
        if len(history) < 2:
            raise HTTPException(status_code=400, detail="Need at least 2 snapshots to diff.")
        diff = diff_schemas(history[-2]["schema"], history[-1]["schema"])
        return {
            "from": {"id": history[-2]["id"], "timestamp": history[-2]["timestamp"]},
            "to": {"id": history[-1]["id"], "timestamp": history[-1]["timestamp"]},
            "diff": diff,
            "has_changes": has_changes(diff),
            "is_breaking": is_breaking(diff),
        }

    @app.get("/diff/{a}/{b}")
    def get_diff_between(a: int, b: int):
        """Get diff between two specific snapshots."""
        history = _load_history()
        if not history:
            raise HTTPException(status_code=404, detail="No snapshots found.")
        try:
            snap_a = history[a]
            snap_b = history[b]
        except IndexError:
            raise HTTPException(status_code=404, detail="Snapshot index out of range.")
        diff = diff_schemas(snap_a["schema"], snap_b["schema"])
        return {
            "from": {"id": snap_a["id"], "timestamp": snap_a["timestamp"]},
            "to": {"id": snap_b["id"], "timestamp": snap_b["timestamp"]},
            "diff": diff,
            "has_changes": has_changes(diff),
            "is_breaking": is_breaking(diff),
        }

    @app.get("/schema")
    def get_schema():
        """Get the latest schema."""
        history = _load_history()
        if not history:
            raise HTTPException(status_code=404, detail="No snapshots found.")
        latest = history[-1]
        return {
            "id": latest["id"],
            "timestamp": latest["timestamp"],
            "message": latest.get("message", ""),
            "schema": latest["schema"],
        }

    return app


def serve(
    storage_path: str | Path = ".schema-drift.json",
    db_url: str = "",
    host: str = "127.0.0.1",
    port: int = 8000,
) -> None:
    """
    Start the REST API server.

    Args:
        storage_path: Path to the snapshot storage file.
        db_url:       Database connection string.
        host:         Host to bind to.
        port:         Port to listen on.
    """
    if not HAS_FASTAPI:
        raise ImportError("FastAPI is required: pip install schema-drift[api]")

    try:
        import uvicorn
    except ImportError:
        raise ImportError("uvicorn is required: pip install schema-drift[api]")

    app = create_app(storage_path=storage_path, db_url=db_url)
    print(f"\033[36mschema-drift API\033[0m  →  http://{host}:{port}")
    print(f"\033[2mdocs at http://{host}:{port}/docs\033[0m")
    print("\033[2mpress Ctrl+C to stop\033[0m")
    uvicorn.run(app, host=host, port=port)
