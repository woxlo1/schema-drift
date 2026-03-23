"""
schema_drift.integrations.team

Team-oriented features: approvals, annotations, and audit trail.

Usage::

    from schema_drift import SchemaDrift
    from schema_drift.integrations.team import approve, annotate, audit_log

    drift = SchemaDrift("postgresql://localhost/mydb")

    # Record who approved a schema change
    approve(drift, snapshot_index=-1, approver="alice", note="reviewed in standup")

    # Add a note to a snapshot
    annotate(drift, snapshot_index=-1, note="related to ticket PROJ-123")

    # Print full audit trail
    audit_log(drift)
"""
from __future__ import annotations
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


RESET  = "\033[0m"
BOLD   = "\033[1m"
GREEN  = "\033[32m"
CYAN   = "\033[36m"
DIM    = "\033[2m"
YELLOW = "\033[33m"


def approve(
    drift: Any,
    snapshot_index: int = -1,
    approver: str = "",
    note: str = "",
) -> None:
    """
    Record an approval for a snapshot.

    Args:
        drift:          SchemaDrift instance.
        snapshot_index: Index of snapshot to approve (default: latest).
        approver:       Name or ID of the approver.
        note:           Optional approval note.
    """
    history = drift._load()
    if not history:
        raise ValueError("No snapshots found.")

    entry = history[snapshot_index]
    if "approvals" not in entry:
        entry["approvals"] = []

    approval = {
        "approver": approver,
        "note": note,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    entry["approvals"].append(approval)
    drift._save(history)

    ts = approval["timestamp"][:10]
    print(f"{GREEN}approved{RESET}  {entry['id']}  {DIM}{ts}{RESET}")
    print(f"          by {BOLD}{approver}{RESET}" + (f" — {note}" if note else ""))


def annotate(
    drift: Any,
    snapshot_index: int = -1,
    note: str = "",
    author: str = "",
) -> None:
    """
    Add an annotation (note) to a snapshot.

    Args:
        drift:          SchemaDrift instance.
        snapshot_index: Index of snapshot to annotate (default: latest).
        note:           Annotation text.
        author:         Optional author name.
    """
    history = drift._load()
    if not history:
        raise ValueError("No snapshots found.")

    entry = history[snapshot_index]
    if "annotations" not in entry:
        entry["annotations"] = []

    annotation = {
        "note": note,
        "author": author,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    entry["annotations"].append(annotation)
    drift._save(history)

    print(f"{CYAN}annotated{RESET}  {entry['id']}")
    print(f"           {note}" + (f"  {DIM}— {author}{RESET}" if author else ""))


def audit_log(drift: Any) -> None:
    """
    Print a full audit trail: snapshots with approvals and annotations.
    """
    history = drift._load()
    if not history:
        print(f"{DIM}No snapshots found.{RESET}")
        return

    print(f"{BOLD}audit log{RESET}  {len(history)} snapshots\n")

    for entry in history:
        date = entry["timestamp"][:10]
        sid = entry["id"]
        msg = entry.get("message", "")
        tables = entry["stats"]["tables"]
        cols = entry["stats"]["columns"]

        print(f"{date}  {DIM}{sid}{RESET}  {msg}  {DIM}({tables}t/{cols}c){RESET}")

        for ann in entry.get("annotations", []):
            author = f" — {ann['author']}" if ann.get("author") else ""
            print(f"  {CYAN}note{RESET}    {ann['note']}{DIM}{author}{RESET}")

        for appr in entry.get("approvals", []):
            note = f" — {appr['note']}" if appr.get("note") else ""
            print(f"  {GREEN}approved{RESET}  by {BOLD}{appr['approver']}{RESET}{DIM}{note}{RESET}")

        print()


def pending_approvals(drift: Any) -> list:
    """
    Return list of snapshots that have changes but no approvals yet.
    """
    history = drift._load()
    pending = []
    for entry in history:
        has_changes = any(entry.get("diff", {}).values())
        has_approval = bool(entry.get("approvals"))
        if has_changes and not has_approval:
            pending.append(entry)
    return pending


def require_approval(drift: Any) -> bool:
    """
    Return True if any snapshots with changes are missing approvals.
    Useful as a CI gate.

    Usage::

        if require_approval(drift):
            print("Schema changes need approval before deploying!")
            exit(1)
    """
    p = pending_approvals(drift)
    if p:
        print(f"{YELLOW}warning:{RESET} {len(p)} snapshot(s) with unapproved schema changes:")
        for entry in p:
            print(f"  {DIM}{entry['id']}{RESET}  {entry.get('message', '')}")
        return True
    return False
