"""schema_drift.diff — schema diffing and pretty printing"""
from __future__ import annotations
import json
import hashlib

RESET  = "\033[0m"
BOLD   = "\033[1m"
GREEN  = "\033[32m"
RED    = "\033[31m"
YELLOW = "\033[33m"
CYAN   = "\033[36m"
DIM    = "\033[2m"


def diff_schemas(old: dict, new: dict) -> dict:
    changes: dict = {
        "tables_added": [],
        "tables_removed": [],
        "columns_added": [],
        "columns_removed": [],
        "columns_modified": [],
        "indexes_added": [],
        "indexes_removed": [],
    }
    old_tables = set(old)
    new_tables = set(new)
    changes["tables_added"] = sorted(new_tables - old_tables)
    changes["tables_removed"] = sorted(old_tables - new_tables)

    for table in old_tables & new_tables:
        old_cols = old[table].get("columns", {})
        new_cols = new[table].get("columns", {})
        for col in sorted(set(new_cols) - set(old_cols)):
            changes["columns_added"].append({"table": table, "column": col, "definition": new_cols[col]})
        for col in sorted(set(old_cols) - set(new_cols)):
            changes["columns_removed"].append({"table": table, "column": col, "was": old_cols[col]})
        for col in sorted(set(old_cols) & set(new_cols)):
            if old_cols[col] != new_cols[col]:
                changes["columns_modified"].append({
                    "table": table, "column": col,
                    "before": old_cols[col], "after": new_cols[col],
                })
        old_idx = old[table].get("indexes", {})
        new_idx = new[table].get("indexes", {})
        for idx in sorted(set(new_idx) - set(old_idx)):
            changes["indexes_added"].append({"table": table, "index": idx, "definition": new_idx[idx]})
        for idx in sorted(set(old_idx) - set(new_idx)):
            changes["indexes_removed"].append({"table": table, "index": idx})

    return changes


def has_changes(diff: dict) -> bool:
    return any(diff.values())


def is_breaking(diff: dict) -> bool:
    return bool(
        diff.get("tables_removed")
        or diff.get("columns_removed")
        or diff.get("columns_modified")
        or diff.get("indexes_removed")
    )


def schema_hash(schema: dict) -> str:
    raw = json.dumps(schema, sort_keys=True)
    return hashlib.sha256(raw.encode()).hexdigest()[:12]


def print_diff(diff: dict) -> None:
    if not has_changes(diff):
        print(f"{DIM}No schema changes detected.{RESET}")
        return
    for table in diff["tables_added"]:
        print(f"{GREEN}+ table  {BOLD}{table}{RESET}")
    for table in diff["tables_removed"]:
        print(f"{RED}- table  {BOLD}{table}{RESET}")
    for c in diff["columns_added"]:
        t = c["definition"].get("type", "")
        print(f"{GREEN}+ {c['table']}.{BOLD}{c['column']}{RESET}{GREEN}  ({t}){RESET}")
    for c in diff["columns_removed"]:
        t = c["was"].get("type", "")
        print(f"{RED}- {c['table']}.{BOLD}{c['column']}{RESET}{RED}  ({t}){RESET}")
    for c in diff["columns_modified"]:
        before = c["before"].get("type", "")
        after  = c["after"].get("type", "")
        print(f"{YELLOW}~ {c['table']}.{BOLD}{c['column']}{RESET}{YELLOW}  {before} → {after}{RESET}")
    for i in diff["indexes_added"]:
        cols = ", ".join(i["definition"].get("columns", []))
        print(f"{GREEN}+ index  {BOLD}{i['index']}{RESET}{GREEN} on {i['table']} ({cols}){RESET}")
    for i in diff["indexes_removed"]:
        print(f"{RED}- index  {BOLD}{i['index']}{RESET}{RED} on {i['table']}{RESET}")
