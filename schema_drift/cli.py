"""
schema-drift CLI

Usage:
    schema-drift snapshot "message" --db postgresql://...
    schema-drift diff
    schema-drift log
    schema-drift rollback 0
"""

from __future__ import annotations

import argparse
import os
import sys

from .core import SchemaDrift, RESET, BOLD, DIM, RED, CYAN


def _get_db(args_db: str | None) -> str:
    """Resolve DB connection string from CLI arg or environment variable."""
    db = args_db or os.environ.get("SCHEMA_DRIFT_DB")
    if not db:
        print(
            f"{RED}error:{RESET} no database specified.\n"
            f"  Pass --db <url> or set the {BOLD}SCHEMA_DRIFT_DB{RESET} environment variable.\n\n"
            f"  Examples:\n"
            f"    {DIM}--db postgresql://user:pass@localhost/mydb{RESET}\n"
            f"    {DIM}--db ./mydb.sqlite{RESET}\n"
            f"    {DIM}export SCHEMA_DRIFT_DB=postgresql://...{RESET}",
            file=sys.stderr,
        )
        sys.exit(1)
    return db


def cmd_snapshot(args: argparse.Namespace) -> None:
    db = _get_db(args.db)
    drift = SchemaDrift(db, storage_path=args.storage)
    drift.snapshot(args.message or "")


def cmd_diff(args: argparse.Namespace) -> None:
    db = _get_db(args.db)
    drift = SchemaDrift(db, storage_path=args.storage)
    drift.diff()


def cmd_log(args: argparse.Namespace) -> None:
    db = _get_db(args.db)
    drift = SchemaDrift(db, storage_path=args.storage)
    drift.log()


def cmd_rollback(args: argparse.Namespace) -> None:
    db = _get_db(args.db)
    drift = SchemaDrift(db, storage_path=args.storage)
    drift.rollback(args.index)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="schema-drift",
        description="Track why your schema changed, not just what changed.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
examples:
  {DIM}schema-drift snapshot "add users.email" --db postgresql://localhost/mydb{RESET}
  {DIM}schema-drift log --db ./mydb.sqlite{RESET}
  {DIM}schema-drift diff{RESET}
  {DIM}export SCHEMA_DRIFT_DB=postgresql://localhost/mydb{RESET}
  {DIM}schema-drift snapshot "initial schema"{RESET}
        """,
    )

    parser.add_argument(
        "--db",
        metavar="URL",
        help="database connection string (or set SCHEMA_DRIFT_DB env var)",
    )
    parser.add_argument(
        "--storage",
        metavar="FILE",
        default=".schema-drift.json",
        help="snapshot storage file (default: .schema-drift.json)",
    )

    sub = parser.add_subparsers(dest="command", metavar="command")
    sub.required = True

    # snapshot
    p_snap = sub.add_parser("snapshot", help="capture the current schema")
    p_snap.add_argument("message", nargs="?", default="", help="description of this change")
    p_snap.set_defaults(func=cmd_snapshot)

    # diff
    p_diff = sub.add_parser("diff", help="show diff between last two snapshots")
    p_diff.set_defaults(func=cmd_diff)

    # log
    p_log = sub.add_parser("log", help="show snapshot history")
    p_log.set_defaults(func=cmd_log)

    # rollback
    p_roll = sub.add_parser("rollback", help="inspect schema at a given snapshot index")
    p_roll.add_argument("index", type=int, help="snapshot index (0 = first, -1 = latest)")
    p_roll.set_defaults(func=cmd_rollback)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
