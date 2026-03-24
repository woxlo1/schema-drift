"""schema-drift CLI"""
from __future__ import annotations
import argparse
import os
import sys

from .diff import RESET, BOLD, DIM, RED, CYAN
from .core import SchemaDrift


def _get_db(args_db):
    db = args_db or os.environ.get("SCHEMA_DRIFT_DB")
    if not db:
        print(
            f"{RED}error:{RESET} no database specified.\n"
            f"  Pass --db <url> or set {BOLD}SCHEMA_DRIFT_DB{RESET} env var.\n"
            f"  Examples:\n"
            f"    {DIM}--db postgresql://user:pass@localhost/mydb{RESET}\n"
            f"    {DIM}--db ./mydb.sqlite{RESET}",
            file=sys.stderr,
        )
        sys.exit(1)
    return db


def cmd_snapshot(args):
    SchemaDrift(_get_db(args.db), storage_path=args.storage).snapshot(args.message or "")

def cmd_diff(args):
    SchemaDrift(_get_db(args.db), storage_path=args.storage).diff()

def cmd_log(args):
    SchemaDrift(_get_db(args.db), storage_path=args.storage).log()

def cmd_rollback(args):
    SchemaDrift(_get_db(args.db), storage_path=args.storage).rollback(args.index)

def cmd_watch(args):
    from .diff import RED, RESET
    drift = SchemaDrift(_get_db(args.db), storage_path=args.storage)
    def on_breaking(diff):
        tables = diff.get("tables_removed", [])
        cols = [f"{c['table']}.{c['column']}" for c in diff.get("columns_removed", [])]
        print(f"{RED}breaking: {', '.join(tables + cols)}{RESET}")
    drift.watch(
        interval=args.interval,
        on_breaking=on_breaking,
        auto_snapshot=not args.no_snapshot,
        message=args.message or "auto-snapshot",
    )

def cmd_export(args):
    drift = SchemaDrift(_get_db(args.db), storage_path=args.storage)
    if args.format == "json":
        print(drift.export_json())
    elif args.format == "csv":
        print(drift.export_csv())
    elif args.format == "sql":
        print(drift.export_sql(dialect=args.dialect))

def cmd_web(args):
    from .web import serve
    serve(storage_path=args.storage, host=args.host, port=args.port)

def cmd_api(args):
    from .api import serve
    serve(storage_path=args.storage, db_url=_get_db(args.db) if args.db else "", host=args.host, port=args.port)


def build_parser():
    parser = argparse.ArgumentParser(
        prog="schema-drift",
        description="Track why your schema changed, not just what changed.",
    )
    parser.add_argument("--db", metavar="URL", help="database connection string")
    parser.add_argument("--storage", metavar="FILE", default=".schema-drift.json")

    sub = parser.add_subparsers(dest="command", metavar="command")
    sub.required = True

    p = sub.add_parser("snapshot", help="capture the current schema")
    p.add_argument("message", nargs="?", default="")
    p.set_defaults(func=cmd_snapshot)

    p = sub.add_parser("diff", help="show diff between last two snapshots")
    p.set_defaults(func=cmd_diff)

    p = sub.add_parser("log", help="show snapshot history")
    p.set_defaults(func=cmd_log)

    p = sub.add_parser("rollback", help="inspect schema at a snapshot index")
    p.add_argument("index", type=int)
    p.set_defaults(func=cmd_rollback)

    p = sub.add_parser("watch", help="poll for schema changes")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--no-snapshot", action="store_true")
    p.add_argument("--message", default="auto-snapshot")
    p.set_defaults(func=cmd_watch)

    p = sub.add_parser("export", help="export schema to JSON/CSV/SQL")
    p.add_argument("--format", choices=["json", "csv", "sql"], default="json")
    p.add_argument("--dialect", default="generic", help="SQL dialect (generic/postgres/mysql/sqlite)")
    p.set_defaults(func=cmd_export)

    p = sub.add_parser("web", help="start web UI")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8080)
    p.set_defaults(func=cmd_web)

    p = sub.add_parser("api", help="start REST API server")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8000)
    p.set_defaults(func=cmd_api)

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
