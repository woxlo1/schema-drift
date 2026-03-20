import sqlite3
from schema_drift import SchemaDrift

conn = sqlite3.connect("test.db")
conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
conn.commit()

drift = SchemaDrift(conn)
drift.snapshot("initial schema")

conn.execute("ALTER TABLE users ADD COLUMN email TEXT")
conn.commit()

drift.snapshot("add users.email")
drift.log()