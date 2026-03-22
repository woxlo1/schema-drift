"""Basic schema-drift usage with SQLite."""
import os
import sqlite3
from schema_drift import SchemaDrift

# Clean up from previous runs
for f in ["example.db", ".schema-drift.json"]:
    if os.path.exists(f):
        os.remove(f)

# Create a database
conn = sqlite3.connect("example.db")
conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
conn.execute("CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT, user_id INTEGER)")
conn.commit()

drift = SchemaDrift("example.db")
drift.snapshot("initial schema")

# Add a column
conn.execute("ALTER TABLE users ADD COLUMN email TEXT")
conn.commit()
drift.snapshot("add users.email")

# Add a table
conn.execute("CREATE TABLE tags (id INTEGER PRIMARY KEY, name TEXT)")
conn.commit()
drift.snapshot("add tags table")

# View history
print("\n--- log ---")
drift.log()

print("\n--- diff (last two) ---")
drift.diff()

conn.close()
