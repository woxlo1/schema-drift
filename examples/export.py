"""Export schema snapshots to JSON, CSV, and SQL."""
import os
import sqlite3
from schema_drift import SchemaDrift
from schema_drift.export import save

for f in ["export_example.db", ".schema-drift.json"]:
    if os.path.exists(f):
        os.remove(f)

conn = sqlite3.connect("export_example.db")
conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT)")
conn.execute("CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT, user_id INTEGER)")
conn.commit()
conn.close()

drift = SchemaDrift("export_example.db")
drift.snapshot("initial schema")

# Export to JSON
print("--- JSON ---")
print(drift.export_json())

# Export to CSV
print("\n--- CSV ---")
print(drift.export_csv())

# Export to SQL
print("\n--- SQL ---")
print(drift.export_sql(dialect="sqlite"))

# Save to files
save(drift.export_csv(), "schema.csv")
save(drift.export_sql(dialect="sqlite"), "schema.sql")
print("\nSaved to schema.csv and schema.sql")
