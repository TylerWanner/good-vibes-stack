"""One-shot script to create the ingests table and update alembic version.
Run inside the worker container: python /app/scripts/create_ingests_table.py
"""
import os
import psycopg2

dsn = os.environ["DATABASE_URL"]
conn = psycopg2.connect(dsn)
conn.autocommit = True
cur = conn.cursor()

# Check current state
cur.execute("SELECT version_num FROM alembic_version")
current = cur.fetchone()
print(f"Current alembic version: {current}")

cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name='ingests')")
exists = cur.fetchone()[0]
print(f"ingests table exists: {exists}")

if not exists:
    print("Creating ingests table...")
    cur.execute("""
        CREATE TABLE ingests (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            url TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            destination TEXT,
            flow_run_id TEXT,
            error TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            completed_at TIMESTAMPTZ,
            notify JSONB
        )
    """)
    cur.execute("CREATE INDEX idx_ingests_created_at ON ingests USING btree (created_at)")
    cur.execute("CREATE INDEX idx_ingests_status ON ingests (status)")
    cur.execute("CREATE INDEX idx_ingests_url ON ingests (url)")
    print("Table and indexes created.")

    # Fix stale processed_at on pending articles
    cur.execute("UPDATE articles SET processed_at = NULL WHERE status = 'pending'")
    print(f"Cleared processed_at on {cur.rowcount} pending articles.")

    # Update alembic version
    cur.execute("UPDATE alembic_version SET version_num = '0010'")
    print("Alembic version updated to 0010.")
else:
    print("ingests table already exists — nothing to do.")

cur.close()
conn.close()
print("Done.")
