"""
Run SQL migrations against the shared Supabase PG database.

Usage:
    python migrations/run_migration.py
"""

import os
import sys
from pathlib import Path

# Ensure repo root is on sys.path so backend imports work if needed
_repo_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_repo_root))

try:
    import psycopg2
except ImportError:
    print("ERROR: psycopg2 is not installed. Run: pip install psycopg2-binary")
    sys.exit(1)

MIGRATION_DIR = Path(__file__).resolve().parent


def run_migrations():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("ERROR: DATABASE_URL environment variable is not set.")
        sys.exit(1)

    sql_files = sorted(MIGRATION_DIR.glob("*.sql"))
    if not sql_files:
        print("No .sql migration files found.")
        return

    conn = psycopg2.connect(database_url)
    try:
        conn.autocommit = False
        cur = conn.cursor()
        applied = 0
        for sql_file in sql_files:
            print(f"Running {sql_file.name} ...")
            sql = sql_file.read_text()
            # CREATE INDEX CONCURRENTLY cannot run inside a transaction block.
            # Detect and run that file standalone in autocommit mode.
            if "CONCURRENTLY" in sql.upper():
                conn.commit()  # flush any pending transactional migrations
                conn.autocommit = True
                cur.execute(sql)
                conn.autocommit = False
            else:
                cur.execute(sql)
            print(f"  OK — {sql_file.name}")
            applied += 1
        conn.commit()
        print(f"\nAll {applied} migration(s) applied successfully.")
    except Exception as e:
        if not conn.autocommit:
            conn.rollback()
        print(f"\nMIGRATION FAILED: {e}")
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    run_migrations()
