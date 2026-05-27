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

# Query used to find indexes left in indisvalid=false state. CREATE INDEX
# CONCURRENTLY can leave an index live + ready but not valid when conflicting
# writes hit during its second build pass; PG refuses to use such indexes for
# query planning. REINDEX INDEX CONCURRENTLY remediates without locking writes.
_INVALID_INDEX_Q = """
    SELECT n.nspname || '.' || c.relname AS qualified_name
    FROM pg_index pi
    JOIN pg_class c ON c.oid = pi.indexrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE pi.indisvalid = false
      AND pi.indislive = true
      AND pi.indisready = true
    ORDER BY qualified_name
"""


def _remediate_invalid_indexes(cur) -> None:
    """Detect and reindex any indisvalid=false indexes.

    Caller MUST have the connection in autocommit mode — REINDEX INDEX
    CONCURRENTLY cannot run inside a transaction block. Raises RuntimeError
    if any index remains invalid after the remediation pass.
    """
    cur.execute(_INVALID_INDEX_Q)
    invalid = [row[0] for row in cur.fetchall()]
    if not invalid:
        print("  post-CONCURRENTLY check: no invalid indexes.")
        return

    print(f"  post-CONCURRENTLY check: {len(invalid)} invalid index(es) — remediating: {invalid}")
    for qualified_name in invalid:
        print(f"    REINDEX INDEX CONCURRENTLY {qualified_name} ...")
        cur.execute(f"REINDEX INDEX CONCURRENTLY {qualified_name}")

    cur.execute(_INVALID_INDEX_Q)
    still_invalid = [row[0] for row in cur.fetchall()]
    if still_invalid:
        raise RuntimeError(
            f"post-CONCURRENTLY check FAILED: {len(still_invalid)} index(es) "
            f"still indisvalid=false after REINDEX: {still_invalid}. "
            f"Manual intervention required (drop + recreate, or investigate "
            f"why REINDEX did not converge)."
        )
    print(f"  post-CONCURRENTLY check: {len(invalid)} index(es) now valid.")


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
            # Detect and run that file standalone in autocommit mode. After
            # the file applies, remediate any indisvalid=false indexes via
            # REINDEX (also requires autocommit).
            if "CONCURRENTLY" in sql.upper():
                conn.commit()  # flush any pending transactional migrations
                conn.autocommit = True
                cur.execute(sql)
                _remediate_invalid_indexes(cur)
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
