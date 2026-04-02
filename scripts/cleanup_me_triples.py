"""
One-time cleanup: purge ME entity triples from DCL's semantic_triples.

ME data (entity_ids like meridian, cascadia, combined) should never exist in
DCL — it routes to Convergence (port 8010). This script removes contamination
from a misrouted ME pipeline run.

Usage:
    cd /home/ilyac/code/dcl
    .venv/bin/python scripts/cleanup_me_triples.py --entity-ids meridian cascadia combined

Dry-run (default): shows counts without deleting.
    .venv/bin/python scripts/cleanup_me_triples.py --entity-ids meridian cascadia combined

Live run: actually deletes.
    .venv/bin/python scripts/cleanup_me_triples.py --entity-ids meridian cascadia combined --execute
"""

import argparse
import os
import sys

# Add project root to path so backend imports resolve
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

import psycopg2


def get_connection():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("ERROR: DATABASE_URL not set. Cannot connect to database.")
        sys.exit(1)
    return psycopg2.connect(db_url)


def main():
    parser = argparse.ArgumentParser(description="Purge ME entity triples from DCL")
    parser.add_argument(
        "--entity-ids",
        nargs="+",
        required=True,
        help="Entity IDs to purge (e.g. meridian cascadia combined)",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually delete rows. Without this flag, only counts are shown (dry run).",
    )
    args = parser.parse_args()

    entity_ids = args.entity_ids
    execute = args.execute

    print(f"{'LIVE RUN' if execute else 'DRY RUN'}: targeting entity_ids={entity_ids}")
    print()

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # --- Count contaminated semantic_triples ---
            placeholders = ", ".join(["%s"] * len(entity_ids))
            cur.execute(
                f"SELECT entity_id, is_active, COUNT(*) "
                f"FROM semantic_triples "
                f"WHERE entity_id IN ({placeholders}) "
                f"GROUP BY entity_id, is_active "
                f"ORDER BY entity_id, is_active",
                entity_ids,
            )
            rows = cur.fetchall()
            if not rows:
                print("No contaminated triples found in semantic_triples. Clean.")
                return

            total = 0
            print("semantic_triples contamination:")
            for entity_id, is_active, count in rows:
                status = "active" if is_active else "inactive"
                print(f"  entity_id={entity_id}  is_active={status}  count={count}")
                total += count
            print(f"  TOTAL: {total} rows")
            print()

            # --- Count contaminated ingest_log entries ---
            cur.execute(
                f"SELECT entity_id, COUNT(*) "
                f"FROM ingest_log "
                f"WHERE entity_id IN ({placeholders}) "
                f"GROUP BY entity_id "
                f"ORDER BY entity_id",
                entity_ids,
            )
            log_rows = cur.fetchall()
            if log_rows:
                print("ingest_log contamination:")
                for entity_id, count in log_rows:
                    print(f"  entity_id={entity_id}  count={count}")
                print()

            if not execute:
                print("DRY RUN complete. Use --execute to delete.")
                return

            # --- Delete from semantic_triples ---
            cur.execute(
                f"DELETE FROM semantic_triples WHERE entity_id IN ({placeholders})",
                entity_ids,
            )
            deleted_triples = cur.rowcount
            print(f"DELETED {deleted_triples} rows from semantic_triples")

            # --- Delete from ingest_log ---
            cur.execute(
                f"DELETE FROM ingest_log WHERE entity_id IN ({placeholders})",
                entity_ids,
            )
            deleted_logs = cur.rowcount
            print(f"DELETED {deleted_logs} rows from ingest_log")

            conn.commit()
            print()

            # --- Verify ---
            cur.execute(
                f"SELECT COUNT(*) FROM semantic_triples WHERE entity_id IN ({placeholders})",
                entity_ids,
            )
            remaining = cur.fetchone()[0]
            if remaining == 0:
                print("VERIFIED: zero ME entity rows remain in semantic_triples.")
            else:
                print(f"WARNING: {remaining} rows still remain — investigate.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
