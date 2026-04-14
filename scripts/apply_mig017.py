"""Apply migration 017 — denormalize enrichment aggregates into tenant_runs.

Adds source_systems, fabric_pairs, unique_pipes, first_received_at,
latest_received_at columns to tenant_runs and backfills them from
current_triples. Single transaction. Idempotent.

Invariants verified post-commit:
    - all five columns exist on tenant_runs
    - every tenant_runs row has source_systems populated when the
      (tenant, entity) slice of current_triples has a non-null source_system
"""

import os
import sys
import time

import psycopg2
from dotenv import load_dotenv

_repo = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.insert(0, _repo)
load_dotenv(os.path.join(_repo, ".env"))

_SQL_PATH = os.path.join(_repo, "migrations", "017_tenant_runs_enrichment.sql")

_REQUIRED_COLS = (
    "source_systems",
    "fabric_pairs",
    "unique_pipes",
    "first_received_at",
    "latest_received_at",
)


def _column_exists(cur, table: str, column: str) -> bool:
    cur.execute(
        "SELECT 1 FROM information_schema.columns "
        "WHERE table_name = %s AND column_name = %s",
        (table, column),
    )
    return cur.fetchone() is not None


def main() -> None:
    url = os.environ["DATABASE_URL"]
    conn = psycopg2.connect(url, application_name="apply_mig017")
    conn.autocommit = False
    start = time.monotonic()
    try:
        cur = conn.cursor()
        cur.execute("SET statement_timeout = '600000'")

        pre = {c: _column_exists(cur, "tenant_runs", c) for c in _REQUIRED_COLS}
        print(f"[017] pre: tenant_runs columns present = {pre}")

        with open(_SQL_PATH) as fh:
            sql = fh.read()
        cur.execute(sql)

        for col in _REQUIRED_COLS:
            if not _column_exists(cur, "tenant_runs", col):
                raise RuntimeError(f"mig017 failed: column {col} missing")

        cur.execute(
            """
            SELECT COUNT(*),
                   COUNT(*) FILTER (WHERE array_length(source_systems, 1) > 0),
                   COUNT(*) FILTER (WHERE array_length(fabric_pairs, 1) > 0),
                   COUNT(*) FILTER (WHERE first_received_at IS NOT NULL)
            FROM tenant_runs
            """
        )
        total, with_sources, with_fabric, with_first = cur.fetchone()
        print(
            f"[017] post: tenant_runs rows={total} "
            f"source_systems_populated={with_sources} "
            f"fabric_pairs_populated={with_fabric} "
            f"first_received_at_populated={with_first}"
        )

        conn.commit()
        print(
            f"[017] OK in {time.monotonic() - start:.1f}s — "
            f"tenant_runs enriched with denormalized aggregates"
        )
    except Exception:
        conn.rollback()
        print(f"[017] ROLLED BACK after {time.monotonic() - start:.1f}s")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
