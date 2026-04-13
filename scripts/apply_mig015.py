"""Apply migration 015 — Part A (single transaction).

Populates current_triples from tenant_runs, backfills tenant_runs counts,
and sweeps stale rows for known entities. Does NOT clean up orphaned rows
for tenants/entities not in tenant_runs — that is Part B
(apply_mig015_b.py), which runs in many small CTID-paged transactions.

Part A invariant: COUNT(current_triples) == SUM(tenant_runs.run_row_count).
Commits on pass. After commit, the system is functional for reads; Part B
is bloat cleanup, not a correctness blocker.
"""

import os
import sys
import time

import psycopg2
from dotenv import load_dotenv

_repo = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
load_dotenv(os.path.join(_repo, ".env"))

INSERT_CURRENT_SQL = """
INSERT INTO current_triples (
    id, tenant_id, entity_id, concept, property, value, period,
    currency, unit, source_system, source_table, source_field,
    pipe_id, source_run_tag,
    confidence_score, confidence_tier,
    canonical_id, resolution_method, resolution_confidence,
    fabric_plane, fabric_product, created_at
)
SELECT
    s.id, s.tenant_id, s.entity_id, s.concept, s.property, s.value, s.period,
    s.currency, s.unit, s.source_system, s.source_table, s.source_field,
    s.pipe_id, s.source_run_tag,
    s.confidence_score, s.confidence_tier,
    s.canonical_id, s.resolution_method, s.resolution_confidence,
    s.fabric_plane, s.fabric_product, s.created_at
FROM semantic_triples s
WHERE s.tenant_id = %s
  AND s.entity_id = %s
  AND s.run_id = %s
  AND s.is_active = true
ON CONFLICT (id) DO NOTHING
"""

ARCHIVE_NON_CURRENT_SQL = """
INSERT INTO semantic_triples_archive (
    id, tenant_id, entity_id, concept, property, value, period,
    currency, unit, source_system, source_table, source_field,
    pipe_id, run_id, source_run_tag,
    confidence_score, confidence_tier,
    canonical_id, resolution_method, resolution_confidence,
    fabric_plane, fabric_product, created_at, updated_at
)
SELECT
    s.id, s.tenant_id, s.entity_id, s.concept, s.property, s.value, s.period,
    s.currency, s.unit, s.source_system, s.source_table, s.source_field,
    s.pipe_id, s.run_id, s.source_run_tag,
    s.confidence_score, s.confidence_tier,
    s.canonical_id, s.resolution_method, s.resolution_confidence,
    s.fabric_plane, s.fabric_product, s.created_at, s.updated_at
FROM semantic_triples s
WHERE s.tenant_id = %s
  AND s.entity_id = %s
  AND (s.run_id <> %s OR s.is_active = false)
"""

DELETE_NON_CURRENT_SQL = """
DELETE FROM semantic_triples s
WHERE s.tenant_id = %s
  AND s.entity_id = %s
  AND (s.run_id <> %s OR s.is_active = false)
"""


def main() -> None:
    url = os.environ["DATABASE_URL"]
    conn = psycopg2.connect(url, application_name="mig015a_py")
    conn.autocommit = False
    start = time.monotonic()
    try:
        cur = conn.cursor()
        cur.execute("SET statement_timeout = '1800000'")  # 30 min per statement

        # Idempotency guard
        cur.execute("SELECT EXISTS(SELECT 1 FROM current_triples LIMIT 1)")
        if cur.fetchone()[0]:
            print("[015A] current_triples already populated — skipping")
            conn.rollback()
            return

        # Step 1: pre-total
        t = time.monotonic()
        cur.execute("SELECT COUNT(*) FROM semantic_triples")
        pre_total = cur.fetchone()[0]
        print(f"[015A] pre_total_count = {pre_total} (took {time.monotonic()-t:.1f}s)")

        # Fetch tenant_runs list (driver for all loops)
        cur.execute(
            "SELECT tenant_id, entity_id, current_run_id "
            "FROM tenant_runs ORDER BY tenant_id, entity_id"
        )
        entities = cur.fetchall()
        print(f"[015A] {len(entities)} tenant_runs entries to process")

        # Step 2: load current_triples per entity
        loaded_total = 0
        for (tenant_id, entity_id, current_run_id) in entities:
            t = time.monotonic()
            cur.execute(INSERT_CURRENT_SQL, (tenant_id, entity_id, current_run_id))
            dt = time.monotonic() - t
            loaded_total += cur.rowcount
            print(
                f"[015A] load {tenant_id}/{entity_id}: "
                f"rows={cur.rowcount} total={loaded_total} took={dt:.2f}s"
            )

        # ANALYZE so the archive/delete planner knows current_triples' shape
        t = time.monotonic()
        cur.execute("ANALYZE current_triples")
        print(f"[015A] ANALYZE current_triples in {time.monotonic()-t:.1f}s")

        # Step 3: backfill tenant_runs counts.
        # previous_run_row_count coerced to 0 (was NULL after mig014 added column).
        t = time.monotonic()
        cur.execute(
            "UPDATE tenant_runs t "
            "SET run_row_count = COALESCE(("
            "  SELECT COUNT(*)::INT FROM current_triples ct "
            "  WHERE ct.tenant_id = t.tenant_id AND ct.entity_id = t.entity_id"
            "), 0), "
            "previous_run_row_count = COALESCE(previous_run_row_count, 0)"
        )
        print(f"[015A] tenant_runs counts backfill in {time.monotonic()-t:.1f}s")

        # Step 4: per-entity sweep (known entities only; catch-all is Part B)
        archived_total = 0
        deleted_total = 0
        for (tenant_id, entity_id, current_run_id) in entities:
            t = time.monotonic()
            cur.execute(
                ARCHIVE_NON_CURRENT_SQL, (tenant_id, entity_id, current_run_id)
            )
            a = cur.rowcount
            archived_total += a
            cur.execute(
                DELETE_NON_CURRENT_SQL, (tenant_id, entity_id, current_run_id)
            )
            d = cur.rowcount
            deleted_total += d
            if a != d:
                raise RuntimeError(
                    f"archive/delete mismatch for {tenant_id}/{entity_id}: "
                    f"archived={a} deleted={d}"
                )
            print(
                f"[015A] sweep {tenant_id}/{entity_id}: "
                f"archived={a} deleted={d} "
                f"archived_total={archived_total} took={time.monotonic()-t:.2f}s"
            )

        # Part A invariant: current_triples count == sum(run_row_count).
        # semantic_triples count intentionally NOT checked here — orphans
        # from unknown tenant/entity tuples are cleaned up in Part B.
        cur.execute("SELECT COUNT(*) FROM current_triples")
        post_current = cur.fetchone()[0]
        cur.execute("SELECT COALESCE(SUM(run_row_count),0) FROM tenant_runs")
        sum_run = cur.fetchone()[0]

        print(
            f"[015A] pre_total={pre_total} "
            f"post_current={post_current} sum_run_row_count={sum_run} "
            f"sweep_archived={archived_total}"
        )

        if post_current != sum_run:
            raise RuntimeError(
                f"current_triples ({post_current}) != sum(run_row_count) ({sum_run})"
            )

        conn.commit()
        print(f"[015A] OK in {time.monotonic()-start:.1f}s")
    except Exception:
        conn.rollback()
        print(f"[015A] ROLLED BACK after {time.monotonic()-start:.1f}s")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
