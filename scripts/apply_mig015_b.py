"""Apply migration 015 — Part B (truncate + reload).

Synthetic dev data — nothing to preserve. Replaces the original batched
archive-and-delete loop, which stalled under IO contention from external
traffic: per-row DELETE cost on a 12-index table was the bottleneck.

Steps (single transaction):
    TRUNCATE semantic_triples
    INSERT INTO semantic_triples SELECT FROM current_triples JOIN tenant_runs
        synthesizing run_id / is_active / updated_at
    Verify COUNT(semantic_triples) == COUNT(current_triples) == SUM(tenant_runs.run_row_count)
    COMMIT

semantic_triples_archive stays empty — the schema exists (mig014), but
read paths don't use it. Expected runtime: under 2 minutes.
"""

import os
import time

import psycopg2
from dotenv import load_dotenv

_repo = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
load_dotenv(os.path.join(_repo, ".env"))

INSERT_SQL = """
INSERT INTO semantic_triples (
    id, tenant_id, entity_id, concept, property, value, period,
    currency, unit, source_system, source_table, source_field,
    pipe_id, run_id, confidence_score, confidence_tier,
    canonical_id, resolution_method, resolution_confidence,
    created_at, updated_at, is_active,
    source_run_tag, fabric_plane, fabric_product
)
SELECT
    ct.id, ct.tenant_id, ct.entity_id, ct.concept, ct.property, ct.value, ct.period,
    ct.currency, ct.unit, ct.source_system, ct.source_table, ct.source_field,
    ct.pipe_id, tr.current_run_id, ct.confidence_score, ct.confidence_tier,
    ct.canonical_id, ct.resolution_method, ct.resolution_confidence,
    ct.created_at, NOW(), TRUE,
    ct.source_run_tag, ct.fabric_plane, ct.fabric_product
FROM current_triples ct
JOIN tenant_runs tr
  ON tr.tenant_id = ct.tenant_id AND tr.entity_id = ct.entity_id
"""


def main() -> None:
    url = os.environ["DATABASE_URL"]
    conn = psycopg2.connect(url, application_name="mig015b_py")
    conn.autocommit = False
    start = time.monotonic()
    try:
        cur = conn.cursor()
        cur.execute("SET statement_timeout = '600000'")  # 10 min cap

        cur.execute("SELECT COUNT(*) FROM current_triples")
        pre_current = cur.fetchone()[0]
        print(f"[015B] pre_current={pre_current}")

        t = time.monotonic()
        cur.execute("TRUNCATE semantic_triples")
        print(f"[015B] TRUNCATE semantic_triples in {time.monotonic()-t:.2f}s")

        t = time.monotonic()
        cur.execute(INSERT_SQL)
        inserted = cur.rowcount
        print(f"[015B] INSERT {inserted} rows in {time.monotonic()-t:.2f}s")

        cur.execute("SELECT COUNT(*) FROM semantic_triples")
        post_semantic = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM current_triples")
        post_current = cur.fetchone()[0]
        cur.execute("SELECT COALESCE(SUM(run_row_count),0) FROM tenant_runs")
        sum_run = cur.fetchone()[0]

        print(
            f"[015B] post_semantic={post_semantic} post_current={post_current} "
            f"sum_run_row_count={sum_run}"
        )

        if post_semantic != post_current:
            raise RuntimeError(
                f"semantic_triples ({post_semantic}) != current_triples ({post_current})"
            )
        if post_current != sum_run:
            raise RuntimeError(
                f"current_triples ({post_current}) != sum(run_row_count) ({sum_run})"
            )

        conn.commit()
        print(f"[015B] OK in {time.monotonic()-start:.1f}s")
    except Exception:
        conn.rollback()
        print(f"[015B] ROLLED BACK after {time.monotonic()-start:.1f}s")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
