"""One-time prune to cap tenant_runs at TENANT_RUNS_CAP entities per tenant, LIFO by updated_at.

Three steps, single transaction:
    1. Reconcile: for every surviving (tenant, entity) in tenant_runs rebuild
       current_triples from semantic_triples (current_run_id slice) and reset
       run_row_count to match. Repairs any pre-existing drift left by prior
       failed writes.
    2. Evict: per tenant, keep the N most-recently-updated entities and hard-
       delete the rest via TripleStore._enforce_tenant_cap — the same helper
       swap_and_delete uses going forward.
    3. Verify invariants: semantic_triples == current_triples ==
       SUM(tenant_runs.run_row_count); no tenant exceeds TENANT_RUNS_CAP.

Prints reconcile deltas, the eviction list, and pre/post counts.
"""

import os
import sys
import time

import psycopg2
from dotenv import load_dotenv

_repo = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.insert(0, _repo)
load_dotenv(os.path.join(_repo, ".env"))

from backend.db.triple_store import TripleStore, TENANT_RUNS_CAP  # noqa: E402


def _count(cur, table: str) -> int:
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    return cur.fetchone()[0]


def _sum_run_row_count(cur) -> int:
    cur.execute("SELECT COALESCE(SUM(run_row_count), 0) FROM tenant_runs")
    return cur.fetchone()[0]


def main() -> None:
    url = os.environ["DATABASE_URL"]
    conn = psycopg2.connect(url, application_name="prune_tenant_runs_cap")
    conn.autocommit = False
    start = time.monotonic()
    try:
        cur = conn.cursor()
        cur.execute("SET statement_timeout = '600000'")

        pre_tenant_runs = _count(cur, "tenant_runs")
        pre_semantic = _count(cur, "semantic_triples")
        pre_current = _count(cur, "current_triples")
        pre_sum_run = _sum_run_row_count(cur)
        print(
            f"[PRUNE] pre: tenant_runs={pre_tenant_runs} "
            f"semantic={pre_semantic} current={pre_current} "
            f"sum_run_row_count={pre_sum_run}"
        )

        # Step 1: reconcile. For every (tenant, entity) in tenant_runs, rebuild
        # the current_triples slice from semantic_triples.current_run_id and
        # reset run_row_count to match. Repairs any pre-existing drift left by
        # earlier failed writes (e.g., InfoWave-4AIF semantic=19945 vs run_row_count=5000).
        cur.execute(
            "SELECT tenant_id, entity_id, current_run_id, run_row_count "
            "FROM tenant_runs ORDER BY tenant_id, entity_id"
        )
        tenant_runs_rows = cur.fetchall()
        print(f"[PRUNE] reconcile: {len(tenant_runs_rows)} tenant_runs rows")
        reconcile_deltas: list[tuple] = []
        for (rc_tenant, rc_entity, rc_run_id, rc_declared) in tenant_runs_rows:
            cur.execute(
                "DELETE FROM current_triples "
                "WHERE tenant_id = %s AND entity_id = %s",
                (rc_tenant, rc_entity),
            )
            cur.execute(
                """
                INSERT INTO current_triples (
                    id, tenant_id, entity_id, concept, property, value, period,
                    currency, unit, source_system, source_table, source_field,
                    pipe_id, source_run_tag,
                    confidence_score, confidence_tier,
                    canonical_id, resolution_method, resolution_confidence,
                    fabric_plane, fabric_product, created_at
                )
                SELECT
                    id, tenant_id, entity_id, concept, property, value, period,
                    currency, unit, source_system, source_table, source_field,
                    pipe_id, source_run_tag,
                    confidence_score, confidence_tier,
                    canonical_id, resolution_method, resolution_confidence,
                    fabric_plane, fabric_product, created_at
                FROM semantic_triples
                WHERE tenant_id = %s AND entity_id = %s AND run_id = %s
                ON CONFLICT (id) DO NOTHING
                """,
                (rc_tenant, rc_entity, rc_run_id),
            )
            inserted = cur.rowcount
            cur.execute(
                "UPDATE tenant_runs SET run_row_count = %s "
                "WHERE tenant_id = %s AND entity_id = %s",
                (inserted, rc_tenant, rc_entity),
            )
            if inserted != rc_declared:
                reconcile_deltas.append(
                    (str(rc_tenant), rc_entity, rc_declared, inserted)
                )

        if reconcile_deltas:
            print(f"[PRUNE] reconcile drifted {len(reconcile_deltas)} entities:")
            for (t, e, was, now) in reconcile_deltas:
                print(f"        - {t}/{e} run_row_count {was} -> {now}")
        else:
            print("[PRUNE] reconcile: no drift detected")

        mid_semantic = _count(cur, "semantic_triples")
        mid_current = _count(cur, "current_triples")
        mid_sum_run = _sum_run_row_count(cur)
        print(
            f"[PRUNE] post-reconcile: semantic={mid_semantic} "
            f"current={mid_current} sum_run_row_count={mid_sum_run}"
        )
        if mid_semantic != mid_sum_run:
            raise RuntimeError(
                f"reconcile failed: semantic_triples ({mid_semantic}) != "
                f"sum(run_row_count) ({mid_sum_run}) — semantic holds rows "
                f"for (tenant, entity, run_id) triples not reachable from tenant_runs"
            )
        if mid_current != mid_semantic:
            raise RuntimeError(
                f"reconcile failed: current_triples ({mid_current}) != "
                f"semantic_triples ({mid_semantic})"
            )

        # Step 2: evict per-tenant beyond cap.
        cur.execute("SELECT DISTINCT tenant_id FROM tenant_runs ORDER BY tenant_id")
        tenants = [r[0] for r in cur.fetchall()]
        print(f"[PRUNE] tenants found: {len(tenants)} cap={TENANT_RUNS_CAP}")

        all_evictions: list[dict] = []
        for t in tenants:
            evicted = TripleStore._enforce_tenant_cap(cur, str(t), TENANT_RUNS_CAP)
            if evicted:
                print(f"[PRUNE] tenant={t} evicted={len(evicted)}")
                for ev in evicted:
                    print(
                        f"        - {ev['entity_id']} "
                        f"rows={ev['run_row_count']} "
                        f"updated_at={ev['updated_at']} "
                        f"semantic_deleted={ev['semantic_deleted']} "
                        f"current_deleted={ev['current_deleted']}"
                    )
                all_evictions.extend(evicted)
            else:
                print(f"[PRUNE] tenant={t} already at or below cap")

        post_tenant_runs = _count(cur, "tenant_runs")
        post_semantic = _count(cur, "semantic_triples")
        post_current = _count(cur, "current_triples")
        post_sum_run = _sum_run_row_count(cur)
        print(
            f"[PRUNE] post: tenant_runs={post_tenant_runs} "
            f"semantic={post_semantic} current={post_current} "
            f"sum_run_row_count={post_sum_run}"
        )

        if post_semantic != post_sum_run:
            raise RuntimeError(
                f"invariant broken: semantic_triples ({post_semantic}) != "
                f"sum(run_row_count) ({post_sum_run})"
            )
        if post_current != post_semantic:
            raise RuntimeError(
                f"invariant broken: current_triples ({post_current}) != "
                f"semantic_triples ({post_semantic})"
            )

        cur.execute(
            "SELECT tenant_id, COUNT(*) FROM tenant_runs "
            "GROUP BY tenant_id HAVING COUNT(*) > %s",
            (TENANT_RUNS_CAP,),
        )
        over_cap = cur.fetchall()
        if over_cap:
            raise RuntimeError(
                f"cap invariant broken: {over_cap} tenants still over {TENANT_RUNS_CAP}"
            )

        conn.commit()
        print(
            f"[PRUNE] OK in {time.monotonic() - start:.1f}s — "
            f"{len(all_evictions)} entities evicted across {len(tenants)} tenants"
        )
    except Exception:
        conn.rollback()
        print(f"[PRUNE] ROLLED BACK after {time.monotonic() - start:.1f}s")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
