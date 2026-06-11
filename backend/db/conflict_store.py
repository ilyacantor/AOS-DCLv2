"""ConflictStore — data access for the Conflict Register (Gate 1A, ContextOS §8).

Tables: conflict_register (queryable conflicts, two classes), conflict_dispositions
(append-only decision trace — the Gate 2 seed), tenant_authority_map (per-tenant
source authority; tenant '*' = defaults), tenant_conflict_policy (materiality).

Sync psycopg2, parameterized queries, no business logic (detection/recommendation
live in backend/engine/conflict_detection.py).
"""

import json
from typing import Any, Optional

from backend.core.db import get_connection
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

_REGISTER_COLS = (
    "id, tenant_id, entity_id, conflict_type, conflict_class, concept, property, "
    "period, dcl_ingest_id, status, claims, materiality, recommended, "
    "root_cause_explanation, root_cause_source, detected_at, updated_at"
)


def _row_to_register(row: tuple, cols: list[str]) -> dict:
    d = dict(zip(cols, row))
    d["conflict_id"] = str(d.pop("id"))
    d["tenant_id"] = str(d["tenant_id"])
    d["dcl_ingest_id"] = str(d["dcl_ingest_id"])  # column is born namespaced (I1)
    for k in ("detected_at", "updated_at"):
        if d.get(k) is not None:
            d[k] = d[k].isoformat()
    return d


class ConflictStore:

    # ── register ──────────────────────────────────────────────────────────

    def upsert_conflicts(self, rows: list[dict]) -> list[tuple[str, bool]]:
        """Insert or refresh register rows for (coords, run) — ONE statement,
        one commit, regardless of row count (the detection pass runs inside
        ingest; per-row round trips to the remote pooler are a latency bug).
        Returns [(conflict_id, created), ...] in input order. Re-detection
        refreshes claims/recommended but never clobbers a disposition: status
        is only set on INSERT."""
        if not rows:
            return []
        from psycopg2.extras import execute_values
        sql = """
            INSERT INTO conflict_register
                (tenant_id, entity_id, conflict_type, conflict_class, concept,
                 property, period, dcl_ingest_id, claims, materiality, recommended,
                 root_cause_explanation, root_cause_source)
            VALUES %s
            ON CONFLICT (tenant_id, entity_id, concept, property, COALESCE(period, ''), dcl_ingest_id)
            DO UPDATE SET
                claims = EXCLUDED.claims,
                materiality = EXCLUDED.materiality,
                recommended = CASE WHEN conflict_register.status = 'open'
                                   THEN EXCLUDED.recommended
                                   ELSE conflict_register.recommended END,
                conflict_type = EXCLUDED.conflict_type,
                conflict_class = EXCLUDED.conflict_class,
                root_cause_explanation = EXCLUDED.root_cause_explanation,
                root_cause_source = EXCLUDED.root_cause_source,
                updated_at = now()
            RETURNING id, (xmax = 0) AS created
        """
        template = ("(%(tenant_id)s, %(entity_id)s, %(conflict_type)s, "
                    "%(conflict_class)s, %(concept)s, %(property)s, %(period)s, "
                    "%(dcl_ingest_id)s, %(claims)s::jsonb, %(materiality)s::jsonb, "
                    "%(recommended)s::jsonb, %(root_cause_explanation)s, "
                    "%(root_cause_source)s)")
        params = [
            {
                **r,
                "claims": json.dumps(r["claims"]),
                "materiality": json.dumps(r["materiality"]) if r.get("materiality") is not None else None,
                "recommended": json.dumps(r["recommended"]) if r.get("recommended") is not None else None,
            }
            for r in rows
        ]
        with get_connection() as conn:
            with conn.cursor() as cur:
                out = execute_values(cur, sql, params, template=template,
                                     page_size=200, fetch=True)
                conn.commit()
        return [(str(r[0]), bool(r[1])) for r in out]

    def latest_precedents(self, tenant_id: str) -> dict[str, dict]:
        """Latest non-escalate disposition per conflict class for a tenant —
        ONE query (the detection pass attaches precedents per group)."""
        sql = """
            SELECT DISTINCT ON (conflict_class)
                   conflict_class, id, conflict_id, action, winner_source,
                   decided_by, rationale, decided_at
            FROM conflict_dispositions
            WHERE tenant_id = %s AND action != 'escalate'
            ORDER BY conflict_class, decided_at DESC, id DESC
        """
        out: dict[str, dict] = {}
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (tenant_id,))
                for cls, pid, cid, action, winner, by, why, at in cur.fetchall():
                    out[cls] = {
                        "disposition_id": str(pid), "conflict_id": str(cid),
                        "action": action, "winner_source": winner,
                        "decided_by": by, "rationale": why,
                        "decided_at": at.isoformat(),
                    }
        return out

    def list_conflicts(
        self, tenant_id: str, *, entity_id: str | None = None,
        status: str | None = None, conflict_type: str | None = None,
        concept: str | None = None, conflict_class: str | None = None,
        limit: int = 100, offset: int = 0,
    ) -> tuple[list[dict], int]:
        clauses = ["tenant_id = %s"]
        params: list[Any] = [tenant_id]
        for col, val in (("entity_id", entity_id), ("status", status),
                         ("conflict_type", conflict_type), ("concept", concept),
                         ("conflict_class", conflict_class)):
            if val is not None:
                clauses.append(f"{col} = %s")
                params.append(val)
        where = " AND ".join(clauses)
        safe_limit = max(1, min(int(limit), 500))
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM conflict_register WHERE {where}", params)
                total = cur.fetchone()[0]
                cur.execute(
                    f"SELECT {_REGISTER_COLS} FROM conflict_register WHERE {where} "
                    f"ORDER BY detected_at DESC, id DESC LIMIT %s OFFSET %s",
                    params + [safe_limit, max(0, int(offset))],
                )
                cols = [d[0] for d in cur.description]
                rows = [_row_to_register(r, cols) for r in cur.fetchall()]
        return rows, total

    def get_conflict(self, tenant_id: str, conflict_id: str) -> Optional[dict]:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT {_REGISTER_COLS} FROM conflict_register "
                    f"WHERE tenant_id = %s AND id = %s",
                    (tenant_id, conflict_id),
                )
                row = cur.fetchone()
                if row is None:
                    return None
                cols = [d[0] for d in cur.description]
                return _row_to_register(row, cols)

    def count_open(self, tenant_id: str | None = None, entity_id: str | None = None) -> int:
        clauses = ["status IN ('open', 'escalated')"]
        params: list[Any] = []
        if tenant_id:
            clauses.append("tenant_id = %s")
            params.append(tenant_id)
        if entity_id:
            clauses.append("entity_id = %s")
            params.append(entity_id)
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT COUNT(*) FROM conflict_register WHERE {' AND '.join(clauses)}",
                    params,
                )
                return cur.fetchone()[0]

    # ── dispositions (append-only) ────────────────────────────────────────

    def record_disposition(
        self, *, conflict_id: str, tenant_id: str, entity_id: str,
        conflict_class: str, action: str, winner_source: str | None,
        loser_sources: list[str], superseded_triple_ids: list[str],
        decided_by: str, rationale: str, context: dict | None,
        new_status: str,
    ) -> dict:
        """Append the disposition, flip register status, and supersede the losing
        triples — one transaction. Returns the disposition record."""
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT status FROM conflict_register WHERE tenant_id = %s AND id = %s FOR UPDATE",
                    (tenant_id, conflict_id),
                )
                row = cur.fetchone()
                if row is None:
                    raise LookupError(
                        f"conflict {conflict_id} not found for tenant {tenant_id}"
                    )
                if row[0] == "dispositioned":
                    raise ValueError(
                        f"conflict {conflict_id} is already dispositioned — "
                        f"dispositions are append-only decisions, not edits"
                    )
                cur.execute(
                    """
                    INSERT INTO conflict_dispositions
                        (conflict_id, tenant_id, entity_id, conflict_class, action,
                         winner_source, loser_sources, superseded_triple_ids,
                         decided_by, rationale, context)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s::uuid[], %s, %s, %s::jsonb)
                    RETURNING id, decided_at
                    """,
                    (conflict_id, tenant_id, entity_id, conflict_class, action,
                     winner_source, loser_sources, superseded_triple_ids,
                     decided_by, rationale,
                     json.dumps(context) if context is not None else None),
                )
                disp_id, decided_at = cur.fetchone()
                cur.execute(
                    "UPDATE conflict_register SET status = %s, updated_at = now() "
                    "WHERE id = %s",
                    (new_status, conflict_id),
                )
                superseded = 0
                if superseded_triple_ids:
                    cur.execute(
                        "UPDATE semantic_triples "
                        "SET superseded_at = now(), updated_at = now() "
                        "WHERE id = ANY(%s::uuid[]) AND tenant_id = %s AND is_active = true",
                        (superseded_triple_ids, tenant_id),
                    )
                    superseded = cur.rowcount
                conn.commit()
        logger.info(
            "[conflict-disposition] conflict=%s action=%s winner=%s superseded=%d by=%s",
            conflict_id, action, winner_source, superseded, decided_by,
        )
        return {
            "disposition_id": str(disp_id),
            "conflict_id": conflict_id,
            "action": action,
            "winner_source": winner_source,
            "loser_sources": loser_sources,
            "superseded_count": superseded,
            "decided_by": decided_by,
            "decided_at": decided_at.isoformat(),
        }

    def latest_precedent(self, tenant_id: str, conflict_class: str) -> Optional[dict]:
        """Most recent non-escalate disposition for a conflict class (precedent
        lookup, §8). Escalations are not precedents — they made no choice."""
        sql = """
            SELECT id, conflict_id, action, winner_source, decided_by, rationale, decided_at
            FROM conflict_dispositions
            WHERE tenant_id = %s AND conflict_class = %s AND action != 'escalate'
            ORDER BY decided_at DESC, id DESC LIMIT 1
        """
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (tenant_id, conflict_class))
                row = cur.fetchone()
        if row is None:
            return None
        return {
            "disposition_id": str(row[0]),
            "conflict_id": str(row[1]),
            "action": row[2],
            "winner_source": row[3],
            "decided_by": row[4],
            "rationale": row[5],
            "decided_at": row[6].isoformat(),
        }

    def list_dispositions(self, tenant_id: str, conflict_id: str) -> list[dict]:
        sql = """
            SELECT id, action, winner_source, loser_sources, superseded_triple_ids,
                   decided_by, rationale, decided_at
            FROM conflict_dispositions
            WHERE tenant_id = %s AND conflict_id = %s
            ORDER BY decided_at ASC, id ASC
        """
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (tenant_id, conflict_id))
                rows = cur.fetchall()

        def _uuid_array(v) -> list[str]:
            # psycopg2 returns uuid[] as a raw '{a,b}' string unless the
            # extras adapter is registered globally — normalize either shape.
            if v is None:
                return []
            if isinstance(v, str):
                return [x for x in v.strip("{}").split(",") if x]
            return [str(t) for t in v]

        return [
            {
                "disposition_id": str(r[0]), "action": r[1], "winner_source": r[2],
                "loser_sources": list(r[3] or []),
                "superseded_triple_ids": _uuid_array(r[4]),
                "decided_by": r[5], "rationale": r[6], "decided_at": r[7].isoformat(),
            }
            for r in rows
        ]

    # ── authority map + policy ────────────────────────────────────────────

    def load_authority_map(self, tenant_id: str) -> dict[str, list[str]]:
        """Effective authority map for a tenant: '*' defaults overlaid by the
        tenant's own rows (tenant wins per concept_prefix)."""
        sql = """
            SELECT concept_prefix, ranked_sources FROM tenant_authority_map
            WHERE tenant_id = '*' OR tenant_id = %s
            ORDER BY CASE WHEN tenant_id = '*' THEN 0 ELSE 1 END
        """
        amap: dict[str, list[str]] = {}
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (str(tenant_id),))
                for prefix, sources in cur.fetchall():
                    amap[prefix] = list(sources)  # tenant rows ordered last → override
        return amap

    def put_authority_entry(self, tenant_id: str, concept_prefix: str,
                            ranked_sources: list[str]) -> None:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO tenant_authority_map (tenant_id, concept_prefix, ranked_sources)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (tenant_id, concept_prefix)
                    DO UPDATE SET ranked_sources = EXCLUDED.ranked_sources, updated_at = now()
                    """,
                    (str(tenant_id), concept_prefix, ranked_sources),
                )
                conn.commit()

    def load_policy(self, tenant_id: str) -> dict:
        """Effective materiality policy: tenant row else '*' default."""
        sql = """
            SELECT tenant_id, abs_threshold, rel_threshold FROM tenant_conflict_policy
            WHERE tenant_id IN ('*', %s)
            ORDER BY CASE WHEN tenant_id = '*' THEN 1 ELSE 0 END
            LIMIT 1
        """
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (str(tenant_id),))
                row = cur.fetchone()
        if row is None:
            raise RuntimeError(
                "tenant_conflict_policy has no '*' default row — migration 018 "
                "seeds it; the policy table is required for conflict detection."
            )
        return {
            "policy_tenant": row[0],
            "abs_threshold": float(row[1]) if row[1] is not None else None,
            "rel_threshold": float(row[2]) if row[2] is not None else None,
        }

    def put_policy(self, tenant_id: str, abs_threshold: float | None,
                   rel_threshold: float | None) -> None:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO tenant_conflict_policy (tenant_id, abs_threshold, rel_threshold)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (tenant_id)
                    DO UPDATE SET abs_threshold = EXCLUDED.abs_threshold,
                                  rel_threshold = EXCLUDED.rel_threshold,
                                  updated_at = now()
                    """,
                    (str(tenant_id), abs_threshold, rel_threshold),
                )
                conn.commit()
