"""TraceStore — read-only data access over the decision_traces VIEW
(Gate 2A, ContextOS §9, migration 020).

decision_traces is a UNION-ALL projection over the three existing decision
stores (mai_mcp_audit, conflict_dispositions⋈conflict_register,
resolver_hitl_audit⋈resolver_hitl_queue). The base tables remain the ONLY
write paths — this store issues SELECTs only, by construction and by rule.

Sync psycopg2, parameterized queries, no business logic. Identity (tenant_id)
is required on every call: missing ⇒ ValueError, which routes surface as 422
(I2, no fallback). The view exposes no run_id field (I1).
"""

from typing import Any, Optional

from backend.core.db import get_connection
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

_TRACE_COLS = (
    "trace_id, trace_type, tenant_id, entity_id, agent, decision_type, "
    "concept, conflict_class, period, outcome, rationale, payload, "
    "result_summary, refs, occurred_at, ingested_at, superseded_at"
)

_TRACE_TYPES = ("mcp_call", "conflict_disposition", "er_confirmation", "proposal_decision")


def _row_to_trace(row: tuple, cols: list[str]) -> dict:
    d = dict(zip(cols, row))
    d["trace_id"] = str(d["trace_id"])
    d["tenant_id"] = str(d["tenant_id"])
    for k in ("occurred_at", "ingested_at", "superseded_at"):
        if d.get(k) is not None:
            d[k] = d[k].isoformat()
    return d


def _require_tenant(tenant_id: Optional[str], op: str) -> str:
    if not tenant_id or not str(tenant_id).strip():
        raise ValueError(
            f"{op} requires tenant_id — decision traces are tenant-scoped (I2); "
            f"refusing an unscoped read."
        )
    return str(tenant_id)


class TraceStore:
    """Read-only queries over decision_traces. No writes, ever."""

    def search_traces(
        self,
        tenant_id: str,
        *,
        entity_id: Optional[str] = None,
        concept: Optional[str] = None,
        agent: Optional[str] = None,
        decision_type: Optional[str] = None,
        trace_type: Optional[str] = None,
        conflict_class: Optional[str] = None,
        period: Optional[str] = None,
        since: Optional[str] = None,
        until: Optional[str] = None,
        as_of: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[list[dict], int]:
        """Search the unified trace view on every Gate 2A axis.

        since/until bound occurred_at (event time). as_of is the knowledge-time
        read: ingested_at <= as_of — the same predicate shape as
        EdgeStore._temporal_clause MINUS the superseded leg, because traces are
        events and are never superseded (the view pins superseded_at to NULL),
        so the "(superseded_at IS NULL OR superseded_at > T)" half is
        vacuously true and deliberately omitted.

        Returns (rows, total_count) — total_count is the filtered count before
        limit/offset, mirroring ConflictStore.list_conflicts.
        """
        tenant = _require_tenant(tenant_id, "search_traces")
        if trace_type is not None and trace_type not in _TRACE_TYPES:
            raise ValueError(
                f"trace_type must be one of {_TRACE_TYPES}; got {trace_type!r}"
            )
        clauses = ["tenant_id = %s"]
        params: list[Any] = [tenant]
        for col, val in (
            ("entity_id", entity_id),
            ("concept", concept),
            ("agent", agent),
            ("decision_type", decision_type),
            ("trace_type", trace_type),
            ("conflict_class", conflict_class),
            ("period", period),
        ):
            if val is not None:
                clauses.append(f"{col} = %s")
                params.append(val)
        if since is not None:
            clauses.append("occurred_at >= %s")
            params.append(since)
        if until is not None:
            clauses.append("occurred_at <= %s")
            params.append(until)
        if as_of is not None:
            clauses.append("ingested_at <= %s")
            params.append(as_of)
        where = " AND ".join(clauses)
        safe_limit = max(1, min(int(limit), 500))
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT COUNT(*) FROM decision_traces WHERE {where}", params
                )
                total = cur.fetchone()[0]
                cur.execute(
                    f"SELECT {_TRACE_COLS} FROM decision_traces WHERE {where} "
                    f"ORDER BY occurred_at DESC, trace_id DESC LIMIT %s OFFSET %s",
                    params + [safe_limit, max(0, int(offset))],
                )
                cols = [d[0] for d in cur.description]
                rows = [_row_to_trace(r, cols) for r in cur.fetchall()]
        return rows, total

    def get_trace(self, tenant_id: str, trace_id: str) -> Optional[dict]:
        """One trace by id, tenant-scoped. None when absent for that tenant."""
        tenant = _require_tenant(tenant_id, "get_trace")
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT {_TRACE_COLS} FROM decision_traces "
                    f"WHERE tenant_id = %s AND trace_id = %s",
                    (tenant, trace_id),
                )
                row = cur.fetchone()
                if row is None:
                    return None
                cols = [d[0] for d in cur.description]
                return _row_to_trace(row, cols)

    def recurring_disposition_patterns(
        self, tenant_id: str, conflict_class: str
    ) -> list[dict]:
        """Recurring non-escalate disposition patterns for one conflict class,
        grouped by (action, winner_source), ordered by count DESC.

        Reads conflict_dispositions directly (the disposition branch's base
        table — its id IS the view's trace_id for trace_type
        'conflict_disposition'), so the returned trace_ids resolve through
        decision_traces. Escalations are excluded: they made no choice, so
        they are precedent for nothing (same rule as
        ConflictStore.latest_precedent).
        """
        tenant = _require_tenant(tenant_id, "recurring_disposition_patterns")
        if not conflict_class or not str(conflict_class).strip():
            raise ValueError(
                "recurring_disposition_patterns requires conflict_class — "
                "patterns are per-class by definition."
            )
        sql = """
            SELECT action, winner_source, COUNT(*) AS n,
                   array_agg(id::text ORDER BY decided_at ASC, id ASC) AS trace_ids,
                   MAX(decided_at) AS latest_decided_at
            FROM conflict_dispositions
            WHERE tenant_id = %s AND conflict_class = %s AND action != 'escalate'
            GROUP BY action, winner_source
            ORDER BY n DESC, MAX(decided_at) DESC
        """
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (tenant, conflict_class))
                rows = cur.fetchall()
        return [
            {
                "action": r[0],
                "winner_source": r[1],
                "count": int(r[2]),
                "trace_ids": list(r[3]),
                "latest_decided_at": r[4].isoformat(),
            }
            for r in rows
        ]
