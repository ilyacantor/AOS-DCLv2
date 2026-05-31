"""Resolver HITL queue — Postgres store for record-level identity decisions
that need (or recorded) human review.

Brought into DCL from AAM (app/db/hitl_store.py) per AAM Blueprint v3.1 §3.6
decision (c). AAM's store was SQLite + `INSERT OR IGNORE`; this is the psycopg2
port onto DCL's pooled connection — `INSERT ... ON CONFLICT DO NOTHING
RETURNING`, ISO-string timestamps -> TIMESTAMPTZ, extra_json -> JSONB. The
operator contract is unchanged:

  status='pending'      fuzzy match in [fuzzy_threshold, auto_threshold); the
                        resolver proposed a canonical, operator approves/rejects.
  status='auto_applied' fuzzy match >= auto_threshold; resolver already applied
                        it; NOT operator-actionable, surfaced for audit.
  status='approved'     operator confirmed; downstream triple resolution_method
                        flips fuzzy -> manual at confidence 0.99.
  status='rejected'     operator rejected the proposed match.

Idempotency: dedup_key collapses (tenant, domain, norm(left), norm(right),
status) so a replayed ingest converges to one row instead of duplicating
(the same guard AAM added after 5 replay runs produced ~10x duplicate rows).

Hard requirements (loud-fail, no silent fallback):
  tenant_id, entity_id, domain, left_value, right_value NOT NULL.
  confidence in [0.0, 1.0].
"""
from __future__ import annotations

import json
import re
import uuid
from typing import Optional

import psycopg2.extras

from backend.core.db import get_connection
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

_NORM_SEP = re.compile(r"[\s\-_./,;:]+")


def _normalize(s: str) -> str:
    """Same normalization as the resolver/registry — collapse separators,
    lowercase, strip. Kept in-module to avoid an import cycle."""
    return _NORM_SEP.sub(" ", str(s).lower()).strip()


def _dedup_key(*, tenant_id: str, domain: str, left_value: str,
               right_value: str, status: str) -> str:
    """Canonical dedup key. Two inserts with the same key converge to one row
    regardless of pipe_id / record_key / confidence / proposed_canonical_id —
    those are write-time noise; the dedup semantics are the operator-visible
    pair + status."""
    return "|".join([
        tenant_id, domain.lower(), _normalize(left_value),
        _normalize(right_value), status,
    ])


def _query(sql: str, params: tuple, *, fetch: bool = True) -> list[dict]:
    """Parameterized query on a pooled connection with explicit commit."""
    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = [dict(r) for r in cur.fetchall()] if fetch and cur.description else []
        conn.commit()
    return rows


def _insert_with_dedup(
    *, status: str, decided_by, tenant_id, entity_id, domain,
    left_pipe_id, left_record_key, left_value,
    right_pipe_id, right_record_key, right_value,
    confidence, canonical_id, extra: Optional[dict], created_event: str,
    reseen_details: dict,
) -> str:
    """Shared insert path for pending + auto_applied. Returns the hitl_queue_id.

    On dedup conflict, returns the existing row's id and appends a 'reseen'
    audit event (the resolver saw this pair again, but the queue stays one row
    per pair+status).
    """
    hitl_queue_id = str(uuid.uuid4())
    audit_id = str(uuid.uuid4())
    extra_json = json.dumps(extra) if extra is not None else None
    dedup = _dedup_key(
        tenant_id=tenant_id, domain=domain, left_value=left_value,
        right_value=right_value, status=status,
    )

    rows = _query(
        "INSERT INTO resolver_hitl_queue "
        "(hitl_queue_id, tenant_id, entity_id, domain, "
        " left_pipe_id, left_record_key, left_value, "
        " right_pipe_id, right_record_key, right_value, "
        " confidence, status, proposed_canonical_id, "
        " decided_by, audit_id, extra_json, dedup_key) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s) "
        "ON CONFLICT (dedup_key) WHERE dedup_key IS NOT NULL DO NOTHING "
        "RETURNING hitl_queue_id",
        (
            hitl_queue_id, tenant_id, entity_id, domain,
            left_pipe_id, left_record_key, left_value,
            right_pipe_id, right_record_key, right_value,
            confidence, status, canonical_id,
            decided_by, audit_id, extra_json, dedup,
        ),
    )
    if not rows:
        # Dedup conflict — fetch existing id, append a 'reseen' audit event.
        existing = _query(
            "SELECT hitl_queue_id FROM resolver_hitl_queue WHERE dedup_key=%s",
            (dedup,),
        )
        if not existing:
            raise RuntimeError(
                f"insert: ON CONFLICT DO NOTHING returned 0 rows AND no row found "
                f"for dedup_key={dedup!r} — DB inconsistency"
            )
        existing_qid = str(existing[0]["hitl_queue_id"])
        _append_audit_row(existing_qid, "reseen", reseen_details, "resolver")
        return existing_qid

    _append_audit_row(hitl_queue_id, created_event, reseen_details, "resolver",
                      audit_id=audit_id)
    return hitl_queue_id


def _append_audit_row(hitl_queue_id: str, event: str, details: dict,
                      actor: str, *, audit_id: Optional[str] = None) -> None:
    _query(
        "INSERT INTO resolver_hitl_audit (audit_id, hitl_queue_id, event, details, actor) "
        "VALUES (%s, %s, %s, %s::jsonb, %s)",
        (audit_id or str(uuid.uuid4()), hitl_queue_id, event, json.dumps(details), actor),
        fetch=False,
    )


def insert_pending(
    *,
    tenant_id: str,
    entity_id: str,
    domain: str,
    left_pipe_id: Optional[str],
    left_record_key: Optional[str],
    left_value: str,
    right_pipe_id: Optional[str],
    right_record_key: Optional[str],
    right_value: str,
    confidence: float,
    proposed_canonical_id: str,
    extra: Optional[dict] = None,
) -> str:
    """Insert a pending HITL row (fuzzy band). Idempotent. Returns hitl_queue_id."""
    if not tenant_id or not entity_id or not domain:
        raise ValueError(
            f"insert_pending: tenant_id, entity_id, domain required "
            f"(got tenant_id={tenant_id!r} entity_id={entity_id!r} domain={domain!r})"
        )
    if not left_value or not right_value:
        raise ValueError(
            f"insert_pending: left_value and right_value required "
            f"(got left={left_value!r} right={right_value!r})"
        )
    if not (0.0 <= confidence <= 1.0):
        raise ValueError(f"insert_pending: confidence must be in [0, 1] (got {confidence})")
    if not proposed_canonical_id:
        raise ValueError("insert_pending: proposed_canonical_id required")

    return _insert_with_dedup(
        status="pending", decided_by=None,
        tenant_id=tenant_id, entity_id=entity_id, domain=domain,
        left_pipe_id=left_pipe_id, left_record_key=left_record_key, left_value=left_value,
        right_pipe_id=right_pipe_id, right_record_key=right_record_key, right_value=right_value,
        confidence=confidence, canonical_id=proposed_canonical_id, extra=extra,
        created_event="created",
        reseen_details={"confidence": confidence, "domain": domain},
    )


def insert_auto_applied(
    *,
    tenant_id: str,
    entity_id: str,
    domain: str,
    left_pipe_id: Optional[str],
    left_record_key: Optional[str],
    left_value: str,
    right_pipe_id: Optional[str],
    right_record_key: Optional[str],
    right_value: str,
    confidence: float,
    canonical_id: str,
    match_rule: str,
    extra: Optional[dict] = None,
) -> str:
    """Insert an auto-applied resolver match (>= auto_threshold). Idempotent.

    NOT operator-actionable; surfaces for audit. proposed_canonical_id stores
    the already-bound canonical_id (the match is applied, not proposed).
    """
    if not tenant_id or not entity_id or not domain:
        raise ValueError(
            f"insert_auto_applied: tenant_id, entity_id, domain required "
            f"(got tenant_id={tenant_id!r} entity_id={entity_id!r} domain={domain!r})"
        )
    if not left_value or not right_value:
        raise ValueError(
            f"insert_auto_applied: left_value and right_value required "
            f"(got left={left_value!r} right={right_value!r})"
        )
    if not (0.0 <= confidence <= 1.0):
        raise ValueError(f"insert_auto_applied: confidence must be in [0, 1] (got {confidence})")
    if not canonical_id:
        raise ValueError("insert_auto_applied: canonical_id required")
    if not match_rule:
        raise ValueError("insert_auto_applied: match_rule required")

    enriched = dict(extra or {})
    enriched["match_rule"] = match_rule
    return _insert_with_dedup(
        status="auto_applied", decided_by="resolver",
        tenant_id=tenant_id, entity_id=entity_id, domain=domain,
        left_pipe_id=left_pipe_id, left_record_key=left_record_key, left_value=left_value,
        right_pipe_id=right_pipe_id, right_record_key=right_record_key, right_value=right_value,
        confidence=confidence, canonical_id=canonical_id, extra=enriched,
        created_event="auto_applied",
        reseen_details={"confidence": confidence, "domain": domain,
                        "match_rule": match_rule, "canonical_id": canonical_id},
    )


def get_pending(*, tenant_id: str, entity_id: Optional[str] = None,
                domain: Optional[str] = None, limit: int = 50) -> list[dict]:
    if not tenant_id:
        raise ValueError("get_pending: tenant_id required")
    sql = "SELECT * FROM resolver_hitl_queue WHERE tenant_id=%s AND status='pending'"
    params: list = [tenant_id]
    if entity_id:
        sql += " AND entity_id=%s"
        params.append(entity_id)
    if domain:
        sql += " AND domain=%s"
        params.append(domain)
    sql += " ORDER BY created_at DESC LIMIT %s"
    params.append(limit)
    return [_row_to_dict(r) for r in _query(sql, tuple(params))]


def list_auto_applied(*, tenant_id: str, domain: Optional[str] = None,
                      limit: int = 50) -> list[dict]:
    if not tenant_id:
        raise ValueError("list_auto_applied: tenant_id required")
    sql = "SELECT * FROM resolver_hitl_queue WHERE tenant_id=%s AND status='auto_applied'"
    params: list = [tenant_id]
    if domain:
        sql += " AND domain=%s"
        params.append(domain)
    sql += " ORDER BY created_at DESC LIMIT %s"
    params.append(limit)
    return [_row_to_dict(r) for r in _query(sql, tuple(params))]


def list_all(*, tenant_id: str, status: Optional[str] = None, limit: int = 200) -> list[dict]:
    if not tenant_id:
        raise ValueError("list_all: tenant_id required")
    sql = "SELECT * FROM resolver_hitl_queue WHERE tenant_id=%s"
    params: list = [tenant_id]
    if status:
        sql += " AND status=%s"
        params.append(status)
    sql += " ORDER BY created_at DESC LIMIT %s"
    params.append(limit)
    return [_row_to_dict(r) for r in _query(sql, tuple(params))]


def get_by_id(hitl_queue_id: str) -> Optional[dict]:
    if not hitl_queue_id:
        raise ValueError("get_by_id: hitl_queue_id required")
    rows = _query("SELECT * FROM resolver_hitl_queue WHERE hitl_queue_id=%s", (hitl_queue_id,))
    return _row_to_dict(rows[0]) if rows else None


def decide(*, hitl_queue_id: str, decision: str, decided_by: str) -> dict:
    """Finalize a pending row to approved/rejected + append an audit event.

    Raises if the row is missing or not currently pending (no overwrite of a
    finalized decision).
    """
    if decision not in ("approved", "rejected"):
        raise ValueError(f"decide: decision must be 'approved' or 'rejected' (got {decision!r})")
    if not decided_by:
        raise ValueError("decide: decided_by required (audit trail)")

    row = get_by_id(hitl_queue_id)
    if not row:
        raise LookupError(f"decide: hitl_queue_id {hitl_queue_id} not found")
    if row["status"] != "pending":
        raise ValueError(
            f"decide: hitl_queue_id {hitl_queue_id} is already {row['status']}; "
            f"refusing to overwrite a finalized decision"
        )
    updated = _query(
        "UPDATE resolver_hitl_queue SET status=%s, decided_by=%s, decided_at=now() "
        "WHERE hitl_queue_id=%s RETURNING *",
        (decision, decided_by, hitl_queue_id),
    )
    _append_audit_row(hitl_queue_id, f"decided_{decision}",
                      {"prior_status": "pending"}, decided_by,
                      audit_id=str(row["audit_id"]))
    return _row_to_dict(updated[0])


def get_audit(hitl_queue_id: str) -> list[dict]:
    if not hitl_queue_id:
        raise ValueError("get_audit: hitl_queue_id required")
    rows = _query(
        "SELECT * FROM resolver_hitl_audit WHERE hitl_queue_id=%s ORDER BY occurred_at ASC",
        (hitl_queue_id,),
    )
    return [_audit_row_to_dict(r) for r in rows]


def reset_for_tenant(tenant_id: str) -> int:
    """Test helper: delete every queue row + its audit entries for a tenant."""
    if not tenant_id:
        raise ValueError("reset_for_tenant: tenant_id required")
    _query(
        "DELETE FROM resolver_hitl_audit WHERE hitl_queue_id IN "
        "(SELECT hitl_queue_id FROM resolver_hitl_queue WHERE tenant_id=%s)",
        (tenant_id,), fetch=False,
    )
    rows = _query(
        "DELETE FROM resolver_hitl_queue WHERE tenant_id=%s RETURNING hitl_queue_id",
        (tenant_id,),
    )
    return len(rows)


def _row_to_dict(row: Optional[dict]) -> dict:
    if row is None:
        return {}
    d = dict(row)
    for k in ("hitl_queue_id", "tenant_id", "proposed_canonical_id", "audit_id"):
        if d.get(k) is not None:
            d[k] = str(d[k])
    if d.get("confidence") is not None:
        d["confidence"] = float(d["confidence"])
    for k in ("created_at", "decided_at"):
        if d.get(k) is not None:
            d[k] = d[k].isoformat()
    if d.get("extra_json") is not None:
        d["extra"] = d["extra_json"]
    return d


def _audit_row_to_dict(row: dict) -> dict:
    d = dict(row)
    for k in ("id", "audit_id", "hitl_queue_id"):
        if d.get(k) is not None:
            d[k] = str(d[k])
    if d.get("occurred_at") is not None:
        d["occurred_at"] = d["occurred_at"].isoformat()
    return d
