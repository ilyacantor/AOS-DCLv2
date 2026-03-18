"""
Merge conflict endpoints — COFA conflict resolution workflow.

GET   /api/dcl/merge/conflicts                         — list all conflicts
PATCH /api/dcl/merge/conflicts/{conflict_id}/resolve    — resolve one conflict
POST  /api/dcl/merge/conflicts/batch-resolve            — resolve multiple conflicts
"""

from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Optional

from backend.core.db import get_connection
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

router = APIRouter(tags=["Merge Conflicts"])


class ResolveRequest(BaseModel):
    resolution: str = Field(..., pattern="^(acquirer|target|keep_both|post_close)$")
    notes: str = ""
    resolved_by: str = Field(..., min_length=1)


class BatchResolveRequest(BaseModel):
    conflict_ids: list[str] = Field(..., min_length=1)
    resolution: str = Field(..., pattern="^(acquirer|target|keep_both|post_close)$")
    notes: str = ""
    resolved_by: str = Field(..., min_length=1)


def _jsonb_str(value) -> str:
    """Strip JSONB double-encoding quotes."""
    s = str(value)
    if len(s) >= 2 and s.startswith('"') and s.endswith('"'):
        return s[1:-1]
    return s


def _jsonb_float(value) -> float:
    """Convert a JSONB value to float.

    JSONB dollar_impact values are always numeric strings written by
    cofa_mapping_writer. A non-numeric value indicates upstream data
    corruption — surface it as an error rather than silently zeroing.
    """
    s = str(value).strip('"')
    if not s:
        raise ValueError(f"_jsonb_float received empty value — expected numeric JSONB, got: {value!r}")
    return float(s)


def _load_conflicts(cur) -> list[dict]:
    """Load all COFA conflicts from semantic_triples, grouped by concept.

    Reads cofa_conflict.* triples and groups properties per conflict.
    Also reads resolution properties if they exist.
    Sorted by dollar_impact descending.
    """
    cur.execute(
        "SELECT DISTINCT ON (concept, property) concept, property, value "
        "FROM semantic_triples "
        "WHERE is_active = true AND concept LIKE 'cofa_conflict.%%' "
        "ORDER BY concept, property, created_at DESC"
    )
    rows = cur.fetchall()

    grouped: dict[str, dict[str, str]] = {}
    for concept, prop, value in rows:
        if concept not in grouped:
            grouped[concept] = {}
        grouped[concept][prop] = value

    conflicts = []
    for concept in sorted(grouped.keys()):
        props = grouped[concept]
        conflict_id = concept.split(".")[-1] if "." in concept else concept
        dollar_impact = _jsonb_float(props.get("dollar_impact", "0"))

        conflicts.append({
            "conflict_id": conflict_id,
            "concept": concept,
            "conflict_type": _jsonb_str(props.get("conflict_type", "")),
            "severity": _jsonb_str(props.get("severity", "")),
            "description": _jsonb_str(props.get("description", "")),
            "dollar_impact": dollar_impact,
            "acquirer_treatment": _jsonb_str(props.get("acquirer_treatment", "")),
            "target_treatment": _jsonb_str(props.get("target_treatment", "")),
            "resolution_status": _jsonb_str(props.get("resolution_status", "pending")),
            "resolution": _jsonb_str(props.get("resolution", "")),
            "resolved_by": _jsonb_str(props.get("resolved_by", "")),
            "resolved_at": _jsonb_str(props.get("resolved_at", "")),
            "resolution_notes": _jsonb_str(props.get("resolution_notes", "")),
        })

    conflicts.sort(key=lambda c: c["dollar_impact"], reverse=True)
    return conflicts


def _resolve_conflict(cur, conn, conflict_id: str, resolution: str, notes: str, resolved_by: str) -> dict:
    """Write resolution properties for a single conflict.

    Uses deactivate + insert pattern for resolution_status.
    Inserts new triples for resolution, resolved_by, resolved_at, resolution_notes.
    """
    concept = f"cofa_conflict.{conflict_id}"

    # Verify the conflict exists
    cur.execute(
        "SELECT COUNT(*) FROM semantic_triples "
        "WHERE is_active = true AND concept = %s",
        (concept,),
    )
    if cur.fetchone()[0] == 0:
        raise HTTPException(
            status_code=404,
            detail=f"Conflict '{conflict_id}' not found in active semantic_triples. "
                   f"Verify the conflict_id matches a cofa_conflict.* concept.",
        )

    # Get tenant_id and entity_id from existing conflict triple
    cur.execute(
        "SELECT tenant_id, entity_id, run_id FROM semantic_triples "
        "WHERE is_active = true AND concept = %s LIMIT 1",
        (concept,),
    )
    row = cur.fetchone()
    tenant_id, entity_id, run_id = row[0], row[1], row[2]

    now_iso = datetime.now(timezone.utc).isoformat()

    # Deactivate old resolution-related triples for this conflict
    resolution_props = ("resolution_status", "resolution", "resolved_by", "resolved_at", "resolution_notes")
    for prop in resolution_props:
        cur.execute(
            "UPDATE semantic_triples SET is_active = false, updated_at = now() "
            "WHERE is_active = true AND concept = %s AND property = %s",
            (concept, prop),
        )

    # Insert new resolution triples
    import json
    new_props = {
        "resolution_status": "resolved",
        "resolution": resolution,
        "resolved_by": resolved_by,
        "resolved_at": now_iso,
        "resolution_notes": notes,
    }

    cols = [
        "tenant_id", "entity_id", "concept", "property", "value",
        "period", "currency", "unit",
        "source_system", "source_table", "source_field",
        "pipe_id", "run_id", "source_run_tag",
        "confidence_score", "confidence_tier",
        "canonical_id", "resolution_method", "resolution_confidence",
    ]
    col_names = ", ".join(cols)
    placeholders = ", ".join(["%s"] * len(cols))
    sql = f"INSERT INTO semantic_triples ({col_names}) VALUES ({placeholders})"

    for prop, val in new_props.items():
        cur.execute(sql, (
            tenant_id, entity_id, concept, prop, json.dumps(val),
            None, None, None,  # period, currency, unit
            "operator", None, "conflict_resolution",  # source_system, source_table, source_field
            None, run_id, None,  # pipe_id, run_id, source_run_tag
            1.0, "exact",  # confidence_score, confidence_tier
            None, None, None,  # canonical_id, resolution_method, resolution_confidence
        ))

    conn.commit()

    return {
        "conflict_id": conflict_id,
        "resolution_status": "resolved",
        "resolution": resolution,
        "resolved_by": resolved_by,
        "resolved_at": now_iso,
        "resolution_notes": notes,
    }


@router.get("/api/dcl/merge/conflicts")
def list_conflicts():
    """List all COFA conflicts sorted by dollar_impact descending."""
    with get_connection() as conn:
        if conn is None:
            raise HTTPException(
                status_code=503,
                detail="merge/conflicts failed: database connection unavailable. "
                       "Check DATABASE_URL and Supabase connectivity.",
            )
        with conn.cursor() as cur:
            conflicts = _load_conflicts(cur)

    total = len(conflicts)
    resolved = sum(1 for c in conflicts if c["resolution_status"] == "resolved")
    pending = total - resolved

    return {
        "conflicts": conflicts,
        "summary": {
            "total": total,
            "pending": pending,
            "resolved": resolved,
        },
    }


@router.patch("/api/dcl/merge/conflicts/{conflict_id}/resolve")
def resolve_conflict(conflict_id: str, body: ResolveRequest):
    """Resolve a single COFA conflict."""
    with get_connection() as conn:
        if conn is None:
            raise HTTPException(
                status_code=503,
                detail="merge/conflicts/resolve failed: database connection unavailable.",
            )
        with conn.cursor() as cur:
            result = _resolve_conflict(
                cur, conn, conflict_id,
                body.resolution, body.notes, body.resolved_by,
            )
    return result


@router.post("/api/dcl/merge/conflicts/batch-resolve")
def batch_resolve_conflicts(body: BatchResolveRequest):
    """Resolve multiple COFA conflicts at once."""
    with get_connection() as conn:
        if conn is None:
            raise HTTPException(
                status_code=503,
                detail="merge/conflicts/batch-resolve failed: database connection unavailable.",
            )
        with conn.cursor() as cur:
            resolved_conflicts = []
            for cid in body.conflict_ids:
                result = _resolve_conflict(
                    cur, conn, cid,
                    body.resolution, body.notes, body.resolved_by,
                )
                resolved_conflicts.append(result)

    return {
        "resolved": len(resolved_conflicts),
        "conflicts": resolved_conflicts,
    }
