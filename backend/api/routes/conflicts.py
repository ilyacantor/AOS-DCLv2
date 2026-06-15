"""Conflict Register API (Gate 1A, ContextOS §8).

POST /api/dcl/conflicts/detect                — run detection (sweep or scoped)
GET  /api/dcl/conflicts                       — queryable register
GET  /api/dcl/conflicts/{conflict_id}         — entry + disposition history
POST /api/dcl/conflicts/{conflict_id}/disposition — HITL decision (append-only)
GET/PUT /api/dcl/conflicts/policy             — per-tenant materiality thresholds
GET/PUT /api/dcl/conflicts/authority-map      — per-tenant source authority

Identity: operator surfaces are entity-scoped (#29 entity axis — entity_id in,
tenant resolved server-side via tenant_runs); explicit tenant_id always wins.
Every response carries the tenant_id + entity_id pair (I2). No bare run_id
anywhere (I1): the register column itself is dcl_ingest_id.
"""

import uuid as _uuid
from typing import Optional

from fastapi import APIRouter, Body, HTTPException, Query
from pydantic import BaseModel

from backend.core.db import get_connection
from backend.db.conflict_store import ConflictStore
from backend.db.triple_store import TripleStore
from backend.engine.authority_resolution import resolve_conflict
from backend.engine.conflict_detection import detect_and_register
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

router = APIRouter(tags=["Conflict Register"])

_conflicts = ConflictStore()
_triples = TripleStore()


def _resolve_identity(tenant_id: Optional[str], entity_id: Optional[str]) -> tuple[str, Optional[str]]:
    """Resolve the (tenant_id, entity_id) pair from whichever the caller gave.
    Explicit tenant_id wins; else the entity's tenant via tenant_runs. Loud 422
    when neither resolves (I2 — no silent fallback)."""
    if tenant_id:
        try:
            _uuid.UUID(tenant_id)
        except (ValueError, AttributeError):
            raise HTTPException(status_code=400, detail=f"tenant_id must be a UUID; got {tenant_id!r}")
        return tenant_id, entity_id
    if entity_id:
        try:
            return _triples.resolve_tenant_for_entity(entity_id), entity_id
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e))
    raise HTTPException(
        status_code=422,
        detail="Provide entity_id (operator surface, tenant resolves from "
               "tenant_runs) or tenant_id explicitly — identity is required (I2).",
    )


# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------

@router.post("/api/dcl/conflicts/detect")
def conflicts_detect(body: dict = Body(default={})):
    """Run conflict detection. Scoped: {entity_id[, dcl_ingest_id]} detects one
    entity's run (default: its current run). Unscoped (empty body): sweep every
    (tenant, entity) current run — the deliberate full-register refresh."""
    entity_id = (body or {}).get("entity_id")
    tenant_id = (body or {}).get("tenant_id")
    run_id = (body or {}).get("dcl_ingest_id")

    targets: list[tuple[str, str, str]] = []
    if entity_id:
        resolved_tenant, _ = _resolve_identity(tenant_id, entity_id)
        if run_id:
            targets.append((resolved_tenant, entity_id, run_id))
        else:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT current_run_id FROM tenant_runs WHERE tenant_id = %s AND entity_id = %s",
                        (resolved_tenant, entity_id),
                    )
                    row = cur.fetchone()
            if row is None or row[0] is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"No current run for entity_id={entity_id!r} — ingest first.",
                )
            targets.append((resolved_tenant, entity_id, str(row[0])))
    else:
        # Full sweep over every current (tenant, entity) pointer.
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT tenant_id, entity_id, current_run_id FROM tenant_runs "
                    "WHERE current_run_id IS NOT NULL ORDER BY tenant_id, entity_id"
                )
                targets = [(str(t), e, str(r)) for (t, e, r) in cur.fetchall()]

    all_conflicts: list[dict] = []
    detected_new = refreshed = 0
    per_entity = []
    for t_id, e_id, r_id in targets:
        result = detect_and_register(t_id, e_id, r_id)
        detected_new += result["detected_new"]
        refreshed += result["refreshed"]
        for c in result["conflicts"]:
            c["tenant_id"] = t_id
            c["entity_id"] = e_id
            c["dcl_ingest_id"] = r_id
        all_conflicts.extend(result["conflicts"])
        if result["conflicts"]:
            per_entity.append({"tenant_id": t_id, "entity_id": e_id,
                               "conflicts": len(result["conflicts"])})

    return {
        "tenant_id": targets[0][0] if len(targets) == 1 else None,
        "entity_id": entity_id,
        "scanned_runs": len(targets),
        "detected_new": detected_new,
        "refreshed": refreshed,
        "per_entity": per_entity,
        "conflicts": all_conflicts,
    }


# ---------------------------------------------------------------------------
# Register reads
# ---------------------------------------------------------------------------

@router.get("/api/dcl/conflicts")
def conflicts_list(
    entity_id: Optional[str] = Query(None),
    tenant_id: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    conflict_type: Optional[str] = Query(None),
    concept: Optional[str] = Query(None),
    conflict_class: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    resolved_tenant, _ = _resolve_identity(tenant_id, entity_id)
    rows, total = _conflicts.list_conflicts(
        resolved_tenant, entity_id=entity_id, status=status,
        conflict_type=conflict_type, concept=concept,
        conflict_class=conflict_class, limit=limit, offset=offset,
    )
    # Computed authority resolution (Stage 5, Gate 1): each value conflict
    # carries its decisive value + disclosure, or status="escalated" with no
    # silent pick. Additive — the existing register shape is untouched.
    for r in rows:
        r["resolved"] = resolve_conflict(r)
    return {"tenant_id": resolved_tenant, "entity_id": entity_id,
            "conflicts": rows, "total_count": total}


@router.get("/api/dcl/conflicts/policy")
def conflicts_policy_get(
    entity_id: Optional[str] = Query(None),
    tenant_id: Optional[str] = Query(None),
):
    resolved_tenant, _ = _resolve_identity(tenant_id, entity_id)
    policy = _conflicts.load_policy(resolved_tenant)
    return {"tenant_id": resolved_tenant, "entity_id": entity_id, **policy}


class PolicyPut(BaseModel):
    entity_id: Optional[str] = None
    tenant_id: Optional[str] = None
    abs_threshold: Optional[float] = None
    rel_threshold: Optional[float] = None


@router.put("/api/dcl/conflicts/policy")
def conflicts_policy_put(body: PolicyPut):
    resolved_tenant, _ = _resolve_identity(body.tenant_id, body.entity_id)
    if body.abs_threshold is None and body.rel_threshold is None:
        raise HTTPException(
            status_code=422,
            detail="At least one of abs_threshold / rel_threshold must be set — "
                   "a policy with both off would disable value-conflict detection silently.",
        )
    for name, v in (("abs_threshold", body.abs_threshold), ("rel_threshold", body.rel_threshold)):
        if v is not None and v < 0:
            raise HTTPException(status_code=422, detail=f"{name} must be >= 0; got {v}")
    _conflicts.put_policy(resolved_tenant, body.abs_threshold, body.rel_threshold)
    return {"tenant_id": resolved_tenant, "entity_id": body.entity_id,
            **_conflicts.load_policy(resolved_tenant)}


@router.get("/api/dcl/conflicts/authority-map")
def authority_map_get(
    entity_id: Optional[str] = Query(None),
    tenant_id: Optional[str] = Query(None),
):
    resolved_tenant, _ = _resolve_identity(tenant_id, entity_id)
    amap = _conflicts.load_authority_map(resolved_tenant)
    return {"tenant_id": resolved_tenant, "entity_id": entity_id,
            "authority_map": [
                {"concept_prefix": p, "ranked_sources": s} for p, s in sorted(amap.items())
            ]}


class AuthorityPut(BaseModel):
    entity_id: Optional[str] = None
    tenant_id: Optional[str] = None
    concept_prefix: str
    ranked_sources: list[str]


@router.put("/api/dcl/conflicts/authority-map")
def authority_map_put(body: AuthorityPut):
    resolved_tenant, _ = _resolve_identity(body.tenant_id, body.entity_id)
    if not body.concept_prefix.strip() or not body.ranked_sources:
        raise HTTPException(
            status_code=422,
            detail="concept_prefix and a non-empty ranked_sources list are required.",
        )
    _conflicts.put_authority_entry(resolved_tenant, body.concept_prefix.strip(),
                                   body.ranked_sources)
    return authority_map_get(entity_id=body.entity_id, tenant_id=resolved_tenant)


@router.get("/api/dcl/conflicts/{conflict_id}")
def conflicts_get(
    conflict_id: str,
    entity_id: Optional[str] = Query(None),
    tenant_id: Optional[str] = Query(None),
):
    try:
        _uuid.UUID(conflict_id)
    except (ValueError, AttributeError):
        raise HTTPException(status_code=400, detail=f"conflict_id must be a UUID; got {conflict_id!r}")
    resolved_tenant, _ = _resolve_identity(tenant_id, entity_id)
    row = _conflicts.get_conflict(resolved_tenant, conflict_id)
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"Conflict {conflict_id} not found for tenant {resolved_tenant} — "
                   f"check the register list for this entity.",
        )
    row["dispositions"] = _conflicts.list_dispositions(resolved_tenant, conflict_id)
    precedent = _conflicts.latest_precedent(resolved_tenant, row["conflict_class"])
    row["precedent"] = precedent
    return row


# ---------------------------------------------------------------------------
# Disposition (HITL — append-only decision trace)
# ---------------------------------------------------------------------------

class DispositionRequest(BaseModel):
    action: str                      # accept_a | accept_b | escalate | manual
    decided_by: str
    rationale: str
    winner_source: Optional[str] = None   # required for manual
    entity_id: Optional[str] = None
    tenant_id: Optional[str] = None


_ACTIONS = {"accept_a", "accept_b", "escalate", "manual"}


@router.post("/api/dcl/conflicts/{conflict_id}/disposition")
def conflicts_disposition(conflict_id: str, body: DispositionRequest):
    """Disposition a conflict. accept_a/accept_b pick the first/second claim
    (claims are ordered by source_system); manual picks winner_source
    explicitly; escalate records the hand-off and leaves all claims live.
    The losing claims' triples are SUPERSEDED (Gate 0 — as-of still shows
    what each source said). Append-only: a dispositioned conflict refuses a
    second disposition."""
    try:
        _uuid.UUID(conflict_id)
    except (ValueError, AttributeError):
        raise HTTPException(status_code=400, detail=f"conflict_id must be a UUID; got {conflict_id!r}")
    if body.action not in _ACTIONS:
        raise HTTPException(
            status_code=422,
            detail=f"action must be one of {sorted(_ACTIONS)}; got {body.action!r}",
        )
    if not body.decided_by.strip() or not body.rationale.strip():
        raise HTTPException(
            status_code=422,
            detail="decided_by and rationale are required — dispositions are the "
                   "decision trace (Gate 2 seed); an unattributed or unexplained "
                   "decision is not a decision.",
        )

    resolved_tenant, _ = _resolve_identity(body.tenant_id, body.entity_id)
    conflict = _conflicts.get_conflict(resolved_tenant, conflict_id)
    if conflict is None:
        raise HTTPException(
            status_code=404,
            detail=f"Conflict {conflict_id} not found for tenant {resolved_tenant}.",
        )

    claims = conflict["claims"]
    sources = [c["source_system"] for c in claims]
    winner: Optional[str] = None
    if body.action == "accept_a":
        winner = sources[0] if sources else None
    elif body.action == "accept_b":
        if len(sources) < 2:
            raise HTTPException(status_code=422, detail="accept_b: conflict has no second claim.")
        winner = sources[1]
    elif body.action == "manual":
        if not body.winner_source:
            raise HTTPException(status_code=422, detail="manual disposition requires winner_source.")
        if body.winner_source not in sources:
            raise HTTPException(
                status_code=422,
                detail=f"winner_source {body.winner_source!r} is not among the claims "
                       f"({sources}) — pick one of the competing sources.",
            )
        winner = body.winner_source

    losers = [s for s in sources if s != winner] if winner else []
    superseded_ids = [
        c["triple_id"] for c in claims
        if winner and c["source_system"] != winner and c.get("triple_id")
    ]
    new_status = "escalated" if body.action == "escalate" else "dispositioned"

    try:
        disposition = _conflicts.record_disposition(
            conflict_id=conflict_id, tenant_id=resolved_tenant,
            entity_id=conflict["entity_id"], conflict_class=conflict["conflict_class"],
            action=body.action, winner_source=winner, loser_sources=losers,
            superseded_triple_ids=superseded_ids, decided_by=body.decided_by.strip(),
            rationale=body.rationale.strip(),
            context={"claims": claims, "materiality": conflict.get("materiality"),
                     "recommended": conflict.get("recommended")},
            new_status=new_status,
        )
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except LookupError as e:
        raise HTTPException(status_code=404, detail=str(e))

    return {
        "tenant_id": resolved_tenant,
        "entity_id": conflict["entity_id"],
        "conflict_id": conflict_id,
        "status": new_status,
        **disposition,
    }
