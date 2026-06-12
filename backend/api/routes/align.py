"""Align Proposal API (ContextOS Gate 3A §4).

POST /api/dcl/align/proposals                     — batch intake from Align
GET  /api/dcl/align/proposals                     — list (tenant-scoped, filtered)
POST /api/dcl/align/proposals/{proposal_id}/decide — approve or reject
GET  /api/dcl/align/contour                       — approved contour (composed)
GET  /api/dcl/align/concept-lookup                — vocabulary alias lookup

Identity: tenant_id is REQUIRED (or entity_id for operator surfaces, resolved
server-side via tenant_runs, same pattern as conflicts.py). Missing → 422 loud (I2).
No run_id in any response (I1).

Duplicate handling: explicit detection — never ON CONFLICT DO NOTHING.
Each duplicate is reported as {'status': 'duplicate', 'duplicate_of': <proposal_id>}.

Canonical provenance: approval applies the canonical artifact in the same
transaction as the status flip. Rejection leaves zero canonical residue.
"""

import uuid as _uuid
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from backend.db.align_store import AlignStore, _VALID_PROPOSAL_TYPES, _natural_key
from backend.db.conflict_store import ConflictStore
from backend.db.triple_store import TripleStore
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

router = APIRouter(tags=["Align Proposals"])

_store = AlignStore()
_conflicts = ConflictStore()
_triples = TripleStore()


def _resolve_align_tenant(
    tenant_id: Optional[str], entity_id: Optional[str], operation: str
) -> Optional[str]:
    """Resolve tenant from entity_id if tenant_id not given (operator surface pattern).
    Returns tenant_id string, or None if neither given (caller will 422 via _require_tenant).
    Raises 404 if entity_id given but not found in tenant_runs."""
    if tenant_id:
        return tenant_id
    if entity_id:
        try:
            return _triples.resolve_tenant_for_entity(entity_id)
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e))
    return None

_VALID_BASES = {"confirmed", "inferred"}
_DECISIONS = {"approve", "reject"}


def _require_tenant(tenant_id: Optional[str], operation: str) -> str:
    """422 on missing, 400 on malformed — loud, naming the operation (I2)."""
    if not tenant_id or not str(tenant_id).strip():
        raise HTTPException(
            status_code=422,
            detail=(
                f"{operation} requires tenant_id — Align proposals are tenant-scoped (I2); "
                f"no silent fallback."
            ),
        )
    try:
        _uuid.UUID(tenant_id)
    except (ValueError, AttributeError):
        raise HTTPException(
            status_code=400,
            detail=f"tenant_id must be a UUID; got {tenant_id!r}",
        )
    return str(tenant_id)


def _validate_proposal_element(elem: dict, idx: int) -> tuple[str, str, dict]:
    """Validate one proposal element. Returns (proposal_type, natural_key, validated_elem).
    Raises HTTPException with 422 on any field violation."""
    ptype = (elem.get("proposal_type") or "").strip()
    if not ptype:
        raise HTTPException(
            status_code=422,
            detail=f"proposals[{idx}]: proposal_type is required.",
        )
    if ptype not in _VALID_PROPOSAL_TYPES:
        raise HTTPException(
            status_code=422,
            detail=(
                f"proposals[{idx}]: unknown proposal_type={ptype!r}. "
                f"Valid: {sorted(_VALID_PROPOSAL_TYPES)}"
            ),
        )
    confidence = elem.get("confidence")
    if confidence is None:
        raise HTTPException(
            status_code=422,
            detail=f"proposals[{idx}] ({ptype}): confidence is required (0.0–1.0).",
        )
    try:
        confidence = float(confidence)
    except (TypeError, ValueError):
        raise HTTPException(
            status_code=422,
            detail=f"proposals[{idx}] ({ptype}): confidence must be a number; got {confidence!r}",
        )
    if not (0.0 <= confidence <= 1.0):
        raise HTTPException(
            status_code=422,
            detail=f"proposals[{idx}] ({ptype}): confidence must be in [0, 1]; got {confidence}",
        )
    prov = elem.get("provenance")
    if not prov or not isinstance(prov, dict):
        raise HTTPException(
            status_code=422,
            detail=f"proposals[{idx}] ({ptype}): provenance is required (object with basis field).",
        )
    basis = (prov.get("basis") or "").strip()
    if basis not in _VALID_BASES:
        raise HTTPException(
            status_code=422,
            detail=(
                f"proposals[{idx}] ({ptype}): provenance.basis must be "
                f"one of {sorted(_VALID_BASES)}; got {basis!r}"
            ),
        )
    payload = elem.get("payload")
    if not payload or not isinstance(payload, dict):
        raise HTTPException(
            status_code=422,
            detail=f"proposals[{idx}] ({ptype}): payload is required (non-empty object).",
        )

    try:
        nkey = _natural_key(ptype, payload)
    except (KeyError, ValueError) as e:
        raise HTTPException(
            status_code=422,
            detail=f"proposals[{idx}] ({ptype}): cannot derive natural key — {e}. "
                   f"Check that payload contains the required key field for this proposal_type.",
        )
    if not nkey:
        raise HTTPException(
            status_code=422,
            detail=(
                f"proposals[{idx}] ({ptype}): natural key field in payload is empty — "
                f"ensure the payload contains the primary identifying field for this proposal_type."
            ),
        )

    return ptype, nkey, {
        "proposal_type": ptype,
        "natural_key": nkey,
        "payload": payload,
        "confidence": confidence,
        "provenance": prov,
        "entity_id": elem.get("entity_id"),
    }


# ---------------------------------------------------------------------------
# Intake
# ---------------------------------------------------------------------------

class IntakeRequest(BaseModel):
    tenant_id: str
    proposals: list[dict]


@router.post("/api/dcl/align/proposals", status_code=201)
def proposals_intake(body: IntakeRequest):
    """Batch intake from Align. 422 on: missing tenant_id, unknown proposal_type,
    element missing confidence or provenance.basis, empty batch.

    Duplicate detection is EXPLICIT: for each proposal, if a pending proposal
    with the same (tenant_id, proposal_type, natural_key) exists, it is reported
    as a duplicate with 'duplicate_of: <proposal_id>' in the response — the
    proposal is not inserted (and not silently dropped with ON CONFLICT DO NOTHING).

    Response declares what was consumed (I3): per-element accepted/duplicate.
    """
    tenant = _require_tenant(body.tenant_id, "POST /api/dcl/align/proposals")
    if not body.proposals:
        raise HTTPException(
            status_code=422,
            detail="POST /api/dcl/align/proposals — proposals list is empty. "
                   "Align must send at least one proposal per call.",
        )

    # Validate every element first — fail fast on the first invalid one.
    validated: list[dict] = []
    for i, elem in enumerate(body.proposals):
        ptype, nkey, v = _validate_proposal_element(elem, i)
        validated.append({"_ptype": ptype, "_nkey": nkey, **v})

    # One query to find all pending duplicates.
    pairs = [(v["_ptype"], v["_nkey"]) for v in validated]
    duplicate_map = _store.check_duplicates(tenant, pairs)

    to_insert: list[dict] = []
    results: list[dict] = []
    for v in validated:
        key = (v["_ptype"], v["_nkey"])
        existing_id = duplicate_map.get(key)
        if existing_id:
            results.append({
                "status": "duplicate",
                "duplicate_of": existing_id,
                "proposal_type": v["proposal_type"],
                "natural_key": v["_nkey"],
            })
        else:
            row = {k: val for k, val in v.items() if not k.startswith("_")}
            row["tenant_id"] = tenant
            to_insert.append(row)

    inserted = _store.insert_proposals(to_insert)
    inserted_by_key = {(r["proposal_type"], r["natural_key"]): r for r in inserted}

    final: list[dict] = []
    insert_idx = 0
    for v in validated:
        key = (v["_ptype"], v["_nkey"])
        if duplicate_map.get(key):
            final.append(next(r for r in results
                              if r.get("natural_key") == v["_nkey"]
                              and r.get("proposal_type") == v["_ptype"]))
        else:
            inserted_row = inserted_by_key[key]
            final.append({
                "status": "accepted",
                "proposal_id": inserted_row["proposal_id"],
                "proposal_type": inserted_row["proposal_type"],
                "natural_key": inserted_row["natural_key"],
                "created_at": inserted_row["created_at"],
            })

    accepted_count = sum(1 for r in final if r["status"] == "accepted")
    duplicate_count = sum(1 for r in final if r["status"] == "duplicate")

    logger.info(
        "[align-intake] tenant=%s accepted=%d duplicates=%d",
        tenant, accepted_count, duplicate_count,
    )
    return {
        "tenant_id": tenant,
        "accepted_count": accepted_count,
        "duplicate_count": duplicate_count,
        "proposals": final,
    }


# ---------------------------------------------------------------------------
# List
# ---------------------------------------------------------------------------

@router.get("/api/dcl/align/proposals")
def proposals_list(
    tenant_id: Optional[str] = Query(None, description="Tenant UUID."),
    entity_id: Optional[str] = Query(None, description="Entity ID — resolves tenant server-side (operator surface)."),
    status: Optional[str] = Query(None, description="pending | approved | rejected"),
    proposal_type: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    resolved = _resolve_align_tenant(tenant_id, entity_id, "GET /api/dcl/align/proposals")
    tenant = _require_tenant(resolved, "GET /api/dcl/align/proposals")
    if status and status not in ("pending", "approved", "rejected"):
        raise HTTPException(
            status_code=422,
            detail=f"status must be one of (pending, approved, rejected); got {status!r}",
        )
    if proposal_type and proposal_type not in _VALID_PROPOSAL_TYPES:
        raise HTTPException(
            status_code=422,
            detail=f"proposal_type must be one of {sorted(_VALID_PROPOSAL_TYPES)}; got {proposal_type!r}",
        )
    rows, total = _store.list_proposals(
        tenant, status=status, proposal_type=proposal_type, limit=limit, offset=offset,
    )
    return {
        "tenant_id": tenant,
        "proposals": rows,
        "total_count": total,
        "limit": limit,
        "offset": offset,
    }


# ---------------------------------------------------------------------------
# Decide
# ---------------------------------------------------------------------------

class DecideRequest(BaseModel):
    tenant_id: Optional[str] = None
    decision: str              # approve | reject
    decided_by: str
    note: Optional[str] = None


@router.post("/api/dcl/align/proposals/{proposal_id}/decide")
def proposals_decide(proposal_id: str, body: DecideRequest):
    """Approve or reject a pending proposal.

    On approve: canonical artifact is written in the same transaction as the
    status flip (authority_map → tenant_authority_map; conflict_candidate →
    conflict_register; vocabulary_alias → tenant_concept_aliases;
    org_hierarchy/management_overlay/priority_query → tenant_contour).

    On reject: the decision is recorded; zero canonical residue is written.

    The decision is written to alignment_decisions, visible via GET /api/dcl/traces
    as trace_type='align_decision'. decided_by is recorded; proposer≠approver
    enforcement is Gate 3C — not built here, not precluded.
    """
    try:
        _uuid.UUID(proposal_id)
    except (ValueError, AttributeError):
        raise HTTPException(
            status_code=400,
            detail=f"proposal_id must be a UUID; got {proposal_id!r}",
        )
    tenant = _require_tenant(body.tenant_id, f"POST /api/dcl/align/proposals/{proposal_id}/decide")
    if body.decision not in _DECISIONS:
        raise HTTPException(
            status_code=422,
            detail=f"decision must be one of {sorted(_DECISIONS)}; got {body.decision!r}",
        )
    if not body.decided_by or not body.decided_by.strip():
        raise HTTPException(
            status_code=422,
            detail="decided_by is required — an unattributed decision is not a decision (§9).",
        )

    try:
        result = _store.decide_proposal(
            proposal_id=proposal_id,
            tenant_id=tenant,
            decision=body.decision,
            decided_by=body.decided_by.strip(),
            decision_note=body.note,
        )
    except LookupError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        logger.error("[align-decide] ERROR proposal=%s: %s", proposal_id, e, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Align proposal decision failed — could not apply canonical artifact: {e}",
        )

    proposal = _store.get_proposal(tenant, proposal_id)
    return {
        "tenant_id": tenant,
        "entity_id": proposal.get("entity_id") if proposal else None,
        **result,
    }


# ---------------------------------------------------------------------------
# Approved contour (composed: hierarchy + management_overlay + projected sor_authority)
# ---------------------------------------------------------------------------

@router.get("/api/dcl/align/contour")
def contour_get(
    tenant_id: Optional[str] = Query(None, description="Tenant UUID."),
    entity_id: Optional[str] = Query(None, description="Entity ID — resolves tenant server-side."),
):
    """Return the approved org contour for a tenant, composed as:
    - hierarchy: from tenant_contour (approved org_hierarchy proposals)
    - management_overlay: from tenant_contour (approved management_overlay proposals)
    - priority_queries: from tenant_contour (approved priority_query proposals)
    - sor_authority: projected FROM tenant_authority_map — never stored in tenant_contour.
      One source of truth for source authority; no split brain.

    If no approved contour exists for this tenant, returns {contour_source: 'none'}.
    """
    resolved = _resolve_align_tenant(tenant_id, entity_id, "GET /api/dcl/align/contour")
    tenant = _require_tenant(resolved, "GET /api/dcl/align/contour")
    contour = _store.get_tenant_contour(tenant)
    if contour is None:
        return {
            "tenant_id": tenant,
            "contour_source": "none",
            "hierarchy": {},
            "management_overlay": [],
            "priority_queries": [],
            "sor_authority": {},
        }

    authority_map = _conflicts.load_authority_map(tenant)
    sor_authority: dict = {}
    for prefix, sources in authority_map.items():
        if sources:
            sor_authority[prefix] = {"system": sources[0], "confidence": 0.9}

    return {
        "tenant_id": tenant,
        "contour_source": "approved",
        "hierarchy": contour["hierarchy"],
        "management_overlay": contour["management_overlay"],
        "priority_queries": contour["priority_queries"],
        "sor_authority": sor_authority,
        "align_proposal_ids": contour["align_proposal_ids"],
        "updated_at": contour["updated_at"],
    }


# ---------------------------------------------------------------------------
# Vocabulary alias lookup (the one real wired reader for tenant_concept_aliases)
# ---------------------------------------------------------------------------

@router.get("/api/dcl/align/concept-lookup")
def concept_lookup(
    tenant_id: Optional[str] = Query(None, description="Tenant UUID — REQUIRED."),
    alias: Optional[str] = Query(None, description="The alias to look up."),
):
    """Look up a tenant-scoped vocabulary alias and return the canonical concept_id.

    Returns {resolved: true, concept_id: ..., alias: ...} when found.
    Returns {resolved: false} when the alias has not been approved for this tenant.

    This is the wired reader for tenant_concept_aliases populated by approved
    vocabulary_alias proposals. 422 loud on missing tenant_id or alias (I2).
    """
    tenant = _require_tenant(tenant_id, "GET /api/dcl/align/concept-lookup")
    if not alias or not alias.strip():
        raise HTTPException(
            status_code=422,
            detail="GET /api/dcl/align/concept-lookup requires alias — "
                   "it is the term to look up; an empty alias resolves nothing.",
        )
    result = _store.resolve_concept_alias(tenant, alias.strip())
    if result is None:
        return {
            "tenant_id": tenant,
            "alias": alias.strip(),
            "resolved": False,
        }
    return {
        "tenant_id": tenant,
        "alias": result["alias"],
        "concept_id": result["concept_id"],
        "align_proposal_id": result["align_proposal_id"],
        "resolved": True,
        "created_at": result["created_at"],
    }
