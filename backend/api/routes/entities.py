"""
DCL Entity Resolution + Conflict Detection routes.

Handles:
  POST /api/dcl/entities/resolve              — run entity resolution
  POST /api/dcl/entities/confirm/{id}         — confirm/reject match
  POST /api/dcl/entities/undo/{id}            — undo a merge
  GET  /api/dcl/entities/{search}             — browse entities
  GET  /api/dcl/entities/canonical/{id}       — get canonical entity
  POST /api/dcl/conflicts/detect              — run conflict detection
  GET  /api/dcl/conflicts                     — list active conflicts
  POST /api/dcl/conflicts/{id}/resolve        — resolve a conflict
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from backend.engine.entity_resolution import get_entity_store
from backend.engine.conflict_detection import get_conflict_store
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

router = APIRouter(tags=["Entities & Conflicts"])


# ---------------------------------------------------------------------------
# Entity Resolution
# ---------------------------------------------------------------------------

@router.post("/api/dcl/entities/resolve")
def run_entity_resolution(entity_type: str = "company"):
    """
    Run entity resolution across all source records.

    v1 scope: Companies/Customers ONLY.
    """
    store = get_entity_store()

    if not store.is_entity_type_allowed(entity_type):
        raise HTTPException(
            status_code=400,
            detail={
                "error": "ENTITY_TYPE_NOT_SUPPORTED",
                "message": f"Entity type '{entity_type}' is not supported in v1. Only company/customer entities are supported.",
                "supported_types": ["company", "customer"],
            }
        )

    candidates = store.run_entity_resolution()
    return {
        "status": "ok",
        "candidates": [c.model_dump() for c in candidates],
        "canonical_entities": [e.model_dump() for e in store.get_all_canonical_entities()],
    }


class ConfirmMatchRequest(BaseModel):
    approved: bool
    resolved_by: str = "admin"


@router.post("/api/dcl/entities/confirm/{candidate_id}")
def confirm_entity_match(candidate_id: str, request: ConfirmMatchRequest):
    """Confirm or reject a match candidate."""
    store = get_entity_store()
    candidate = store.confirm_match(candidate_id, request.approved, request.resolved_by)
    if not candidate:
        raise HTTPException(
            status_code=404,
            detail={"error": "CANDIDATE_NOT_FOUND", "candidate_id": candidate_id}
        )
    return {
        "status": "ok",
        "candidate": candidate.model_dump(),
    }


@router.post("/api/dcl/entities/undo/{dcl_global_id}")
def undo_entity_merge(dcl_global_id: str, performed_by: str = "admin"):
    """Undo a confirmed merge - split entity back into separate records."""
    store = get_entity_store()
    success = store.undo_merge(dcl_global_id, performed_by)
    if not success:
        raise HTTPException(
            status_code=404,
            detail={"error": "ENTITY_NOT_FOUND", "dcl_global_id": dcl_global_id}
        )
    return {"status": "ok", "message": "Merge undone successfully", "dcl_global_id": dcl_global_id}


@router.get("/api/dcl/entities/{search_term}")
def browse_entities(search_term: str):
    """Browse entities matching a search term across all systems."""
    store = get_entity_store()
    results = store.browse_entities(search_term)
    return {
        "search_term": search_term,
        "results": results,
        "count": len(results),
    }


@router.get("/api/dcl/entities/canonical/{dcl_global_id}")
def get_canonical_entity(dcl_global_id: str):
    """Get a canonical entity by its global ID."""
    store = get_entity_store()
    entity = store.get_canonical_entity(dcl_global_id)
    if not entity:
        raise HTTPException(
            status_code=404,
            detail={"error": "ENTITY_NOT_FOUND", "dcl_global_id": dcl_global_id}
        )
    return entity.model_dump()


# ---------------------------------------------------------------------------
# Conflict Detection
# ---------------------------------------------------------------------------

@router.post("/api/dcl/conflicts/detect")
def run_conflict_detection():
    """Run conflict detection across all resolved entities."""
    store = get_conflict_store()
    conflicts = store.detect_conflicts()
    return {
        "status": "ok",
        "conflicts": [c.model_dump() for c in conflicts],
        "count": len(conflicts),
    }


@router.get("/api/dcl/conflicts")
def get_conflicts():
    """Get all active conflicts sorted by severity (conflict dashboard)."""
    store = get_conflict_store()
    conflicts = store.get_active_conflicts()
    return {
        "conflicts": [c.model_dump() for c in conflicts],
        "count": len(conflicts),
    }


class ConflictResolutionRequest(BaseModel):
    decision: str
    rationale: str
    resolved_by: str = "admin"


@router.post("/api/dcl/conflicts/{conflict_id}/resolve")
def resolve_conflict(conflict_id: str, request: ConflictResolutionRequest):
    """Resolve a conflict with a decision and rationale."""
    store = get_conflict_store()
    conflict = store.resolve_conflict(
        conflict_id=conflict_id,
        decision=request.decision,
        rationale=request.rationale,
        resolved_by=request.resolved_by,
    )
    if not conflict:
        raise HTTPException(
            status_code=404,
            detail={"error": "CONFLICT_NOT_FOUND", "conflict_id": conflict_id}
        )
    return {
        "status": "ok",
        "conflict": conflict.model_dump(),
    }
