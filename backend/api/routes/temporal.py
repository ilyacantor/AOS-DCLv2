"""
DCL Temporal Versioning, Provenance, and Persona-Definitions routes.

Handles:
  GET    /api/dcl/temporal/history/{metric_id}           — version history
  POST   /api/dcl/temporal/change                         — record a change
  DELETE /api/dcl/temporal/history/{metric_id}/{version}  — always 403
  PUT    /api/dcl/temporal/history/{metric_id}/{version}  — always 403
  GET    /api/dcl/provenance/{metric_id}                  — lineage trace
  GET    /api/dcl/persona-definitions/{metric_id}         — persona defs
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from backend.engine.temporal_versioning import get_temporal_store
from backend.engine.provenance_service import get_provenance
from backend.engine.persona_definitions import get_persona_definition_store
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

router = APIRouter(tags=["Temporal & Provenance"])


# ---------------------------------------------------------------------------
# Temporal Versioning
# ---------------------------------------------------------------------------

@router.get("/api/dcl/temporal/history/{metric_id}")
def get_metric_version_history(metric_id: str):
    """Get version history for a metric definition."""
    store = get_temporal_store()
    history = store.get_history(metric_id)
    if history is None:
        raise HTTPException(
            status_code=404,
            detail={"error": "METRIC_NOT_FOUND", "metric": metric_id}
        )
    return {"metric": metric_id, "version_history": [h.model_dump() for h in history]}


class DefinitionChangeRequest(BaseModel):
    metric_id: str
    changed_by: str
    change_description: str
    previous_value: str
    new_value: str


@router.post("/api/dcl/temporal/change")
def create_definition_change(request: DefinitionChangeRequest):
    """Record a definition change (append-only)."""
    store = get_temporal_store()
    entry = store.add_version(
        metric_id=request.metric_id,
        changed_by=request.changed_by,
        change_description=request.change_description,
        previous_value=request.previous_value,
        new_value=request.new_value,
    )
    return {"status": "ok", "entry": entry.model_dump()}


@router.delete("/api/dcl/temporal/history/{metric_id}/{version}")
def delete_version_entry(metric_id: str, version: int):
    """Attempt to delete a version entry - ALWAYS FAILS (append-only)."""
    raise HTTPException(
        status_code=403,
        detail={
            "error": "APPEND_ONLY",
            "message": "Version history is append-only. Entries cannot be deleted or modified."
        }
    )


@router.put("/api/dcl/temporal/history/{metric_id}/{version}")
def update_version_entry(metric_id: str, version: int):
    """Attempt to update a version entry - ALWAYS FAILS (append-only)."""
    raise HTTPException(
        status_code=403,
        detail={
            "error": "APPEND_ONLY",
            "message": "Version history is append-only. Entries cannot be deleted or modified."
        }
    )


# ---------------------------------------------------------------------------
# Provenance
# ---------------------------------------------------------------------------

@router.get("/api/dcl/provenance/{metric_id}")
def get_metric_provenance(metric_id: str):
    """
    Trace a metric back to its source systems, tables, and fields.

    Returns complete lineage with freshness and quality information.
    """
    trace = get_provenance(metric_id)
    if trace is None:
        raise HTTPException(
            status_code=404,
            detail={
                "error": "METRIC_NOT_FOUND",
                "metric": metric_id,
                "message": f"Metric '{metric_id}' not found in the semantic catalog"
            }
        )
    return trace.model_dump()


# ---------------------------------------------------------------------------
# Persona-Contextual Definitions
# ---------------------------------------------------------------------------

@router.get("/api/dcl/persona-definitions/{metric_id}")
def get_persona_definitions(metric_id: str):
    """Get all persona-specific definitions for a metric."""
    store = get_persona_definition_store()
    defs = store.get_all_definitions(metric_id)
    return {
        "metric": metric_id,
        "definitions": [d.model_dump() for d in defs],
    }
