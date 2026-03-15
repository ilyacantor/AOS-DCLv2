"""
Resolution V2 routes — PG-backed entity resolution from triple overlap.

Mounted at /api/dcl/resolution/v2 in main.py.
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional

from backend.engine.entity_resolution_v2 import EntityResolutionV2
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/api/dcl/resolution/v2", tags=["Resolution V2"])

# Seed constants — these match the 3B harness
_DEFAULT_TENANT_ID = "400aa910-a6b4-5d44-ab9f-e6aecde37721"
_DEFAULT_RUN_ID = "6754a9d7-387a-553f-8c4c-978bfbbfca13"


def _get_resolver(tenant_id: str | None = None, run_id: str | None = None) -> EntityResolutionV2:
    """Build a resolver from query params or defaults."""
    return EntityResolutionV2(
        tenant_id=tenant_id or _DEFAULT_TENANT_ID,
        run_id=run_id or _DEFAULT_RUN_ID,
    )


class CreateWorkspacesRequest(BaseModel):
    tenant_id: Optional[str] = None
    run_id: Optional[str] = None


class ConfirmRequest(BaseModel):
    canonical_id: str
    decided_by: str = "system"


class EscalateRequest(BaseModel):
    reason: str
    decided_by: str = "system"


class DecisionRequest(BaseModel):
    decided_by: str = "system"


@router.post("/create-workspaces")
def create_workspaces(request: CreateWorkspacesRequest = CreateWorkspacesRequest()):
    """Create resolution workspaces from triple overlap."""
    resolver = _get_resolver(request.tenant_id, request.run_id)
    result = resolver.create_workspaces_from_overlap()
    return {"status": "ok", **result}


@router.get("/workspaces")
def list_workspaces(
    domain: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    tenant_id: Optional[str] = Query(None),
    run_id: Optional[str] = Query(None),
):
    """List resolution workspaces with optional filters."""
    resolver = _get_resolver(tenant_id, run_id)
    workspaces = resolver.list_workspaces(domain=domain, status=status)
    return {"workspaces": workspaces, "count": len(workspaces)}


@router.get("/workspaces/{workspace_id}")
def get_workspace(
    workspace_id: str,
    tenant_id: Optional[str] = Query(None),
    run_id: Optional[str] = Query(None),
):
    """Get a single workspace by ID."""
    resolver = _get_resolver(tenant_id, run_id)
    try:
        ws = resolver.get_workspace(workspace_id)
        return ws
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/workspaces/{workspace_id}/confirm")
def confirm_match(
    workspace_id: str,
    request: ConfirmRequest,
    tenant_id: Optional[str] = Query(None),
    run_id: Optional[str] = Query(None),
):
    """Confirm that overlapping concepts are the same real-world entity."""
    resolver = _get_resolver(tenant_id, run_id)
    try:
        ws = resolver.confirm_match(
            workspace_id, request.canonical_id, request.decided_by
        )
        return ws
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/workspaces/{workspace_id}/reject")
def reject_match(
    workspace_id: str,
    request: DecisionRequest = DecisionRequest(),
    tenant_id: Optional[str] = Query(None),
    run_id: Optional[str] = Query(None),
):
    """Reject the match — concepts are different entities."""
    resolver = _get_resolver(tenant_id, run_id)
    try:
        ws = resolver.reject_match(workspace_id, request.decided_by)
        return ws
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/workspaces/{workspace_id}/escalate")
def escalate(
    workspace_id: str,
    request: EscalateRequest,
    tenant_id: Optional[str] = Query(None),
    run_id: Optional[str] = Query(None),
):
    """Escalate for human review."""
    resolver = _get_resolver(tenant_id, run_id)
    try:
        ws = resolver.escalate(
            workspace_id, request.reason, request.decided_by
        )
        return ws
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/workspaces/{workspace_id}/undo")
def undo_decision(
    workspace_id: str,
    tenant_id: Optional[str] = Query(None),
    run_id: Optional[str] = Query(None),
):
    """Undo a decision — reset to pending."""
    resolver = _get_resolver(tenant_id, run_id)
    try:
        ws = resolver.undo_decision(workspace_id)
        return ws
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/stats")
def get_stats(
    tenant_id: Optional[str] = Query(None),
    run_id: Optional[str] = Query(None),
):
    """Get resolution statistics."""
    resolver = _get_resolver(tenant_id, run_id)
    return resolver.get_resolution_stats()
