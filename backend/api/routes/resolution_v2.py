"""
Resolution V2 routes — PG-backed entity resolution from triple overlap.

Mounted at /api/dcl/resolution/v2 in main.py.
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional

from backend.api.routes.v2_helpers import resolve_tenant_and_run
from backend.engine.entity_resolution_v2 import EntityResolutionV2
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/api/dcl/resolution/v2", tags=["Resolution V2"])


def _get_resolver(tenant_id: str, run_id: str) -> EntityResolutionV2:
    """Build a resolver from already-resolved tenant/run IDs."""
    return EntityResolutionV2(tenant_id=tenant_id, run_id=run_id)


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
    tid, rid = resolve_tenant_and_run(request.tenant_id, request.run_id)
    resolver = _get_resolver(tid, rid)
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
    tid, rid = resolve_tenant_and_run(tenant_id, run_id)
    resolver = _get_resolver(tid, rid)
    workspaces = resolver.list_workspaces(domain=domain, status=status)
    return {"workspaces": workspaces, "count": len(workspaces)}


@router.get("/workspaces/{workspace_id}")
def get_workspace(
    workspace_id: str,
    tenant_id: Optional[str] = Query(None),
    run_id: Optional[str] = Query(None),
):
    """Get a single workspace by ID."""
    tid, rid = resolve_tenant_and_run(tenant_id, run_id)
    resolver = _get_resolver(tid, rid)
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
    tid, rid = resolve_tenant_and_run(tenant_id, run_id)
    resolver = _get_resolver(tid, rid)
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
    tid, rid = resolve_tenant_and_run(tenant_id, run_id)
    resolver = _get_resolver(tid, rid)
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
    tid, rid = resolve_tenant_and_run(tenant_id, run_id)
    resolver = _get_resolver(tid, rid)
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
    tid, rid = resolve_tenant_and_run(tenant_id, run_id)
    resolver = _get_resolver(tid, rid)
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
    tid, rid = resolve_tenant_and_run(tenant_id, run_id)
    resolver = _get_resolver(tid, rid)
    return resolver.get_resolution_stats()
