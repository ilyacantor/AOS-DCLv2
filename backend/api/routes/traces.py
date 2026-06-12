"""Decision Traces + Standing Rules API (Gate 2A, ContextOS §9).

GET  /api/dcl/traces                    — unified trace search (mcp_call +
                                          conflict_disposition + er_confirmation)
GET  /api/dcl/traces/{trace_id}         — one trace
POST /api/dcl/rules/propose             — promote recurring dispositions to a
                                          PROPOSED standing rule (proposal-only)
POST /api/dcl/rules/{rule_id}/decide    — approve/reject exactly once
GET  /api/dcl/rules                     — rules with provenance trace ids

Identity: tenant_id is a REQUIRED query/body field on every endpoint and every
read is tenant-scoped (WHERE tenant_id = %s) — the same posture as
/api/dcl/mcp/audit (tenant scoping, not authentication; same deferred
follow-up, dcl_deferred_work.md#26). Missing tenant_id ⇒ 422, loud (I2).
No run_id field anywhere in these responses (I1).

Gate 2A binds NO engine behavior to standing rules — registry only.
"""

import uuid as _uuid
from datetime import datetime as _dt
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from backend.db.rule_store import (
    NoQualifyingPatternError,
    RuleAlreadyDecidedError,
    RuleStore,
)
from backend.db.trace_store import TraceStore
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

router = APIRouter(tags=["Decision Traces"])

_traces = TraceStore()
_rules = RuleStore()

_TRACE_TYPES = ("mcp_call", "conflict_disposition", "er_confirmation", "proposal_decision")


def _require_tenant(tenant_id: Optional[str], operation: str) -> str:
    """422 on missing, 400 on malformed — loud, naming the operation (I2)."""
    if not tenant_id or not tenant_id.strip():
        raise HTTPException(
            status_code=422,
            detail=f"{operation} requires tenant_id — decision traces and "
                   f"standing rules are tenant-scoped (I2); no fallback.",
        )
    try:
        _uuid.UUID(tenant_id)
    except (ValueError, AttributeError):
        raise HTTPException(
            status_code=400,
            detail=f"tenant_id must be a UUID; got {tenant_id!r}",
        )
    return tenant_id


def _validate_iso(value: Optional[str], name: str) -> Optional[str]:
    if value is None:
        return None
    try:
        _dt.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise HTTPException(
            status_code=422,
            detail=f"{name} must be an ISO-8601 timestamp; got {value!r}",
        )
    return value


def _validate_uuid(value: str, name: str) -> str:
    try:
        _uuid.UUID(value)
    except (ValueError, AttributeError):
        raise HTTPException(
            status_code=400, detail=f"{name} must be a UUID; got {value!r}"
        )
    return value


# ---------------------------------------------------------------------------
# Trace reads
# ---------------------------------------------------------------------------

@router.get("/api/dcl/traces")
def traces_search(
    tenant_id: Optional[str] = Query(None, description="Tenant UUID — REQUIRED. Every read is tenant-scoped."),
    entity_id: Optional[str] = Query(None),
    concept: Optional[str] = Query(None),
    agent: Optional[str] = Query(None, description="mcp_call: caller token id; conflict_disposition: decided_by; er_confirmation: actor"),
    decision_type: Optional[str] = Query(None, description="mcp_call: tool name; conflict_disposition: action; er_confirmation: event"),
    trace_type: Optional[str] = Query(None, description="mcp_call | conflict_disposition | er_confirmation"),
    conflict_class: Optional[str] = Query(None),
    period: Optional[str] = Query(None),
    since: Optional[str] = Query(None, description="ISO-8601 — traces that occurred at or after this instant"),
    until: Optional[str] = Query(None, description="ISO-8601 — traces that occurred at or before this instant"),
    as_of: Optional[str] = Query(None, description="ISO-8601 knowledge-time read: ingested_at <= as_of (traces are never superseded)"),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """Search the unified decision-trace view on every Gate 2A axis."""
    tenant = _require_tenant(tenant_id, "GET /api/dcl/traces")
    if trace_type is not None and trace_type not in _TRACE_TYPES:
        raise HTTPException(
            status_code=422,
            detail=f"trace_type must be one of {_TRACE_TYPES}; got {trace_type!r}",
        )
    for name, val in (("since", since), ("until", until), ("as_of", as_of)):
        _validate_iso(val, name)
    rows, total = _traces.search_traces(
        tenant, entity_id=entity_id, concept=concept, agent=agent,
        decision_type=decision_type, trace_type=trace_type,
        conflict_class=conflict_class, period=period,
        since=since, until=until, as_of=as_of, limit=limit, offset=offset,
    )
    return {
        "tenant_id": tenant,
        "traces": rows,
        "total_count": total,
        "limit": limit,
        "offset": offset,
    }


@router.get("/api/dcl/traces/{trace_id}")
def traces_get(
    trace_id: str,
    tenant_id: Optional[str] = Query(None, description="Tenant UUID — REQUIRED."),
):
    """One trace by id, tenant-scoped. 404 loud when absent for that tenant."""
    tenant = _require_tenant(tenant_id, "GET /api/dcl/traces/{trace_id}")
    _validate_uuid(trace_id, "trace_id")
    row = _traces.get_trace(tenant, trace_id)
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"Trace {trace_id} not found for tenant {tenant} — "
                   f"check GET /api/dcl/traces for this tenant.",
        )
    return row


# ---------------------------------------------------------------------------
# Standing rules (registry only — Gate 2A binds no engine behavior to these)
# ---------------------------------------------------------------------------

class ProposeRuleRequest(BaseModel):
    tenant_id: Optional[str] = None
    conflict_class: str
    proposed_by: str
    entity_id: Optional[str] = None
    min_recurrence: int = 2


@router.post("/api/dcl/rules/propose")
def rules_propose(body: ProposeRuleRequest):
    """Promote the tenant's top recurring same-class disposition pattern into
    a PROPOSED standing rule carrying the justifying disposition trace ids.
    Proposal-only: approval is a separate, attributed decision."""
    tenant = _require_tenant(body.tenant_id, "POST /api/dcl/rules/propose")
    if not body.conflict_class.strip():
        raise HTTPException(status_code=422, detail="conflict_class is required.")
    if not body.proposed_by.strip():
        raise HTTPException(
            status_code=422,
            detail="proposed_by is required — an unattributed proposal is not "
                   "a proposal (decision-trace rule, §9).",
        )
    if body.min_recurrence < 1:
        raise HTTPException(
            status_code=422,
            detail=f"min_recurrence must be >= 1; got {body.min_recurrence}",
        )
    try:
        rule = _rules.propose_rule(
            tenant, body.conflict_class.strip(), body.proposed_by.strip(),
            entity_id=body.entity_id, min_recurrence=body.min_recurrence,
        )
    except NoQualifyingPatternError as e:
        raise HTTPException(status_code=422, detail=str(e))
    return rule


class DecideRuleRequest(BaseModel):
    tenant_id: Optional[str] = None
    decision: str                  # approved | rejected
    decided_by: str
    decision_rationale: str


@router.post("/api/dcl/rules/{rule_id}/decide")
def rules_decide(rule_id: str, body: DecideRuleRequest):
    """Approve or reject a proposed rule — the status flips exactly once;
    a second decision is refused with 409."""
    tenant = _require_tenant(body.tenant_id, "POST /api/dcl/rules/{rule_id}/decide")
    _validate_uuid(rule_id, "rule_id")
    if body.decision not in ("approved", "rejected"):
        raise HTTPException(
            status_code=422,
            detail=f"decision must be 'approved' or 'rejected'; got {body.decision!r}",
        )
    if not body.decided_by.strip() or not body.decision_rationale.strip():
        raise HTTPException(
            status_code=422,
            detail="decided_by and decision_rationale are required — an "
                   "unattributed or unexplained rule decision is not a decision (§9).",
        )
    try:
        rule = _rules.decide_rule(
            rule_id, tenant, body.decision,
            body.decided_by.strip(), body.decision_rationale.strip(),
        )
    except RuleAlreadyDecidedError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except LookupError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return rule


@router.get("/api/dcl/rules")
def rules_list(
    tenant_id: Optional[str] = Query(None, description="Tenant UUID — REQUIRED."),
    status: Optional[str] = Query(None, description="proposed | approved | rejected"),
    conflict_class: Optional[str] = Query(None),
):
    """Standing rules for a tenant, each with its provenance trace ids."""
    tenant = _require_tenant(tenant_id, "GET /api/dcl/rules")
    if status is not None and status not in ("proposed", "approved", "rejected"):
        raise HTTPException(
            status_code=422,
            detail=f"status must be one of ('proposed', 'approved', 'rejected'); got {status!r}",
        )
    rules = _rules.list_rules(tenant, status=status, conflict_class=conflict_class)
    return {"tenant_id": tenant, "rules": rules, "total_count": len(rules)}
