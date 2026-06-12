"""
GET /api/dcl/mcp/audit — read surface for the MCP per-call audit ledger.

mai_mcp_audit is written by the wire-protocol MCP server on every tool
invocation (backend/api/mcp_server_real.py via backend/api/mcp_audit.py).
Until now it was write-only. This endpoint is the read half: the
decision-trace seed (ContextOS_Blueprint_v1 §9 — "the existing MCP audit
table is the seed; it is a product asset, not a compliance artifact").

Tenant-scoped like /api/dcl/triples/browse: tenant_id is REQUIRED and the
query always filters WHERE tenant_id = %s. This is tenant scoping, not
authentication — same posture and same deferred follow-up as the browse
endpoints (dcl_deferred_work.md#26).

Read-only. No demo logic — callers include operators (curl), the grounded
demo sequence (externality proof: a Panel-B agent's MCP calls are visible
here by caller_token_id), and any future trace/precedent consumer.
"""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from backend.core.db import get_connection

router = APIRouter()

_OUTCOMES = ("success", "error", "rate_limited", "unauthorized")


@router.get("/api/dcl/mcp/audit")
def mcp_audit_read(
    tenant_id: str = Query(..., description="Tenant UUID — REQUIRED. Every read is tenant-scoped."),
    caller_token_id: Optional[str] = Query(None, description="Filter to one token's calls (16-hex token id, never the secret)"),
    tool_name: Optional[str] = Query(None, description="Filter to one MCP tool"),
    outcome: Optional[str] = Query(None, description="success | error | rate_limited | unauthorized"),
    since: Optional[str] = Query(None, description="ISO-8601 — rows created at or after this instant"),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """List MCP audit rows for one tenant, newest first."""
    if outcome is not None and outcome not in _OUTCOMES:
        raise HTTPException(
            status_code=422,
            detail=f"outcome must be one of {_OUTCOMES}; got {outcome!r}",
        )
    clauses = ["tenant_id = %s"]
    params: list = [tenant_id]
    if caller_token_id:
        clauses.append("caller_token_id = %s")
        params.append(caller_token_id)
    if tool_name:
        clauses.append("tool_name = %s")
        params.append(tool_name)
    if outcome:
        clauses.append("outcome = %s")
        params.append(outcome)
    if since:
        from datetime import datetime as _dt
        try:
            _dt.fromisoformat(since.replace("Z", "+00:00"))
        except ValueError:
            raise HTTPException(
                status_code=422,
                detail=f"since must be an ISO-8601 timestamp; got {since!r}",
            )
        clauses.append("created_at >= %s")
        params.append(since)

    where = " AND ".join(clauses)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(*) FROM mai_mcp_audit WHERE {where}", params
            )
            total_count = cur.fetchone()[0]
            cur.execute(
                "SELECT tool_name, caller_token_id, arguments_hash, latency_ms, "
                "       outcome, error_summary, transport, created_at, "
                "       entity_id, arguments, result_summary "
                f"FROM mai_mcp_audit WHERE {where} "
                "ORDER BY created_at DESC LIMIT %s OFFSET %s",
                params + [limit, offset],
            )
            rows = cur.fetchall()

    entries = [
        {
            "tool_name": r[0],
            "caller_token_id": r[1],
            "arguments_hash": r[2],
            "latency_ms": r[3],
            "outcome": r[4],
            "error_summary": r[5],
            "transport": r[6],
            "created_at": r[7].isoformat() if r[7] is not None else None,
            # Migration 020 go-forward enrichment — additive fields; historical
            # rows are NULL (knowledge honestly not captured at the time).
            "entity_id": r[8],
            "arguments": r[9],
            "result_summary": r[10],
        }
        for r in rows
    ]
    return {
        "tenant_id": tenant_id,
        "entries": entries,
        "total_count": total_count,
        "limit": limit,
        "offset": offset,
    }
