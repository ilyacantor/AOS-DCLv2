"""Real fabric connect — raw enterprise records in, mapped/resolved/converted
triples out (AAM Blueprint v3.1 §3.6 decision (c)).

POST /api/dcl/ingest-records          — raw records + per-pipe metadata -> triples
GET  /api/dcl/resolver/hitl           — resolver queue (pending + auto-applied)
POST /api/dcl/resolver/hitl/{id}/decide — operator approves/rejects a pending match

AAM used to convert records to triples and POST triples to /api/dcl/ingest-triples.
Decision (c) moves mapping + resolution + conversion into DCL: AAM now transports
raw records to this endpoint, DCL maps (Live Semantic Mapper) + resolves (SE-path
4-tier fuzzy + HITL) + converts on the way in. The converted triples are persisted
through the SAME validated path as the triples endpoint — this handler builds an
IngestRequest from the converted payloads and calls ingest_triples() in-process,
so Farm's path and the idempotency / pointer-swap / provenance contract are reused
verbatim, not reimplemented.
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field
from typing import Any, Literal, Optional

from backend.api.routes.ingest_triples import (
    IngestRequest,
    IngestResponse,
    _validate_uuid,
    ingest_triples,
    promote_canonical_to_manual,
)
from backend.db import resolver_hitl_store as hitl_store
from backend.resolver.record_converter import get_converter
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

router = APIRouter(tags=["Fabric Connect Ingest"])


# ---------------------------------------------------------------------------
# Request / response models — THE PUBLISHED CONTRACT (see docs/INGEST_RECORDS_CONTRACT.md)
# ---------------------------------------------------------------------------

class RecordPipe(BaseModel):
    """One source pipe's worth of raw records plus the schema AAM inferred.

    domain + identity_key are how AAM hands off "this is a customer pipe and
    company_name is the identity field". When both are present DCL resolves that
    field's value to a canonical identity. Absent (e.g. cloud-spend metrics),
    DCL skips resolution and lets the Live Semantic Mapper classify each field.
    """
    pipe_id: str
    source_system: str
    fabric_plane: str
    fabric_product: Optional[str] = None
    domain: Optional[str] = None
    identity_key: Optional[str] = None
    record_key_field: Optional[str] = None
    records: list[dict[str, Any]]


class IngestRecordsRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    tenant_id: str
    dcl_ingest_id: str = Field(..., alias="run_id")
    entity_id: str
    snapshot_name: Optional[str] = None
    source_run_tag: Optional[str] = None
    source_farm_manifest_id: Optional[str] = None
    run_mode: Literal["Dev", "Prod"] = "Dev"
    pipes: list[RecordPipe]


class IngestRecordsResponse(BaseModel):
    dcl_ingest_id: str
    tenant_id: str
    entity_id: str
    records_seen: int
    pipes: int
    triple_count: int
    triples_written: int
    concept_summary: dict
    # method -> count across all resolved records (exact/alias/pattern/fuzzy/
    # hitl_pending/discovery/rejected). Empty when no pipe declared an identity.
    resolution_summary: dict
    hitl_queue_ids: list[str]
    # Loud, non-silent record of every field DCL could not place (unmapped by the
    # Live Mapper, or mapped to a non-persona concept) and every rejected identity.
    warnings: list[dict]
    # Gate 1B (§7): relationships DCL derived from record structure on this
    # ingest (e.g. org-unit membership from workforce records) + any constraint
    # violations — flagged into the conflict register, never silently dropped.
    edges_derived: int = 0
    edges_written: int = 0
    edge_violations: list[dict] = []


# ---------------------------------------------------------------------------
# Ingest endpoint
# ---------------------------------------------------------------------------

@router.post("/api/dcl/ingest-records", status_code=201, response_model=IngestRecordsResponse)
def ingest_records(
    req: IngestRecordsRequest,
    replace: bool = Query(False),
    append: bool = Query(False),
):
    """Map -> resolve -> convert raw records inbound, then persist via the shared
    triple path. Idempotent on dcl_ingest_id (?replace=true re-runs cleanly:
    triples replaced, canonicals deduped by normalized value, HITL rows deduped
    by pair+status)."""
    _validate_uuid(req.tenant_id, "tenant_id")
    _validate_uuid(req.dcl_ingest_id, "dcl_ingest_id")
    if not req.entity_id or not req.entity_id.strip():
        raise HTTPException(
            status_code=422,
            detail={"error": "ENTITY_ID_REQUIRED",
                    "message": "entity_id is required on the ingest-records envelope (I2)."},
        )
    if not req.pipes:
        raise HTTPException(
            status_code=400,
            detail={"error": "VALIDATION_FAILED", "message": "pipes list must not be empty."},
        )

    pipes_as_dicts: list[dict] = []
    for p_idx, pipe in enumerate(req.pipes):
        _validate_uuid(pipe.pipe_id, f"pipes[{p_idx}].pipe_id")
        if not pipe.fabric_plane or not pipe.fabric_plane.strip():
            raise HTTPException(
                status_code=422,
                detail={"error": "PROVENANCE_INCOMPLETE",
                        "message": f"pipes[{p_idx}] (pipe_id={pipe.pipe_id}): fabric_plane is "
                                   f"required — every triple carries fabric_plane."},
            )
        if pipe.identity_key and not pipe.domain:
            raise HTTPException(
                status_code=422,
                detail={"error": "RESOLVER_CONTRACT",
                        "message": f"pipes[{p_idx}]: identity_key set without domain. The "
                                   f"resolver needs a domain (customer/vendor/...) to scope "
                                   f"the canonical registry."},
            )
        pipes_as_dicts.append(pipe.model_dump())

    # --- Map + resolve + convert (DCL's Live Semantic Mapper + SE-path resolver) ---
    try:
        conv = get_converter().convert_pipes(
            tenant_id=req.tenant_id, entity_id=req.entity_id, pipes=pipes_as_dicts,
        )
    except ValueError as e:
        # Resolver/converter contract violations (e.g. a record missing its
        # declared identity_key) — surface loudly as 422, not a 500.
        raise HTTPException(
            status_code=422,
            detail={"error": "RECORD_CONVERSION_FAILED", "message": str(e)},
        )

    records_seen = sum(len(p.get("records") or []) for p in pipes_as_dicts)

    if not conv.payloads:
        raise HTTPException(
            status_code=422,
            detail={
                "error": "NO_TRIPLES_PRODUCED",
                "message": (
                    f"{records_seen} record(s) across {len(req.pipes)} pipe(s) produced no "
                    f"triples — every field was unmapped or routed to a non-persona concept. "
                    f"See warnings."
                ),
                "warnings": conv.warnings,
            },
        )

    # --- Persist via the shared triples path (Farm-identical contract) ---
    envelope = IngestRequest(
        tenant_id=req.tenant_id,
        dcl_ingest_id=req.dcl_ingest_id,
        source_run_tag=req.source_run_tag,
        source_farm_manifest_id=req.source_farm_manifest_id,
        entity_id=req.entity_id,
        source_rows=records_seen,
        snapshot_name=req.snapshot_name,
        run_mode=req.run_mode,
        triples=conv.payloads,
    )
    ingest_resp: IngestResponse = ingest_triples(envelope, replace=replace, append=append)

    # --- Gate 1B (§7): derive entity↔entity edges from record structure ---
    # DCL classifies relationships exactly where it classifies values; AAM
    # stays transport-only. Derived edges ride the SAME ingest identity
    # (dcl_ingest_id) and replace semantics as the facts they came from.
    from backend.db.edge_store import get_edge_store
    from backend.resolver.edge_deriver import derive_edges_from_pipes

    derived = derive_edges_from_pipes(req.entity_id, pipes_as_dicts)
    edges_written = 0
    edge_violations: list[dict] = []
    if derived:
        for e in derived:
            e["dcl_ingest_id"] = req.dcl_ingest_id
            e["source_run_tag"] = req.source_run_tag
        edge_result = get_edge_store().assert_edges(
            req.tenant_id, req.entity_id, derived, replace=replace,
        )
        edges_written = edge_result.written
        edge_violations = edge_result.violations

    logger.info(
        "[ingest-records] tenant_id=%s entity_id=%s pipes=%d records=%d -> %d triples; "
        "edges derived=%d written=%d violations=%d; resolution=%s warnings=%d",
        req.tenant_id, req.entity_id, len(req.pipes), records_seen,
        ingest_resp.triples_written, len(derived), edges_written,
        len(edge_violations), conv.resolution_summary, len(conv.warnings),
    )

    return IngestRecordsResponse(
        dcl_ingest_id=ingest_resp.dcl_ingest_id,
        tenant_id=ingest_resp.tenant_id,
        entity_id=req.entity_id,
        records_seen=records_seen,
        pipes=len(req.pipes),
        triple_count=ingest_resp.triple_count,
        triples_written=ingest_resp.triples_written,
        concept_summary=ingest_resp.concept_summary,
        resolution_summary=conv.resolution_summary,
        hitl_queue_ids=conv.hitl_queue_ids,
        warnings=conv.warnings,
        edges_derived=len(derived),
        edges_written=edges_written,
        edge_violations=edge_violations,
    )


# ---------------------------------------------------------------------------
# Resolver HITL queue — operator surface (makes AAM's resolver retire-able)
# ---------------------------------------------------------------------------

class DecideRequest(BaseModel):
    decision: Literal["approved", "rejected"]
    decided_by: str


@router.get("/api/dcl/resolver/hitl")
def list_resolver_hitl(
    tenant_id: str,
    status: Optional[str] = Query(None, description="pending | auto_applied | approved | rejected"),
    domain: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
):
    """Resolver queue for a tenant. Default returns pending + auto-applied so the
    operator sees both what needs review and what the resolver auto-applied."""
    _validate_uuid(tenant_id, "tenant_id")
    if status:
        rows = hitl_store.list_all(tenant_id=tenant_id, status=status, limit=limit)
    else:
        rows = (hitl_store.get_pending(tenant_id=tenant_id, domain=domain, limit=limit)
                + hitl_store.list_auto_applied(tenant_id=tenant_id, domain=domain, limit=limit))
    return {"tenant_id": tenant_id, "count": len(rows), "items": rows}


@router.post("/api/dcl/resolver/hitl/{hitl_queue_id}/decide")
def decide_resolver_hitl(hitl_queue_id: str, body: DecideRequest):
    """Approve or reject a pending match. Approval promotes the bound triples from
    fuzzy to manual (resolution_confidence 0.99) — the hitl_confirmed path."""
    _validate_uuid(hitl_queue_id, "hitl_queue_id")
    try:
        updated = hitl_store.decide(
            hitl_queue_id=hitl_queue_id, decision=body.decision, decided_by=body.decided_by,
        )
    except LookupError as e:
        raise HTTPException(status_code=404, detail={"error": "HITL_ROW_NOT_FOUND", "message": str(e)})
    except ValueError as e:
        raise HTTPException(status_code=409, detail={"error": "HITL_ALREADY_DECIDED", "message": str(e)})

    promoted = 0
    if body.decision == "approved":
        # hitl_confirmed path: promote the canonical's fuzzy-bound triples to
        # manual @ 0.99 via the store-layer helper (the triple-table write stays
        # in the ingest/store boundary module).
        promoted = promote_canonical_to_manual(
            updated["tenant_id"], updated["proposed_canonical_id"],
        )
    return {"hitl_queue_id": hitl_queue_id, "status": updated["status"],
            "decided_by": updated["decided_by"], "triples_promoted": promoted}
