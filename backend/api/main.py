"""
DCL Engine API - Metadata-only semantic mapping layer + DCL expansion capabilities.

NLQ and BLL functionality has been moved to AOS-NLQ repository.
DCL focuses on:
- Schema structures and semantic mappings
- Ontology management
- Graph visualization (Sankey diagrams)
- Topology API
- Temporal Versioning
- Provenance Trace
- Persona-Contextual Definitions
- Entity Resolution
- Conflict Detection
- MCP Server
"""
import os
import uuid
from pathlib import Path
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from typing import List, Literal, Optional, Dict, Any
from backend.domain import Persona, GraphSnapshot, RunMetrics
from backend.engine import DCLEngine
from backend.engine.schema_loader import SchemaLoader
from backend.semantic_mapper import SemanticMapper
from backend.utils.log_utils import get_logger
from backend.api.semantic_export import (
    get_semantic_export,
    resolve_metric,
    resolve_entity,
    SemanticExport,
    MetricDefinition,
    EntityDefinition,
    ModeInfo,
)
from backend.api.query import (
    QueryRequest,
    QueryResponse,
    QueryError,
    handle_query,
)
from backend.engine.temporal_versioning import get_temporal_store
from backend.engine.provenance_service import get_provenance, ProvenanceTrace
from backend.engine.persona_definitions import get_persona_definition_store
from backend.engine.entity_resolution import get_entity_store
from backend.engine.conflict_detection import get_conflict_store
from backend.engine.reconciliation import reconcile
from backend.engine.sor_reconciliation import reconcile_sor
from backend.api.ingest import (
    IngestRequest,
    IngestResponse,
    get_ingest_store,
    compute_schema_hash,
)
from backend.api.mcp_server import (
    MCPToolCall,
    MCPToolResult,
    MCPServerInfo,
    get_server_info,
    handle_tool_call,
    validate_api_key,
)

logger = get_logger(__name__)

from backend.core.security_constraints import (
    validate_no_disk_payload_writes,
    assert_metadata_only_mode,
)
from backend.core.mode_state import get_current_mode, set_current_mode
from backend.core.constants import CORS_ORIGINS

app = FastAPI(title="DCL Engine API")


@app.on_event("startup")
async def enforce_security_constraints():
    """Enforce Zero-Trust metadata-only constraints at startup."""
    logger.info("=== DCL Zero-Trust Security Check ===")
    
    try:
        assert_metadata_only_mode()
        logger.info("[SECURITY] Metadata-only mode: ENABLED")
    except Exception as e:
        logger.warning(f"[SECURITY] Metadata-only assertion failed: {e}")
    
    violations = validate_no_disk_payload_writes()
    if violations:
        logger.warning(f"[SECURITY] Found {len(violations)} potential payload write paths:")
        for v in violations[:5]:
            logger.warning(f"  - {v}")
        logger.warning("[SECURITY] Review ARCH-GLOBAL-PIVOT.md for migration guidance")
    else:
        logger.info("[SECURITY] No payload write violations detected")
    
    logger.info("=== DCL Engine Ready (Metadata-Only Mode) ===")

if CORS_ORIGINS == ["*"]:
    logger.warning("[SECURITY] CORS allows all origins. Set CORS_ORIGINS env var for production.")

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

engine = DCLEngine()
app.state.loaded_sources = []
app.state.loaded_source_ids = []


def _invalidate_aam_caches():
    """Clear all caches that could return stale AAM data on a new run."""
    # 1. Mapping persistence cache (60s TTL normally)
    try:
        from backend.semantic_mapper.persist_mappings import MappingPersistence
        MappingPersistence.clear_all_caches()
        logger.info("[AAM] Cleared mapping persistence caches")
    except Exception as e:
        logger.warning(f"[AAM] Failed to clear mapping caches: {e}")

    # 2. AAM client singleton — recreate so httpx picks up fresh AAM state
    try:
        import backend.aam.client as aam_mod
        if aam_mod._aam_client is not None:
            aam_mod._aam_client.close()
            aam_mod._aam_client = None
            logger.info("[AAM] Reset AAM client singleton")
    except Exception as e:
        logger.warning(f"[AAM] Failed to reset AAM client: {e}")

    # 3. Schema loader caches
    SchemaLoader._demo_cache = None
    SchemaLoader._stream_cache = None
    SchemaLoader._cache_time = 0
    SchemaLoader._aam_cache = None
    SchemaLoader._aam_cache_time = 0
    logger.info("[AAM] All stale caches invalidated for fresh AAM run")


class RunRequest(BaseModel):
    mode: Literal["Demo", "Farm", "AAM"] = "Demo"
    run_mode: Literal["Dev", "Prod"] = "Dev"
    personas: Optional[List[Persona]] = None
    source_limit: Optional[int] = 1000
    aod_run_id: Optional[str] = Field(None, description="AOD run ID for AAM mode")
    force_refresh: bool = Field(False, description="Force clear all caches and fetch fresh data from AAM")


class RunResponse(BaseModel):
    graph: GraphSnapshot
    run_metrics: RunMetrics
    run_id: str


@app.get("/api/health")
def health():
    return {
        "status": "DCL Engine API is running",
        "version": "2.0.0",
        "mode": "metadata-only",
        "note": "NLQ/BLL moved to AOS-NLQ"
    }


@app.post("/api/dcl/run", response_model=RunResponse)
def run_dcl(request: RunRequest):
    run_id = str(uuid.uuid4())

    set_current_mode(
        data_mode=request.mode,
        run_mode=request.run_mode,
        run_id=run_id
    )

    if request.mode == "AAM" and request.force_refresh:
        _invalidate_aam_caches()

    personas = request.personas or [Persona.CFO, Persona.CRO, Persona.COO, Persona.CTO]

    try:
        snapshot, metrics = engine.build_graph_snapshot(
            mode=request.mode,
            run_mode=request.run_mode,
            personas=personas,
            run_id=run_id,
            source_limit=request.source_limit or 1000,
            aod_run_id=request.aod_run_id
        )

        # Derive loaded sources from source data (in meta), NOT from graph nodes.
        # Graph nodes are display-mode dependent (kind="source" vs kind="fabric")
        # but meta.source_names is populated from the raw sources list before
        # any display-mode decisions.
        app.state.loaded_sources = snapshot.meta.get("source_names", [])
        app.state.loaded_source_ids = snapshot.meta.get("source_canonical_ids", [])

        return RunResponse(
            graph=snapshot,
            run_metrics=metrics,
            run_id=run_id
        )
    except Exception as e:
        logger.error(f"DCL run failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/dcl/narration/{run_id}")
def get_narration(run_id: str):
    messages = engine.narration.get_messages(run_id)
    return {"run_id": run_id, "messages": messages}


@app.post("/api/ingest/provision")
@app.get("/api/ingest/provision")
def ingest_provision_gone():
    """Ingest pipeline moved to AAM."""
    raise HTTPException(status_code=410, detail={"error": "MOVED_TO_AAM"})


@app.get("/api/ingest/config")
def ingest_config_gone():
    """Ingest pipeline moved to AAM."""
    raise HTTPException(status_code=410, detail={"error": "MOVED_TO_AAM"})


@app.get("/api/dcl/monitor/{run_id}")
def get_monitor(run_id: str):
    return {
        "run_id": run_id,
        "monitor_data": {
            "message": "Monitor data endpoint ready",
            "sources": [],
            "ontology": [],
            "conflicts": []
        }
    }


@app.get("/api/ingest/telemetry")
def ingest_telemetry_gone():
    """Ingest pipeline moved to AAM."""
    raise HTTPException(status_code=410, detail={"error": "MOVED_TO_AAM"})


# =============================================================================
# DCL Ingestion — Runner push endpoint
# =============================================================================


@app.get("/api/dcl/ingest")
def dcl_ingest_ping():
    """Connectivity check — Farm can GET this to verify the ingest endpoint is reachable."""
    store = get_ingest_store()
    stats = store.get_stats()
    return {
        "status": "ready",
        "message": "POST payloads to this URL. GET is for connectivity testing only.",
        "ingest_stats": stats,
    }


@app.post("/api/dcl/ingest")
async def dcl_ingest(
    request: Request,
    x_run_id: Optional[str] = Header(None),
    x_pipe_id: Optional[str] = Header(None),
    x_schema_hash: Optional[str] = Header(None),
    x_api_key: Optional[str] = Header(None),
):
    """
    Accept a data push from an AAM Runner or Farm.
    Accepts any JSON body and adapts to match IngestRequest schema.
    """
    # Log raw request info for debugging connectivity issues
    client_host = request.client.host if request.client else "unknown"
    content_type = request.headers.get("content-type", "missing")
    content_length = request.headers.get("content-length", "missing")
    logger.info(
        f"[Ingest] Incoming POST from {client_host} | "
        f"content-type={content_type} content-length={content_length} | "
        f"x-run-id={x_run_id} x-pipe-id={x_pipe_id}"
    )

    try:
        raw_body = await request.json()
    except Exception as e:
        raw_bytes = await request.body()
        logger.error(
            f"[Ingest] JSON parse failed from {client_host}: {e} | "
            f"raw body ({len(raw_bytes)} bytes): {raw_bytes[:500]!r}"
        )
        raise HTTPException(status_code=400, detail=f"Invalid JSON body: {e}")

    logger.info(f"[Ingest] Received keys: {list(raw_body.keys()) if isinstance(raw_body, dict) else type(raw_body).__name__}")

    if isinstance(raw_body, dict):
        body = dict(raw_body)
        if "source_system" not in body and "source" in body:
            body["source_system"] = body.pop("source")
        if "tenant_id" not in body:
            body.setdefault("tenant_id", body.get("tenantId", body.get("tenant", "default")))
        if "snapshot_name" not in body:
            body.setdefault("snapshot_name", body.get("snapshotName", body.get("snapshot", "default")))
        if "run_timestamp" not in body:
            body.setdefault("run_timestamp", body.get("runTimestamp", body.get("timestamp", datetime.now(timezone.utc).isoformat())))
        if "schema_version" not in body:
            body.setdefault("schema_version", body.get("schemaVersion", body.get("schema_ver", "1.0")))
        if "rows" not in body:
            if "data" in body:
                body["rows"] = body.pop("data")
            elif "records" in body:
                body["rows"] = body.pop("records")
            elif "payload" in body:
                body["rows"] = body.pop("payload")
            else:
                body["rows"] = []
        if "row_count" not in body:
            body["row_count"] = body.get("rowCount", len(body.get("rows", [])))
        if "runner_id" not in body and "runnerId" in body:
            body["runner_id"] = body.pop("runnerId")
    else:
        body = raw_body

    try:
        ingest_req = IngestRequest(**body)
    except Exception as e:
        logger.error(f"[Ingest] Validation failed: {e} | keys={list(body.keys()) if isinstance(body, dict) else 'N/A'}")
        raise HTTPException(status_code=422, detail=str(e))

    expected_key = os.environ.get("DCL_INGEST_KEY")
    if expected_key and x_api_key != expected_key:
        raise HTTPException(status_code=401, detail="Invalid or missing x-api-key")

    run_id = x_run_id or str(uuid.uuid4())
    pipe_id = x_pipe_id or f"pipe_{ingest_req.source_system}"

    if x_schema_hash:
        schema_hash = x_schema_hash
    else:
        schema_hash = compute_schema_hash(ingest_req.rows)

    actual_rows = len(ingest_req.rows)
    if actual_rows != ingest_req.row_count:
        logger.warning(
            f"[Ingest] Row count mismatch: declared={ingest_req.row_count} "
            f"actual={actual_rows} pipe={pipe_id} run={run_id}"
        )

    store = get_ingest_store()
    try:
        receipt = store.ingest(
            run_id=run_id,
            pipe_id=pipe_id,
            schema_hash=schema_hash,
            request=ingest_req,
        )
    except Exception as e:
        logger.error(f"[Ingest] Failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

    logger.info(
        f"[Ingest] Accepted {actual_rows} rows from {ingest_req.source_system} "
        f"pipe={pipe_id} run={run_id} drift={receipt.schema_drift}"
    )

    return IngestResponse(
        status="ingested",
        run_id=run_id,
        pipe_id=pipe_id,
        rows_accepted=actual_rows,
        schema_drift=receipt.schema_drift,
        drift_fields=receipt.drift_fields,
    )


@app.get("/api/dcl/ingest/runs")
def list_ingest_runs():
    """List all ingestion run receipts (metadata only)."""
    store = get_ingest_store()
    receipts = store.get_all_receipts()
    return {
        "runs": [
            {
                "run_id": r.run_id,
                "pipe_id": r.pipe_id,
                "source_system": r.source_system,
                "canonical_source_id": r.canonical_source_id,
                "tenant_id": r.tenant_id,
                "snapshot_name": r.snapshot_name,
                "run_timestamp": r.run_timestamp,
                "received_at": r.received_at,
                "schema_version": r.schema_version,
                "row_count": r.row_count,
                "schema_drift": r.schema_drift,
                "drift_fields": r.drift_fields,
                "runner_id": r.runner_id,
            }
            for r in receipts
        ],
        "stats": store.get_stats(),
    }


@app.get("/api/dcl/ingest/batches")
def list_ingest_batches():
    """List ingestion batches grouped by snapshot_name."""
    store = get_ingest_store()
    return {"batches": store.get_batches()}


@app.get("/api/dcl/ingest/runs/{run_id}")
def get_ingest_run(run_id: str):
    """Get a single run receipt + buffered rows."""
    store = get_ingest_store()
    receipt = store.get_receipt(run_id)
    if not receipt:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")
    rows = store.get_rows(run_id)
    return {
        "receipt": {
            "run_id": receipt.run_id,
            "pipe_id": receipt.pipe_id,
            "source_system": receipt.source_system,
            "canonical_source_id": receipt.canonical_source_id,
            "tenant_id": receipt.tenant_id,
            "snapshot_name": receipt.snapshot_name,
            "run_timestamp": receipt.run_timestamp,
            "received_at": receipt.received_at,
            "schema_version": receipt.schema_version,
            "schema_hash": receipt.schema_hash,
            "row_count": receipt.row_count,
            "schema_drift": receipt.schema_drift,
            "drift_fields": receipt.drift_fields,
        },
        "row_count": len(rows),
        "rows": rows,
    }


@app.get("/api/dcl/ingest/drift")
def list_schema_drift():
    """List all schema drift events."""
    store = get_ingest_store()
    events = store.get_drift_events()
    return {
        "drift_events": [
            {
                "pipe_id": e.pipe_id,
                "run_id": e.run_id,
                "previous_hash": e.previous_hash,
                "incoming_hash": e.incoming_hash,
                "added_fields": e.added_fields,
                "removed_fields": e.removed_fields,
                "detected_at": e.detected_at,
            }
            for e in events
        ],
        "total": len(events),
    }


@app.get("/api/dcl/ingest/stats")
def get_ingest_stats():
    """Quick summary of what's in the ingest store."""
    store = get_ingest_store()
    return store.get_stats()


class MappingRequest(BaseModel):
    mode: Literal["Demo", "Farm"] = "Demo"
    mapping_mode: Literal["heuristic", "full"] = "heuristic"
    clear_existing: bool = False


class MappingResponse(BaseModel):
    status: str
    mappings_created: int
    sources_processed: int
    stats: dict


@app.post("/api/dcl/batch-mapping", response_model=MappingResponse)
def run_batch_mapping(request: MappingRequest):
    
    try:
        if request.mode == "Demo":
            sources = SchemaLoader.load_demo_schemas()
        else:
            sources = SchemaLoader.load_farm_schemas(engine.narration, str(uuid.uuid4()))
        
        semantic_mapper = SemanticMapper()
        mappings, stats = semantic_mapper.run_mapping(
            sources=sources,
            mode=request.mapping_mode,
            clear_existing=request.clear_existing
        )
        
        return MappingResponse(
            status="success",
            mappings_created=stats['mappings_created'],
            sources_processed=stats['sources_processed'],
            stats=stats
        )
    except Exception as e:
        logger.error(f"Batch mapping failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


from backend.core.topology_api import topology_api
from backend.farm.routes import router as farm_router
from backend.dcl.routes import router as dcl_router

app.include_router(farm_router)
app.include_router(dcl_router)


class TopologyResponse(BaseModel):
    nodes: List[Dict[str, Any]]
    links: List[Dict[str, Any]]
    metadata: Dict[str, Any]


@app.get("/api/topology", response_model=TopologyResponse)
async def get_topology(include_health: bool = True):
    """
    Get the unified topology graph.
    
    Merges DCL semantic graph with AAM health data.
    This is the TopologyAPI service that absorbs visualization from AAM.
    """
    try:
        return await topology_api.get_topology(include_health=include_health)
    except Exception as e:
        logger.error(f"Failed to get topology: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/topology/health")
async def get_connection_health(connector_id: Optional[str] = None):
    """
    Get connection health data from the mesh.
    
    This ingests data from AAM's GetConnectionHealth endpoint.
    """
    try:
        return await topology_api.get_connection_health(connector_id)
    except Exception as e:
        logger.error(f"Failed to get connection health: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/topology/stats")
def get_topology_stats():
    """Get topology service statistics."""
    return topology_api.get_stats()


@app.get("/api/dcl/semantic-export", response_model=SemanticExport)
def semantic_export(tenant_id: str = "default"):
    """
    Export full semantic catalog for NLQ consumption.
    
    Returns all published metrics, entities (dimensions), persona mappings,
    and the metric-entity compatibility matrix.
    
    NLQ uses this to:
    - Resolve aliases ("AR" → "ar")
    - Know valid dimensions per metric
    - Fail fast with helpful messages for unknown metrics
    """
    return get_semantic_export(tenant_id)


@app.get("/api/dcl/semantic-export/resolve/metric")
def resolve_metric_alias(q: str):
    """
    Resolve a metric alias to its canonical definition.
    
    Args:
        q: Query string (e.g., "AR", "accounts receivable")
    
    Returns:
        Canonical metric definition or 404 if not found
    """
    metric = resolve_metric(q)
    if not metric:
        raise HTTPException(
            status_code=404,
            detail={
                "error": "METRIC_NOT_FOUND",
                "query": q,
                "suggestion": "Use GET /api/dcl/semantic-export to see all available metrics"
            }
        )
    return metric


@app.get("/api/dcl/semantic-export/resolve/entity")
def resolve_entity_alias(q: str):
    """
    Resolve an entity/dimension alias to its canonical definition.
    
    Args:
        q: Query string (e.g., "account", "customer")
    
    Returns:
        Canonical entity definition or 404 if not found
    """
    entity = resolve_entity(q)
    if not entity:
        raise HTTPException(
            status_code=404,
            detail={
                "error": "ENTITY_NOT_FOUND",
                "query": q,
                "suggestion": "Use GET /api/dcl/semantic-export to see all available entities"
            }
        )
    return entity


@app.post("/api/dcl/query")
def execute_dcl_query(request: QueryRequest):
    """
    Execute a data query against DCL's fact base.
    
    This endpoint validates the query against the semantic catalog and returns
    data from the appropriate source (demo data or farm connections).
    
    Request:
    {
        "metric": "arr",
        "dimensions": ["segment"],
        "filters": {"region": "AMER"},
        "time_range": {"start": "2025-Q1", "end": "2025-Q4"},
        "grain": "quarter"
    }
    
    Returns:
    - 200: Query results with data points and metadata
    - 400: Invalid dimension or grain for the requested metric
    - 404: Metric not found
    """
    result = handle_query(request)
    
    if isinstance(result, QueryError):
        if result.code == "METRIC_NOT_FOUND":
            raise HTTPException(status_code=404, detail=result.model_dump())
        else:
            raise HTTPException(status_code=400, detail=result.model_dump())
    
    return result


# =============================================================================
# Temporal Versioning Endpoints
# =============================================================================


@app.get("/api/dcl/temporal/history/{metric_id}")
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


@app.post("/api/dcl/temporal/change")
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


@app.delete("/api/dcl/temporal/history/{metric_id}/{version}")
def delete_version_entry(metric_id: str, version: int):
    """Attempt to delete a version entry - ALWAYS FAILS (append-only)."""
    raise HTTPException(
        status_code=403,
        detail={
            "error": "APPEND_ONLY",
            "message": "Version history is append-only. Entries cannot be deleted or modified."
        }
    )


@app.put("/api/dcl/temporal/history/{metric_id}/{version}")
def update_version_entry(metric_id: str, version: int):
    """Attempt to update a version entry - ALWAYS FAILS (append-only)."""
    raise HTTPException(
        status_code=403,
        detail={
            "error": "APPEND_ONLY",
            "message": "Version history is append-only. Entries cannot be deleted or modified."
        }
    )


# =============================================================================
# Provenance Trace Endpoints
# =============================================================================


@app.get("/api/dcl/provenance/{metric_id}")
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


# =============================================================================
# Persona-Contextual Definitions Endpoints
# =============================================================================


@app.get("/api/dcl/persona-definitions/{metric_id}")
def get_persona_definitions(metric_id: str):
    """Get all persona-specific definitions for a metric."""
    store = get_persona_definition_store()
    defs = store.get_all_definitions(metric_id)
    return {
        "metric": metric_id,
        "definitions": [d.model_dump() for d in defs],
    }


# =============================================================================
# Entity Resolution Endpoints
# =============================================================================


@app.post("/api/dcl/entities/resolve")
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


@app.post("/api/dcl/entities/confirm/{candidate_id}")
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


@app.post("/api/dcl/entities/undo/{dcl_global_id}")
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


@app.get("/api/dcl/entities/{search_term}")
def browse_entities(search_term: str):
    """Browse entities matching a search term across all systems."""
    store = get_entity_store()
    results = store.browse_entities(search_term)
    return {
        "search_term": search_term,
        "results": results,
        "count": len(results),
    }


@app.get("/api/dcl/entities/canonical/{dcl_global_id}")
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


# =============================================================================
# Conflict Detection Endpoints
# =============================================================================


@app.post("/api/dcl/conflicts/detect")
def run_conflict_detection():
    """Run conflict detection across all resolved entities."""
    store = get_conflict_store()
    conflicts = store.detect_conflicts()
    return {
        "status": "ok",
        "conflicts": [c.model_dump() for c in conflicts],
        "count": len(conflicts),
    }


@app.get("/api/dcl/conflicts")
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


@app.post("/api/dcl/conflicts/{conflict_id}/resolve")
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


# =============================================================================
# MCP Server Endpoints
# =============================================================================


@app.get("/api/mcp/info")
def mcp_server_info():
    """Get MCP server information and available tools."""
    return get_server_info().model_dump()


@app.post("/api/mcp/tools/call")
def mcp_tool_call(tool_call: MCPToolCall):
    """Execute an MCP tool call."""
    result = handle_tool_call(tool_call)
    if not result.success and result.error and "Authentication" in result.error:
        raise HTTPException(status_code=401, detail=result.model_dump())
    return result.model_dump()


@app.get("/api/dcl/reconciliation")
def get_reconciliation(aod_run_id: Optional[str] = None):
    """Stateless reconciliation — fetches from AAM fresh each time, no dependency on prior run."""
    try:
        _invalidate_aam_caches()
        from backend.aam.client import get_aam_client
        from backend.aam.ingress import AAMIngressAdapter
        import time as _time

        adapter = AAMIngressAdapter()
        client = get_aam_client()

        # Discover aod_run_id from latest push if not provided
        push_meta = None
        push_pipe_count = 0
        pushes = []
        try:
            pushes_raw = client.get_push_history()
            pushes = adapter.ingest_push_history(pushes_raw)
            if pushes:
                latest = pushes[0]
                push_pipe_count = latest.pipe_count
                push_meta = {
                    "pushId": latest.push_id,
                    "pushedAt": latest.pushed_at,
                    "pipeCount": push_pipe_count,
                    "payloadHash": latest.payload_hash,
                    "aodRunId": latest.aod_run_id,
                }
                # Auto-discover aod_run_id from latest push
                if not aod_run_id and latest.aod_run_id:
                    aod_run_id = latest.aod_run_id
                    logger.info(f"[Recon] Auto-discovered aod_run_id={aod_run_id} from latest push")
        except Exception as e:
            logger.warning(f"Push history unavailable: {e}")

        # Fetch pipes from AAM using the (possibly discovered) aod_run_id
        aam_export = client.get_pipes(aod_run_id=aod_run_id)
        payload = adapter.ingest_pipes(aam_export)

        # Update push_meta hash now that we have the payload
        if push_meta:
            push_meta["payloadHash"] = push_meta["payloadHash"] or payload.payload_hash

        # DCL side: what DCL would load = all normalized pipes from AAM
        # (since DCL loads via the same adapter, canonical IDs are identical)
        dcl_canonical_ids = sorted(p.canonical_id for p in payload.pipes)

        result = reconcile(payload.pipes, dcl_canonical_ids)

        if not push_meta:
            push_meta = {
                "pushId": "export-pipes",
                "pushedAt": _time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "pipeCount": payload.total_connections_actual,
                "payloadHash": payload.payload_hash,
                "aodRunId": aod_run_id,
            }

        result["pushMeta"] = push_meta

        current_mode = get_current_mode()
        result["reconMeta"] = {
            "dclRunId": current_mode.last_run_id,
            "dclRunAt": current_mode.last_updated,
            "reconAt": _time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "aodRunId": aod_run_id,
            "dataMode": current_mode.data_mode,
            "dclSourceCount": len(dcl_canonical_ids),
            "aamConnectionCount": payload.total_connections_actual,
        }

        aam_names = sorted(p.display_name for p in payload.pipes)
        result["trace"] = {
            "aamPipeNames": aam_names,
            "dclLoadedSourceNames": dcl_canonical_ids,
            "exportPipeCount": payload.total_connections_actual,
            "pushPipeCount": push_pipe_count,
            "unmappedCount": sum(1 for p in payload.pipes if p.fabric_plane == "unmapped"),
        }

        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Reconciliation failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/dcl/reconciliation/sor")
def get_sor_reconciliation():
    try:
        import yaml
        config_dir = Path(__file__).parent.parent / "config" / "definitions"

        bindings_path = config_dir / "bindings.yaml"
        metrics_path = config_dir / "metrics.yaml"
        entities_path = config_dir / "entities.yaml"

        bindings = []
        if bindings_path.exists():
            with open(bindings_path) as f:
                bindings = yaml.safe_load(f).get("bindings", [])

        metrics_list = []
        if metrics_path.exists():
            with open(metrics_path) as f:
                metrics_list = yaml.safe_load(f).get("metrics", [])

        entities_list = []
        if entities_path.exists():
            with open(entities_path) as f:
                entities_list = yaml.safe_load(f).get("entities", [])

        # Prefer canonical IDs (stable across display-name changes) over
        # raw display names.  The reconciliation function canonicalizes
        # both sides internally, but feeding it IDs that are already
        # canonical avoids double-normalization edge cases.
        loaded_source_ids = list(app.state.loaded_source_ids)
        loaded_sources = loaded_source_ids if loaded_source_ids else list(app.state.loaded_sources)

        # Self-sufficient fallback: if no DCL run has happened yet,
        # derive loaded sources from the bindings' source_system values
        # so the recon can still show the binding structure (all sources
        # will appear as "missing" until a run populates actual data).
        if not loaded_sources and bindings:
            loaded_sources = sorted(set(
                b.get("source_system", "") for b in bindings if b.get("source_system")
            ))
            logger.info(f"[SOR] No prior run — derived {len(loaded_sources)} sources from bindings")

        result = reconcile_sor(bindings, metrics_list, entities_list, loaded_sources)

        import time as _time
        sor_current_mode = get_current_mode()
        result["reconMeta"] = {
            "dclRunId": sor_current_mode.last_run_id,
            "dclRunAt": sor_current_mode.last_updated,
            "reconAt": _time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "dataMode": sor_current_mode.data_mode,
            "loadedSourceCount": len(loaded_sources),
        }

        return result
    except Exception as e:
        logger.error(f"SOR Reconciliation failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# AAM Reconciliation Endpoints
# =============================================================================


class ReconcileRequest(BaseModel):
    """Request to reconcile AAM payload against DCL's ingested state."""
    aod_run_id: Optional[str] = Field(None, description="AOD run ID to reconcile against")
    aam_source_ids: Optional[List[str]] = Field(
        None,
        description="Expected source IDs from AAM payload. If omitted, fetches live from AAM.",
    )


@app.post("/api/reconcile")
def reconcile_aam(request: ReconcileRequest):
    """
    Compare what AAM sent (expected) vs what DCL would ingest (actual).

    Stateless — fetches fresh from AAM each time, no dependency on prior run.
    Uses AAMIngressAdapter for consistent normalization on both sides.
    """
    from backend.aam.ingress import AAMIngressAdapter, normalize_source_id
    from backend.aam.client import get_aam_client

    adapter = AAMIngressAdapter()
    aam_client = get_aam_client()

    # Auto-discover aod_run_id from latest push if not provided
    effective_run_id = request.aod_run_id
    if not effective_run_id:
        try:
            pushes_raw = aam_client.get_push_history()
            pushes = adapter.ingest_push_history(pushes_raw)
            if pushes and pushes[0].aod_run_id:
                effective_run_id = pushes[0].aod_run_id
                logger.info(f"[Reconcile] Auto-discovered aod_run_id={effective_run_id} from latest push")
        except Exception:
            pass

    # ── 1. Build "expected" set from AAM ────────────────────────────────
    expected_sources: Dict[str, Dict[str, Any]] = {}
    payload = None

    if request.aam_source_ids:
        # Caller supplied the expected list — normalize through the same function
        for sid in request.aam_source_ids:
            canonical = normalize_source_id(sid)
            expected_sources[canonical] = {"source_id": canonical, "origin": "caller"}
    else:
        # Fetch live from AAM via ingress adapter
        try:
            pipes_data = aam_client.get_pipes(aod_run_id=effective_run_id)
            payload = adapter.ingest_pipes(pipes_data)

            for pipe in payload.pipes:
                expected_sources[pipe.canonical_id] = {
                    "source_id": pipe.canonical_id,
                    "source_name": pipe.display_name,
                    "plane_type": pipe.fabric_plane,
                    "field_count": pipe.field_count,
                    "origin": "aam_live",
                }
        except Exception as e:
            raise HTTPException(
                status_code=502,
                detail=f"Cannot reach AAM to fetch expected sources: {e}",
            )

    if not expected_sources:
        raise HTTPException(
            status_code=400,
            detail="No expected sources — provide aam_source_ids or ensure AAM returns data.",
        )

    # ── 2. Build "actual" set — what DCL would load from AAM ────────────
    if payload is not None:
        # Reuse the same fetch — DCL loads via the same adapter
        actual_canonical_ids = {p.canonical_id for p in payload.pipes}
    else:
        # Caller supplied expected IDs; fetch fresh for the actual set
        try:
            pipes_data = aam_client.get_pipes(aod_run_id=effective_run_id)
            payload = adapter.ingest_pipes(pipes_data)
            actual_canonical_ids = {p.canonical_id for p in payload.pipes}
        except Exception as e:
            raise HTTPException(
                status_code=502,
                detail=f"Cannot reach AAM to fetch actual sources: {e}",
            )

    # ── 3. Reconcile ────────────────────────────────────────────────────
    matched = []
    missing_in_dcl = []
    extra_in_dcl = []

    for sid, aam_info in expected_sources.items():
        if sid in actual_canonical_ids:
            matched.append({
                "source_id": sid,
                "aam": aam_info,
                "status": "matched",
            })
        else:
            missing_in_dcl.append({
                "source_id": sid,
                "aam": aam_info,
                "status": "missing_in_dcl",
            })

    for cid in sorted(actual_canonical_ids):
        if cid not in expected_sources:
            extra_in_dcl.append({
                "source_id": cid,
                "status": "extra_in_dcl",
            })

    total_expected = len(expected_sources)
    total_actual = len(actual_canonical_ids)
    match_count = len(matched)

    if match_count == total_expected and not extra_in_dcl:
        verdict = "fully_reconciled"
    elif match_count == total_expected:
        verdict = "reconciled_with_extras"
    elif missing_in_dcl:
        verdict = "drift_detected"
    else:
        verdict = "partial_match"

    return {
        "status": verdict,
        "run_id": None,
        "aod_run_id": effective_run_id,
        "expected_count": total_expected,
        "actual_count": total_actual,
        "matched_count": match_count,
        "missing_in_dcl_count": len(missing_in_dcl),
        "extra_in_dcl_count": len(extra_in_dcl),
        "matched": matched,
        "missing_in_dcl": missing_in_dcl,
        "extra_in_dcl": extra_in_dcl,
    }


# =============================================================================
# Deprecated / Moved Endpoints
# =============================================================================


@app.get("/api/nlq/ask")
def nlq_ask_moved():
    """NLQ functionality moved to AOS-NLQ."""
    raise HTTPException(status_code=410, detail={"error": "MOVED_TO_AOS_NLQ", "note": "Use AOS-NLQ service for NLQ queries"})


@app.post("/api/nlq/ask")
def nlq_ask_post_moved():
    """NLQ functionality moved to AOS-NLQ."""
    raise HTTPException(status_code=410, detail={"error": "MOVED_TO_AOS_NLQ", "note": "Use AOS-NLQ service for NLQ queries"})


@app.get("/api/bll/{path:path}")
def bll_moved(path: str):
    """BLL functionality moved to AOS-NLQ."""
    raise HTTPException(status_code=410, detail={"error": "MOVED_TO_AOS_NLQ", "note": "Use AOS-NLQ service for BLL operations"})


@app.post("/api/bll/{path:path}")
def bll_post_moved(path: str):
    """BLL functionality moved to AOS-NLQ."""
    raise HTTPException(status_code=410, detail={"error": "MOVED_TO_AOS_NLQ", "note": "Use AOS-NLQ service for BLL operations"})


@app.get("/api/execute")
def execute_moved():
    """Execute functionality moved to AOS-NLQ."""
    raise HTTPException(status_code=410, detail={"error": "MOVED_TO_AOS_NLQ", "note": "Use AOS-NLQ service for query execution"})


@app.post("/api/execute")
def execute_post_moved():
    """Execute functionality moved to AOS-NLQ."""
    raise HTTPException(status_code=410, detail={"error": "MOVED_TO_AOS_NLQ", "note": "Use AOS-NLQ service for query execution"})


DIST_DIR = Path(__file__).parent.parent.parent / "dist"

if DIST_DIR.exists() and (DIST_DIR / "assets").exists():
    app.mount("/assets", StaticFiles(directory=DIST_DIR / "assets"), name="assets")


@app.get("/")
async def serve_root():
    index_file = DIST_DIR / "index.html"
    if index_file.exists():
        return FileResponse(index_file)
    return {"status": "DCL Engine API is running", "version": "2.0.0", "note": "Frontend not built"}


@app.get("/{full_path:path}")
async def serve_spa(full_path: str):
    if full_path.startswith("api/"):
        raise HTTPException(status_code=404, detail="API route not found")
    index_file = DIST_DIR / "index.html"
    if index_file.exists():
        return FileResponse(index_file)
    raise HTTPException(status_code=404, detail="Frontend not built")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)
