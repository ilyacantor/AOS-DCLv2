"""
DCL Engine API — app factory.

All domain endpoints live in backend/api/routes/*.py.
This file owns: app creation, middleware, startup, and the core
DCL run/narration/mapping/topology/semantic/query/MCP endpoints
that are tightly coupled to the DCLEngine singleton.
"""

import os
import uuid
from pathlib import Path
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException
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
    search_metrics,
    search_entities,
    SemanticExport,
)
from backend.api.query import (
    QueryRequest,
    QueryError,
    handle_query,
)
from backend.api.ingest import get_ingest_store, ActivityEntry
from backend.api.pipe_store import get_pipe_store
from backend.api.mcp_server import (
    MCPToolCall,
    get_server_info,
    handle_tool_call,
)
from backend.core.topology_api import topology_api
from backend.core.security_constraints import (
    validate_no_disk_payload_writes,
    assert_metadata_only_mode,
)
from backend.core.mode_state import set_current_mode
from backend.core.constants import CORS_ORIGINS, API_VERSION, utc_now

# Route modules
from backend.api.routes.ingest import router as ingest_router
from backend.api.routes.export_pipes import router as export_pipes_router
from backend.api.routes.reconciliation import router as reconciliation_router
from backend.api.routes.temporal import router as temporal_router
from backend.api.routes.entities import router as entities_router
from backend.api.routes.deprecated import router as deprecated_router
from backend.farm.routes import router as farm_router
from backend.dcl.routes import router as dcl_router

logger = get_logger(__name__)


# =============================================================================
# App setup
# =============================================================================

app = FastAPI(title="DCL Engine API")


@app.on_event("startup")
async def enforce_security_constraints():
    """Enforce Zero-Trust metadata-only constraints at startup."""
    logger.info("=== DCL Zero-Trust Security Check ===")

    try:
        assert_metadata_only_mode()
        logger.info("[SECURITY] Metadata-only mode: ENABLED")
    except Exception as e:
        if os.getenv("DCL_ENV", "dev").lower() == "production":
            logger.error(f"[SECURITY] Metadata-only assertion FAILED in production: {e}")
            raise
        logger.warning(f"[SECURITY] Metadata-only assertion failed (non-prod, continuing): {e}")

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


# =============================================================================
# Mount extracted route modules
# =============================================================================

app.include_router(ingest_router)
app.include_router(export_pipes_router)
app.include_router(reconciliation_router)
app.include_router(temporal_router)
app.include_router(entities_router)
app.include_router(deprecated_router)
app.include_router(farm_router)
app.include_router(dcl_router)


# =============================================================================
# Cache invalidation helper (used by run_dcl and reconciliation)
# =============================================================================

def _invalidate_aam_caches():
    """Clear all caches that could return stale AAM data on a new run."""
    try:
        from backend.semantic_mapper.persist_mappings import MappingPersistence
        MappingPersistence.clear_all_caches()
        logger.info("[AAM] Cleared mapping persistence caches")
    except Exception as e:
        logger.error(f"[AAM] Failed to clear mapping caches: {e}", exc_info=True)

    try:
        import backend.aam.client as aam_mod
        if aam_mod._aam_client is not None:
            aam_mod._aam_client.close()
            aam_mod._aam_client = None
            logger.info("[AAM] Reset AAM client singleton")
    except Exception as e:
        logger.warning(f"[AAM] Failed to reset AAM client: {e}")

    SchemaLoader._demo_cache = None
    SchemaLoader._stream_cache = None
    SchemaLoader._cache_time = 0
    SchemaLoader._aam_cache = None
    SchemaLoader._aam_cache_time = 0
    logger.info("[AAM] All stale caches invalidated for fresh AAM run")


# =============================================================================
# Core DCL endpoints (tightly coupled to engine singleton)
# =============================================================================


@app.get("/api/health")
def health():
    return {
        "status": "DCL Engine API is running",
        "version": API_VERSION,
        "mode": "metadata-only",
        "note": "NLQ/BLL moved to AOS-NLQ",
    }


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


@app.post("/api/dcl/run", response_model=RunResponse)
def run_dcl(request: RunRequest):
    run_id = str(uuid.uuid4())

    set_current_mode(
        data_mode=request.mode,
        run_mode=request.run_mode,
        run_id=run_id,
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
            aod_run_id=request.aod_run_id,
        )

        app.state.loaded_sources = snapshot.meta.get("source_names", [])
        app.state.loaded_source_ids = snapshot.meta.get("source_canonical_ids", [])

        if request.mode == "AAM":
            try:
                store = get_ingest_store()
                source_names = snapshot.meta.get("source_names", [])
                source_ids = snapshot.meta.get("source_canonical_ids", [])
                fabric_planes = snapshot.meta.get("source_fabric_planes", [])
                aam_kpis = metrics.payload_kpis if metrics.payload_kpis else {}
                aam_count, aam_snap = store.record_aam_pull(
                    run_id=run_id,
                    source_names=source_names,
                    source_ids=source_ids,
                    kpis=aam_kpis,
                    fabric_planes=fabric_planes,
                )
                app.state.aam_snapshot_name = aam_snap
                logger.info(f"[AAM] Recorded {aam_count} AAM pull receipts as '{aam_snap}'")
            except Exception as e:
                logger.warning(f"[AAM] Failed to record AAM pull in IngestStore: {e}")

        if request.mode == "Farm":
            _ensure_farm_content_activity()

        return RunResponse(
            graph=snapshot,
            run_metrics=metrics,
            run_id=run_id,
        )
    except Exception as e:
        logger.error(f"DCL run failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


def _ensure_farm_content_activity() -> None:
    """Ensure a content-phase activity entry exists for the current Farm dispatch.

    When Farm pushes data to POST /api/dcl/ingest, the ingest handler records
    the content activity.  But data pushed *before* the activity-recording code
    was deployed has receipts in IngestStore with no matching activity entry.

    This function reads the actual receipt data and backfills if needed.
    All values come from the receipts themselves — no fabrication.
    """
    from backend.core.constants import utc_now

    store = get_ingest_store()
    pipe_store = get_pipe_store()

    # Use the same dispatch selection as build_sources_from_ingest:
    # latest dispatch, or fall back to all non-AAM receipts.
    dispatches = store.get_dispatches()
    if not dispatches:
        return

    latest = dispatches[0]  # sorted by latest_received_at desc
    did = latest["dispatch_id"]

    # Already have a content entry for this dispatch? Nothing to do.
    if store.has_phase(did, "content"):
        return

    receipts = store.get_receipts_by_dispatch(did)
    if not receipts:
        return

    # All values from real receipt data
    snapshot_name = receipts[0].snapshot_name
    farm_run_id = receipts[0].run_id
    total_rows = sum(r.row_count for r in receipts)
    total_pipes = len(receipts)
    unique_sors = set(r.canonical_source_id for r in receipts)
    mapped = sum(1 for r in receipts if pipe_store.lookup(r.pipe_id) is not None)
    unmapped = total_pipes - mapped

    # Link to the export receipt if available
    export_receipts = pipe_store.get_export_receipts()
    aod_run_id = export_receipts[-1].aod_run_id if export_receipts else ""

    store.record_activity(ActivityEntry(
        phase="content",
        source="Farm",
        snapshot_name=snapshot_name,
        run_id=farm_run_id,
        timestamp=utc_now(),
        pipes=total_pipes,
        sors=len(unique_sors),
        rows=total_rows,
        records=total_rows,
        mapped_pipes=mapped,
        unmapped_pipes=unmapped,
        dispatch_id=did,
        aod_run_id=aod_run_id,
    ))
    logger.info(
        f"[Farm] Backfilled content activity from receipts: "
        f"snapshot={snapshot_name} run_id={farm_run_id} "
        f"{total_pipes} pipes, {total_rows:,} rows, {len(unique_sors)} SORs"
    )


@app.post("/api/dcl/ingest/reset")
def reset_ingest():
    """Clear all ingest data and pipe definitions."""
    get_ingest_store().reset()
    get_pipe_store().reset()
    logger.info("[Reset] All ingest data and pipe definitions cleared via API")
    return {"status": "reset", "message": "All ingest data and pipe definitions cleared"}


@app.get("/api/dcl/narration/{run_id}")
def get_narration(run_id: str):
    messages = engine.narration.get_messages(run_id)
    return {"run_id": run_id, "messages": messages}


@app.get("/api/dcl/monitor/{run_id}")
def get_monitor(run_id: str):
    return {
        "run_id": run_id,
        "monitor_data": {
            "message": "Monitor data endpoint ready",
            "sources": [],
            "ontology": [],
            "conflicts": [],
        },
    }


# =============================================================================
# Batch mapping
# =============================================================================


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
            clear_existing=request.clear_existing,
        )

        return MappingResponse(
            status="success",
            mappings_created=stats["mappings_created"],
            sources_processed=stats["sources_processed"],
            stats=stats,
        )
    except Exception as e:
        logger.error(f"Batch mapping failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Topology
# =============================================================================


class TopologyResponse(BaseModel):
    nodes: List[Dict[str, Any]]
    links: List[Dict[str, Any]]
    metadata: Dict[str, Any]


@app.get("/api/topology", response_model=TopologyResponse)
async def get_topology(include_health: bool = True):
    """Get the unified topology graph."""
    try:
        return await topology_api.get_topology(include_health=include_health)
    except Exception as e:
        logger.error(f"Failed to get topology: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/topology/health")
async def get_connection_health(connector_id: Optional[str] = None):
    """Get connection health data from the mesh."""
    try:
        return await topology_api.get_connection_health(connector_id)
    except Exception as e:
        logger.error(f"Failed to get connection health: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/topology/stats")
def get_topology_stats():
    """Get topology service statistics."""
    return topology_api.get_stats()


# =============================================================================
# Semantic export + query
# =============================================================================


@app.get("/api/dcl/semantic-export", response_model=SemanticExport)
def semantic_export(tenant_id: str = "default"):
    """Export full semantic catalog for NLQ consumption."""
    return get_semantic_export(tenant_id)


@app.get("/api/dcl/semantic-export/resolve/metric")
def resolve_metric_alias(q: str):
    """Resolve a metric alias to its canonical definition."""
    metric = resolve_metric(q)
    if not metric:
        candidates = search_metrics(q, limit=5)
        suggestions = [{"id": c.id, "name": c.name} for c in candidates]
        raise HTTPException(
            status_code=404,
            detail={
                "error": "METRIC_NOT_FOUND",
                "query": q,
                "suggestions": suggestions,
                "suggestion": "Use GET /api/dcl/semantic-export/search?q=... to search the catalog",
            },
        )
    return metric


@app.get("/api/dcl/semantic-export/resolve/entity")
def resolve_entity_alias(q: str):
    """Resolve an entity/dimension alias to its canonical definition."""
    entity = resolve_entity(q)
    if not entity:
        candidates = search_entities(q, limit=5)
        suggestions = [{"id": c.id, "name": c.name} for c in candidates]
        raise HTTPException(
            status_code=404,
            detail={
                "error": "ENTITY_NOT_FOUND",
                "query": q,
                "suggestions": suggestions,
                "suggestion": "Use GET /api/dcl/semantic-export/search?q=... to search the catalog",
            },
        )
    return entity


@app.get("/api/dcl/semantic-export/search")
def search_semantic_catalog(q: str, limit: int = 5):
    """Search both metrics and entities using fuzzy matching.

    Returns ranked candidates from the semantic catalog, giving NLQ
    a single endpoint to resolve natural language to catalog items.
    """
    matched_metrics = search_metrics(q, limit=limit)
    matched_entities = search_entities(q, limit=limit)
    return {
        "query": q,
        "metrics": [m.model_dump() for m in matched_metrics],
        "entities": [e.model_dump() for e in matched_entities],
        "total": len(matched_metrics) + len(matched_entities),
    }


@app.post("/api/dcl/query")
def execute_dcl_query(request: QueryRequest):
    """Execute a data query against DCL's fact base."""
    result = handle_query(request)

    if isinstance(result, QueryError):
        if result.code == "METRIC_NOT_FOUND":
            raise HTTPException(status_code=404, detail=result.model_dump())
        else:
            raise HTTPException(status_code=400, detail=result.model_dump())

    return result


# =============================================================================
# MCP Server
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


# =============================================================================
# SPA serving (must be last — catch-all routes)
# =============================================================================

DIST_DIR = Path(__file__).parent.parent.parent / "dist"

if DIST_DIR.exists() and (DIST_DIR / "assets").exists():
    app.mount("/assets", StaticFiles(directory=DIST_DIR / "assets"), name="assets")


@app.get("/")
async def serve_root():
    index_file = DIST_DIR / "index.html"
    if index_file.exists():
        return FileResponse(index_file)
    return {"status": "DCL Engine API is running", "version": API_VERSION, "note": "Frontend not built"}


@app.get("/{full_path:path}")
async def serve_spa(full_path: str):
    if full_path.startswith("api/"):
        raise HTTPException(status_code=404, detail="API route not found")
    blocked = ("data/", "data\\", "fact_base", ".json", ".yaml", ".yml", ".csv", ".env")
    if any(full_path.lower().startswith(b) or full_path.lower().endswith(b) for b in blocked):
        raise HTTPException(status_code=403, detail="Direct file access is blocked. Use the query API.")
    index_file = DIST_DIR / "index.html"
    if index_file.exists():
        return FileResponse(index_file)
    raise HTTPException(status_code=404, detail="Frontend not built")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)
