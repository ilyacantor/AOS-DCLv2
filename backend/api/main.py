"""
DCL Engine API — app factory.

All domain endpoints live in backend/api/routes/*.py.
This file owns: app creation, middleware, startup, and the core
DCL run/narration/mapping/topology/semantic/query/MCP endpoints
that are tightly coupled to the DCLEngine singleton.
"""

import asyncio
import os
import signal
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from pathlib import Path
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
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
from backend.api.ingest import get_ingest_store, ActivityEntry, DropEntry
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
from backend.core.redis_client import is_redis_available

# Route modules
from backend.api.routes.ingest import router as ingest_router
from backend.api.routes.export_pipes import router as export_pipes_router
from backend.api.routes.reconciliation import router as reconciliation_router
from backend.api.routes.temporal import router as temporal_router
from backend.api.routes.entities import router as entities_router
from backend.api.routes.deprecated import router as deprecated_router
from backend.farm.routes import router as farm_router
from backend.dcl.routes import router as dcl_router
from backend.api.routes.graph_traversal import router as graph_traversal_router
from backend.api.routes.reports import router as reports_router
from backend.api.routes.compat import router as compat_router
from backend.api.routes.maestra import router as maestra_router
from backend.api.routes.ingest_triples import router as ingest_triples_router
from backend.api.routes.resolution_v2 import router as resolution_v2_router
from backend.api.routes.reports_combining_v2 import router as reports_combining_v2_router
from backend.api.routes.reports_overlap_v2 import router as reports_overlap_v2_router
from backend.api.routes.reports_bridge_v2 import router as reports_bridge_v2_router
from backend.api.routes.reports_whatif_v2 import router as reports_whatif_v2_router

logger = get_logger(__name__)


# =============================================================================
# Startup readiness state
# =============================================================================

# Phase transitions: "starting" → "warming" → "ready" or "degraded"
_startup_phase: str = "starting"
_startup_error: Optional[str] = None
_startup_ready: Optional[asyncio.Event] = None

_WARMUP_TIMEOUT_SECONDS = 60


def _is_graph_required_endpoint(path: str) -> bool:
    """Return True if this endpoint requires the semantic graph to be built."""
    graph_required = (
        "/api/dcl/run",
        "/api/dcl/semantic-export",
        "/api/dcl/query",
        "/api/dcl/batch-mapping",
    )
    return any(path.startswith(p) for p in graph_required)


# =============================================================================
# App setup
# =============================================================================


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown lifecycle for the DCL Engine."""
    global _startup_phase, _startup_error, _startup_ready

    # ---- Fast startup (sync, <100ms) ----
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

    logger.info("=== DCL Engine Starting (Metadata-Only Mode) ===")

    # Set up readiness event and launch background warmup
    _startup_ready = asyncio.Event()
    _startup_phase = "warming"

    warmup_task = asyncio.create_task(_warm_up())

    yield  # ← App starts accepting requests NOW

    # ---- Shutdown ----
    # Cancel warmup if still running
    if not warmup_task.done():
        warmup_task.cancel()
        try:
            await warmup_task
        except asyncio.CancelledError:
            pass

    # Flush ALL pending debounced writes before closing pools.
    try:
        store = get_ingest_store()
        store._flush_to_disk()
        store._flush_activity_log()
        logger.info("[Shutdown] IngestStore disk + activity log flush complete")
    except Exception as e:
        logger.warning(f"[Shutdown] IngestStore flush error: {e}")

    logger.info("[Shutdown] Closing database connection pool...")
    try:
        from backend.semantic_mapper.persist_mappings import MappingPersistence
        MappingPersistence.close_pool()
    except Exception as e:
        logger.warning(f"[Shutdown] Pool close error: {e}")
    logger.info("[Shutdown] Database pool closed")


async def _warm_up():
    """Background warmup: build graph + check ingest store.

    Runs after the app is already accepting requests. Sets _startup_ready
    when done so endpoints that need the graph can proceed.
    """
    global _startup_phase, _startup_error, _startup_ready

    started = time.monotonic()

    try:
        # Run blocking I/O in executor to not block the event loop
        loop = asyncio.get_running_loop()

        # 1. Build semantic graph (DB + AAM remote I/O)
        try:
            await asyncio.wait_for(
                loop.run_in_executor(None, _sync_rebuild_graph),
                timeout=_WARMUP_TIMEOUT_SECONDS,
            )
            logger.info("[Startup] Semantic graph built")
        except asyncio.TimeoutError:
            logger.error(
                f"[Startup] Semantic graph build timed out after {_WARMUP_TIMEOUT_SECONDS}s. "
                f"Check Supabase/AAM connectivity."
            )
            _startup_phase = "degraded"
            _startup_error = f"Graph build timed out after {_WARMUP_TIMEOUT_SECONDS}s"
            _startup_ready.set()
            return
        except Exception as e:
            logger.warning(f"[Startup] Semantic graph build deferred: {e}")

        # 2. Auto-promote mode if ingest buffer has data
        try:
            await loop.run_in_executor(None, _sync_check_ingest_mode)
        except Exception as e:
            logger.warning(f"[Startup] Ingest store check failed (non-fatal): {e}")

        elapsed = time.monotonic() - started
        _startup_phase = "ready"
        _startup_ready.set()
        logger.info(f"=== DCL Engine Ready ({elapsed:.1f}s warmup) ===")

    except asyncio.CancelledError:
        logger.info("[Startup] Warmup cancelled (shutdown)")
        raise
    except Exception as e:
        logger.error(f"[Startup] Warmup failed: {e}", exc_info=True)
        _startup_phase = "degraded"
        _startup_error = str(e)
        _startup_ready.set()


def _sync_rebuild_graph():
    """Synchronous wrapper for rebuild_graph (runs in executor thread)."""
    from backend.engine.graph_store import rebuild_graph
    rebuild_graph()


def _sync_check_ingest_mode():
    """Check ingest buffer and auto-promote mode if data exists."""
    store = get_ingest_store()
    stats = store.get_stats()
    buffered = stats.get("total_rows_buffered", 0)
    if buffered > 0:
        set_current_mode("Ingest", run_mode="Dev")
        logger.info(
            f"[Startup] Mode auto-promoted: Empty → Ingest "
            f"({buffered} buffered rows, {stats.get('unique_sources', 0)} sources)"
        )


def _sigterm_flush(signum, frame):
    """Flush all pending writes on SIGTERM before the process exits."""
    try:
        store = get_ingest_store()
        store._flush_to_disk()
        store._flush_activity_log()
        logger.info("[SIGTERM] IngestStore emergency flush complete")
    except Exception as e:
        logger.warning(f"[SIGTERM] IngestStore flush error: {e}")
    raise SystemExit(0)


signal.signal(signal.SIGTERM, _sigterm_flush)


app = FastAPI(title="DCL Engine API", lifespan=lifespan)


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

# Shared executor for offloading sync engine work
_run_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="dcl-run")


# =============================================================================
# Startup-phase middleware
# =============================================================================


@app.middleware("http")
async def startup_gate_middleware(request: Request, call_next):
    """Return 503 for graph-dependent endpoints during warmup."""
    if _startup_phase not in ("ready", "degraded"):
        if _is_graph_required_endpoint(request.url.path):
            return JSONResponse(
                status_code=503,
                content={
                    "error": "DCL is warming up — semantic graph not yet built. Retry in a few seconds.",
                    "phase": _startup_phase,
                },
            )
    return await call_next(request)


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
app.include_router(graph_traversal_router)
if os.environ.get("LEGACY_JSON_LOAD", "").lower() in ("true", "1", "yes"):
    app.include_router(reports_router)
    logger.info("[main] LEGACY_JSON_LOAD=true — old /api/reports/* routes active (JSON-backed)")
else:
    app.include_router(compat_router)
    logger.info("[main] /api/reports/* routes serving v2 engine data via compat layer")
app.include_router(maestra_router)
app.include_router(ingest_triples_router)
app.include_router(resolution_v2_router)
app.include_router(reports_combining_v2_router)
app.include_router(reports_overlap_v2_router)
app.include_router(reports_bridge_v2_router)
app.include_router(reports_whatif_v2_router)


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

    # Clear SchemaLoader caches with lock protection
    with SchemaLoader._cache_lock:
        SchemaLoader._demo_cache = None
        SchemaLoader._stream_cache = None
        SchemaLoader._cache_time = 0
        SchemaLoader._aam_cache = None
        SchemaLoader._aam_cache_time = 0
    logger.info("[AAM] All stale caches invalidated for fresh AAM run")


# =============================================================================
# Core DCL endpoints (tightly coupled to engine singleton)
# =============================================================================


@app.get("/health")
@app.get("/api/health")
def health():
    from backend.core.mode_state import get_current_mode
    from backend.engine.graph_store import get_semantic_graph
    from backend.core.db import get_connection as _pg_health_conn

    mode = get_current_mode()
    graph = get_semantic_graph()
    query_ready = (
        _startup_phase in ("ready", "degraded")
        and graph is not None
    )

    # Quick Postgres connectivity check
    pg_ok = False
    try:
        with _pg_health_conn() as conn:
            pg_ok = conn is not None
    except Exception:
        pass

    return {
        "status": "DCL Engine API is running",
        "version": API_VERSION,
        "phase": _startup_phase,
        "graph_ready": graph is not None,
        "query_ready": query_ready,
        "redis_available": is_redis_available(),
        "postgres_available": pg_ok,
        "error": _startup_error,
        "data_mode": mode.data_mode,
        "last_run_id": mode.last_run_id,
        "last_updated": mode.last_updated,
    }


class RunRequest(BaseModel):
    mode: Literal["Farm", "AAM"] = "Farm"
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
async def run_dcl(request: RunRequest):
    run_id = str(uuid.uuid4())

    set_current_mode(
        data_mode=request.mode,
        run_mode=request.run_mode,
        run_id=run_id,
    )

    if request.force_refresh:
        _invalidate_aam_caches()

    personas = request.personas or [Persona.CFO, Persona.CRO, Persona.COO, Persona.CTO]

    loop = asyncio.get_running_loop()

    def _sync_run():
        """Run the graph build in a thread (uses ThreadedConnectionPool)."""
        return engine.build_graph_snapshot(
            mode=request.mode,
            run_mode=request.run_mode,
            personas=personas,
            run_id=run_id,
            source_limit=request.source_limit or 1000,
            aod_run_id=request.aod_run_id,
        )

    try:
        snapshot, metrics = await asyncio.wait_for(
            loop.run_in_executor(_run_executor, _sync_run),
            timeout=120.0,
        )
    except asyncio.TimeoutError:
        logger.error(f"DCL run timed out after 120s (run_id={run_id})")
        raise HTTPException(
            status_code=504,
            detail="Graph build timed out after 120s. Check Supabase/AAM connectivity.",
        )
    except Exception as e:
        logger.error(f"DCL run failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

    app.state.loaded_sources = snapshot.meta.get("sourceNames", [])
    app.state.loaded_source_ids = snapshot.meta.get("sourceCanonicalIds", [])

    if request.mode in ("AAM", "Farm"):
        try:
            store = get_ingest_store()
            source_names = snapshot.meta.get("sourceNames", [])
            source_ids = snapshot.meta.get("sourceCanonicalIds", [])
            fabric_planes = snapshot.meta.get("sourceFabricPlanes", [])
            aam_kpis = metrics.payload_kpis if metrics.payload_kpis else {}
            aam_count, aam_snap = store.record_aam_pull(
                run_id=run_id,
                source_names=source_names,
                source_ids=source_ids,
                kpis=aam_kpis,
                fabric_planes=fabric_planes,
            )
            app.state.aam_snapshot_name = aam_snap
            logger.info(f"[{request.mode}] Recorded {aam_count} AAM pull receipts as '{aam_snap}'")
        except Exception as e:
            logger.warning(f"[{request.mode}] Failed to record AAM pull in IngestStore: {e}")

    # build_graph_snapshot already builds and sets the graph via set_semantic_graph().
    # No redundant rebuild_graph() call — that would double DB queries and AAM calls.

    return RunResponse(
        graph=snapshot,
        run_metrics=metrics,
        run_id=run_id,
    )


@app.post("/api/dcl/ingest/seed")
async def seed_ingest(request: Request):
    """Seed local IngestStore from remote snapshot (dev only)."""
    payload = await request.json()
    store = get_ingest_store()
    activity_items = payload.get("activity", [])
    drop_items = payload.get("drops", [])
    added_activity = 0
    added_drops = 0
    with store._lock:
        for item in activity_items:
            store._activity_log.append(ActivityEntry(
                phase=item.get("phase", ""),
                source=item.get("source", ""),
                snapshot_name=item.get("snapshot_name", ""),
                run_id=item.get("run_id", ""),
                timestamp=item.get("timestamp", ""),
                pipes=item.get("pipes", 0),
                sors=item.get("sors", 0),
                tooling_pipes=item.get("tooling_pipes", 0),
                fabrics=item.get("fabrics", 0),
                mapped_pipes=item.get("mapped_pipes", 0),
                unmapped_pipes=item.get("unmapped_pipes", 0),
                rows=item.get("rows", 0),
                records=item.get("records", 0),
                sor_pipes=item.get("sor_pipes", 0),
                other_pipes=item.get("other_pipes", 0),
                dispatch_id=item.get("dispatch_id", ""),
                aod_run_id=item.get("aod_run_id", ""),
            ))
            added_activity += 1
        for item in drop_items:
            store._drop_log.append(DropEntry(
                pipe_id=item.get("pipe_id", ""),
                reason=item.get("reason", ""),
                error_code=item.get("error_code", ""),
                source_system=item.get("source_system", ""),
                timestamp=item.get("timestamp", ""),
                run_id=item.get("run_id", ""),
                dispatch_id=item.get("dispatch_id", ""),
                snapshot_name=item.get("snapshot_name", ""),
                tenant_id=item.get("tenant_id", ""),
            ))
            added_drops += 1
    store._save_to_disk()
    return {"status": "seeded", "activity_added": added_activity, "drops_added": added_drops}


## /api/dcl/ingest/reset removed — use /api/dcl/ingest/flush (the router
## endpoint) which calls store.reset() + pipe_store.reset() and returns
## before/after counts.  reset() now clears memory + Redis + Postgres + disk.


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
    mode: Literal["Farm"] = "Farm"
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
    """Execute a data query against DCL's ingest store."""
    result = handle_query(request)

    if isinstance(result, QueryError):
        if result.code == "METRIC_NOT_FOUND":
            raise HTTPException(status_code=404, detail=result.model_dump())
        else:
            raise HTTPException(status_code=400, detail=result.model_dump())

    return result



# =============================================================================
# Drill-through
# =============================================================================

@app.get("/api/dcl/drill-through")
def drill_through(
    level: str,
    parent: Optional[str] = None,
    quarter: Optional[str] = None,
):
    """Revenue drill-through — not yet implemented for ingested data."""
    raise HTTPException(
        status_code=501,
        detail="Drill-through is not yet implemented for ingested data. "
               "This feature previously relied on fact_base.json which has been removed.",
    )


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


@app.get("/favicon.png")
async def serve_favicon():
    favicon = DIST_DIR / "favicon.png"
    if favicon.exists():
        return FileResponse(favicon, media_type="image/png", headers={"Cache-Control": "public, max-age=86400"})
    raise HTTPException(status_code=404, detail="Favicon not found")


@app.get("/")
async def serve_root():
    index_file = DIST_DIR / "index.html"
    if index_file.exists():
        return FileResponse(
            index_file,
            headers={"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache"},
        )
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
        return FileResponse(
            index_file,
            headers={"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache"},
        )
    raise HTTPException(status_code=404, detail="Frontend not built")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("BACKEND_PORT", "8000")))
