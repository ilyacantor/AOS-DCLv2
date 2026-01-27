"""
DCL Engine API - Metadata-only semantic mapping layer.

NLQ and BLL functionality has been moved to AOS-NLQ repository.
DCL focuses on:
- Schema structures and semantic mappings
- Ontology management
- Graph visualization (Sankey diagrams)
- Topology API
"""
import os
import uuid
from pathlib import Path
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import List, Literal, Optional, Dict, Any
from backend.domain import Persona, GraphSnapshot, RunMetrics
from backend.engine import DCLEngine
from backend.engine.schema_loader import SchemaLoader
from backend.semantic_mapper import SemanticMapper
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

from backend.core.security_constraints import (
    validate_no_disk_payload_writes,
    assert_metadata_only_mode,
)

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

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

engine = DCLEngine()


class RunRequest(BaseModel):
    mode: Literal["Demo", "Farm"] = "Demo"
    run_mode: Literal["Dev", "Prod"] = "Dev"
    personas: Optional[List[Persona]] = None
    source_limit: Optional[int] = 5


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
    
    personas = request.personas or [Persona.CFO, Persona.CRO, Persona.COO, Persona.CTO]
    
    try:
        snapshot, metrics = engine.build_graph_snapshot(
            mode=request.mode,
            run_mode=request.run_mode,
            personas=personas,
            run_id=run_id,
            source_limit=request.source_limit or 5
        )
        
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
    uvicorn.run(app, host="0.0.0.0", port=8000)
