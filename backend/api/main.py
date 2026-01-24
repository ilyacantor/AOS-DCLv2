import os
import json
import uuid
from pathlib import Path
from fastapi import FastAPI, HTTPException, Request
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
    source_limit: Optional[int] = 5  # Number of sources to fetch from Farm


class RunResponse(BaseModel):
    graph: GraphSnapshot
    run_metrics: RunMetrics
    run_id: str


@app.get("/api/health")
def health():
    return {"status": "DCL Engine API is running", "version": "1.0.0"}


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


from backend.core.topology_api import topology_api, ConnectionHealth, ConnectionStatus

# =============================================================================
# NLQ Answerability Circles Endpoints
# =============================================================================

from backend.nlq import (
    AnswerabilityRequest,
    AnswerabilityResponse,
    ExplainRequest,
    ExplainResponse,
    AnswerabilityScorer,
    NLQPersistence,
)
from backend.nlq.explainer import HypothesisExplainer

# Initialize NLQ components
nlq_persistence = NLQPersistence()
answerability_scorer = AnswerabilityScorer(persistence=nlq_persistence)
hypothesis_explainer = HypothesisExplainer(persistence=nlq_persistence)


@app.post("/api/nlq/answerability_rank", response_model=AnswerabilityResponse)
def rank_answerability(request: AnswerabilityRequest):
    """
    Rank hypotheses for a natural language question.

    Returns 2-3 "answer circles" (hypotheses) with:
    - size = probability_of_answer
    - rank = left→right order (most likely answerable first)
    - color = confidence (evidence quality: hot/warm/cool)

    No LLM calls in the hot path. Uses deterministic rules + stored metadata.

    Example request:
    {
        "question": "Services revenue (25% of total) is down 50% QoQ — what's happening?",
        "tenant_id": "t_123",
        "context": {
            "time_window": "QoQ",
            "metric_hint": "services_revenue"
        }
    }
    """
    try:
        # Rank hypotheses
        circles = answerability_scorer.rank_hypotheses(
            question=request.question,
            tenant_id=request.tenant_id,
            context=request.context,
        )

        # Check if clarification needed
        needs_context = answerability_scorer.get_needs_context(circles)

        return AnswerabilityResponse(
            question=request.question,
            circles=circles,
            needs_context=needs_context,
        )
    except Exception as e:
        logger.error(f"Answerability ranking failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/nlq/explain", response_model=ExplainResponse)
def explain_hypothesis(request: ExplainRequest):
    """
    Get a deterministic explanation for a hypothesis.

    Returns a short explanation with:
    - headline: Summary of the finding
    - why: List of supporting facts with confidence
    - go_deeper: Bridge analysis and drilldown options
    - proof: Source system pointers and query hashes
    - next: Suggested next actions

    For MVP, facts and proof are stubbed. No real query execution.

    Example request:
    {
        "question": "Services revenue is down 50% QoQ — what's happening?",
        "tenant_id": "t_123",
        "hypothesis_id": "h_volume",
        "plan_id": "plan_services_rev_bridge"
    }
    """
    try:
        response = hypothesis_explainer.explain(request)
        return response
    except Exception as e:
        logger.error(f"Explanation generation failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


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


DIST_DIR = Path(__file__).parent.parent.parent / "dist"

if DIST_DIR.exists() and (DIST_DIR / "assets").exists():
    app.mount("/assets", StaticFiles(directory=DIST_DIR / "assets"), name="assets")


@app.get("/")
async def serve_root():
    index_file = DIST_DIR / "index.html"
    if index_file.exists():
        return FileResponse(index_file)
    return {"status": "DCL Engine API is running", "version": "1.0.0", "note": "Frontend not built"}


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
