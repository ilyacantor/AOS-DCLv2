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
    # Data model types for registration
    CanonicalEvent,
    Entity,
    Binding,
    Definition,
    DefinitionVersion,
    DefinitionVersionSpec,
    ProofHook,
)
from backend.nlq.explainer import HypothesisExplainer
from backend.nlq.routes_registry import router as registry_router

# Include the registry router
app.include_router(registry_router)

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


# =============================================================================
# NLQ Registration API - Semantic Layer Management
# =============================================================================


@app.post("/api/nlq/bindings", response_model=Binding, tags=["NLQ Registration"])
def register_binding(binding: Binding):
    """
    Register or update a source system binding.

    Bindings map source system fields to canonical event fields.
    This enables the semantic layer to understand how source data
    relates to canonical business events.

    Example:
    {
        "id": "netsuite_revenue",
        "tenant_id": "t_123",
        "source_system": "NetSuite",
        "canonical_event_id": "revenue_recognized",
        "mapping_json": {
            "tran_date": "recognized_at",
            "amount": "amount",
            "customer": "customer_id"
        },
        "dims_coverage_json": {
            "customer": true,
            "service_line": true
        },
        "quality_score": 0.9,
        "freshness_score": 0.95
    }
    """
    try:
        return nlq_persistence.register_binding(binding)
    except Exception as e:
        logger.error(f"Binding registration failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/nlq/bindings", response_model=List[Binding], tags=["NLQ Registration"])
def list_bindings(tenant_id: str = "default"):
    """List all bindings for a tenant."""
    return nlq_persistence.get_bindings(tenant_id)


@app.get("/api/nlq/bindings/{binding_id}", response_model=Binding, tags=["NLQ Registration"])
def get_binding(binding_id: str, tenant_id: str = "default"):
    """Get a specific binding by ID."""
    bindings = nlq_persistence.get_bindings(tenant_id)
    for b in bindings:
        if b.id == binding_id:
            return b
    raise HTTPException(status_code=404, detail=f"Binding {binding_id} not found")


@app.delete("/api/nlq/bindings/{binding_id}", tags=["NLQ Registration"])
def delete_binding(binding_id: str, tenant_id: str = "default"):
    """Delete a binding."""
    if nlq_persistence.delete_binding(binding_id, tenant_id):
        return {"status": "deleted", "id": binding_id}
    raise HTTPException(status_code=404, detail=f"Binding {binding_id} not found")


@app.post("/api/nlq/events", response_model=CanonicalEvent, tags=["NLQ Registration"])
def register_event(event: CanonicalEvent):
    """
    Register or update a canonical event type.

    Canonical events are system-agnostic business event types like
    revenue_recognized, invoice_posted, contract_signed.

    Example:
    {
        "id": "revenue_recognized",
        "tenant_id": "t_123",
        "schema_json": {
            "fields": [
                {"name": "amount", "type": "decimal"},
                {"name": "customer_id", "type": "string"},
                {"name": "recognized_at", "type": "timestamp"}
            ]
        },
        "time_semantics_json": {
            "event_time": "recognized_at",
            "calendar": "fiscal"
        }
    }
    """
    try:
        return nlq_persistence.register_event(event)
    except Exception as e:
        logger.error(f"Event registration failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/nlq/events", response_model=List[CanonicalEvent], tags=["NLQ Registration"])
def list_events(tenant_id: str = "default"):
    """List all canonical events for a tenant."""
    return nlq_persistence.get_events(tenant_id)


@app.post("/api/nlq/entities", response_model=Entity, tags=["NLQ Registration"])
def register_entity(entity: Entity):
    """
    Register or update an entity (dimension).

    Entities are business dimensions like customer, service_line, region
    that events can be grouped/filtered by.

    Example:
    {
        "id": "customer",
        "tenant_id": "t_123",
        "identifiers_json": {
            "primary": "customer_id",
            "aliases": ["account_id", "client_id"]
        }
    }
    """
    try:
        return nlq_persistence.register_entity(entity)
    except Exception as e:
        logger.error(f"Entity registration failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/nlq/entities", response_model=List[Entity], tags=["NLQ Registration"])
def list_entities(tenant_id: str = "default"):
    """List all entities for a tenant."""
    return nlq_persistence.get_entities(tenant_id)


@app.post("/api/nlq/definitions", response_model=Definition, tags=["NLQ Registration"])
def register_definition(definition: Definition):
    """
    Register or update a metric/view definition.

    Definitions describe business metrics like services_revenue, ARR, DSO.

    Example:
    {
        "id": "services_revenue",
        "tenant_id": "t_123",
        "kind": "metric",
        "description": "Revenue from professional services",
        "default_time_semantics_json": {
            "event": "revenue_recognized",
            "time_field": "recognized_at",
            "calendar": "fiscal"
        }
    }
    """
    try:
        return nlq_persistence.register_definition(definition)
    except Exception as e:
        logger.error(f"Definition registration failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/nlq/definitions", response_model=List[Definition], tags=["NLQ Registration"])
def list_definitions(tenant_id: str = "default"):
    """List all definitions for a tenant."""
    return nlq_persistence.get_definitions(tenant_id)


@app.get("/api/nlq/definitions/{definition_id}", response_model=Definition, tags=["NLQ Registration"])
def get_definition(definition_id: str, tenant_id: str = "default"):
    """Get a specific definition by ID."""
    definition = nlq_persistence.get_definition(definition_id, tenant_id)
    if definition:
        return definition
    raise HTTPException(status_code=404, detail=f"Definition {definition_id} not found")


@app.post("/api/nlq/definition_versions", response_model=DefinitionVersion, tags=["NLQ Registration"])
def register_definition_version(version: DefinitionVersion):
    """
    Register or update a definition version.

    Definition versions contain the full spec for computing a metric:
    - required_events: Events needed to compute the metric
    - measure: Aggregation operation (sum, avg, count)
    - filters: Filter DSL conditions
    - allowed_dims: Dimensions that can be used for grouping
    - joins: How to join events to entities
    - time_field: Field to use for time-based filtering

    Example:
    {
        "id": "services_revenue_v1",
        "tenant_id": "t_123",
        "definition_id": "services_revenue",
        "version": "v1",
        "status": "published",
        "spec": {
            "required_events": ["revenue_recognized"],
            "measure": {"op": "sum", "field": "amount"},
            "filters": {
                "service_line": {"op": "in", "values": ["Professional Services"]}
            },
            "allowed_dims": ["customer", "service_line"],
            "time_field": "recognized_at"
        }
    }
    """
    try:
        return nlq_persistence.register_definition_version(version)
    except Exception as e:
        logger.error(f"Definition version registration failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/nlq/definition_versions", response_model=List[DefinitionVersion], tags=["NLQ Registration"])
def list_definition_versions(tenant_id: str = "default"):
    """List all definition versions for a tenant."""
    return nlq_persistence.get_definition_versions(tenant_id)


@app.post("/api/nlq/proof_hooks", response_model=ProofHook, tags=["NLQ Registration"])
def register_proof_hook(hook: ProofHook):
    """
    Register or update a proof hook.

    Proof hooks link definitions to source system evidence for explainability.

    Example:
    {
        "id": "services_revenue_netsuite",
        "tenant_id": "t_123",
        "definition_id": "services_revenue",
        "pointer_template_json": {
            "system": "NetSuite",
            "type": "saved_search",
            "ref_template": "saved_search:{search_id}"
        },
        "availability_score": 0.9
    }
    """
    try:
        return nlq_persistence.register_proof_hook(hook)
    except Exception as e:
        logger.error(f"Proof hook registration failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/nlq/proof_hooks", response_model=List[ProofHook], tags=["NLQ Registration"])
def list_proof_hooks(tenant_id: str = "default"):
    """List all proof hooks for a tenant."""
    return nlq_persistence.get_proof_hooks(tenant_id)


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
