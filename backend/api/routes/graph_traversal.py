"""
DCL Semantic Graph Traversal routes.

Handles:
  POST /api/dcl/resolve               — resolve a query against the semantic graph
  GET  /api/dcl/graph/stats            — graph node/edge counts and connectivity
  GET  /api/dcl/graph/path             — debug: show join path between concept and dimension
"""

from typing import Any, Dict, List, Optional, Union

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

router = APIRouter(tags=["Graph Traversal"])


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class FilterParam(BaseModel):
    dimension: str
    operator: str = "equals"
    value: str | list[str] = ""


class ResolveRequest(BaseModel):
    concepts: List[str] = Field(..., min_length=1)
    dimensions: List[str] = Field(default_factory=list)
    filters: List[FilterParam] = Field(default_factory=list)
    persona: Optional[str] = None
    # Required when persona is set (Gate 2B): the persona-scoped decision
    # trace must carry the tenant identity (I2). Unused otherwise.
    tenant_id: Optional[str] = None


class HopResponse(BaseModel):
    from_system: str
    from_field: str
    to_system: str
    to_field: str
    via: str
    confidence: float


class JoinPathResponse(BaseModel):
    hops: List[HopResponse]
    total_confidence: float
    description: str


class ConfidenceResponse(BaseModel):
    overall: float
    per_hop: Dict[str, float]
    weakest_link: str
    weakest_confidence: float


class DataQueryHintResponse(BaseModel):
    primary_system: str
    tables: List[str]
    join_keys: List[Dict[str, Any]]
    filters: List[Dict[str, Any]]
    description: str


class ResolvedFilterResponse(BaseModel):
    dimension: str
    original_value: str
    resolved_values: List[str]
    resolution_type: str


class FieldLocationResponse(BaseModel):
    system: str
    object_name: str
    field: str
    concept: str
    confidence: float


class ResolveResponse(BaseModel):
    can_answer: bool
    confidence: Optional[ConfidenceResponse] = None
    provenance: str = ""
    data_query: Optional[DataQueryHintResponse] = None
    warnings: List[str] = Field(default_factory=list)
    reason: Optional[str] = None
    concept_sources: List[FieldLocationResponse] = Field(default_factory=list)
    join_paths: List[JoinPathResponse] = Field(default_factory=list)
    resolved_filters: List[ResolvedFilterResponse] = Field(default_factory=list)


class PersonaResolveResponse(ResolveResponse):
    """Persona-scoped resolve answer (Gate 2B). Carries the identity pair
    context (I2) on top of the unchanged base shape; personaless responses
    keep the exact base shape — no new keys."""
    tenant_id: str
    persona: str


class GraphStatsResponse(BaseModel):
    concept_nodes: int
    dimension_nodes: int
    system_nodes: int
    field_nodes: int
    dimension_value_nodes: int
    edges_by_type: Dict[str, int]
    connected_systems: int
    avg_path_confidence: float


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_graph_and_resolver():
    """Import the singleton graph and resolver (lazy to avoid circular imports)."""
    from backend.engine.graph_store import get_semantic_graph, get_query_resolver
    graph = get_semantic_graph()
    resolver = get_query_resolver()
    if graph is None or resolver is None:
        raise HTTPException(
            status_code=503,
            detail="Semantic graph not initialized. Run a DCL pipeline first.",
        )
    return graph, resolver


# ---------------------------------------------------------------------------
# POST /api/dcl/resolve
# ---------------------------------------------------------------------------

@router.post(
    "/api/dcl/resolve",
    response_model=Union[PersonaResolveResponse, ResolveResponse],
)
def resolve_query(request: ResolveRequest):
    """Resolve a query against the semantic graph (8-step traversal).

    persona (Gate 2B): concept location is scoped to the persona's domain
    list (config/persona_domains.yaml, exact key). Unknown persona → 422
    naming the valid keys. persona requires tenant_id (the scoped answer's
    decision trace carries the tenant, I2) → 422 when missing. Every
    persona-scoped answer appends one mai_mcp_audit row (transport='http',
    tool_name='resolve'); personaless calls write nothing and behave
    exactly as before.
    """
    from backend.engine.graph_types import QueryFilter, QueryIntent

    if request.persona is not None:
        from backend.engine.persona_view import (
            UnknownPersonaError,
            resolve_persona_domains,
        )
        try:
            resolve_persona_domains(request.persona)
        except UnknownPersonaError as e:
            raise HTTPException(status_code=422, detail=str(e))
        if not request.tenant_id or not request.tenant_id.strip():
            raise HTTPException(
                status_code=422,
                detail=(
                    "Persona-scoped resolve requires tenant_id — the "
                    "scoped answer's decision trace must carry the tenant "
                    "identity (I2). Pass tenant_id alongside persona "
                    f"{request.persona!r}."
                ),
            )

    graph, resolver = _get_graph_and_resolver()

    intent = QueryIntent(
        concepts=request.concepts,
        dimensions=request.dimensions,
        filters=[
            QueryFilter(
                dimension=f.dimension,
                operator=f.operator,
                value=f.value,
            )
            for f in request.filters
        ],
        persona=request.persona,
    )

    if request.persona is not None:
        from backend.api.mcp_audit import time_call
        with time_call() as timing:
            result = resolver.resolve(intent)
    else:
        result = resolver.resolve(intent)

    # Convert dataclass result to Pydantic response
    confidence_resp = None
    if result.can_answer:
        cb = result.confidence
        confidence_resp = ConfidenceResponse(
            overall=cb.overall,
            per_hop=cb.per_hop,
            weakest_link=cb.weakest_link,
            weakest_confidence=cb.weakest_confidence,
        )

    dq_resp = None
    if result.data_query:
        dq = result.data_query
        dq_resp = DataQueryHintResponse(
            primary_system=dq.primary_system,
            tables=dq.tables,
            join_keys=dq.join_keys,
            filters=dq.filters,
            description=dq.description,
        )

    base_fields = dict(
        can_answer=result.can_answer,
        confidence=confidence_resp,
        provenance=result.provenance,
        data_query=dq_resp,
        warnings=result.warnings,
        reason=result.reason,
        concept_sources=[
            FieldLocationResponse(
                system=s.system, object_name=s.object_name,
                field=s.field, concept=s.concept, confidence=s.confidence,
            )
            for s in result.concept_sources
        ],
        join_paths=[
            JoinPathResponse(
                hops=[
                    HopResponse(
                        from_system=h.from_system, from_field=h.from_field,
                        to_system=h.to_system, to_field=h.to_field,
                        via=h.via, confidence=h.confidence,
                    )
                    for h in jp.hops
                ],
                total_confidence=jp.total_confidence,
                description=jp.description,
            )
            for jp in result.join_paths
        ],
        resolved_filters=[
            ResolvedFilterResponse(
                dimension=rf.dimension, original_value=rf.original_value,
                resolved_values=rf.resolved_values,
                resolution_type=rf.resolution_type,
            )
            for rf in result.resolved_filters
        ],
    )

    if request.persona is None:
        return ResolveResponse(**base_fields)

    # Persona-scoped answer: one decision-trace row (mai_mcp_audit →
    # decision_traces view as trace_type='mcp_call', decision_type='resolve')
    # carrying the persona in its payload. Personaless calls never reach
    # this block.
    from backend.api.mcp_audit import AuditRow, hash_arguments, write_audit
    arguments: Dict[str, Any] = {
        "concepts": request.concepts,
        "persona": request.persona,
        "tenant_id": request.tenant_id,
    }
    if request.dimensions:
        arguments["dimensions"] = request.dimensions
    if request.filters:
        arguments["filters"] = [f.model_dump() for f in request.filters]
    write_audit(AuditRow(
        tenant_id=request.tenant_id,
        tool_name="resolve",
        caller_token_id="http:resolve",
        arguments_hash=hash_arguments(arguments),
        latency_ms=timing["latency_ms"],
        outcome="success",
        transport="http",
        entity_id=None,
        arguments=arguments,
        result_summary={
            "can_answer": result.can_answer,
            "concept_sources": len(result.concept_sources),
            "warnings": len(result.warnings),
        },
    ))
    return PersonaResolveResponse(
        tenant_id=request.tenant_id,
        persona=request.persona,
        **base_fields,
    )


# ---------------------------------------------------------------------------
# GET /api/dcl/graph/stats
# ---------------------------------------------------------------------------

@router.get("/api/dcl/graph/stats", response_model=GraphStatsResponse)
def graph_stats():
    """Get semantic graph node/edge counts and connectivity metrics."""
    graph, _ = _get_graph_and_resolver()
    s = graph.stats
    return GraphStatsResponse(
        concept_nodes=s.concept_nodes,
        dimension_nodes=s.dimension_nodes,
        system_nodes=s.system_nodes,
        field_nodes=s.field_nodes,
        dimension_value_nodes=s.dimension_value_nodes,
        edges_by_type=s.edges_by_type,
        connected_systems=s.connected_systems,
        avg_path_confidence=s.avg_path_confidence,
    )


# ---------------------------------------------------------------------------
# GET /api/dcl/graph/path
# ---------------------------------------------------------------------------

@router.get("/api/dcl/graph/path", response_model=JoinPathResponse)
def graph_path(from_concept: str, to_dimension: str):
    """Show join path from a concept's primary system to a dimension's SOR.

    For debugging/visualization — shows how DCL would connect these.
    """
    graph, _ = _get_graph_and_resolver()

    # Find primary system for concept
    sources = graph.find_concept_sources(from_concept)
    if not sources:
        raise HTTPException(
            status_code=404,
            detail=f"No sources found for concept '{from_concept}'",
        )

    # Find SOR for dimension
    auth = graph.find_dimension_authority(to_dimension)
    if not auth:
        raise HTTPException(
            status_code=404,
            detail=f"No authoritative system for dimension '{to_dimension}'",
        )

    primary_system = sources[0].system

    path = graph.find_join_path(primary_system, auth.system)
    if path is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No join path from {primary_system} (concept '{from_concept}') "
                f"to {auth.system} (dimension '{to_dimension}')"
            ),
        )

    return JoinPathResponse(
        hops=[
            HopResponse(
                from_system=h.from_system, from_field=h.from_field,
                to_system=h.to_system, to_field=h.to_field,
                via=h.via, confidence=h.confidence,
            )
            for h in path.hops
        ],
        total_confidence=path.total_confidence,
        description=path.description,
    )
