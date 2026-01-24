"""
Registry API routes for NLQ Semantic Layer.

Provides endpoints for:
- Definition registry management
- Consistency validation
- Lineage queries
- Schema validation
- Query execution
- Proof resolution
"""

from typing import List, Optional, Dict, Any
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from backend.nlq.persistence import NLQPersistence
from backend.nlq.registry import DefinitionRegistry, DefinitionSummary, DefinitionDetail, CatalogStats
from backend.nlq.consistency import ConsistencyValidator, FullConsistencyReport, ConsistencyCheckResult
from backend.nlq.lineage import LineageService, LineageGraph, ImpactAnalysis
from backend.nlq.schema_enforcer import SchemaEnforcer, SchemaValidationResult
from backend.nlq.executor import QueryExecutor, QueryResult
from backend.nlq.proof import ProofResolver, ProofChain, ResolvedProof
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

# Initialize router
router = APIRouter(prefix="/api/nlq/registry", tags=["NLQ Registry"])

# Initialize services
persistence = NLQPersistence()
registry = DefinitionRegistry(persistence)
consistency = ConsistencyValidator(persistence)
lineage = LineageService(persistence)
schema_enforcer = SchemaEnforcer(persistence)
executor = QueryExecutor(persistence)
proof_resolver = ProofResolver(persistence)


# =============================================================================
# Request/Response Models
# =============================================================================

class DefinitionListResponse(BaseModel):
    """Response for definition listing."""
    definitions: List[Dict[str, Any]]
    total: int
    offset: int
    limit: int


class CreateDefinitionRequest(BaseModel):
    """Request to create a definition."""
    id: str
    kind: str = "metric"
    pack: Optional[str] = None
    description: Optional[str] = None
    default_time_semantics: Optional[Dict[str, Any]] = None
    spec: Optional[Dict[str, Any]] = None


class CreateVersionRequest(BaseModel):
    """Request to create a definition version."""
    version: str
    spec: Dict[str, Any]


class PublishRequest(BaseModel):
    """Request to publish a version."""
    version: str


class ExecuteQueryRequest(BaseModel):
    """Request to execute a query."""
    definition_id: str
    version: str = "v1"
    dims: Optional[List[str]] = None
    time_window: Optional[str] = None
    filters: Optional[Dict[str, Any]] = None
    skip_cache: bool = False


class RawQueryRequest(BaseModel):
    """Request to execute raw SQL."""
    sql: str
    params: Optional[List[Any]] = None


class ImpactRequest(BaseModel):
    """Request for impact analysis."""
    object_type: str
    object_id: str


# =============================================================================
# Definition Registry Endpoints
# =============================================================================

@router.get("/definitions", response_model=DefinitionListResponse)
def list_definitions(
    tenant_id: str = Query(default="default"),
    pack: Optional[str] = None,
    kind: Optional[str] = None,
    status: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = Query(default=100, le=500),
    offset: int = Query(default=0, ge=0),
):
    """
    List definitions with optional filtering.

    - **pack**: Filter by pack (cfo, cto, coo, ceo)
    - **kind**: Filter by kind (metric, view)
    - **status**: Filter by status (draft, published, deprecated)
    - **search**: Search in ID and description
    """
    summaries, total = registry.list_definitions(
        tenant_id=tenant_id,
        pack=pack,
        kind=kind,
        status=status,
        search=search,
        limit=limit,
        offset=offset,
    )

    return DefinitionListResponse(
        definitions=[s.to_dict() for s in summaries],
        total=total,
        offset=offset,
        limit=limit,
    )


@router.get("/definitions/search")
def search_definitions(
    q: str = Query(..., min_length=2),
    tenant_id: str = Query(default="default"),
    limit: int = Query(default=10, le=50),
):
    """
    Search definitions by query string.

    Searches in ID, description, events, and dimensions.
    """
    results = registry.search_definitions(q, tenant_id, limit)
    return {"results": [r.to_dict() for r in results], "query": q}


@router.get("/definitions/{definition_id}")
def get_definition_detail(
    definition_id: str,
    tenant_id: str = Query(default="default"),
):
    """
    Get full details for a definition.

    Returns definition info, versions, events, bindings, and lineage.
    """
    detail = registry.get_definition_detail(definition_id, tenant_id)
    if not detail:
        raise HTTPException(status_code=404, detail=f"Definition '{definition_id}' not found")
    return detail.to_dict()


@router.post("/definitions")
def create_definition(
    request: CreateDefinitionRequest,
    tenant_id: str = Query(default="default"),
):
    """
    Create a new definition.

    Optionally include initial version spec.
    """
    definition, errors = registry.create_definition(
        definition_id=request.id,
        kind=request.kind,
        pack=request.pack,
        description=request.description,
        default_time_semantics=request.default_time_semantics,
        spec=request.spec,
        tenant_id=tenant_id,
    )

    if errors and not definition:
        raise HTTPException(status_code=400, detail={"errors": errors})

    return {
        "definition_id": definition.id if definition else request.id,
        "created": definition is not None,
        "warnings": errors if definition else [],
    }


@router.post("/definitions/{definition_id}/versions")
def create_version(
    definition_id: str,
    request: CreateVersionRequest,
    tenant_id: str = Query(default="default"),
):
    """
    Create a new version for a definition.
    """
    version, errors = registry.create_version(
        definition_id=definition_id,
        version=request.version,
        spec=request.spec,
        tenant_id=tenant_id,
    )

    if errors and not version:
        raise HTTPException(status_code=400, detail={"errors": errors})

    return {
        "version_id": version.id if version else None,
        "created": version is not None,
        "warnings": errors if version else [],
    }


@router.post("/definitions/{definition_id}/publish")
def publish_definition(
    definition_id: str,
    request: PublishRequest,
    tenant_id: str = Query(default="default"),
):
    """
    Publish a definition version.

    This deprecates any previously published version.
    """
    success, errors = registry.publish_definition(
        definition_id=definition_id,
        version=request.version,
        tenant_id=tenant_id,
    )

    if not success:
        raise HTTPException(status_code=400, detail={"errors": errors})

    return {
        "published": True,
        "definition_id": definition_id,
        "version": request.version,
        "warnings": errors,
    }


@router.post("/definitions/{definition_id}/deprecate")
def deprecate_definition(
    definition_id: str,
    request: PublishRequest,
    tenant_id: str = Query(default="default"),
):
    """
    Deprecate a definition version.
    """
    success, errors = registry.deprecate_definition(
        definition_id=definition_id,
        version=request.version,
        tenant_id=tenant_id,
    )

    if not success:
        raise HTTPException(status_code=400, detail={"errors": errors})

    return {
        "deprecated": True,
        "definition_id": definition_id,
        "version": request.version,
        "warnings": errors,
    }


# =============================================================================
# Catalog Stats Endpoints
# =============================================================================

@router.get("/catalog/stats")
def get_catalog_stats(tenant_id: str = Query(default="default")):
    """
    Get catalog statistics.

    Returns counts, coverage metrics, and breakdowns by pack/kind.
    """
    stats = registry.get_catalog_stats(tenant_id)
    return stats.to_dict()


@router.get("/catalog/packs")
def get_packs(tenant_id: str = Query(default="default")):
    """
    Get all packs with definition counts.
    """
    packs = registry.get_packs(tenant_id)
    return {"packs": packs}


# =============================================================================
# Consistency Validation Endpoints
# =============================================================================

@router.get("/consistency/check")
def run_consistency_check(tenant_id: str = Query(default="default")):
    """
    Run all consistency checks.

    Returns a full report with all issues found.
    """
    report = consistency.run_all_checks(tenant_id)
    return report.to_dict()


@router.get("/consistency/orphan-events")
def check_orphan_events(tenant_id: str = Query(default="default")):
    """Check for events without bindings."""
    result = consistency.check_orphan_events(tenant_id)
    return result.to_dict()


@router.get("/consistency/orphan-definitions")
def check_orphan_definitions(tenant_id: str = Query(default="default")):
    """Check for definitions with missing events or no versions."""
    result = consistency.check_orphan_definitions(tenant_id)
    return result.to_dict()


@router.get("/consistency/binding-coverage")
def check_binding_coverage(tenant_id: str = Query(default="default")):
    """Check if bindings provide adequate coverage."""
    result = consistency.check_binding_coverage(tenant_id)
    return result.to_dict()


# =============================================================================
# Lineage Endpoints
# =============================================================================

@router.get("/lineage/graph")
def get_lineage_graph(tenant_id: str = Query(default="default")):
    """
    Get the full lineage graph.

    Returns all nodes (definitions, events, bindings, entities, sources)
    and edges showing dependencies.
    """
    graph = lineage.build_full_graph(tenant_id)
    return graph.to_dict()


@router.get("/lineage/definition/{definition_id}")
def get_definition_lineage(
    definition_id: str,
    tenant_id: str = Query(default="default"),
):
    """
    Get lineage for a specific definition.

    Shows what events, bindings, and sources the definition depends on.
    """
    result = lineage.get_definition_lineage(definition_id, tenant_id)
    return result


@router.get("/lineage/event/{event_id}/consumers")
def get_event_consumers(
    event_id: str,
    tenant_id: str = Query(default="default"),
):
    """
    Get all definitions that consume a specific event.
    """
    consumers = lineage.get_event_consumers(event_id, tenant_id)
    return {"event_id": event_id, "consumers": consumers}


@router.post("/lineage/impact")
def analyze_impact(
    request: ImpactRequest,
    tenant_id: str = Query(default="default"),
):
    """
    Analyze the impact of removing or modifying an object.

    Shows what would be affected and severity level.
    """
    analysis = lineage.analyze_impact(
        object_type=request.object_type,
        object_id=request.object_id,
        tenant_id=tenant_id,
    )
    return analysis.to_dict()


@router.get("/lineage/upstream/{object_type}/{object_id}")
def get_upstream_dependencies(
    object_type: str,
    object_id: str,
    tenant_id: str = Query(default="default"),
    max_depth: int = Query(default=10, le=20),
):
    """
    Get upstream dependencies (what does this depend on?).
    """
    graph = lineage.get_upstream_dependencies(
        object_type=object_type,
        object_id=object_id,
        tenant_id=tenant_id,
        max_depth=max_depth,
    )
    return graph.to_dict()


@router.get("/lineage/downstream/{object_type}/{object_id}")
def get_downstream_dependencies(
    object_type: str,
    object_id: str,
    tenant_id: str = Query(default="default"),
    max_depth: int = Query(default=10, le=20),
):
    """
    Get downstream dependencies (what depends on this?).
    """
    graph = lineage.get_downstream_dependencies(
        object_type=object_type,
        object_id=object_id,
        tenant_id=tenant_id,
        max_depth=max_depth,
    )
    return graph.to_dict()


# =============================================================================
# Schema Validation Endpoints
# =============================================================================

@router.get("/schema/validate")
def validate_all_schemas(tenant_id: str = Query(default="default")):
    """
    Validate all schemas in the semantic layer.

    Checks events, bindings, and definition specs for schema violations.
    """
    result = schema_enforcer.validate_all(tenant_id)
    return result.to_dict()


@router.post("/schema/validate/event")
def validate_event_schema(
    event_id: str,
    schema_json: Dict[str, Any],
    time_semantics_json: Optional[Dict[str, Any]] = None,
):
    """
    Validate an event schema before creation.
    """
    valid, errors = schema_enforcer.validate_event(
        event_id=event_id,
        schema_json=schema_json,
        time_semantics_json=time_semantics_json or {},
    )
    return {"valid": valid, "errors": errors}


@router.post("/schema/validate/binding")
def validate_binding_schema(
    binding_id: str,
    canonical_event_id: str,
    mapping_json: Dict[str, str],
    tenant_id: str = Query(default="default"),
):
    """
    Validate a binding before creation.
    """
    valid, errors = schema_enforcer.validate_binding(
        binding_id=binding_id,
        canonical_event_id=canonical_event_id,
        mapping_json=mapping_json,
        tenant_id=tenant_id,
    )
    return {"valid": valid, "errors": errors}


@router.get("/schema/suggestions/{event_id}")
def get_schema_suggestions(
    event_id: str,
    tenant_id: str = Query(default="default"),
):
    """
    Get suggestions to improve an event schema.
    """
    suggestions = schema_enforcer.suggest_schema_improvements(event_id, tenant_id)
    return {"event_id": event_id, "suggestions": suggestions}


# =============================================================================
# Query Execution Endpoints
# =============================================================================

@router.post("/execute")
def execute_query(
    request: ExecuteQueryRequest,
    tenant_id: str = Query(default="default"),
):
    """
    Execute a query for a definition.

    Returns results from the configured data warehouse backend.
    """
    result = executor.execute_definition(
        definition_id=request.definition_id,
        version=request.version,
        requested_dims=request.dims,
        time_window=request.time_window,
        additional_filters=request.filters,
        tenant_id=tenant_id,
        skip_cache=request.skip_cache,
    )
    return result.to_dict()


@router.post("/execute/raw")
def execute_raw_query(
    request: RawQueryRequest,
    tenant_id: str = Query(default="default"),
):
    """
    Execute raw SQL (for advanced users).
    """
    result = executor.execute_raw_sql(
        sql=request.sql,
        params=request.params,
        tenant_id=tenant_id,
    )
    return result.to_dict()


@router.get("/execute/stats")
def get_execution_stats(tenant_id: str = Query(default="default")):
    """
    Get query execution statistics.
    """
    return executor.get_execution_stats(tenant_id)


@router.get("/execute/audit")
def get_execution_audit(
    tenant_id: str = Query(default="default"),
    definition_id: Optional[str] = None,
    limit: int = Query(default=100, le=500),
):
    """
    Get execution audit log.
    """
    audits = executor.get_audit_log(
        tenant_id=tenant_id,
        definition_id=definition_id,
        limit=limit,
    )
    return {"audits": [a.to_dict() for a in audits]}


@router.delete("/execute/cache")
def clear_execution_cache():
    """
    Clear the query result cache.
    """
    executor.clear_cache()
    return {"cleared": True}


# =============================================================================
# Proof Resolution Endpoints
# =============================================================================

@router.get("/proof/definition/{definition_id}")
def get_definition_proofs(
    definition_id: str,
    version: str = Query(default="v1"),
    tenant_id: str = Query(default="default"),
):
    """
    Get resolved proofs for a definition.

    Returns clickable URLs to source systems.
    """
    proofs = proof_resolver.resolve_definition_proofs(
        definition_id=definition_id,
        version=version,
        context={"definition_id": definition_id},
        tenant_id=tenant_id,
    )
    return {"definition_id": definition_id, "proofs": [p.to_dict() for p in proofs]}


@router.get("/proof/chain/{definition_id}")
def get_proof_chain(
    definition_id: str,
    version: str = Query(default="v1"),
    tenant_id: str = Query(default="default"),
):
    """
    Get complete proof chain from definition to sources.
    """
    chain = proof_resolver.build_proof_chain(
        definition_id=definition_id,
        version=version,
        tenant_id=tenant_id,
    )
    return chain.to_dict()


@router.get("/proof/coverage")
def get_proof_coverage(tenant_id: str = Query(default="default")):
    """
    Get proof coverage statistics.
    """
    return proof_resolver.get_proof_coverage(tenant_id)


# =============================================================================
# Health Check
# =============================================================================

@router.get("/health")
def registry_health():
    """
    Health check for registry services.
    """
    backend_ok = executor.test_backend_connection()
    return {
        "status": "healthy",
        "backend_connected": backend_ok,
        "services": {
            "registry": "ok",
            "consistency": "ok",
            "lineage": "ok",
            "schema_enforcer": "ok",
            "executor": "ok" if backend_ok else "degraded",
            "proof_resolver": "ok",
        },
    }
