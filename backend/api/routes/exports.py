"""Standards-track export routes (ContextOS Gate 2C). Read-only, tenant-scoped.

  GET /api/dcl/export/graph.ttl     — the tenant's graph as Turtle (text/turtle)
  GET /api/dcl/export/graph.jsonld  — the same graph as JSON-LD with a compact
                                      @context (application/ld+json)
  GET /api/dcl/export/metrics.yaml  — the metric catalog as MetricFlow-spec
                                      multi-document YAML (application/x-yaml)

Contract (campaign-pinned):
  - tenant_id REQUIRED on all three — missing 422 (FastAPI), malformed 422
    loud (I2; this surface deliberately answers 422, not the legacy ingest
    helper's 400);
  - entity_id optional filter — scopes the graph walk and the existence check;
  - a tenant (or tenant+entity) with NO live entity graph gets a loud 404
    naming the scope on all three endpoints — never an empty or hollow file;
  - export bodies carry NO tenant UUID and NO run_id wording (I1/I2) — edge
    ingest ids surface as dclIngestId;
  - every response is a downloadable attachment;
  - assembled fresh per request — no caching layers.

The retained JSON export (/api/dcl/semantic-export) is a separate, untouched
surface — these routes only read the same loaded catalog object.
"""

import uuid
from typing import Optional

from fastapi import APIRouter, HTTPException, Response

from backend.api.semantic_export import PUBLISHED_METRICS
from backend.engine.metricflow_export import build_metricflow_yaml
from backend.engine.rdf_export import (
    GraphExportEmpty,
    build_export_graph,
    jsonld_context,
    list_graph_entities,
)
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

router = APIRouter(tags=["Exports"])


def _require_tenant_uuid_422(tenant_id: str) -> None:
    """Malformed tenant_id is an identity failure on this read surface: 422 (I2)."""
    try:
        uuid.UUID(str(tenant_id))
    except (ValueError, AttributeError, TypeError):
        raise HTTPException(
            status_code=422,
            detail={
                "error": "TENANT_ID_INVALID",
                "message": f"tenant_id must be a valid UUID. Got: {tenant_id!r}",
            },
        )


def _graph_or_404(tenant_id: str, entity_id: Optional[str]):
    try:
        return build_export_graph(tenant_id, entity_id)
    except GraphExportEmpty as exc:
        raise HTTPException(
            status_code=404,
            detail={"error": "NO_GRAPH_FOR_TENANT", "message": str(exc)},
        )


def _attachment(body: str, media_type: str, filename: str) -> Response:
    return Response(
        content=body,
        media_type=media_type,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/api/dcl/export/graph.ttl")
def export_graph_turtle(tenant_id: str, entity_id: Optional[str] = None):
    """The tenant's graph — ontology classes, edge-type vocabulary, entity
    individuals, typed edges with OWL axiom-annotated provenance — as Turtle."""
    _require_tenant_uuid_422(tenant_id)
    graph = _graph_or_404(tenant_id, entity_id)
    body = graph.serialize(format="turtle")
    return _attachment(body, "text/turtle", "graph.ttl")


@router.get("/api/dcl/export/graph.jsonld")
def export_graph_jsonld(tenant_id: str, entity_id: Optional[str] = None):
    """The same graph as JSON-LD, compacted against a @context that maps the
    urn:dcl:* namespaces and every urn:dcl:meta: annotation term."""
    _require_tenant_uuid_422(tenant_id)
    graph = _graph_or_404(tenant_id, entity_id)
    body = graph.serialize(format="json-ld", context=jsonld_context(), indent=2)
    return _attachment(body, "application/ld+json", "graph.jsonld")


@router.get("/api/dcl/export/metrics.yaml")
def export_metrics_yaml(tenant_id: str, entity_id: Optional[str] = None):
    """The DCL metric catalog as MetricFlow-spec YAML (dbt-semantic-interfaces
    parseable). Tenant-scoped like the graph exports: a tenant with no graph
    has nothing to run metrics over, so the same loud 404 applies."""
    _require_tenant_uuid_422(tenant_id)
    try:
        list_graph_entities(tenant_id, entity_id)  # existence check only
    except GraphExportEmpty as exc:
        raise HTTPException(
            status_code=404,
            detail={"error": "NO_GRAPH_FOR_TENANT", "message": str(exc)},
        )
    body = build_metricflow_yaml(PUBLISHED_METRICS)
    return _attachment(body, "application/x-yaml", "metrics.yaml")
