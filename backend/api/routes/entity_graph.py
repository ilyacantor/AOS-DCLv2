"""Entity graph routes (ContextOS Gate 1B, Blueprint §7).

The ENTITY graph — persisted, bi-temporal, typed entity↔entity edges — as
distinct from the in-memory semantic/metadata graph served by
graph_traversal.py (/api/dcl/resolve, /graph/stats, /graph/path).

  POST /api/dcl/ingest-edges            — declared relationships in (provenance
                                          + identity enforced; constraint
                                          violations -> conflict register)
  GET  /api/dcl/graph/neighbors         — one node's edges (type filter, as-of)
  GET  /api/dcl/graph/subgraph          — the enterprise's nodes+edges (hero)
  GET  /api/dcl/graph/inspector         — one node: values + relationships (hero)
  GET  /api/dcl/graph/edge-types        — built-in + tenant types
  PUT  /api/dcl/graph/edge-types        — define a tenant type
  GET  /api/dcl/concepts/hierarchy      — concept tree (node or full view)
  PUT  /api/dcl/concepts/hierarchy      — tenant parent link

Reads hard-require tenant_id (the R3 read-surface convention). Node values on
subgraph/inspector are joined from semantic_triples by ONE deterministic rule:
a node (type, key) carries the active triples whose property == key and whose
concept ends in ".by_<type>" (the records-path/SE breakdown shapes); the
org_unit root carries the headline trio (workforce.headcount.total,
revenue.total, arr.ending), latest period each.
"""
from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field

from backend.api.routes.ingest_triples import _validate_uuid
from backend.core.db import get_connection
from backend.db.triple_store import TripleStore
from backend.db.edge_store import (
    EdgeContractError,
    EdgeIdentityError,
    get_edge_store,
    load_edge_types,
    put_edge_type,
)
from backend.engine.edge_derivation import EdgeDerivationError, derive_edges
from backend.registry import concept_hierarchy
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

router = APIRouter(tags=["Entity Graph"])

_triples = TripleStore()


def _resolve_read_tenant(tenant_id: Optional[str], entity_id: str) -> str:
    """Resolve the read tenant for an operator graph surface. Explicit tenant_id
    wins (validated); else the entity's tenant via tenant_runs — the operator
    surface pattern, same as conflicts.py / proposals.py (I2/I4: the operator
    selects an entity, never types a tenant; tenant_id is machine-only and
    never displayed). Loud 422/404 when neither resolves — no silent fallback
    (A1)."""
    if tenant_id:
        _validate_uuid(tenant_id, "tenant_id")
        return tenant_id
    if not entity_id or not entity_id.strip():
        raise HTTPException(
            status_code=422,
            detail="Provide entity_id (operator surface, tenant resolves from "
                   "tenant_runs) or tenant_id explicitly — identity is required (I2).",
        )
    try:
        return _triples.resolve_tenant_for_entity(entity_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


# ---------------------------------------------------------------------------
# Declared-edge ingest
# ---------------------------------------------------------------------------

class DeclaredEdge(BaseModel):
    src_type: str
    src_key: str
    edge_type: str
    dst_type: str
    dst_key: str
    properties: Optional[dict[str, Any]] = None
    valid_from: Optional[str] = None  # ISO timestamp; omitted -> now()


class IngestEdgesRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    tenant_id: str
    dcl_ingest_id: str = Field(..., alias="run_id")
    entity_id: str
    source_system: str
    pipe_id: Optional[str] = None
    source_run_tag: Optional[str] = None
    edges: list[DeclaredEdge]


class IngestEdgesResponse(BaseModel):
    dcl_ingest_id: str
    tenant_id: str
    entity_id: str
    edges_seen: int
    edges_written: int
    edges_superseded: int
    violations: list[dict]


@router.post("/api/dcl/ingest-edges", status_code=201, response_model=IngestEdgesResponse)
def ingest_edges(req: IngestEdgesRequest, replace: bool = Query(False)):
    """Declared entity↔entity relationships in. Identity pair + provenance are
    required (I2/I3 — 422 on missing). Constraint violations are excluded from
    the graph, flagged into the conflict register's structural class, and
    returned in the response — never silently dropped. ?replace=true is the
    clean re-run (every live edge for the entity superseded first)."""
    _validate_uuid(req.tenant_id, "tenant_id")
    _validate_uuid(req.dcl_ingest_id, "dcl_ingest_id")
    if not req.entity_id or not req.entity_id.strip():
        raise HTTPException(
            status_code=422,
            detail={"error": "ENTITY_ID_REQUIRED",
                    "message": "entity_id is required on the ingest-edges envelope (I2)."},
        )
    if not req.source_system or not req.source_system.strip():
        raise HTTPException(
            status_code=422,
            detail={"error": "PROVENANCE_INCOMPLETE",
                    "message": "source_system is required — every edge carries provenance (I3)."},
        )
    if not req.edges:
        raise HTTPException(
            status_code=400,
            detail={"error": "VALIDATION_FAILED", "message": "edges list must not be empty."},
        )
    if req.pipe_id:
        _validate_uuid(req.pipe_id, "pipe_id")

    payloads = [
        {
            "src_type": e.src_type, "src_key": e.src_key, "edge_type": e.edge_type,
            "dst_type": e.dst_type, "dst_key": e.dst_key,
            "properties": e.properties, "valid_from": e.valid_from,
            "source_system": req.source_system, "source_table": None, "source_field": None,
            "pipe_id": req.pipe_id, "dcl_ingest_id": req.dcl_ingest_id,
            "source_run_tag": req.source_run_tag,
            "confidence_score": 1.0, "confidence_tier": "exact",  # declared = asserted by the source
            "fabric_plane": None, "fabric_product": None,
            "derivation": "declared",
        }
        for e in req.edges
    ]

    try:
        result = get_edge_store().assert_edges(
            req.tenant_id, req.entity_id, payloads, replace=replace,
        )
    except EdgeIdentityError as e:
        raise HTTPException(status_code=422, detail={"error": "IDENTITY_REQUIRED", "message": str(e)})
    except EdgeContractError as e:
        raise HTTPException(status_code=422, detail={"error": "EDGE_CONTRACT", "message": str(e)})

    return IngestEdgesResponse(
        dcl_ingest_id=req.dcl_ingest_id,
        tenant_id=req.tenant_id,
        entity_id=req.entity_id,
        edges_seen=len(req.edges),
        edges_written=result.written,
        edges_superseded=result.superseded,
        violations=result.violations,
    )


# ---------------------------------------------------------------------------
# Stage-3 edge derivation
# ---------------------------------------------------------------------------

class DeriveEdgesRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    tenant_id: str
    entity_id: str
    dcl_ingest_id: str = Field(..., alias="run_id")


@router.post("/api/dcl/graph/derive", status_code=201)
def derive_graph_edges(req: DeriveEdgesRequest):
    """Stage-3 stitched graph: derive typed edges ACROSS resolved entities from
    Stage-2 current-state triples and persist them in entity_edges. Derives the
    cross-source comp-gap (BELOW_MARKET), the dominant exit driver (DRIVEN_BY),
    and the declared department->job_family resolution (RESOLVES_TO). Identity
    pair required (I2 — 422 on missing); one dcl_ingest_id stamps the batch (I1).
    A department with a comp_band median but no resolvable market median fails
    loud (A1 — 422 EDGE_DERIVATION), never a silent skip. Returns counts + edges.
    (No UI — that is Stage 4.)"""
    _validate_uuid(req.tenant_id, "tenant_id")
    _validate_uuid(req.dcl_ingest_id, "dcl_ingest_id")
    if not req.entity_id or not req.entity_id.strip():
        raise HTTPException(
            status_code=422,
            detail={"error": "ENTITY_ID_REQUIRED",
                    "message": "entity_id is required for edge derivation (I2)."},
        )
    try:
        result = derive_edges(req.tenant_id, req.entity_id, req.dcl_ingest_id)
    except EdgeDerivationError as e:
        raise HTTPException(status_code=422, detail={"error": "EDGE_DERIVATION", "message": str(e)})
    except (EdgeIdentityError, EdgeContractError) as e:
        raise HTTPException(status_code=422, detail={"error": "EDGE_CONTRACT", "message": str(e)})
    return result


# ---------------------------------------------------------------------------
# Node values (inspector/subgraph) — one deterministic join rule
# ---------------------------------------------------------------------------

_ORG_HEADLINE_CONCEPTS = ("workforce.headcount.total", "revenue.total", "arr.ending")


def _node_values(tenant_id: str, entity_id: str, node_type: str, node_key: str) -> dict[str, dict]:
    """Active triples for one node. Rule: property == node_key AND concept ends
    in '.by_<node_type>' (covers both records-path 'headcount.by_department'
    and SE families); org_unit nodes get the headline trio. Latest period per
    concept wins (lexicographic period sort — periods are 'YYYY-Qn')."""
    out: dict[str, dict] = {}
    with get_connection() as conn:
        with conn.cursor() as cur:
            if node_type == "org_unit":
                cur.execute(
                    "SELECT concept, property, value, period FROM semantic_triples "
                    "WHERE tenant_id = %s AND entity_id = %s AND is_active = true "
                    "AND concept = ANY(%s) ORDER BY concept, period",
                    [tenant_id, entity_id, list(_ORG_HEADLINE_CONCEPTS)],
                )
            else:
                cur.execute(
                    "SELECT concept, property, value, period FROM semantic_triples "
                    "WHERE tenant_id = %s AND entity_id = %s AND is_active = true "
                    "AND property = %s AND concept LIKE %s ORDER BY concept, period",
                    [tenant_id, entity_id, node_key, f"%.by_{node_type}"],
                )
            for concept, prop, value, period in cur.fetchall():
                cur_best = out.get(concept)
                if cur_best is None or (period or "") >= (cur_best.get("period") or ""):
                    out[concept] = {"value": value, "property": prop, "period": period}
    return out


# ---------------------------------------------------------------------------
# Traversal reads
# ---------------------------------------------------------------------------

@router.get("/api/dcl/graph/neighbors")
def graph_neighbors(
    tenant_id: str,
    entity_id: str,
    node_type: str,
    node_key: str,
    edge_type: Optional[str] = Query(None),
    direction: str = Query("both", pattern="^(out|in|both)$"),
    as_of: Optional[str] = Query(None, description="ISO timestamp — knowledge-time as-of read"),
    limit: int = Query(500, ge=1, le=5000),
):
    """Edges touching one node + the neighbor node list, with type/direction
    filters and bi-temporal as-of support."""
    _validate_uuid(tenant_id, "tenant_id")
    try:
        edges = get_edge_store().get_neighbors(
            tenant_id, entity_id, node_type, node_key,
            edge_type=edge_type, direction=direction, as_of=as_of, limit=limit,
        )
    except EdgeIdentityError as e:
        raise HTTPException(status_code=422, detail={"error": "IDENTITY_REQUIRED", "message": str(e)})
    except EdgeContractError as e:
        raise HTTPException(status_code=422, detail={"error": "EDGE_CONTRACT", "message": str(e)})

    neighbors: dict[tuple, dict] = {}
    for e in edges:
        for t, k in ((e["src_type"], e["src_key"]), (e["dst_type"], e["dst_key"])):
            if (t, k) != (node_type, node_key):
                neighbors.setdefault((t, k), {"node_type": t, "node_key": k})
    return {
        "tenant_id": tenant_id, "entity_id": entity_id,
        "node": {"node_type": node_type, "node_key": node_key},
        "as_of": as_of, "edge_count": len(edges),
        "edges": edges, "neighbors": list(neighbors.values()),
    }


@router.get("/api/dcl/graph/subgraph")
def graph_subgraph(
    entity_id: str,
    tenant_id: Optional[str] = Query(None, description="Tenant UUID — omit on operator surfaces; resolves from entity_id via tenant_runs (I4)."),
    edge_types: Optional[str] = Query(None, description="Comma-separated edge-type filter"),
    as_of: Optional[str] = Query(None),
    include_values: bool = Query(True),
    limit: int = Query(2000, ge=1, le=10000),
):
    """The enterprise's typed-edge subgraph — the platform-site hero read.
    nodes[] carry type/key/label (+ values when include_values), edges[] carry
    type + properties + provenance + temporal columns. tenant_id is optional on
    the operator surface: when omitted it resolves from entity_id via tenant_runs
    (I4 — the operator picks an entity, never types a tenant)."""
    tenant_id = _resolve_read_tenant(tenant_id, entity_id)
    types = [t.strip() for t in edge_types.split(",") if t.strip()] if edge_types else None
    try:
        sub = get_edge_store().get_subgraph(
            tenant_id, entity_id, edge_types=types, as_of=as_of, limit=limit,
        )
    except EdgeIdentityError as e:
        raise HTTPException(status_code=422, detail={"error": "IDENTITY_REQUIRED", "message": str(e)})

    nodes = sub["nodes"]
    for n in nodes:
        n["label"] = n["node_key"] if n["node_type"] != "org_unit" else entity_id
        if include_values:
            n["values"] = _node_values(tenant_id, entity_id, n["node_type"], n["node_key"])

    by_type: dict[str, int] = {}
    for e in sub["edges"]:
        by_type[e["edge_type"]] = by_type.get(e["edge_type"], 0) + 1

    return {
        "tenant_id": tenant_id, "entity_id": entity_id, "as_of": as_of,
        "counts": {"nodes": len(nodes), "edges": len(sub["edges"]), "by_type": by_type},
        "nodes": nodes, "edges": sub["edges"],
    }


@router.get("/api/dcl/graph/inspector")
def graph_inspector(
    tenant_id: str,
    entity_id: str,
    node_type: str,
    node_key: str,
    as_of: Optional[str] = Query(None),
):
    """One node for the hero inspector panel: type, domain, values, and its
    typed relationship list."""
    _validate_uuid(tenant_id, "tenant_id")
    try:
        edges = get_edge_store().get_neighbors(
            tenant_id, entity_id, node_type, node_key, as_of=as_of,
        )
    except EdgeIdentityError as e:
        raise HTTPException(status_code=422, detail={"error": "IDENTITY_REQUIRED", "message": str(e)})

    values = _node_values(tenant_id, entity_id, node_type, node_key)
    domains = sorted({c.split(".")[0] for c in values}) if values else []
    relationships = [
        {
            "edge_type": e["edge_type"],
            "direction": "out" if (e["src_type"], e["src_key"]) == (node_type, node_key) else "in",
            "other": ({"node_type": e["dst_type"], "node_key": e["dst_key"]}
                      if (e["src_type"], e["src_key"]) == (node_type, node_key)
                      else {"node_type": e["src_type"], "node_key": e["src_key"]}),
            "derivation": e["derivation"],
            "source_system": e["source_system"],
            "confidence_tier": e["confidence_tier"],
            "ingested_at": e["ingested_at"],
        }
        for e in edges
    ]
    return {
        "tenant_id": tenant_id, "entity_id": entity_id, "as_of": as_of,
        "node": {"node_type": node_type, "node_key": node_key,
                 "label": node_key if node_type != "org_unit" else entity_id},
        "domains": domains,
        "values": values,
        "relationships": relationships,
        "relationship_count": len(relationships),
    }


# ---------------------------------------------------------------------------
# Edge types
# ---------------------------------------------------------------------------

class EdgeTypeRequest(BaseModel):
    tenant_id: str
    edge_type: str
    description: str
    cardinality: str = "many_to_many"
    allowed_pairs: Optional[list[list[str]]] = None


@router.get("/api/dcl/graph/edge-types")
def get_graph_edge_types(tenant_id: str):
    """Built-in + tenant-defined edge types (tenant rows overlay built-ins)."""
    _validate_uuid(tenant_id, "tenant_id")
    return {"tenant_id": tenant_id, "edge_types": load_edge_types(tenant_id)}


@router.put("/api/dcl/graph/edge-types", status_code=201)
def put_graph_edge_type(req: EdgeTypeRequest):
    """Define (or update) a tenant edge type with its constraint rules."""
    _validate_uuid(req.tenant_id, "tenant_id")
    try:
        return put_edge_type(
            req.tenant_id, req.edge_type, req.description,
            cardinality=req.cardinality, allowed_pairs=req.allowed_pairs,
        )
    except EdgeContractError as e:
        raise HTTPException(status_code=422, detail={"error": "EDGE_TYPE_CONTRACT", "message": str(e)})


# ---------------------------------------------------------------------------
# Concept hierarchy
# ---------------------------------------------------------------------------

class HierarchyLinkRequest(BaseModel):
    tenant_id: str
    concept: str
    parent_concept: str


@router.get("/api/dcl/concepts/hierarchy")
def get_concepts_hierarchy(
    tenant_id: str,
    concept: Optional[str] = Query(None),
    include_descendants: bool = Query(False),
):
    """The concept tree: one node's parent/children, the full domain->root map,
    or (include_descendants) the read-expansion a parent resolves to."""
    _validate_uuid(tenant_id, "tenant_id")
    if concept and include_descendants:
        return {
            "tenant_id": tenant_id, "concept": concept,
            "expansion": concept_hierarchy.expand_for_read(tenant_id, concept),
        }
    return {"tenant_id": tenant_id, **concept_hierarchy.hierarchy_view(tenant_id, concept)}


@router.put("/api/dcl/concepts/hierarchy", status_code=201)
def put_concepts_hierarchy(req: HierarchyLinkRequest):
    """Define (or move) a tenant parent link in the concept hierarchy."""
    _validate_uuid(req.tenant_id, "tenant_id")
    try:
        return concept_hierarchy.put_link(req.tenant_id, req.concept, req.parent_concept)
    except ValueError as e:
        raise HTTPException(status_code=422, detail={"error": "HIERARCHY_CONTRACT", "message": str(e)})
