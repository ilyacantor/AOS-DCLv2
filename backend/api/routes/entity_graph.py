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
from backend.engine.edge_derivation import (
    EdgeDerivationError,
    _EXIT_CONCEPT_TMPL,
    _HEADLINE_PERIOD,
    derive_edges,
)
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
# Edge provenance — drill from a synthesized edge to its source records
# ---------------------------------------------------------------------------


def _load_live_edge(
    tenant_id: str, entity_id: str,
    src_type: str, src_key: str, edge_type: str, dst_type: str, dst_key: str,
) -> dict:
    """The one live entity_edges row at this exact coordinate (its properties +
    derivation + provenance). 404 — never an empty 200 — when the coordinate
    names no live edge (A1: a non-existent edge is a provenance gap surfaced
    loud, not a silent empty)."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT properties, derivation, source_system, confidence_tier, confidence_score "
                "FROM entity_edges "
                "WHERE tenant_id = %s AND entity_id = %s AND is_active = true "
                "AND src_type = %s AND src_key = %s AND edge_type = %s "
                "AND dst_type = %s AND dst_key = %s",
                [tenant_id, entity_id, src_type, src_key, edge_type, dst_type, dst_key],
            )
            rows = cur.fetchall()
    if not rows:
        raise HTTPException(
            status_code=404,
            detail={
                "error": "EDGE_NOT_FOUND",
                "message": (
                    f"No live {edge_type} edge {src_type}:{src_key} -> {dst_type}:{dst_key} "
                    f"for entity {entity_id!r} — cannot reveal source records for an edge that "
                    f"does not exist. Check the edge coordinate against the rendered graph."
                ),
            },
        )
    if len(rows) > 1:
        # Bi-temporal store: at most one row is_active per coordinate. >1 is a
        # store-integrity break — surface it, do not pick one silently (A1).
        raise HTTPException(
            status_code=500,
            detail={
                "error": "EDGE_NOT_UNIQUE",
                "message": (
                    f"{len(rows)} live {edge_type} edges at {src_type}:{src_key} -> "
                    f"{dst_type}:{dst_key} — the active set must be unique per coordinate; "
                    f"refusing to synthesize provenance from an ambiguous edge."
                ),
            },
        )
    props, derivation, source_system, tier, score = rows[0]
    return {
        "properties": props or {},
        "derivation": derivation,
        "source_system": source_system,
        "confidence_tier": tier,
        "confidence_score": float(score) if score is not None else None,
    }


def _consumed_concepts(
    edge_type: str, properties: dict, src_key: str, dst_key: str,
) -> list[str]:
    """The concepts an edge consumed, as the derivation recorded them.

    BELOW_MARKET / RESOLVES_TO carry an explicit `consumed` list (the derivation
    stamps it). DRIVEN_BY does not — its inputs are the four
    workforce.exit_theme.<reason>.by_department counts whose ranking produced the
    edge; reconstruct that list from the `breakdown` keys it stamped (those keys
    ARE the reasons it ranked). An edge with neither is a provenance gap — raise
    (A1), never return an empty list that would read as 'no sources'."""
    consumed = properties.get("consumed")
    if isinstance(consumed, list) and consumed:
        return [str(c) for c in consumed]

    if edge_type == "DRIVEN_BY":
        breakdown = properties.get("breakdown")
        if isinstance(breakdown, dict) and breakdown:
            return [_EXIT_CONCEPT_TMPL.format(reason=r) for r in breakdown]

    raise HTTPException(
        status_code=422,
        detail={
            "error": "PROVENANCE_UNRESOLVABLE",
            "message": (
                f"{edge_type} edge {src_key} -> {dst_key} declares no `consumed` concepts "
                f"(and no breakdown to reconstruct them from); its source records cannot be "
                f"identified. The derivation must stamp what it consumed (I3) — fix the "
                f"derivation, do not return an empty audit trail."
            ),
        },
    )


def _provenance_property_for(concept: str, src_type: str, src_key: str,
                             dst_type: str, dst_key: str) -> str:
    """Which property the consumed concept keys on for THIS edge. A `.by_<src_type>`
    concept (e.g. comp_band.median.by_department on a department-sourced edge) keys
    on src_key; a `.by_<dst_type>` concept (market_benchmark.median.by_job_family)
    keys on dst_key. Neither ⇒ the concept does not key on either endpoint of this
    edge — surface loud (A1), do not guess a property."""
    if concept.endswith(f".by_{src_type}"):
        return src_key
    if concept.endswith(f".by_{dst_type}"):
        return dst_key
    raise HTTPException(
        status_code=422,
        detail={
            "error": "PROVENANCE_KEY_UNRESOLVABLE",
            "message": (
                f"consumed concept {concept!r} does not end in '.by_{src_type}' or "
                f"'.by_{dst_type}' — cannot determine which property keys the source record "
                f"for edge {src_type}:{src_key} -> {dst_type}:{dst_key}. No guess is made."
            ),
        },
    )


def _source_record(tenant_id: str, entity_id: str, concept: str, property: str,
                   period: str) -> Optional[dict]:
    """The one active source triple for (concept, property) at the period the
    derivation read, with full provenance. None when absent (the caller turns a
    None into a loud provenance gap — A1). Latest-id tiebreak mirrors
    mcp_provenance_lookup so a redelivered batch returns one deterministic row."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, concept, property, value, source_system, source_field, "
                "       confidence_score, confidence_tier, ingested_at, normalization_metadata, period "
                "FROM semantic_triples "
                "WHERE tenant_id = %s AND entity_id = %s AND concept = %s AND property = %s "
                "AND period = %s AND is_active = true "
                "ORDER BY created_at DESC, id DESC LIMIT 1",
                [tenant_id, entity_id, concept, property, period],
            )
            r = cur.fetchone()
    if r is None:
        return None
    return {
        "concept": r[1],
        "property": r[2],
        "value": r[3],
        "source_system": r[4],
        "source_field": r[5],
        "confidence_score": float(r[6]) if r[6] is not None else None,
        "confidence_tier": r[7],
        "ingested_at": r[8].isoformat() if r[8] else None,
        "triple_id": str(r[0]),
        "normalization_metadata": r[9],
        "period": r[10],
    }


# Edge properties that are the SYNTHESIZED output (the derived facts the sources
# do NOT hold) vs the provenance/bookkeeping fields. The reveal returns the
# synthesized block so the operator sees what the audit trail must account for.
_PROVENANCE_BOOKKEEPING_KEYS = frozenset({"consumed", "source", "breakdown"})


@router.get("/api/dcl/graph/edge-provenance")
def graph_edge_provenance(
    entity_id: str,
    src_type: str,
    src_key: str,
    edge_type: str,
    dst_type: str,
    dst_key: str,
    tenant_id: Optional[str] = Query(
        None,
        description="Tenant UUID — omit on operator surfaces; resolves from entity_id via tenant_runs (I4).",
    ),
):
    """Drill from a SYNTHESIZED edge to the SOURCE RECORDS it was derived from —
    the audit trail behind the derived fact (Blueprint §13). For the engineering
    BELOW_MARKET edge this returns the two records the 13.16% gap was synthesized
    from: comp_band engineering 165000 (workday_hr) and market_benchmark
    software_engineering 190000 (radford_comp) — each with full provenance
    (triple_id, source_field, confidence, ingested_at, normalization_metadata).

    The edge's `consumed` concepts name the inputs; each is matched to its source
    record by the property the concept keys on (a `.by_<src_type>` concept on
    src_key, a `.by_<dst_type>` concept on dst_key), read at the period the
    derivation consumed. A consumed concept that yields NO source record is a
    provenance gap surfaced loud (422) — never silently dropped (A1). A
    non-existent edge coordinate is a 404, never an empty 200.

    tenant_id is optional on the operator surface (resolves from entity_id, I4)."""
    tenant_id = _resolve_read_tenant(tenant_id, entity_id)

    edge = _load_live_edge(
        tenant_id, entity_id, src_type, src_key, edge_type, dst_type, dst_key,
    )
    properties = edge["properties"]
    consumed = _consumed_concepts(edge_type, properties, src_key, dst_key)

    sources: list[dict] = []
    gaps: list[dict] = []
    for concept in consumed:
        if ".resolution." in concept:
            # A structural join key (e.g. comp_band.resolution.department_to_job_family):
            # the derivation READ it to connect the edge's endpoints (department ->
            # job_family), but it is not a VALUE source record the synthesized gap was
            # computed from — the gap arithmetic uses only the two medians, and a
            # resolution concept keys on neither endpoint's median (its value is the
            # external key, not a number). It stays listed in `consumed` (the full
            # input set) but is not a revealed source record (Blueprint §13: reveal
            # the value records the gap came from).
            continue
        prop = _provenance_property_for(concept, src_type, src_key, dst_type, dst_key)
        rec = _source_record(tenant_id, entity_id, concept, prop, _HEADLINE_PERIOD)
        if rec is None:
            gaps.append({"concept": concept, "property": prop, "period": _HEADLINE_PERIOD})
        else:
            sources.append(rec)

    if gaps:
        # A consumed input has no surviving source record — the edge claims a
        # provenance it cannot back. Surface it loud (A1); the synthesized fact
        # is not honestly auditable without every input it consumed.
        raise HTTPException(
            status_code=422,
            detail={
                "error": "PROVENANCE_GAP",
                "message": (
                    f"{edge_type} edge {src_type}:{src_key} -> {dst_type}:{dst_key} consumed "
                    f"{len(consumed)} concept(s) but {len(gaps)} have no active source record "
                    f"at period {_HEADLINE_PERIOD}: {gaps}. The audit trail is incomplete — "
                    f"refusing to present a partial provenance as complete."
                ),
            },
        )

    synthesized = {
        k: v for k, v in properties.items() if k not in _PROVENANCE_BOOKKEEPING_KEYS
    }

    return {
        "tenant_id": tenant_id,
        "entity_id": entity_id,
        "edge": {
            "src": {"node_type": src_type, "node_key": src_key},
            "edge_type": edge_type,
            "dst": {"node_type": dst_type, "node_key": dst_key},
            "derivation": edge["derivation"],
            "source_system": edge["source_system"],
            "confidence_tier": edge["confidence_tier"],
            "confidence_score": edge["confidence_score"],
        },
        "consumed": consumed,
        "sources": sources,
        "synthesized": synthesized,
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
