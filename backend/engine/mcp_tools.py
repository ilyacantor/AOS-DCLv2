"""
Shared MCP tool implementations (Plan B WP5, §11.4).

Single source of truth for the external tool surface (TOOL_SCHEMAS /
PUBLIC_TOOLS — the §11.4 base tools plus the Gate 1A conflict pair, the
Gate 1B traversal, and the Gate 2A trace_query). Both the legacy HTTP
path (backend/api/mcp_server.py) and the real wire-protocol MCP server
(backend/api/mcp_server_real.py) call these functions.

Every function takes tenant_id as the first argument. tenant_id is derived
from the caller's verified token — it must NEVER be taken from tool
arguments. Per I6, identity is passed through, not computed.

Data access goes through backend.db.triple_store.TripleStore (the
whitelisted data layer). This module does NOT issue raw SQL — that's
the store's concern, not the tool's.
"""

from __future__ import annotations

from typing import Any

from backend.db.triple_store import TripleStore
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

_store = TripleStore()


class MCPToolError(Exception):
    """Raised when a tool cannot satisfy a request. Caller turns into an
    informative MCP error response."""


# =============================================================================
# query_triples — tenant-scoped triple query
# =============================================================================


def tool_query_triples(
    tenant_id: str,
    *,
    domain: str | None = None,
    concept: str | None = None,
    entity_id: str | None = None,
    period: str | None = None,
    limit: int = 100,
    active_only: bool = True,
    include_descendants: bool = False,
) -> list[dict]:
    """Return triples filtered by the calling tenant.

    The tenant_id filter is non-overridable. The 'domain' filter matches
    the root segment of concept names (e.g. domain='cloud_spend' matches
    'cloud_spend.amount_billed', 'cloud_spend.aws_total'). 'concept' is
    either a full dotted path (exact match) or an unqualified catalog id
    (what concept_lookup returns), which matches the exact id, its
    namespace (id.*, e.g. 'revenue' -> 'revenue.total'), and every
    domain-qualified instance (*.id, e.g. 'net_income' ->
    'pnl.net_income') so the two tools compose.

    include_descendants (Gate 1B): expand 'concept' through the concept
    hierarchy — a domain expands to every root beneath it, a root or dotted
    concept to itself plus its subtree (one SQL pass, deterministic order).

    The response contains a per-triple namespaced ingest identifier
    (dcl_ingest_id) — never a bare run_id, per I1.
    """
    if not tenant_id:
        raise MCPToolError(
            "query_triples requires tenant_id — caller's token did not "
            "carry one (I2 violation)."
        )
    if domain is None and concept is None:
        raise MCPToolError(
            "query_triples requires at least one of 'domain' or 'concept' "
            "— refusing to dump a full tenant table."
        )

    if include_descendants:
        from backend.registry.concept_hierarchy import expand_for_read
        parent = concept or domain
        expansion = expand_for_read(tenant_id, parent)
        raw = _store.mcp_query_triples_expanded(
            tenant_id,
            exacts=expansion["exact"],
            prefixes=expansion["prefixes"],
            entity_id=entity_id,
            period=period,
            limit=limit,
            active_only=active_only,
        )
    else:
        raw = _store.mcp_query_triples(
            tenant_id,
            domain=domain,
            concept=concept,
            entity_id=entity_id,
            period=period,
            limit=limit,
            active_only=active_only,
        )
    # Rename bare run_id → dcl_ingest_id per I1 before exposing, and expose
    # the row id under the provenance tool's vocabulary (triple_id) so a
    # consumer can drill THIS exact triple — composite (concept, entity,
    # period) lookups are ambiguous across properties (I3).
    out: list[dict] = []
    for row in raw:
        d = dict(row)
        if "run_id" in d:
            d["dcl_ingest_id"] = d.pop("run_id")
        if "id" in d:
            d["triple_id"] = d["id"]
        out.append(d)
    return out


# =============================================================================
# list_domains — distinct concept roots for the tenant
# =============================================================================


def tool_list_domains(tenant_id: str, entity_id: str | None = None) -> list[dict]:
    """Return distinct concept-root domains with triple counts for tenant,
    optionally scoped to one entity (the selected run)."""
    if not tenant_id:
        raise MCPToolError(
            "list_domains requires tenant_id — caller's token did not "
            "carry one (I2 violation)."
        )
    return _store.mcp_list_domains(tenant_id, entity_id)


def tool_list_runs(tenant_id: str) -> list[dict]:
    """Return the current runs (snapshots) for the tenant — one per (entity,
    active run), newest first. The NLQ-snapshot equivalent for MCP consumers:
    dcl_ingest_id, entity_id, triple_count, created_at. The bare run_id is
    renamed to the namespaced dcl_ingest_id before exposure (I1)."""
    if not tenant_id:
        raise MCPToolError(
            "list_runs requires tenant_id — caller's token did not "
            "carry one (I2 violation)."
        )
    out: list[dict] = []
    for row in _store.mcp_list_runs(tenant_id):
        d = dict(row)
        if "run_id" in d:
            d["dcl_ingest_id"] = d.pop("run_id")
        out.append(d)
    return out


# =============================================================================
# concept_lookup — ontology lookup (tenant-agnostic but audited)
# =============================================================================


def tool_concept_lookup(tenant_id: str, query: str) -> dict:
    """Look up an ontology concept by name or alias. The ontology is
    shared across tenants; the wire-protocol MCP server still requires
    tenant_id for audit observability. The legacy HTTP path may invoke
    with tenant_id='' — that's acceptable here because no tenant-scoped
    SQL runs in this function."""
    # tenant_id captured by caller for audit; not validated here.
    if not query:
        raise MCPToolError("concept_lookup requires a non-empty 'query'.")
    from backend.api.semantic_export import resolve_metric, resolve_entity

    metric = resolve_metric(query)
    if metric:
        return {
            "type": "metric",
            "id": metric.id,
            "name": metric.name,
            "definition": metric.description,
            "aliases": list(metric.aliases),
            "pack": metric.pack.value,
            "allowed_dims": list(metric.allowed_dims),
            "allowed_grains": [g.value for g in metric.allowed_grains],
        }
    entity = resolve_entity(query)
    if entity:
        return {
            "type": "entity",
            "id": entity.id,
            "name": entity.name,
            "definition": entity.description,
            "aliases": list(entity.aliases),
        }
    raise MCPToolError(f"No concept found for '{query}'.")


# =============================================================================
# semantic_export — full ontology catalog
# =============================================================================


def tool_semantic_export(tenant_id: str) -> dict:
    """Return the full semantic catalog. Shared ontology; tenant_id is
    captured by caller for audit but not validated here (the ontology
    is the same for every tenant)."""
    from backend.api.semantic_export import get_semantic_export

    return get_semantic_export().model_dump()


# =============================================================================
# provenance — source trace for a triple
# =============================================================================


def tool_provenance(
    tenant_id: str,
    *,
    triple_id: str | None = None,
    concept: str | None = None,
    property: str | None = None,
    entity_id: str | None = None,
    period: str | None = None,
) -> dict[str, Any]:
    """Return source_system / source_field / pipe_id / confidence_score
    for a triple. Identify the triple by triple_id (preferred) or by
    (concept, entity_id, period) coordinates within the caller's tenant.

    Response uses dcl_ingest_id, never a bare run_id (I1)."""
    if not tenant_id:
        raise MCPToolError(
            "provenance requires tenant_id — caller's token did not "
            "carry one (I2 violation)."
        )
    if triple_id is None and concept is None:
        raise MCPToolError(
            "provenance requires either 'triple_id' or 'concept' "
            "(with optional entity_id/period)."
        )
    result = _store.mcp_provenance_lookup(
        tenant_id,
        triple_id=triple_id,
        concept=concept,
        property=property,
        entity_id=entity_id,
        period=period,
    )
    if result is None:
        raise MCPToolError(
            "provenance: no matching triple found for the given "
            "selector within the calling tenant."
        )
    return result


# =============================================================================
# conflict_query — Conflict Register reads (Gate 1A)
# =============================================================================


def tool_conflict_query(
    tenant_id: str,
    *,
    entity_id: str | None = None,
    status: str | None = None,
    conflict_type: str | None = None,
    concept: str | None = None,
    conflict_class: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    """Query the Conflict Register for the caller's tenant. Read-only."""
    if not tenant_id:
        raise MCPToolError(
            "conflict_query requires tenant_id — caller's token did not "
            "carry one (I2 violation)."
        )
    from backend.db.conflict_store import ConflictStore
    rows, total = ConflictStore().list_conflicts(
        tenant_id, entity_id=entity_id, status=status,
        conflict_type=conflict_type, concept=concept,
        conflict_class=conflict_class, limit=limit,
    )
    return {"tenant_id": str(tenant_id), "conflicts": rows, "total_count": total}


# =============================================================================
# reconciliation_recommend — recommendation + precedent for a conflict (Gate 1A)
# =============================================================================


def tool_reconciliation_recommend(
    tenant_id: str,
    *,
    conflict_id: str | None = None,
    conflict_class: str | None = None,
) -> dict[str, Any]:
    """Return the recommended disposition and precedent chain for a conflict
    (by conflict_id) or a conflict class. Proposal only — HITL decides."""
    if not tenant_id:
        raise MCPToolError(
            "reconciliation_recommend requires tenant_id — caller's token "
            "did not carry one (I2 violation)."
        )
    if conflict_id is None and conflict_class is None:
        raise MCPToolError(
            "reconciliation_recommend requires 'conflict_id' or 'conflict_class'."
        )
    from backend.db.conflict_store import ConflictStore
    store = ConflictStore()
    out: dict[str, Any] = {"tenant_id": str(tenant_id)}
    if conflict_id is not None:
        row = store.get_conflict(tenant_id, conflict_id)
        if row is None:
            raise MCPToolError(
                f"reconciliation_recommend: conflict {conflict_id!r} not found "
                f"within the calling tenant."
            )
        out.update({
            "conflict_id": row["conflict_id"],
            "conflict_class": row["conflict_class"],
            "status": row["status"],
            "recommended": row.get("recommended"),
            "root_cause_explanation": row.get("root_cause_explanation"),
            "dispositions": store.list_dispositions(tenant_id, conflict_id),
        })
        conflict_class = row["conflict_class"]
    out["precedent"] = store.latest_precedent(tenant_id, conflict_class)
    out.setdefault("conflict_class", conflict_class)
    return out


# =============================================================================
# trace_query — unified decision-trace reads (Gate 2A, §9)
# =============================================================================


def tool_trace_query(
    tenant_id: str,
    *,
    entity_id: str | None = None,
    concept: str | None = None,
    agent: str | None = None,
    decision_type: str | None = None,
    trace_type: str | None = None,
    conflict_class: str | None = None,
    period: str | None = None,
    since: str | None = None,
    until: str | None = None,
    as_of: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    """Query the unified decision-trace view (mcp_call + conflict_disposition
    + er_confirmation) for the caller's tenant. Read-only. as_of is the
    knowledge-time read (ingested_at <= as_of; traces are never superseded)."""
    if not tenant_id:
        raise MCPToolError(
            "trace_query requires tenant_id — caller's token did not "
            "carry one (I2 violation)."
        )
    from backend.db.trace_store import TraceStore
    try:
        rows, total = TraceStore().search_traces(
            tenant_id, entity_id=entity_id, concept=concept, agent=agent,
            decision_type=decision_type, trace_type=trace_type,
            conflict_class=conflict_class, period=period,
            since=since, until=until, as_of=as_of, limit=limit,
        )
    except ValueError as e:
        raise MCPToolError(f"trace_query: {e}")
    return {"tenant_id": str(tenant_id), "traces": rows, "total_count": total}


# =============================================================================
# traverse_graph — entity-graph traversal (Gate 1B, §7)
# =============================================================================


def tool_traverse_graph(
    tenant_id: str,
    *,
    entity_id: str,
    node_type: str | None = None,
    node_key: str | None = None,
    edge_type: str | None = None,
    direction: str = "both",
    as_of: str | None = None,
    limit: int = 500,
) -> dict:
    """Traverse the persisted entity↔entity graph.

    With (node_type, node_key): that node's typed edges + neighbor list.
    Without: the entity's whole subgraph (nodes + typed edges). as_of is the
    bi-temporal knowledge-time read — the topology as it was believed at T.

    A NEW tool rather than a query_triples overload: traversal returns
    nodes+edges, a different shape from fact rows — overloading would muddy
    both contracts.
    """
    if not tenant_id:
        raise MCPToolError(
            "traverse_graph requires tenant_id — caller's token did not "
            "carry one (I2 violation)."
        )
    if not entity_id or not str(entity_id).strip():
        raise MCPToolError("traverse_graph requires entity_id (I2).")
    if (node_type is None) != (node_key is None):
        raise MCPToolError(
            "traverse_graph: node_type and node_key must be provided together."
        )

    from backend.db.edge_store import EdgeContractError, EdgeIdentityError, get_edge_store
    store = get_edge_store()
    try:
        if node_type is not None:
            edges = store.get_neighbors(
                tenant_id, entity_id, node_type, node_key,
                edge_type=edge_type, direction=direction, as_of=as_of, limit=limit,
            )
            neighbors: dict[tuple, dict] = {}
            for e in edges:
                for t, k in ((e["src_type"], e["src_key"]), (e["dst_type"], e["dst_key"])):
                    if (t, k) != (node_type, node_key):
                        neighbors.setdefault((t, k), {"node_type": t, "node_key": k})
            return {
                "entity_id": entity_id,
                "node": {"node_type": node_type, "node_key": node_key},
                "as_of": as_of,
                "edges": edges,
                "neighbors": list(neighbors.values()),
            }
        sub = store.get_subgraph(
            tenant_id, entity_id,
            edge_types=[edge_type] if edge_type else None, as_of=as_of, limit=limit,
        )
        return {"entity_id": entity_id, "as_of": as_of,
                "nodes": sub["nodes"], "edges": sub["edges"]}
    except (EdgeIdentityError, EdgeContractError) as e:
        raise MCPToolError(f"traverse_graph: {e}")


# =============================================================================
# Tool registry — the public tools (Gate 1A conflict pair + Gate 1B traversal)
# =============================================================================


TOOL_SCHEMAS: dict[str, dict[str, Any]] = {
    "query_triples": {
        "description": (
            "Query semantic triples for the caller's tenant. Filters by "
            "domain (concept root) or concept; optionally by entity_id "
            "and period. Stored concepts are domain-qualified dotted paths "
            "(e.g. 'pnl.net_income'); an unqualified concept (a catalog id "
            "from concept_lookup, e.g. 'net_income' or 'revenue') matches "
            "the exact id, its namespace (id.*), and every domain-qualified "
            "instance (*.id). tenant_id is derived from the "
            "caller's token and cannot be overridden."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "domain": {"type": "string", "description": "Concept root (e.g. 'cloud_spend')"},
                "concept": {"type": "string", "description": "Dotted concept path (e.g. 'pnl.net_income') or unqualified catalog id (matches all domain-qualified instances)"},
                "entity_id": {"type": "string"},
                "period": {"type": "string", "description": "Period code (e.g. 'Q3-2026')"},
                "limit": {"type": "integer", "default": 100, "maximum": 1000},
                "active_only": {"type": "boolean", "default": True},
                "include_descendants": {
                    "type": "boolean", "default": False,
                    "description": (
                        "Expand the concept through the concept hierarchy — a "
                        "domain expands to every root beneath it, a root/dotted "
                        "concept to itself plus its subtree."
                    ),
                },
            },
        },
    },
    "traverse_graph": {
        "description": (
            "Traverse the persisted entity graph (typed entity-to-entity "
            "edges, bi-temporal). With node_type+node_key: that node's edges "
            "and neighbors; without: the entity's whole subgraph. as_of (ISO "
            "timestamp) reads the topology as it was believed at that time. "
            "tenant_id is derived from the caller's token."
        ),
        "inputSchema": {
            "type": "object",
            "required": ["entity_id"],
            "properties": {
                "entity_id": {"type": "string"},
                "node_type": {"type": "string", "description": "e.g. department | service | org_unit"},
                "node_key": {"type": "string"},
                "edge_type": {"type": "string", "description": "HAS | GENERATES | BELONGS_TO | REPORTS_TO | tenant-defined"},
                "direction": {"type": "string", "enum": ["out", "in", "both"], "default": "both"},
                "as_of": {"type": "string", "description": "ISO timestamp — knowledge-time as-of"},
                "limit": {"type": "integer", "default": 500, "maximum": 5000},
            },
        },
    },
    "list_domains": {
        "description": (
            "List distinct concept-root domains visible to the caller's "
            "tenant with triple counts. Optionally scope to one entity_id "
            "(the selected run's entity) to mirror a snapshot-scoped view."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "entity_id": {
                    "type": "string",
                    "description": "Scope the domain inventory to one run's entity",
                },
            },
        },
    },
    "list_runs": {
        "description": (
            "List the current runs (snapshots) for the caller's tenant — one "
            "per (entity, active run), newest first: dcl_ingest_id, entity_id, "
            "triple_count, created_at. The NLQ-snapshot equivalent — build a "
            "follow-latest run selector from this and scope reads to the "
            "picked run's entity_id."
        ),
        "inputSchema": {"type": "object", "properties": {}},
    },
    "concept_lookup": {
        "description": (
            "Look up an ontology concept (metric or entity) by name or "
            "alias. Returns id, definition, aliases, allowed dimensions."
        ),
        "inputSchema": {
            "type": "object",
            "required": ["query"],
            "properties": {
                "query": {"type": "string", "description": "Concept name or alias"},
            },
        },
    },
    "semantic_export": {
        "description": (
            "Export the full semantic catalog (metrics, entities, bindings). "
            "The ontology is shared across tenants."
        ),
        "inputSchema": {"type": "object", "properties": {}},
    },
    "provenance": {
        "description": (
            "Return source_system, source_field, pipe_id, and confidence "
            "for a triple. Identify the triple by triple_id (preferred) or "
            "by (concept, entity_id, period). The dcl_ingest_id field is "
            "the namespaced ingest run identifier (per I1, no bare run_id)."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "triple_id": {"type": "string"},
                "concept": {"type": "string"},
                "property": {"type": "string"},
                "entity_id": {"type": "string"},
                "period": {"type": "string"},
            },
        },
    },
    "conflict_query": {
        "description": (
            "Query the Conflict Register for the caller's tenant: value-level "
            "and structural conflicts with claims (full provenance drill), "
            "materiality, status, and the recommended disposition. Read-only."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "entity_id": {"type": "string"},
                "status": {"type": "string", "enum": ["open", "dispositioned", "escalated"]},
                "conflict_type": {"type": "string", "enum": ["value", "structural"]},
                "concept": {"type": "string"},
                "conflict_class": {"type": "string"},
                "limit": {"type": "integer", "default": 100, "maximum": 500},
            },
        },
    },
    "reconciliation_recommend": {
        "description": (
            "Recommended disposition + precedent chain for one conflict "
            "(conflict_id) or a conflict class. Precedent beats authority; "
            "proposal only — a human dispositions via the HITL surface."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "conflict_id": {"type": "string"},
                "conflict_class": {"type": "string"},
            },
        },
    },
    "trace_query": {
        "description": (
            "Query the unified decision-trace view: MCP calls, conflict "
            "dispositions, and entity-resolution confirmations as one list "
            "of uniform trace records. Read-only, scoped to the caller's "
            "tenant (tenant_id is derived from the caller's token and "
            "cannot be overridden). as_of (ISO timestamp) is the "
            "knowledge-time read — traces known at that instant; traces "
            "are events and are never superseded."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "entity_id": {"type": "string"},
                "concept": {"type": "string"},
                "agent": {"type": "string", "description": "mcp_call: caller token id; conflict_disposition: decided_by; er_confirmation: actor"},
                "decision_type": {"type": "string", "description": "mcp_call: tool name; conflict_disposition: action; er_confirmation: event"},
                "trace_type": {"type": "string", "enum": ["mcp_call", "conflict_disposition", "er_confirmation"]},
                "conflict_class": {"type": "string"},
                "period": {"type": "string", "description": "Period code (e.g. '2025-Q1')"},
                "since": {"type": "string", "description": "ISO timestamp — occurred at or after"},
                "until": {"type": "string", "description": "ISO timestamp — occurred at or before"},
                "as_of": {"type": "string", "description": "ISO timestamp — knowledge-time as-of"},
                "limit": {"type": "integer", "default": 100, "maximum": 500},
            },
        },
    },
}


PUBLIC_TOOLS = tuple(TOOL_SCHEMAS.keys())


def dispatch(tenant_id: str, tool_name: str, arguments: dict[str, Any]) -> Any:
    """Invoke a tool by name. Tenant_id MUST come from the caller's
    verified token, never from arguments. Returns the tool result."""
    if tool_name not in TOOL_SCHEMAS:
        raise MCPToolError(f"Unknown tool: {tool_name!r}")
    args = dict(arguments or {})
    # Guard against accidental tenant overrides via arguments.
    args.pop("tenant_id", None)
    if tool_name == "query_triples":
        return tool_query_triples(tenant_id, **args)
    if tool_name == "traverse_graph":
        return tool_traverse_graph(tenant_id, **args)
    if tool_name == "list_domains":
        return tool_list_domains(tenant_id, args.get("entity_id"))
    if tool_name == "list_runs":
        return tool_list_runs(tenant_id)
    if tool_name == "concept_lookup":
        return tool_concept_lookup(
            tenant_id, args.get("query") or args.get("concept", "")
        )
    if tool_name == "semantic_export":
        return tool_semantic_export(tenant_id)
    if tool_name == "provenance":
        return tool_provenance(tenant_id, **args)
    if tool_name == "conflict_query":
        return tool_conflict_query(tenant_id, **args)
    if tool_name == "reconciliation_recommend":
        return tool_reconciliation_recommend(tenant_id, **args)
    if tool_name == "trace_query":
        return tool_trace_query(tenant_id, **args)
    raise MCPToolError(f"No dispatch handler for {tool_name!r}")
