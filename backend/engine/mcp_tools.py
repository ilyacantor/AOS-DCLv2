"""
Shared MCP tool implementations (Plan B WP5, §11.4).

Single source of truth for the five external tools. Both the legacy HTTP
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
) -> list[dict]:
    """Return triples filtered by the calling tenant.

    The tenant_id filter is non-overridable. The 'domain' filter matches
    the root segment of concept names (e.g. domain='cloud_spend' matches
    'cloud_spend.amount_billed', 'cloud_spend.aws_total'). 'concept' is
    the full concept name when the caller knows it exactly.

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

    raw = _store.mcp_query_triples(
        tenant_id,
        domain=domain,
        concept=concept,
        entity_id=entity_id,
        period=period,
        limit=limit,
        active_only=active_only,
    )
    # Rename bare run_id → dcl_ingest_id per I1 before exposing.
    out: list[dict] = []
    for row in raw:
        d = dict(row)
        if "run_id" in d:
            d["dcl_ingest_id"] = d.pop("run_id")
        out.append(d)
    return out


# =============================================================================
# list_domains — distinct concept roots for the tenant
# =============================================================================


def tool_list_domains(tenant_id: str) -> list[dict]:
    """Return distinct concept-root domains with triple counts for tenant."""
    if not tenant_id:
        raise MCPToolError(
            "list_domains requires tenant_id — caller's token did not "
            "carry one (I2 violation)."
        )
    return _store.mcp_list_domains(tenant_id)


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
# Tool registry — the public 5
# =============================================================================


TOOL_SCHEMAS: dict[str, dict[str, Any]] = {
    "query_triples": {
        "description": (
            "Query semantic triples for the caller's tenant. Filters by "
            "domain (concept root) or full concept; optionally by entity_id "
            "and period. tenant_id is derived from the caller's token and "
            "cannot be overridden."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "domain": {"type": "string", "description": "Concept root (e.g. 'cloud_spend')"},
                "concept": {"type": "string", "description": "Full concept name"},
                "entity_id": {"type": "string"},
                "period": {"type": "string", "description": "Period code (e.g. 'Q3-2026')"},
                "limit": {"type": "integer", "default": 100, "maximum": 1000},
                "active_only": {"type": "boolean", "default": True},
            },
        },
    },
    "list_domains": {
        "description": (
            "List distinct concept-root domains visible to the caller's "
            "tenant with triple counts."
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
                "entity_id": {"type": "string"},
                "period": {"type": "string"},
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
    if tool_name == "list_domains":
        return tool_list_domains(tenant_id)
    if tool_name == "concept_lookup":
        return tool_concept_lookup(
            tenant_id, args.get("query") or args.get("concept", "")
        )
    if tool_name == "semantic_export":
        return tool_semantic_export(tenant_id)
    if tool_name == "provenance":
        return tool_provenance(tenant_id, **args)
    raise MCPToolError(f"No dispatch handler for {tool_name!r}")
