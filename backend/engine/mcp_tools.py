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
# Tool registry — the public 8
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
    raise MCPToolError(f"No dispatch handler for {tool_name!r}")
