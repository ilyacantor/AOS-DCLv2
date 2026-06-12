"""
Legacy HTTP "MCP" surface for DCL — kept stable for Mai's internal client
(app/mai/tools/mcp_client.py).

WP5 refactor: tool bodies moved to backend/engine/mcp_tools.py and are now
shared between this HTTP path and the real MCP server in
backend/api/mcp_server_real.py. The wire-protocol MCP server is the
canonical surface for external consumers; this HTTP surface stays only
because Mai already speaks it. Migration of Mai to the real MCP transport
is a separate follow-on (§11.4 last paragraph).

Auth is shared-secret API key (legacy). The real MCP surface uses opaque
tenant-scoped tokens (see backend/api/mcp_auth.py).
"""

import os
import time
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from backend.engine.mcp_tools import (
    PUBLIC_TOOLS,
    TOOL_SCHEMAS,
    MCPToolError,
    dispatch,
)
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)


_ONTOLOGY_ONLY_TOOLS = {"concept_lookup", "semantic_export"}


def _legacy_tenant_id(tool_name: str) -> str:
    """Tenant for the legacy HTTP path. Per §11.4 the real MCP exposure
    uses tokens; this internal HTTP path stays unchanged for Mai, which
    forwards AOS_TENANT_ID.

    Ontology-only tools (concept_lookup, semantic_export) accept '' when
    no tenant is set — the ontology is shared across tenants. Tenant-
    scoped tools (query_triples, list_domains, provenance) raise loudly
    so Mai surfaces the missing env (A1: no silent fallback)."""
    tenant = os.environ.get("AOS_TENANT_ID") or ""
    if not tenant and tool_name not in _ONTOLOGY_ONLY_TOOLS:
        raise MCPToolError(
            "Legacy MCP HTTP path requires AOS_TENANT_ID in env for "
            f"tool {tool_name!r} — tenant identity must be present for "
            "triple-store queries (I2)."
        )
    return tenant


# Valid API keys (in production, these would be in a secure store)
VALID_API_KEYS = {
    os.environ.get("MCP_API_KEY", "dcl-mcp-key-v1"),
    "dcl-mcp-test-key",
}


class MCPToolCall(BaseModel):
    """MCP tool call request."""
    tool: str
    arguments: Dict[str, Any] = Field(default_factory=dict)
    api_key: Optional[str] = None


class MCPToolResult(BaseModel):
    """MCP tool call result."""
    tool: str
    success: bool
    result: Any = None
    error: Optional[str] = None


class MCPServerInfo(BaseModel):
    """MCP server information."""
    name: str = "dcl-mcp-server"
    version: str = "1.0.0"
    tools: List[Dict[str, Any]] = Field(default_factory=list)


def validate_api_key(api_key: Optional[str]) -> bool:
    """Validate an MCP API key."""
    if not api_key:
        return False
    return api_key in VALID_API_KEYS


def get_server_info() -> MCPServerInfo:
    """Get MCP server information and available tools.

    The advertised tool list now reflects the shared registry in
    backend/engine/mcp_tools.py plus the legacy 'query' alias.
    """
    tools: list[dict[str, Any]] = []
    for name, schema in TOOL_SCHEMAS.items():
        # Surface a compact parameters dict so the legacy /api/mcp/info
        # contract (consumed by old clients) is not broken.
        props = schema["inputSchema"].get("properties", {})
        tools.append(
            {
                "name": name,
                "description": schema["description"],
                "parameters": {
                    pname: {
                        "type": pspec.get("type", "string"),
                        "description": pspec.get("description", ""),
                    }
                    for pname, pspec in props.items()
                },
            }
        )
    # Legacy alias retained for Mai's existing call sites.
    tools.append(
        {
            "name": "query",
            "description": "Execute a data query against DCL's fact base (legacy alias of metric query path)",
            "parameters": {
                "metric": {"type": "string", "description": "Metric to query"},
                "dimensions": {"type": "array", "description": "Dimensions to group by"},
                "grain": {"type": "string", "description": "Time grain"},
                "time_range": {"type": "object", "description": "Time range filter {start, end}"},
            },
        }
    )
    return MCPServerInfo(
        name="dcl-mcp-server",
        version="1.0.0",
        tools=tools,
    )


def handle_tool_call(tool_call: MCPToolCall) -> MCPToolResult:
    """Handle an MCP tool call by routing to the appropriate DCL function."""

    if not validate_api_key(tool_call.api_key):
        return MCPToolResult(
            tool=tool_call.tool,
            success=False,
            error="Authentication required. Provide a valid api_key.",
        )

    try:
        if tool_call.tool == "query":
            # Legacy metric-based query — stays separate from the real
            # MCP query_triples surface. Used by Mai's existing chat path.
            return _handle_legacy_query(tool_call)
        if tool_call.tool == "provenance" and (
            "metric" in tool_call.arguments or "metric_id" in tool_call.arguments
        ):
            # Legacy metric-trace provenance (ProvenanceTrace shape).
            # The wire-protocol MCP server returns triple-level provenance;
            # Mai-internal callers still want the older metric-trace shape.
            return _handle_legacy_metric_provenance(tool_call)
        if tool_call.tool not in PUBLIC_TOOLS:
            return MCPToolResult(
                tool=tool_call.tool,
                success=False,
                error=f"Unknown tool: {tool_call.tool}",
            )
        tenant_id = _legacy_tenant_id(tool_call.tool)
        if tool_call.arguments.get("persona"):
            result = _dispatch_with_persona_audit(tenant_id, tool_call)
        else:
            result = dispatch(tenant_id, tool_call.tool, tool_call.arguments)
        return MCPToolResult(
            tool=tool_call.tool,
            success=True,
            result=result,
        )
    except MCPToolError as exc:
        return MCPToolResult(
            tool=tool_call.tool,
            success=False,
            error=str(exc),
        )
    except Exception as e:
        logger.error(f"MCP tool call failed: {e}", exc_info=True)
        return MCPToolResult(
            tool=tool_call.tool,
            success=False,
            error=str(e),
        )


def _dispatch_with_persona_audit(tenant_id: str, tool_call: MCPToolCall) -> Any:
    """Gate 2B: a persona-scoped answer through the legacy HTTP shim leaves
    the same decision trace as every other scoped surface — one mai_mcp_audit
    append (transport='http') that the 2A decision_traces view projects as
    trace_type='mcp_call' with decision_type = the dispatched tool and
    payload carrying the persona. Outcome and latency are real; the compact
    result_summary is recorded on success only (AuditRow contract). The
    shim's shared-secret auth carries no verified per-caller token id, so
    caller_token_id names the surface. Personaless shim calls never reach
    this — zero new writes, byte-identical legacy behavior. Wholesale shim
    auditing (ALL calls) stays open in dcl_deferred_work.md #75."""
    from backend.api.mcp_audit import (
        AuditRow,
        hash_arguments,
        summarize_result,
        write_audit,
    )

    args = tool_call.arguments
    raw_entity = args.get("entity_id")
    entity_id = (
        raw_entity.strip()
        if isinstance(raw_entity, str) and raw_entity.strip()
        else None
    )

    def _audit(**outcome_fields: Any) -> None:
        write_audit(AuditRow(
            tenant_id=tenant_id,
            tool_name=tool_call.tool,
            caller_token_id="http:legacy-tools-call",
            arguments_hash=hash_arguments(args),
            latency_ms=int((time.perf_counter() - started) * 1000),
            transport="http",
            entity_id=entity_id,
            arguments=args,
            **outcome_fields,
        ))

    started = time.perf_counter()
    try:
        result = dispatch(tenant_id, tool_call.tool, args)
    except Exception as exc:
        _audit(
            outcome="error",
            error_summary=(
                str(exc) if isinstance(exc, MCPToolError)
                else f"{type(exc).__name__}: {exc}"
            ),
        )
        raise
    _audit(outcome="success", result_summary=summarize_result(result))
    return result


def _handle_legacy_metric_provenance(tool_call: MCPToolCall) -> MCPToolResult:
    """Handle the legacy metric-trace `provenance` shape that Mai's chat
    path still consumes (ProvenanceTrace with .sources list)."""
    from backend.engine.provenance_service import get_provenance

    metric = (
        tool_call.arguments.get("metric")
        or tool_call.arguments.get("metric_id")
    )
    if not metric:
        return MCPToolResult(
            tool="provenance",
            success=False,
            error="Missing 'metric' argument for legacy metric-trace provenance.",
        )
    trace = get_provenance(metric)
    if not trace:
        return MCPToolResult(
            tool="provenance",
            success=False,
            error=f"Metric '{metric}' not found.",
        )
    return MCPToolResult(
        tool="provenance",
        success=True,
        result=trace.model_dump(),
    )


def _handle_legacy_query(tool_call: MCPToolCall) -> MCPToolResult:
    """Handle the legacy metric-based `query` tool (Mai-internal path)."""
    from backend.api.query import QueryRequest, handle_query, QueryError

    metric = tool_call.arguments.get("metric")
    if not metric:
        return MCPToolResult(
            tool="query",
            success=False,
            error="Missing 'metric' argument",
        )

    request = QueryRequest(
        metric=metric,
        dimensions=tool_call.arguments.get("dimensions", []),
        grain=tool_call.arguments.get("grain"),
        time_range=tool_call.arguments.get("time_range"),
        filters=tool_call.arguments.get("filters", {}),
        order_by=tool_call.arguments.get("order_by"),
        limit=tool_call.arguments.get("limit"),
    )

    result = handle_query(request)

    if isinstance(result, QueryError):
        return MCPToolResult(
            tool="query",
            success=False,
            error=result.error,
            result=result.model_dump(),
        )

    return MCPToolResult(
        tool="query",
        success=True,
        result=result.model_dump(),
    )
