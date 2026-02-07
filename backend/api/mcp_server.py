"""
MCP (Model Context Protocol) Server for DCL.

Wraps existing REST endpoints as MCP tools so any external LLM or AI agent
can query everything DCL knows.

Auth: API keys for v1.
"""

import os
import json
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

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
    data: Any = None
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
    """Get MCP server information and available tools."""
    return MCPServerInfo(
        name="dcl-mcp-server",
        version="1.0.0",
        tools=[
            {
                "name": "concept_lookup",
                "description": "Look up an ontology concept or metric by name or alias",
                "parameters": {
                    "query": {"type": "string", "description": "Concept name or alias to look up"},
                },
            },
            {
                "name": "semantic_export",
                "description": "Export the full semantic catalog (metrics, entities, bindings)",
                "parameters": {},
            },
            {
                "name": "query",
                "description": "Execute a data query against DCL's fact base",
                "parameters": {
                    "metric": {"type": "string", "description": "Metric to query"},
                    "dimensions": {"type": "array", "description": "Dimensions to group by"},
                    "grain": {"type": "string", "description": "Time grain (day, week, month, quarter, year)"},
                    "time_range": {"type": "object", "description": "Time range filter {start, end}"},
                },
            },
            {
                "name": "provenance",
                "description": "Get provenance trace for a metric",
                "parameters": {
                    "metric": {"type": "string", "description": "Metric to trace"},
                },
            },
        ],
    )


def handle_tool_call(tool_call: MCPToolCall) -> MCPToolResult:
    """Handle an MCP tool call by routing to the appropriate DCL endpoint."""

    # Auth check
    if not validate_api_key(tool_call.api_key):
        return MCPToolResult(
            tool=tool_call.tool,
            success=False,
            error="Authentication required. Provide a valid api_key.",
        )

    try:
        if tool_call.tool == "concept_lookup":
            return _handle_concept_lookup(tool_call)
        elif tool_call.tool == "semantic_export":
            return _handle_semantic_export(tool_call)
        elif tool_call.tool == "query":
            return _handle_query(tool_call)
        elif tool_call.tool == "provenance":
            return _handle_provenance(tool_call)
        else:
            return MCPToolResult(
                tool=tool_call.tool,
                success=False,
                error=f"Unknown tool: {tool_call.tool}",
            )
    except Exception as e:
        logger.error(f"MCP tool call failed: {e}", exc_info=True)
        return MCPToolResult(
            tool=tool_call.tool,
            success=False,
            error=str(e),
        )


def _handle_concept_lookup(tool_call: MCPToolCall) -> MCPToolResult:
    """Handle concept_lookup tool."""
    from backend.api.semantic_export import resolve_metric, resolve_entity

    query = tool_call.arguments.get("query", "")
    if not query:
        return MCPToolResult(
            tool="concept_lookup",
            success=False,
            error="Missing 'query' argument",
        )

    # Try metric first
    metric = resolve_metric(query)
    if metric:
        return MCPToolResult(
            tool="concept_lookup",
            success=True,
            data={
                "type": "metric",
                "id": metric.id,
                "name": metric.name,
                "definition": metric.description,
                "aliases": metric.aliases,
                "pack": metric.pack.value,
                "allowed_dims": metric.allowed_dims,
                "allowed_grains": [g.value for g in metric.allowed_grains],
            },
        )

    # Try entity
    entity = resolve_entity(query)
    if entity:
        return MCPToolResult(
            tool="concept_lookup",
            success=True,
            data={
                "type": "entity",
                "id": entity.id,
                "name": entity.name,
                "definition": entity.description,
                "aliases": entity.aliases,
            },
        )

    return MCPToolResult(
        tool="concept_lookup",
        success=False,
        error=f"No concept found for '{query}'",
    )


def _handle_semantic_export(tool_call: MCPToolCall) -> MCPToolResult:
    """Handle semantic_export tool."""
    from backend.api.semantic_export import get_semantic_export

    export = get_semantic_export()
    return MCPToolResult(
        tool="semantic_export",
        success=True,
        data=export.model_dump(),
    )


def _handle_query(tool_call: MCPToolCall) -> MCPToolResult:
    """Handle query tool."""
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
            data=result.model_dump(),
        )

    return MCPToolResult(
        tool="query",
        success=True,
        data=result.model_dump(),
    )


def _handle_provenance(tool_call: MCPToolCall) -> MCPToolResult:
    """Handle provenance tool."""
    from backend.engine.provenance_service import get_provenance

    metric = tool_call.arguments.get("metric")
    if not metric:
        return MCPToolResult(
            tool="provenance",
            success=False,
            error="Missing 'metric' argument",
        )

    trace = get_provenance(metric)
    if not trace:
        return MCPToolResult(
            tool="provenance",
            success=False,
            error=f"Metric '{metric}' not found",
        )

    return MCPToolResult(
        tool="provenance",
        success=True,
        data=trace.model_dump(),
    )
