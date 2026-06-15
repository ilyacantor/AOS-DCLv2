"""
Panel B — the AFTER condition: the same model grounded through DCL-MCP.

A real agent client over the real wire-protocol HTTP MCP path: bearer
token (HMAC shim, backend/api/mcp_auth.py), per-call audit rows in
mai_mcp_audit, per-tenant rate limit. Loopback or not — the transport,
auth, audit and rate-limit are exactly what an external consumer gets,
and externality is provable from the audit ledger
(GET /api/dcl/mcp/audit?caller_token_id=…).

Every MCP call the agent makes is itself a manually-runnable platform
operation (see demo/OPERATIONS.md). Run the panel by hand:

    DCL_MCP_TOKEN_SECRET=… python -m demo.panel_b \
        --entity CedarGrid-1823 --tenant <tenant-uuid> \
        --question "What was net income in the most recent quarter?"
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os

from demo.agent_common import DEFAULT_MODEL, dcl_dev_url, emit, load_demo_env, run_agent_loop

# Live dev backend (:8104), prod-guarded — no per-run --dcl-url override needed.
DCL_URL = dcl_dev_url()

# Full tool surface incl. the Gate 1A conflict tools (not in the default
# mint scope — the demo token requests them explicitly).
TOKEN_SCOPE = [
    "query_triples",
    "traverse_graph",
    "list_domains",
    "list_runs",
    "concept_lookup",
    "semantic_export",
    "provenance",
    "conflict_query",
    "reconciliation_recommend",
]

SYSTEM = (
    "You are a data analyst agent for the company, connected to its "
    "governed context layer (DCL) over MCP for entity {entity_id}. Every "
    "number you state must be grounded in triples you actually queried. "
    "Your job is to resolve, not just retrieve: when sources disagree, "
    "check conflict_query for the concepts you rely on, state the conflict "
    "plainly, name which source is authoritative and why, and give the "
    "figure that follows from it. For ANY question about drivers, causes, "
    "concentration, or how facts relate ACROSS departments/teams/sources "
    "(e.g. what is driving attrition), you MUST call traverse_graph and "
    "ground the answer in the derived edges it returns — the below-market "
    "comp gap, the dominant exit reason, the org concentration. These are "
    "synthesized edges no single triple asserts; do NOT reconstruct the "
    "relationship by hand from separate query_triples results. "
    "Dollar figures are in millions unless stated; "
    "periods are quarters like 2026-Q4. The source behind a figure (system, "
    "confidence, triple id) is there to verify on request — cite it when it "
    "is load-bearing, but lead with the answer and the resolution, not the "
    "audit trail. If the store has no data for what was asked, say exactly "
    "that — never estimate around missing data. State the period you used."
)


def mint_demo_token(tenant_id: str) -> dict:
    """Mint the bearer token via the platform's own auth library —
    the same operation as the operator mint flow."""
    from backend.api.mcp_auth import mint_token

    return mint_token(
        tenant_id,
        ttl_seconds=3600,
        scope=TOKEN_SCOPE,
        identity="demo-panel-b",
    )


def _bridge_tool_defs(mcp_tools) -> list[dict]:
    """MCP tool descriptors -> Anthropic tool definitions, verbatim schemas."""
    return [
        {
            "name": t.name,
            "description": t.description or "",
            "input_schema": t.inputSchema,
        }
        for t in mcp_tools
    ]


def _result_to_text(result) -> str:
    parts = []
    for item in result.content:
        text = getattr(item, "text", None)
        parts.append(text if text is not None else json.dumps(item.__dict__, default=str))
    joined = "\n".join(parts)
    if result.isError:
        return f"TOOL ERROR (MCP): {joined}"
    return joined


async def run_panel_b(
    entity_id: str,
    tenant_id: str,
    question: str,
    model: str = DEFAULT_MODEL,
    dcl_url: str = DCL_URL,
) -> dict:
    from mcp import ClientSession
    from mcp.client.sse import sse_client

    minted = mint_demo_token(tenant_id)
    sse_url = f"{dcl_url}/api/mcp/sse"
    headers = {"Authorization": f"Bearer {minted['token']}"}

    async with sse_client(sse_url, headers=headers) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            listed = await session.list_tools()
            tool_defs = _bridge_tool_defs(listed.tools)

            async def execute_tool(name: str, args: dict) -> str:
                result = await session.call_tool(name, args)
                text = _result_to_text(result)
                if result.isError:
                    # surfaced to the model AND kept loud in the capture
                    return text
                return text

            result = await run_agent_loop(
                model=model,
                system=SYSTEM.format(entity_id=entity_id),
                question=question + f"\n(entity_id: {entity_id})",
                tool_defs=tool_defs,
                execute_tool=execute_tool,
            )

    result["panel"] = "b"
    result["access"] = "dcl-mcp-http-sse"
    result["entity_id"] = entity_id
    result["mcp"] = {
        "sse_url": sse_url,
        "caller_token_id": minted["token_id"],
        "scope": TOKEN_SCOPE,
        "tools_listed": [t["name"] for t in tool_defs],
    }
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Panel B — grounded agent over DCL-MCP (HTTP+SSE)")
    parser.add_argument("--entity", required=True)
    parser.add_argument("--tenant", required=True, help="tenant UUID the token is bound to")
    parser.add_argument("--question", required=True)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--dcl-url", default=DCL_URL)
    parser.add_argument("--json", action="store_true", help="emit the capture fragment as JSON")
    args = parser.parse_args()

    load_demo_env()
    result = asyncio.run(
        run_panel_b(args.entity, args.tenant, args.question, args.model, args.dcl_url)
    )
    emit(result, args.json)


if __name__ == "__main__":
    main()
