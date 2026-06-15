"""Semantics panel — the BASE TIER condition (§13).

A TIER contrast, not a fair-access contest. The Semantics panel has the SAME
resolved, governed data the contextOS panel has — the same roster, exit-survey
reasons, internal comp band, external market median, headcount, all resolved in
DCL and reachable over the SAME MCP connection. What it LACKS is the CAPABILITY
to connect: it runs the base read tools only —

    query_triples · concept_lookup · list_domains

and NOT traverse_graph (no relationship-graph walk), NOT conflict_query /
reconciliation_recommend (it can SEE both conflicting source values via
query_triples but never gets the decisive arbitrated 'recommended'), NOT as-of.

This is the honest capability boundary, not a data handicap: full data, full
access, base tools. It answers ONLY from what it can directly query — it computes
correct per-department attrition RATES and names the small-base department, and
lists exit reasons / comp / market as separate clean lists it cannot JOIN into a
driver. The win in the contrast is the capability gap, never less data.

Same wire-protocol path as the contextOS panel: a real agent client over the
HTTP+SSE MCP transport, a bearer token (HMAC shim, backend/api/mcp_auth.py), per-
call audit rows in mai_mcp_audit. The token scope is restricted to the three base
tools, and the tool surface the agent sees is filtered to that scope — so the
absence of traversal/arbitration/as-of is real, enforced at the token AND in the
tools the model is offered.

Containment: operator-gated CLI tool only. Not an API. Not importable as a data
path (backend/* never imports demo.* — test-enforced). Run it by hand:

    DCL_MCP_TOKEN_SECRET=… python -m demo.panel_a \
        --entity ContextOSDemo --tenant <tenant-uuid> \
        --question "Where is attrition highest and what's driving it?"
"""

from __future__ import annotations

import argparse
import asyncio
import json

from demo.agent_common import DEFAULT_MODEL, dcl_dev_url, emit, load_demo_env, run_agent_loop

# Live dev backend (:8104), prod-guarded — no per-run --dcl-url override needed.
DCL_URL = dcl_dev_url()

# Base-tier tool scope: the resolved-data read tools only. NO traverse_graph
# (relationship-graph walk), NO conflict_query / reconciliation_recommend (the
# arbitrated decisive value), NO as-of. The token enforces this; the panel also
# filters the offered tool surface to exactly these so the model is never even
# shown the premium capabilities.
TOKEN_SCOPE = ["query_triples", "concept_lookup", "list_domains"]

SYSTEM = (
    "You are a data analyst agent for the company, connected to its governed "
    "data layer (DCL) over MCP for entity {entity_id}. You have the resolved, "
    "governed facts — query them with query_triples (filter by domain or "
    "concept, and by entity_id/period), discover concepts with concept_lookup, "
    "and see what domains exist with list_domains. Every number you state must "
    "come from triples you actually queried.\n"
    "Your capabilities end at retrieval. You do NOT have a relationship graph or "
    "any graph-traversal tool, you do NOT have conflict arbitration (you can "
    "retrieve the values that different source systems report for the same "
    "fact, and you may state that they differ, but you have no tool that tells "
    "you which source is authoritative or what the decisive reconciled value "
    "is), and you have no time-travel / as-of capability. Work strictly within "
    "those limits: answer from what you can directly query, compute only what "
    "the retrieved figures support, and present related facts (exit reasons, "
    "internal comp, external market benchmarks) as the separate lists they are. "
    "When a question asks where a metric is HIGHEST across a dimension "
    "(departments, teams), do the rate analysis the data supports: a rate is a "
    "count over its base, so look for the per-member count breakdown AND the "
    "matching per-member base and divide. Call list_domains first to see every "
    "concept root that exists, then query each relevant one — the per-member "
    "count and the per-member base often live under DIFFERENT roots (for "
    "example a departures breakdown under one root and a headcount breakdown "
    "under a separate 'headcount' root), so check the breakdown concepts in "
    "every domain that could hold the base before concluding a base is missing. "
    "Join the count and base by the member key yourself and rank by the rate you "
    "compute; do not substitute a pre-aggregated company-wide figure for a "
    "per-member rate, and do not declare a base unavailable until you have "
    "queried the domains that would hold it. Do NOT invent or assert a cross-source join, a causal "
    "driver, or an org-structure relationship that you cannot actually resolve "
    "with your tools — if connecting the facts would require a capability you "
    "don't have, say so plainly rather than guessing. Dollar figures are in millions unless "
    "a triple's unit says otherwise; periods are quarters like 2026-Q4 or months "
    "like 2026-03. If the store has no data for what was asked, say exactly that "
    "— never estimate around missing data. State the period you used."
)


def mint_demo_token(tenant_id: str) -> dict:
    """Mint the bearer token via the platform's own auth library — the same
    operation as the operator mint flow, scoped to the base read tools."""
    from backend.api.mcp_auth import mint_token

    return mint_token(
        tenant_id,
        ttl_seconds=3600,
        scope=TOKEN_SCOPE,
        identity="demo-panel-semantics",
    )


def _bridge_tool_defs(mcp_tools) -> list[dict]:
    """MCP tool descriptors -> Anthropic tool definitions, verbatim schemas,
    FILTERED to the base-tier scope. The server lists every public tool; the
    base tier is only offered the three it is scoped for, so the model never
    sees (and cannot reach for) traverse_graph / conflict_query / as-of."""
    return [
        {
            "name": t.name,
            "description": t.description or "",
            "input_schema": t.inputSchema,
        }
        for t in mcp_tools
        if t.name in TOKEN_SCOPE
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


async def run_panel_a(
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
                return _result_to_text(result)

            result = await run_agent_loop(
                model=model,
                system=SYSTEM.format(entity_id=entity_id),
                question=question + f"\n(entity_id: {entity_id})",
                tool_defs=tool_defs,
                execute_tool=execute_tool,
            )

    result["panel"] = "semantics"
    result["access"] = "dcl-mcp-base-tools"
    result["entity_id"] = entity_id
    result["mcp"] = {
        "sse_url": sse_url,
        "caller_token_id": minted["token_id"],
        "scope": TOKEN_SCOPE,
        "tools_listed": [t["name"] for t in tool_defs],
    }
    return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Semantics panel — base-tier agent over DCL-MCP base tools (operator-gated CLI)"
    )
    parser.add_argument("--entity", required=True)
    parser.add_argument("--tenant", required=True, help="tenant UUID the token is bound to")
    parser.add_argument("--question", required=True)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--dcl-url", default=DCL_URL)
    parser.add_argument("--json", action="store_true", help="emit the capture fragment as JSON")
    args = parser.parse_args()

    load_demo_env()
    result = asyncio.run(
        run_panel_a(args.entity, args.tenant, args.question, args.model, args.dcl_url)
    )
    emit(result, args.json)


if __name__ == "__main__":
    main()
