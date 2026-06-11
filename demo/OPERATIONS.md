# Demo operations — every step, manually runnable

The demo rule: the sequence (demo/sequence.py) only orders the operations
below. Each one is a real platform operation you can run by hand, exactly
as listed. If a step can't be run manually it does not go in the demo.
The wrapper (DCL frontend "Demo" tab) renders captures only.

Environment: dev stack. `DCL` = your DCL backend (default for the demo:
`http://localhost:8014`, a parallel-branch instance per the port-block
strategy; `:8104` is the shared dev instance). `FARM` = `http://localhost:8003`.
Both panels need `ANTHROPIC_API_KEY` (from `.env.development`); Panel B and
the MCP server share `DCL_MCP_TOKEN_SECRET`.

## 1. Health (preflight)

    curl $DCL/health
    curl $FARM/api/health

## 2. Snapshot presence (preflight)

    curl "$DCL/api/dcl/snapshots?limit=50"
    # entity must appear with is_current=true — else run the records-path
    # pipeline for it first (AAM operator fabric run, plane=all + extra_planes).

## 3. Conflict Register read (capability probe + tenant resolution + ground truth for the conflict slot)

    curl "$DCL/api/dcl/conflicts?entity_id=CedarGrid-1823&limit=100"
    # 200 -> {tenant_id, entity_id, conflicts[], total_count}; Gate 1A surface.

## 4. Raw feed read (Panel A's access; also the eval ground-truth source — B10)

Seed contract (records-path, mirrors AAM operator_fabric.py):
`seed = uuid5(NAMESPACE_URL, "fin-seed:{entity_id}") % 2**31`

    python3 -c "import uuid; print(int(uuid.uuid5(uuid.NAMESPACE_URL,'fin-seed:CedarGrid-1823').int % (2**31)))"
    curl "$FARM/api/farm/financial-records?entity_id=CedarGrid-1823&seed=<seed>"
    curl "$FARM/api/farm/operational-records?entity_id=CedarGrid-1823&seed=<seed>"
    curl "$FARM/api/farm/ledger-records?entity_id=CedarGrid-1823&seed=<seed>"

## 5. Malformed-ingest rejection (real-condition beat)

    curl -i -X POST "$DCL/api/dcl/ingest-records" -H 'content-type: application/json' \
      -d '{"tenant_id":"<tenant-uuid>","entity_id":"CedarGrid-1823","snapshot_name":"CedarGrid-1823-demo","pipes":[]}'
    # Expected: HTTP 422 with an informative VALIDATION_FAILED detail. No write occurs.

## 6. MCP token mint (Panel B auth)

    DCL_MCP_TOKEN_SECRET=… python -c "
    from backend.api.mcp_auth import mint_token
    print(mint_token('<tenant-uuid>', ttl_seconds=3600, scope=[
      'query_triples','list_domains','list_runs','concept_lookup',
      'semantic_export','provenance','conflict_query','reconciliation_recommend']))"
    # conflict_query/reconciliation_recommend are NOT in the default scope —
    # the demo token requests the full read surface explicitly.

## 7. MCP tool call over the real wire path (each Panel-B call is one of these)

Any MCP client speaks the same transport: `GET $DCL/api/mcp/sse` with
`Authorization: Bearer <token>`, then JSON-RPC over `POST /api/mcp/messages/`.
Minimal python (the exact client pattern Panel B uses):

    python - <<'EOF'
    import asyncio, os
    from mcp import ClientSession
    from mcp.client.sse import sse_client
    async def main():
        headers={"Authorization": f"Bearer {os.environ['TOKEN']}"}
        async with sse_client(os.environ.get("DCL","http://localhost:8014")+"/api/mcp/sse", headers=headers) as (r,w):
            async with ClientSession(r,w) as s:
                await s.initialize()
                out = await s.call_tool("query_triples", {"domain":"pnl","entity_id":"CedarGrid-1823","limit":5})
                print(out.content[0].text[:500])
    asyncio.run(main())
    EOF

## 8. MCP audit ledger read (externality proof; decision-trace seed §9)

    curl "$DCL/api/dcl/mcp/audit?tenant_id=<tenant-uuid>&caller_token_id=<token_id>&limit=500"
    # Every Panel-B tool call appears here: tool_name, outcome, latency_ms,
    # transport=http+sse, arguments_hash. Row count == calls made.

## 9. Panel A (operator-gated CLI — the BEFORE condition)

    python -m demo.panel_a --entity CedarGrid-1823 \
      --question "What was net income in the most recent quarter?"
    # Containment: CLI only. Not an API; backend/* never imports demo.*
    # (test-enforced). It exists as a reproducible condition, not infrastructure.

## 10. Panel B (grounded agent over DCL-MCP)

    DCL_MCP_TOKEN_SECRET=… python -m demo.panel_b --entity CedarGrid-1823 \
      --tenant <tenant-uuid> --question "What was net income in the most recent quarter?"

## 11. The full headless sequence (= regression run)

    DCL_MCP_TOKEN_SECRET=… python -m demo.sequence --entity CedarGrid-1823
    # Writes public/demo-captures/<stamp>__<entity>.json + latest.json.
    # Exit 0 iff every non-pending beat passes. Pending (Gate 1A scenario)
    # slots are reported pending — never simulated.

## 12. The wrapper (presentation only)

    # DCL frontend -> "Demo" tab (deep link: /?view=demo). Renders the
    # latest capture; zero logic that changes outcomes. Console links here.
