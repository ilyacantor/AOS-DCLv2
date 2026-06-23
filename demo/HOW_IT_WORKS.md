# Semantics vs contextOS demo — how it works (code, not docs)

Traced from code at commit 97b9260. One entity, two agent panels, **same model,
same governed store, same MCP wire**. The only difference is *capability scope* —
the tools the agent may call. Tier contrast, not a data handicap (`demo/panel_a.py:1-26`).

## Layers (`demo/__init__.py`)
1. **Operations** — every step is a real platform op, runnable by hand (`demo/OPERATIONS.md`).
2. **Sequence** — `demo/sequence.py` orders them headless; the headless run *is* the
   regression run; writes `public/demo-captures/latest.json`.
3. **Wrapper** — `src/components/demo/GroundedDemoTab.tsx` renders the capture. Presentation
   only. `backend/*` never imports `demo.*` (test-enforced).

## The two panels
Both run the same agent loop (`demo/agent_common.py:run_agent_loop`, Opus, ≤8 turns) over the
real HTTP+SSE MCP transport (`/api/mcp/sse`), each with an HMAC bearer token, each call audited.
- **Semantics (base)** `panel_a.py` — token scope = 3 read tools: `query_triples`,
  `concept_lookup`, `list_domains`. Can *see* two conflicting source values but gets no
  arbitrated answer, no graph walk, no as-of.
- **contextOS (premium)** `panel_b.py` — scope adds `traverse_graph`, `conflict_query`,
  `reconciliation_recommend`, `provenance`, `semantic_export`, `list_runs`. It *connects* facts.

## Where the tier boundary is actually enforced (followed the server)
- **Execution: server-side, hard.** `mcp_server_real.py:155` — `call_tool` denies any tool not
  in `token.scope` and writes an `unauthorized` audit row. A base token literally cannot run
  `traverse_graph`.
- **Offered surface: client-side, in the panel.** `list_tools` returns *all* tools
  (`mcp_server_real.py:99-107`); `panel_a._bridge_tool_defs` filters to the 3 scoped tools so the
  model is never even shown the premium ones. Both halves together = the panel docstring's claim.

## What the premium tools do
- `traverse_graph` reads **derived edges** from the `entity_edges` table (`edge_derivation.py:derive_edges`).
  Hero edge: `department BELOW_MARKET job_family`, gap% = (market−internal)/market — e.g. **13.16%**
  (165k internal vs 190k market), joined via a *data-declared* dept→job-family resolution. Rules are
  **name-free** (commit 6ceb85d): specifics come from data, not hardcoded entity names. The gap exists
  only in the join — no single triple holds it. Also: dominant exit-reason and team→band `DRIVEN_BY`.
- `conflict_query` / `reconciliation_recommend` serve the **Conflict Register** (`/api/dcl/conflicts`):
  names the disagreeing sources (workday_hr vs netsuite) and the decisive reconciled value.
- as-of = the bi-temporal `as_of` param on the read tools.

## Grounding, scoring, beats
- **Ground truth** resolves at run time from Farm feeds, never hardcoded (`demo/feeds.py`, B10).
- **Scoring** is pure/deterministic (`demo/scoring.py`): numeric match vs ground truth, provenance
  present, conflict disclosed vs the *live* Register.
- **Real-condition beats**: malformed ingest → **400 VALIDATION_FAILED** (`sequence.py:105`); audit-proof
  → every Panel-B MCP call equals a row in `mai_mcp_audit`.

## Run / current state
`DCL_MCP_TOKEN_SECRET=… python -m demo.sequence --entity ContextOSDemo` (dev :8104, prod-guarded).
Latest capture: entity `ContextOSDemo`, model claude-opus-4-8, **sequence PASS**, 10 live / 0 pending,
contextOS 6/6 numeric · 3/3 conflicts disclosed · 1/1 honest no-data; 60 open register conflicts.
