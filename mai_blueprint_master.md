# Mai Blueprint Master

Canonical consolidated specification. Supersedes Maestra spec v2, v4, v5, v7.1, v7.4.1, Phase 1 build prompts, Phase 2 build prompts, Omnipresence Blueprint v0.1, Demo Build Plan v3, convergence_MA_spec_v8 §3 (Mai sections), and mai_blueprint_v8.md. All prior Maestra/Mai specs marked superseded; archive only.

Two-phase scope: BRAIN (foundational runtime, single source of truth) and OMNIPRESENCE (surface rollout against stable brain). Sections tagged accordingly. FinOps and RevOps are explicitly out of scope, permanently.

---

## §1 — Identity and Generalization Charter

### §1.1 Identity

Mai is the AOS comprehension and engagement layer. Prompt-engineered persona on a frontier LLM. She operates across every surface where an operator interacts with the platform: Console, Platform, NLQ, AAM, DCL, AOD, Convergence. Same identity, same memory, same tool access, regardless of surface.

Mai is the concierge agent. She observes and explains engagement-driven workflows (COFA merge, entity resolution, combining, QofE adjustment review) but does not execute them. Convergence owns operational workflow execution. Day-to-day operator work (status checks, data exploration, configuration, troubleshooting) is equally first-class. The brand "AOS AI" applies in external/customer-facing contexts; "Mai" applies in internal operator surfaces.

### §1.2 Rename

Maestra → Mai everywhere in v8.0 brain phase. Includes: code identifiers (`maestra_*` → `mai_*` in all schema, function, file, and route names), constitution file headers, frontend component names (`MaestraChat` → `MaiChat`, `MaestraFloat` → `MaiFloat`, etc.), API routes (`/api/maestra/*` → `/api/mai/*`), database tables (`maestra_plans` → `mai_plans`, `maestra_runs` → `mai_runs`, etc.), environment variables, log prefixes, telemetry tags. One disruptive rename now is cheaper than two later.

User-facing strings ("Mai is thinking…", widget header) already use Mai or are updated as part of this rename.

### §1.3 Generalization Charter

Mai's identity, prompts, constitution, and code must work for an operator with zero engagements, zero M&A context, no Convergence framing. M&A is a workflow Mai recognizes and can explain when engagement context is present; it is never her default identity.

Concrete charter rules, enforced as v8 invariants:

- **No M&A framing in identity prompts.** Identity layer describes Mai as "AOS comprehension and engagement layer." No mention of M&A, integration, acquirer/target, deal lifecycle.
- **Engagement is an optional scope dimension.** Surface envelope may include `engagement_id` when relevant; Mai degrades gracefully when absent. Endpoints must not 404 on missing engagement.
- **M&A vocabulary is workflow-scoped.** Acquirer/target/COFA terminology only loads into context when an active engagement of type MA is in scope. Never leaks into general chat.
- **Module knowledge docs describe modules, not workflows.** `convergence.md` describes the Convergence repo's role and surface — not Mai's identity within it.
- **Status endpoints work for zero-engagement operators.** Every module status endpoint must return a valid response when no engagement exists. No 404, no 422.

Generalization compliance is a hard gate on every blueprint deliverable. Any new code, prompt, or schema that couples Mai to M&A by default is a violation.

---

## §2 — Architecture Overview

### §2.1 Runtime Shape

Shape 2 (HTTP central). Mai's runtime lives in Platform. Every surface calls Platform via the canonical chat endpoint. Platform owns Mai's identity, constitution, memory, tool dispatch, and inference.

Surfaces are clients. Surfaces send the canonical envelope, receive a canonical SSE stream, render markdown + structured tool events. Surfaces expose their state via MCP-shaped endpoints that Mai pulls from when needed.

Shape 4 (event-sourced projection) remains additive future work. Not in scope.

### §2.2 Component Map

| Component | Repo | Role |
|---|---|---|
| Mai runtime | platform | Inference, constitution, tool dispatch, single chat endpoint |
| Mai memory | platform (canonical store) | Plans, chat history, session memory, operator/tenant memory |
| Mai tool catalog | platform | Single registry, MCP-compatible tool schemas |
| Mai chat UI | console (reference), platform, nlq, aam, dcl, aod, convergence | Same component, same envelope, mounted per surface |
| Surface state MCP servers | console, platform, nlq, aam, dcl, aod, convergence | Each surface exposes `get_surface_state` and surface-specific tools |
| Engagement state | convergence | Canonical engagement CRUD |
| Run ledger | convergence | Engagement-scoped workflow step tracking |
| Instrumentation ledger | console | Per-run cost/latency/model logging |
| Triple store | dcl | Semantic data; Mai reads via `query_triples` MCP tool |

### §2.3 Runtime Flow

| # | Step | Component |
|---|---|---|
| 1 | Operator types message in Mai chat widget on any surface | Surface frontend |
| 2 | Surface POSTs canonical envelope to `POST /api/mai/chat` | Surface → Platform |
| 3 | Platform loads identity, constitution layers, scoped memory (operator + tenant + surface + optional engagement), chat history for session | Platform memory layer |
| 4 | Platform assembles minimal push-context (envelope identity + scoped memory headers) | Platform context assembler |
| 5 | Platform sends prompt to Claude with tool catalog (pull-based) | Platform → Claude API |
| 6 | Mai reasons, decides which tools to call: `get_surface_state(surface_id)`, `query_triples(...)`, `get_engagement(...)`, etc. | Claude inference |
| 7 | Tool calls dispatched via MCP to surface MCP servers and internal Platform tools | Platform tool dispatch |
| 8 | Tool results stream back into inference loop | Platform |
| 9 | Mai response streams back to surface via SSE | Platform → surface |
| 10 | Surface renders markdown + structured tool events; appends to global chat store | Surface frontend |
| 11 | Platform persists chat turn (user message + Mai response + tool calls + cost/latency) to `mai_chat_history` and `mai_runs` | Platform memory + Console instrumentation |

### §2.4 Mai Does Not Execute Operational Workflows

This is a structural rule, not a style choice. See convergence_blueprint_master §5.4 for the full category rule.

- **Agent** (Mai): LLM decides control flow. Open tool registry within concierge scope. For open-ended operator interaction.
- **Workflow** (Convergence operational work): code decides control flow. LLM is a bounded reasoning step inside one node. Restricted tool set.

Mai does not call Convergence workflow endpoints. Convergence workflows do not call Mai. The surfaces are independent. Mai observes workflow runs via `GET /api/convergence/runs` for concierge explanation to the operator.

Operational tools (`write_cofa_mapping`, `write_combining_output`, `write_resolution_decision`) are not registered in Mai's tool registry. They exist inside Convergence workflow handlers as internal calls. This is a permanent exclusion.

---

## §3 — Surface Contract

The contract is the spine. Every surface that hosts Mai conforms to this contract. No exceptions, no per-surface envelope shapes, no per-surface response protocols.

### §3.1 Canonical Request Envelope

`POST /api/mai/chat`

```json
{
  "message": "string (required) — user message text",
  "session_id": "string (required) — stable per chat session, generated by surface",
  "surface_id": "string (required) — one of: console, platform, nlq, aam, dcl, aod, convergence",
  "tenant_id": "uuid (required) — canonical tenant_id, never entity_id",
  "operator_id": "string (required) — operator/user identifier",
  "engagement_id": "uuid (optional) — present only when operator is working in engagement context",
  "page_context": {
    "route": "string (required) — current route path",
    "tab_label": "string (optional) — current tab if applicable",
    "surface_state_ref": "string (optional) — opaque ref Mai can pass to get_surface_state if she needs current screen state"
  }
}
```

`page_context` is intentionally minimal. Surfaces register their full state via MCP `get_surface_state`; Mai pulls when she needs it.

### §3.2 Canonical Response Protocol

Single protocol: SSE stream. No exceptions.

Event types:

| Event | Payload | Meaning |
|---|---|---|
| `content` | `{text}` | Markdown content chunk |
| `tool_use` | `{tool_name, tool_input, tool_call_id}` | Mai is invoking a tool |
| `tool_result` | `{tool_call_id, result, error?}` | Tool returned |
| `done` | `{run_id, tokens_in, tokens_out, model, cost_usd}` | Stream complete |
| `error` | `{error_code, message}` | Stream failed |

### §3.3 Mandatory vs. Optional Surface Behavior

**Mandatory for every surface hosting Mai:**
- Send canonical envelope on every chat request
- Handle SSE response with at minimum `content`, `done`, `error` events
- Render markdown
- Mount Mai chat widget consistent across surfaces
- Generate stable `session_id` per chat session, persist across route changes within the surface
- Expose surface state via MCP `get_surface_state` endpoint (see §6)
- Read chat history from Platform on widget mount (`GET /api/mai/chat/history?session_id=...`)

**Optional but recommended:**
- Surface `tool_use`/`tool_result` events for instrumentation
- Provide preset suggestion chips relevant to current surface (config-driven, see §3.4)
- Pass `engagement_id` when operator is in engagement context

### §3.4 Surface Presets

Per-surface preset suggestions are config-driven, never hardcoded into chat components. Each surface ships a `mai_presets.{ts,json}` file with route → suggestions mapping. Presets must respect the generalization charter — no M&A framing on non-engagement routes. Convergence surface presets may use M&A vocabulary because M&A is Convergence's domain.

### §3.5 Surface Conformance Checklist

Every surface must pass this checklist before being declared conformant:

1. Sends canonical envelope (no extra fields, no missing required fields)
2. Handles SSE response with all four event types
3. Renders markdown
4. Mounts Mai widget in canonical position
5. Exposes MCP `get_surface_state` endpoint
6. Reads chat history on mount
7. No M&A framing in presets unless surface is Convergence
8. No 404 / 422 when operator has no engagement
9. Session ID stable across in-surface navigation
10. Logs `done` event metadata to local telemetry

---

## §4 — Memory Model

Four scope dimensions. Each scope has its own table, its own retrieval pattern, its own write rules. All canonical storage lives in Platform, in the `mai_memory` schema.

### §4.1 Scope Dimensions

| Scope | Lifetime | Contains | Retrieval |
|---|---|---|---|
| **Operator** | Cross-session, cross-engagement, cross-surface | Preferences, working style, recurring patterns, explicit "always/never" instructions | Always loaded |
| **Tenant** | Cross-operator within tenant | Tenant-level facts (industry, deployed modules, custom vocabulary, integrations) | Always loaded |
| **Surface** | Cross-session, per-surface | Surface-specific context (last viewed entity in DCL, last query in NLQ, last filter in Console) | Loaded when surface_id matches |
| **Engagement** | Per-engagement | Decisions made, conflicts resolved, entity resolutions confirmed, conversation turns within engagement work | Loaded when engagement_id present in envelope |

Operator and tenant memory are small and curated. Surface and engagement memory grow with use.

### §4.2 Schemas

All under `mai_memory` schema in Platform Postgres. Tenant isolation via RLS plus application-layer `WHERE tenant_id = ?` filtering.

```sql
mai_operator_memory (
  id              uuid PRIMARY KEY,
  tenant_id       uuid NOT NULL,
  operator_id     text NOT NULL,
  memory_type     text NOT NULL,  -- preference | pattern | instruction
  content         jsonb NOT NULL,
  created_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, operator_id, memory_type, content->>'key')
)

mai_tenant_memory (
  id              uuid PRIMARY KEY,
  tenant_id       uuid NOT NULL,
  memory_type     text NOT NULL,  -- fact | vocabulary | integration
  content         jsonb NOT NULL,
  created_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now()
)

mai_surface_memory (
  id              uuid PRIMARY KEY,
  tenant_id       uuid NOT NULL,
  operator_id     text NOT NULL,
  surface_id      text NOT NULL,
  memory_key      text NOT NULL,
  memory_value    jsonb NOT NULL,
  updated_at      timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, operator_id, surface_id, memory_key)
)

mai_engagement_memory (
  id              uuid PRIMARY KEY,
  tenant_id       uuid NOT NULL,
  engagement_id   uuid NOT NULL,
  memory_type     text NOT NULL,  -- decision | resolution | conflict | note
  content         jsonb NOT NULL,
  created_by      text NOT NULL,  -- operator_id or 'mai'
  created_at      timestamptz NOT NULL DEFAULT now()
)

mai_chat_history (
  id              uuid PRIMARY KEY,
  tenant_id       uuid NOT NULL,
  operator_id     text NOT NULL,
  session_id      text NOT NULL,
  surface_id      text NOT NULL,
  engagement_id   uuid,
  turn_index      int NOT NULL,
  role            text NOT NULL,  -- user | assistant | tool
  content         jsonb NOT NULL,
  tool_calls      jsonb,
  run_id          uuid,
  query_embedding vector(1536),
  created_at      timestamptz NOT NULL DEFAULT now(),
  UNIQUE (session_id, turn_index)
)
```

### §4.3 Confidentiality Boundary

Operator memory must never store engagement-specific facts. Engagement memory is the only place deal facts live; filtered by engagement_id at read.

Tenant memory is shared across operators within the same tenant. Tenant memory must not contain operator-personal preferences.

### §4.4 Retrieval Composition

On every chat turn, Platform assembles memory in this order:

1. Identity (Layer 0 constitution)
2. Tenant memory (always)
3. Operator memory (always)
4. Surface memory for `surface_id` (always when surface_id present)
5. Engagement memory for `engagement_id` (only when engagement_id present)
6. Chat history for `session_id` (full or compressed depending on length)

Each section appears in the prompt under labeled headers so Mai never confuses scopes.

### §4.5 Chat History Compression

Chat history retrieval is bounded. Default: last 20 turns full, prior turns compressed to summary every 50 turns. Compression is a load-bearing component — uncompressed history fills context window, degrades attention, causes follow-up failure.

**Compression schedule.** Triggered when a session crosses the 20-turn threshold. Runs as background job, writes summary back to `mai_chat_history` as synthetic turn with `role='assistant'`, `content.compressed=true`.

**Failure handling — no silent fallbacks.**
- Compression failure logged with full error detail to `mai_runs` and surfaced via alert. Never silent.
- If failed or overdue: load only the most recent 20 turns and inject explicit prompt note: "CHAT HISTORY NOTE: Earlier turns in this session could not be loaded. You have access to the most recent 20 turns only."
- This is the no-silent-fallback rule applied to memory: Mai is told what she has and doesn't have, never left to hallucinate from gaps.

**Compression quality.** Compression uses Claude with a constrained summarization prompt that preserves: decisions made, entities discussed, tools called and their results, open questions, operator preferences expressed. Discards: pleasantries, restated context, tool call mechanics.

---

## §5 — Tool Catalog (Pull-Based)

Single canonical registry. All Mai tools live in Platform's `mai_tool_registry`. Tools fall into three classes: internal (Platform-implemented), surface MCP (each surface exposes), data MCP (DCL exposes).

### §5.1 Internal Tools (Platform)

| Tool | Purpose |
|---|---|
| `get_engagement(engagement_id)` | Fetch engagement state from Convergence |
| `list_engagements(tenant_id, status?)` | List engagements operator has access to |
| `get_operator_memory(memory_type?)` | Read operator memory |
| `write_operator_memory(memory_type, content)` | Persist operator preference / pattern |
| `write_engagement_memory(engagement_id, memory_type, content)` | Persist engagement decision |
| `classify_change(message)` | Tier 1-4 classification (supervised execution) |
| `execute_tier1(plan)` | Execute Tier 1 preference change |
| `persist_escalation(plan)` | Persist Tier 3/4 plan |
| `get_chat_history(session_id, range?)` | Fetch chat history |

### §5.2 Surface MCP Tools

Each surface exposes its own MCP server on a known internal route.

Mandatory per surface: `get_surface_state(surface_id)` — current screen state.

Optional surface-specific tools (read-only unless otherwise noted):

| Surface | Tools |
|---|---|
| Console | `get_visible_pipeline_status`, `get_active_filter`, `get_selected_run` |
| NLQ | `get_current_query`, `get_query_history`, `run_query` |
| AAM | `get_manifest_detail`, `get_pipe_schema`, `retry_manifest` |
| DCL | `get_merge_state`, `get_conflict_detail` |
| AOD | `get_discovery_run_detail`, `get_finding_detail` |
| Convergence | `get_engagement_overview`, `get_review_gates`, `get_workflow_run_summary` |

**Convergence surface tools are read-only.** No `trigger_cofa_unification` or other operational dispatch tools. Convergence workflow execution routes through Convergence's own workflow endpoints (POST /api/convergence/workflow/*/run), not through Mai tool dispatch. This is a permanent exclusion per the Mai/workflow category rule.

Surface tools that mutate state require Tier 3/4 plan classification before execution. Read tools execute freely.

### §5.3 Data MCP Tools (DCL)

| Tool | Purpose |
|---|---|
| `query_triples(domain, entity_id?, period?, limit?)` | SQL-native triple retrieval |
| `concept_lookup(concept)` | Resolve concept hierarchy |
| `semantic_export(domain)` | Bulk semantic metadata |
| `provenance(triple_id)` | Trace triple to source |
| `list_domains(entity_id?)` | Domains available |

### §5.4 Tool Discovery

Mai sees the full catalog on every turn. Tool descriptions written for the LLM, not for developers. Hot-reloadable in development; production loads at boot.

### §5.5 MCP Internal Use

MCP protocol is used for surface state pull. Internal MCP only — no external MCP gateway for this phase. Platform acts as MCP client. External wrapping is mechanical when enterprise gateway patterns mature.

---

## §6 — Single Chat Endpoint

### §6.1 Endpoint

`POST /api/mai/chat`

Single endpoint for all Mai chat across all surfaces. Replaces:
- Platform `/api/maestra/chat` (general)
- NLQ `/maestra/chat` (parallel implementation)
- Convergence `/api/convergence/maestra/cofa-chat` (proxy)

All three are removed during the brain phase. NLQ stops running its own Mai chat service entirely; NLQ becomes a surface like every other surface, calling Platform.

Note: The legacy Platform `/api/maestra/cofa-chat` endpoint (engagement-bound, non-streaming) is retired as part of the Convergence workflow cutover (convergence_transition_master), not as part of Mai Brain-4. Its last caller (MergePanel) reroutes to Convergence's workflow endpoint. Brain-4 does not touch it.

### §6.2 Why Collapse

NLQ's parallel chat existed because NLQ was the original Mai surface (per pre-v8 plans). With Platform as canonical Mai runtime, NLQ has no reason to host its own chat.

The legacy COFA chat endpoint existed to return structured `tool_calls` synchronously. COFA execution moved to Convergence's workflow endpoint (`POST /api/convergence/workflow/cofa_merge/run`). MergePanel calls Convergence directly. Mai has no role in the COFA execution path.

### §6.3 Auth

Same auth as existing Platform endpoints. Operator identity established via existing session token; `operator_id` and `tenant_id` in envelope are validated against token claims.

---

## §7 — Constitution

Layered constitution loaded at runtime. Each layer is a Markdown file in `platform/app/mai/constitution/`.

### §7.1 Layer Map

| Layer | Contents | Always Loaded? |
|---|---|---|
| Layer 0 | Identity, voice, generalization charter, scope constraints (read-broad/write-admin-only), admin tool catalog | Yes |
| Layer 1 | Scenario Variant — AOS single-entity / Convergence multi-entity / M&A / portfolio — adjusts navigation vocabulary and surface references | Per engagement type |
| Layer 2 | Observability grammar — how to query and summarize workflow runs, how to explain failures, how to direct operators to appropriate surfaces for action. Workflow rules, act vs. ask, escalation criteria. | Yes |
| Layer 3 | Quality gates | Yes |
| Surface knowledge | One doc per surface (`surfaces/console.md`, `surfaces/aam.md`, etc.) | Only matching surface_id |

Accounting axioms (Dr=Cr, A=L+E, etc.), COFA ontology, and entity policies are NOT part of Mai's constitution. They live in Convergence workflow prompts where they govern operational reasoning. Mai does not need them for concierge work — she explains workflow results by reading structured run summaries, not by re-reasoning over accounting rules.

### §7.2 Renames

`constitution/modules/` → `constitution/surfaces/` (matches surface_id terminology)

`constitution/modules/convergence.md` rewritten: describes Convergence repo's purpose and surfaces, not Mai's identity within it. M&A vocabulary moves to Convergence workflow prompts (convergence_blueprint_master §5.5).

### §7.3 Identity Layer Rewrite

Layer 0 identity is rewritten in brain phase to remove M&A framing. New identity opens:

> Mai is the AOS comprehension and engagement layer. She works with operators across every AOS surface to surface what the platform knows, explain what's happening, and help operators get work done. Operators may be running day-to-day operations, exploring data, configuring the platform, or working through engagement-specific workflows like M&A integration. Mai handles all of these as a single coherent presence. Mai does not execute operational workflows — she observes and explains them.

### §7.4 Hot Reload

Constitution files watched in development; reload on change. Production loads at boot. `GET /api/mai/constitution` exposes current loaded layers for debugging.

---

## §8 — State Ownership / RACI

Single source of truth per object. No duplicate schemas. Brain phase retires all duplicates.

| Object | Canonical Owner | Schema |
|---|---|---|
| Mai chat history | Platform | `mai_memory.mai_chat_history` |
| Mai operator memory | Platform | `mai_memory.mai_operator_memory` |
| Mai tenant memory | Platform | `mai_memory.mai_tenant_memory` |
| Mai surface memory | Platform | `mai_memory.mai_surface_memory` |
| Mai engagement memory | Platform | `mai_memory.mai_engagement_memory` |
| Mai plans (Tier 1-4) | Platform | `mai_plans` (renamed from `maestra_plans`) |
| Tenant preferences | Platform | `tenant_preferences` (existing) |
| Engagements | Convergence | `engagements` (existing canonical) |
| Run ledger | Convergence | `run_ledger` (existing) |
| Instrumentation ledger | Console | `mai_runs` (renamed from `maestra_runs`) |
| Triples | DCL | Existing semantic store |
| Entity policies | Convergence | `convergence/backend/policies/` (Markdown files, read by workflow pre-flight) |

### §8.0 Observability as Code (OaC)

Console owns the instrumentation ledger schema, but observability configuration lives as version-controlled YAML in `console/config/mai_observability.yaml`. Loaded by Console at boot. When a new surface is added, its observability config is added in the same PR.

### §8.1 Retirements

| Retired | Replaced By |
|---|---|
| NLQ `customer_engagements` | Convergence `engagements` |
| NLQ `session_memory` | Platform `mai_chat_history` + `mai_engagement_memory` |
| NLQ `plans` | Platform `mai_plans` |
| NLQ `module_state_cache` | Pull-based via MCP `get_surface_state` |
| NLQ `interaction_log` | Console `mai_runs` |
| NLQ `customer_playbooks` | Platform `mai_tenant_memory` + `mai_operator_memory` |
| DCL `engagement_state` | Convergence `engagements` |
| DCL `run_ledger` | Convergence `run_ledger` |

NLQ becomes a surface, not a Mai runtime. Its 13 Maestra Python modules are deleted. Its constitution file is deleted; Platform owns the canonical constitution.

### §8.2 Cross-Repo Access Pattern

Surfaces never read Mai memory directly. Mai memory access goes through Platform APIs. Convergence reads engagement memory via Platform's `get_engagement_memory` tool, not via direct DB access to `mai_memory` schema.

---

## §9 — Generalization Remediation

Brain phase remediations against diagnostic findings.

### §9.1 Identity / Prompt Remediation

| Violation | Remediation |
|---|---|
| `nlq/src/nlq/maestra/prompts.py:49` — M&A baked into identity | NLQ Mai code deleted entirely; identity comes from Platform Layer 0 |
| `platform/app/maestra/constitution/modules/convergence.md:1-3` — frames Mai as M&A lead | Rewrite as surface description; M&A content irrelevant to Mai |

### §9.2 Schema Remediation

| Violation | Remediation |
|---|---|
| NLQ Maestra schema, seed data, context — 6+ files | NLQ schema deleted entirely with NLQ Mai code |
| `dcl/src/maestra/validation/rules.py:7` — V-004 M&A-only | Moved to engagement-conditional rule set |
| `dcl/migrations/001_semantic_triple_store.sql:95-112` — binary entity pairing | Schema retires when DCL → Convergence migration completes |

### §9.3 Frontend Remediation

| Violation | Remediation |
|---|---|
| `console/frontend/src/components/maestra/presets.ts:21,22,47-50` — M&A presets on every page | Per-route preset config; M&A presets only on Convergence routes |
| `console/frontend/src/context/EngagementContext.tsx:17-26` — MA preferred default | Default selection neutralized |
| `console/frontend/src/context/MaestraPageContext.tsx` — zero callers | Removed; replaced by MCP `get_surface_state` |
| `convergence/src/components/MergePanel.tsx:325-327` — hardcoded message | MergePanel calls Convergence workflow endpoint directly (POST /api/convergence/workflow/cofa_merge/run). No Mai involvement. |

### §9.4 Status Endpoint Remediation

| Violation | Remediation |
|---|---|
| AOD status omits entity_id | Status response includes `entity_id` (nullable) |
| AOD/AAM/Farm/DCL status endpoints lack `engagement_id` | Optional `engagement_id` query param added |

---

## §10 — Build Sequence

Two phases. BRAIN must complete fully before OMNIPRESENCE begins. Each item is a CC session with a defined harness.

### §10.1 BRAIN

#### Brain-1: Schema Foundation

Create `mai_memory` schema in Platform Postgres. Tables per §4.2. RLS policies, indexes, updated_at triggers, application-layer tenant filtering.

Harness: CRUD on every table, tenant isolation tests, write-time confidentiality boundary tests, chat history compression test.

#### Brain-2: Rename Sweep

`maestra` → `mai` across Platform, NLQ, Console, Convergence, DCL, AAM, AOD, Farm.

Harness: grep across all repos for `maestra`, `Maestra`, `MAESTRA`. Zero hits except in archive folders. All tests pass post-rename.

Note: this is the most disruptive single session. Plan Mode required.

#### Brain-3: Constitution Rewrite

Layer 0 identity rewritten per generalization charter. `constitution/modules/` → `constitution/surfaces/`. `convergence.md` rewritten as surface description.

Harness: load each layer in isolation, assert no M&A framing in Layer 0, assert workflow knowledge is absent from Mai's constitution.

#### Brain-4: Single Chat Endpoint

`POST /api/mai/chat` implemented per §3 contract. SSE response with all four event types. Memory composition per §4.4. Constitution loading per §7. Pull-based tool dispatch via MCP.

**Scope:** Delete NLQ parallel chat endpoint. Delete Platform concierge duplicates. Stand up the single canonical concierge endpoint.

**NOT in scope:** The legacy `/api/maestra/cofa-chat` endpoint. Its retirement happens as part of the Convergence workflow cutover, on Convergence's timeline, when MergePanel reroutes to `POST /api/convergence/workflow/cofa_merge/run`.

Harness: send canonical envelope from synthetic surface, assert SSE response, assert all event types, assert no M&A vocabulary in zero-engagement context, assert engagement vocabulary loads when engagement_id present. Negative test: assert no tool in Mai's registry can mutate Convergence workflow state.

#### Brain-5: Tool Catalog Consolidation

Single `mai_tool_registry` in Platform. Internal tools per §5.1, MCP client setup for surface tools per §5.2, DCL data tools per §5.3.

Harness: every tool callable via dispatcher, schemas validated against MCP spec. Negative test: no operational trigger/write tools registered for Convergence surface.

#### Brain-6: NLQ Mai Code Deletion

Delete: `nlq/src/nlq/maestra/` (13 modules), `nlq/src/maestra/`, `nlq/sql/maestra/`, seed data. NLQ becomes a surface with no Mai runtime code.

Harness: NLQ test suite passes, grep shows zero Mai runtime references in NLQ outside surface chat client code.

#### Brain-7: State Migration and Retirement

DCL `engagement_state` and `run_ledger` migration to Convergence completes. Console `mai_runs` confirmed as instrumentation source of truth. NLQ legacy tables dropped.

Harness: existing data migrated without loss, old tables dropped, new tables receive writes from canonical endpoints.

#### Brain-8: Console Reference Surface

Console becomes the conformant reference surface per §3.5. Canonical envelope, SSE handling, MCP `get_surface_state` exposed, chat history read on mount, presets generalized.

Harness: §3.5 conformance checklist 10/10 pass. Day-to-day operator (no engagements) can use Mai across every Console route without 404, 422, or M&A framing leakage.

### §10.2 OMNIPRESENCE

Each surface migrated to canonical contract and conformant per §3.5.

#### Omni-1: Platform Frontend Migration

Platform's own sidebar and float migrated to canonical envelope and SSE protocol. Platform exposes its own MCP `get_surface_state`.

#### Omni-2: Convergence Migration

Convergence surface mounts Mai chat widget for concierge use. Mai answers questions like "what did the last COFA merge find?" by reading Convergence's run ledger via `GET /api/convergence/runs`. M&A presets allowed on Convergence routes per §3.4.

MergePanel COFA execution routes to Convergence's own workflow endpoint (`POST /api/convergence/workflow/cofa_merge/run`), independent of Mai. This is a Convergence concern, not an omnipresence concern. Convergence MCP server exposes read-only tools: `get_engagement_overview`, `get_review_gates`, `get_workflow_run_summary`.

#### Omni-3: NLQ Surface

NLQ frontend mounts Mai chat widget. Canonical envelope. NLQ MCP server exposes `get_current_query`, `get_query_history`, `run_query`.

#### Omni-4: AAM Surface

AAM frontend mounts Mai chat widget. Canonical envelope. AAM MCP server exposes `get_manifest_detail`, `get_pipe_schema`, `retry_manifest`.

#### Omni-5: DCL Surface

DCL frontend mounts Mai chat widget. Canonical envelope. DCL MCP server exposes `get_merge_state`, `get_conflict_detail`.

#### Omni-6: AOD Surface

AOD frontend mounts Mai chat widget. Canonical envelope. AOD MCP server exposes `get_discovery_run_detail`, `get_finding_detail`.

#### Omni-7: Farm Status Generalization

Farm has no chat surface (backend-only). Farm's status endpoint generalized. Farm MCP server exposes `get_generation_progress`, `get_persona_detail`.

### §10.3 Sequencing Constraints

- Brain-2 (rename) before Brain-3, -4, -7 (which use new names)
- Brain-1 (schemas) before Brain-4, -7 (which write to them)
- Brain-3 (constitution) before Brain-4 (which loads it)
- Brain-5 (tool catalog) before Brain-4 final harness (which uses tools)
- Brain-6 (NLQ deletion) and Brain-7 (state migration) can run in parallel after Brain-2
- Brain-8 (Console reference) is the brain phase exit gate
- All Omni-* sessions blocked on Brain-8 completion
- Omni-* sessions can run in any order or in parallel

---

## §11 — Out of Scope

Permanently out of scope (do not propose without explicit re-scoping):

- **FinOps Mai integration.** FinOps continues to use platform intent bus.
- **RevOps Mai integration.** Same rationale.
- **External MCP gateway.** Internal MCP only.
- **Shape 4 (event-sourced projection).** Additive future.
- **Semantic cache.** Deferred until production usage data.
- **Model routing.** Sonnet for everything. Cost lever for later.
- **Fine-tuning, training, custom models.** Mai is prompt-engineered on frontier LLM.
- **Codebase RAG / oracle mode.** Constitution + tool catalog cover capability questions.
- **Proactive insight surfacing without explicit operator query.** Reactive only.
- **Cross-tenant pattern learning.** Patent-sensitive, security-sensitive.
- **Action dispatch for irreversible operations without Tier 3/4 plan classification.**
- **Operational workflow execution via Mai tool dispatch.** COFA merge, combining, entity resolution, adjustment review, and all other Convergence workflows execute via Convergence workflow endpoints, not via Mai tool calls. Mai observes workflow results but does not trigger workflow execution. Permanent exclusion.

### §11.5 Performance and Cost Targets

| Metric | Brain phase | Omnipresence phase |
|---|---|---|
| First-token latency p50 | <2s | <1s |
| First-token latency p95 | <4s | <2s |
| Full-turn cost p50 | <$0.10 | <$0.05 |
| Full-turn cost p95 | <$0.30 | <$0.15 |
| Tool call success rate | >95% | >99% |
| Chat history retrieval p50 | <100ms | <50ms |
| Compression job success rate | >99% | >99.5% |

---

## §12 — Guardrails

Hard rules that govern all work. Violations are blocking.

- **Generalization first.** No new code, prompt, schema, or UI element couples Mai to M&A by default.
- **Single source of truth per state object.** No duplicate tables, no parallel implementations.
- **Surface contract is non-negotiable.** Every surface conforms to §3.
- **Pull over push for surface state.** Surfaces register state via MCP; Mai pulls when needed.
- **Memory scopes never leak.** Operator memory has no engagement content. Tenant memory has no operator-personal content.
- **Platform is canonical Mai runtime.** No parallel Mai implementations in other repos.
- **No silent fallbacks.** If memory retrieval, tool call, or surface state pull fails, Mai is told what failed.
- **No M&A vocabulary in zero-engagement context.**
- **Plan mode for irreversible writes.** Tier 3/4 classification gates all surface tool calls that mutate state.
- **Brain phase completes before omnipresence phase begins.**
- **Mai does not execute operational workflows.** This is the category rule. Mai reads. Convergence acts.

---

## §13 — Diagnostic Baseline

Current state baseline from Mai Diagnostic Report (April 15, 2026):

- 8 of 11 repos have Mai integration; 3 repos (Console, Platform, NLQ) have deep integration
- 14 generalization violations across 6 repos (2 BLOCKER, 4 HIGH, 5 MED, 3 LOW)
- 5 distinct request envelope shapes
- 2 incompatible response protocols inside Platform alone
- 4 repos persist Mai-adjacent state in their own tables
- Zero chat history persistence; all chat ephemeral in React state
- Console's `MaestraPageContext.setPageContext()` has zero callers

Brain phase exit gate: every diagnostic finding addressed, verified via §3.5 conformance checklist for Console and §9 remediation table.

---

## §14 — Governing Documents

| Document | Scope |
|---|---|
| mai_blueprint_master.md (this document) | Canonical Mai specification |
| convergence_blueprint_master.md | Convergence product architecture, workflow pattern, engine inventory |
| convergence_transition_master.md | Convergence transition from fixture-based to generic |
| AOS_MASTER_RACI_v8.6 | Module ownership matrix |
| pipeline_identity_architecture_v1 | Pipeline identity, provenance, naming |
| CLAUDE.md (per-repo) | Agent constitution, harness rules |

---

## §15 — Terminology

| Spec term | Code term | Meaning |
|---|---|---|
| AOS | SE | Single-entity product / pipeline mode |
| Convergence | ME | Multi-entity product / pipeline mode |
| Convergence M&A | MA | Deal-driven M&A engagement workflow |
| Mai | maestra (pre-rename) / mai (post-rename) | Concierge agent |

Product names (AOS, Convergence) are used in specs and operator-facing surfaces. Code identifiers (SE, ME) are retained until the code cleanup phase bundles the rename.

---

End of blueprint.
