# Convergence Blueprint Master

**AOS Convergence M&A Specification**
Version 9 — April 2026

AutonomOS, Inc.

*Canonical governing document. All build decisions, CC prompts, and GTM materials reference this spec. Supersedes convergence_MA_spec_v5 through v8. Companion documents: mai_blueprint_master (Mai concierge), convergence_transition_master (fixture eradication and resolver).*

---

# 1. Product Overview

AOS (autonomOS) is an enterprise platform that delivers unified context across enterprise systems. The platform has three product lines: AOS (single-entity enterprise intelligence), Convergence (multi-entity integration intelligence), and **Convergence M&A (diligence through post-integration).** All share a common engine.

## 1.1 Product Lines

**AOS.** Single-entity enterprise intelligence. Full pipeline: AOD (discovery) → AAM (connection mapping) → Farm (financial model generation) → DCL (semantic context layer) → NLQ (natural language query). Customer connects their systems, AOS builds contextual intelligence.

**Convergence.** Multi-entity integration intelligence for organizations with multiple subsidiaries that need contextual integrated information. Ongoing operating rhythm, not deal-driven. Same engine as AOS and Convergence M&A: entity is a tag, same resolution, same ontology. Multiple entities flow through the pipeline tagged by entity_id into Convergence's own triple store (convergence_triples). Unified reporting, cross-entity analytics, and continuous monitoring across persistent entities. Convergence reads DCL-owned tables (SELECT only) for cross-reference.

**Convergence M&A.** Multi-entity M&A integration intelligence. Deal-driven: diligence through post-integration. Acquirer and Target data flow into one unified context via Convergence's convergence_triples. Entity is a tag, not a separate brain. Same engines as base AOS, plus a bridge that joins Target pipes into Acquirer pipes. COFA unification, combining financial statements, entity resolution, overlap/concentration analysis, cross-sell, EBITDA bridge, QofE — all delivered as Convergence workflows.

PE-specific portfolio product is deferred. The Convergence product line covers multi-entity operating use cases including fund-level visibility across portfolio companies.

### Terminology

| Spec term | Code term | Meaning |
|---|---|---|
| AOS | SE | Single-entity product / pipeline mode |
| Convergence | ME | Multi-entity product / pipeline mode |
| Convergence M&A | MA | Deal-driven engagement workflow |

Product names (AOS, Convergence) are used in specs and operator-facing surfaces. Code identifiers (SE, ME) are retained until the code cleanup phase bundles the rename.

## 1.2 Three User Stories

| Story | Entry Point | Pipeline | Commercial Path |
|---|---|---|---|
| 1. Convergence-Lite | Greenfield M&A, upload-based (GL minimum) | No AOD/AAM. GL+CoA ingest → Convergence workflow chain. | Lands diligence (Explore) → Resolve |
| 2. AOS Single-Entity | Enterprise connects systems | Full AOD→AAM→Farm→DCL→NLQ | Standalone entry, enables Story 3 |
| 3. AOS→Convergence | Acquirer already on platform | Target onboarded via upload or discovery. Convergence runs across both. | Upsell to Resolve, then Operate |
| 3.5 Don't Migrate, Converge | Post-close, target stays on own systems | Target gets AOS + persistent Convergence replaces system migration | Operate tier |

## 1.3 Convergence-Lite: Input / Output Spec

GL detail is the minimum input. We do not design, spec, or build for a CoA-only or summary-TB-only scenario.

### 1.3.1 Required Inputs

| Input | Format | Minimum Scope | What It Enables |
|---|---|---|---|
| General Ledger (both entities) | CSV/Excel upload | 8+ quarters monthly detail | Full fidelity: line-item combining, trending, variance, QofE, EBITDA bridge |
| Chart of Accounts (both entities) | CSV/Excel upload or extracted from GL | Account number, name, type, hierarchy | COFA mapping, domain boundary enforcement, conflict identification |

### 1.3.2 Optional Enrichment Inputs

| Input | What It Unlocks |
|---|---|
| Customer sub-ledger or customer list with revenue by customer | Entity resolution, customer overlap/concentration, cross-sell |
| Vendor sub-ledger or vendor list with spend by vendor | Vendor overlap, procurement synergy identification |
| Employee/headcount data | People overlap, org structure comparison, compensation benchmarking |
| Trial balance (if GL not available at line level) | NOT a substitute for GL. Summary-only output. Not the design target. |

### 1.3.3 Output: Diligence Integration Package

| # | Deliverable | Source |
|---|---|---|
| 1 | Unified Chart of Accounts | COFA merge workflow |
| 2 | Conflict Register (typed, ranked by materiality) | COFA merge workflow |
| 3 | Combining P&L / BS / SOCF (four-column format) | Combining workflow |
| 4 | Unified Trial Balance | Combining workflow |
| 5 | EBITDA Bridge with confidence grades | EBITDA bridge workflow |
| 6 | QofE adjustments (two-axis: fiscal period + lifecycle stage) | QofE workflow |
| 7 | Entity Resolution workspaces | Entity resolution workflow |
| 8 | Overlap & Concentration report | Overlap workflow |
| 9 | Cross-Sell pipeline | Cross-sell workflow |
| 10 | Audit trail for every human decision | Convergence run_ledger |

---

# 2. Architecture Decisions

## 2.1 Core Decisions

| Decision | Ruling | Rationale |
|---|---|---|
| Storage engine | Postgres (Supabase) for MVP | EAV at two-entity scale is fine. Evaluate columnar/graph at 10+ entities. Note: PG-specific optimizations (atomic run swap, COPY ingest, is_active flags) accumulate switching cost. |
| **Agent vs workflow separation** | **Mai is the concierge agent. Mai does not execute operational workflows. Convergence owns operational workflows with code-driven control flow and bounded LLM reasoning.** | **Agency reserved for Mai's concierge scope. Operational execution is code-driven. COFA merge, combining, entity resolution, QofE review, and cross-sell are workflows, not agent tasks.** |
| **Workflow invocation transport** | **Request/response HTTP. SSE is reserved for chat streaming.** | **Structured response contract. No frame-parsing ambiguity.** |
| **Convergence LLM access** | **Convergence calls the model directly via Anthropic SDK inside workflow handlers. No dependency on Platform Mai.** | **Product-local LLM access. Each workflow owns its prompt and structured output contract.** |
| Context management | Staged processing with stored work products | RAG deferred. Each stage fits within context window. |
| Workflow engine | Code-driven control flow. No LangGraph/Temporal. | Workflows: pre-flight → LLM reasoning → validate → persist → respond. |
| Entity resolution scale | Deterministic keys first, LLM for fuzzy residue | O(n²) at portfolio scale. Add blocking/clustering at 10+ entities. |
| COFA accuracy | Validated: 100% completeness, 6/6 conflicts, $1.49/engagement | Gating technical risk retired. |
| Testing | Two-tier: deterministic harness + LLM-as-Judge | CLAUDE.md Sections A–F, 26+ rules. 100% pass or not done. |
| Document ingestion | CSV + Excel for MVP. Automated PDF/Word parsing Phase 2. | Entity policies are manually authored Markdown in Convergence repo. |
| Model routing | Sonnet for everything at MVP | Cost lever activated when volume justifies. |
| Convergence architecture | Same engine as AOS. Entity is a tag. DCL is AOS-only. Convergence triples write to convergence_triples. | No split brain, no query-time composition. |
| Silent fallbacks | Prohibited. Fail loud or not at all. | Hard architectural rule across all repos. |
| No GAAP fallback | Workflow reasoning must not infer from general GAAP when entity policy is missing | Output null with flag. |
| QofE adjustment model | Two-axis: fiscal period attribution + diligence lifecycle stage. Triple key: (entity_id, concept, property, lifecycle_stage). | Enables temporal comparison across lifecycle stages. |
| AOS/Convergence pipeline separation | Strict separation. No shared plumbing. Convergence repo owns all Convergence engines. DCL is AOS-only. | Pipeline isolation spec v1/v2 produced. Carveout blueprint executed. |
| Pipeline identity architecture | run_id banned from payloads. Namespaced stage identifiers. Identity pair on every response. 422 on missing. | Per pipeline_identity_architecture_v1. |
| Supervised execution | Four-tier model governs Convergence workflow invocations. Classification engine in Platform. Console operator feed is review surface. | Workflow actions require oversight proportional to risk. |

---

# 3. Mai (Reference Summary)

Mai is the concierge agent hosted in Platform. See **mai_blueprint_master** for the full specification.

**Scope.** Onboarding guidance, admin (tenant/user/engagement metadata), tech support, navigation, explanation of platform state, observability queries over workflow runs. Reads broadly across the platform, writes narrowly to admin surfaces only.

**Does not.** Execute operational workflows. Mutate engagement state beyond admin metadata. Register operational tools in her tool catalog. Route COFA merge, combining, entity resolution, or QofE adjustment review through her chat endpoint.

**Observability.** Mai reads Convergence workflow run summaries via `GET /api/convergence/runs`. She explains workflow results to operators by reading structured run data, not by re-reasoning over accounting rules.

**Constitution.** Three layers: Identity, Scenario Variant, Observability Grammar. Accounting axioms, COFA ontology, and entity policies are NOT part of Mai's constitution — they live in Convergence workflow prompts.

**Supervised execution.** The four-tier classification engine lives in Platform. Console operator feed is the review surface. Tier 3/4 applies to Convergence workflow invocations that mutate engagement state.

| Tier | Name | Example |
|---|---|---|
| 1 | Auto-execute | Mai read-only queries |
| 2 | Validate | Admin metadata writes |
| 3 | Plan Mode | COFA merge workflow |
| 4 | Escalate Only | Cross-domain reclassification |

**Two run ledgers.** Platform's `mai_runs` (concierge interactions) and Convergence's `run_ledger` (workflow invocations). Not merged. Mai reads Convergence runs via HTTP.

**Entity policies.** Manually authored Markdown files hosted in `convergence/backend/policies/`. Read by workflow pre-flight at invocation time. Not part of Mai's constitution.

---

# 4. DCL (Data Context Layer) — AOS Only

DCL is an AOS-only service. All Convergence engines, engagement lifecycle, and resolution workspace logic live in the Convergence repo (§5A).

## 4.1 Semantic Triple Store

All data in DCL lives as semantic triples: (entity_id, concept, property, value, period). Every triple carries provenance: source_system, source_field, confidence_score, confidence_tier, pipe_id, run_id, created_at. Stored in Postgres (Supabase).

Concepts use dot-separated hierarchical naming. Domains are the first segment of the concept name.

DCL owns all write-side invariants: old-run deactivation, COPY-based bulk ingest, is_active flag management, tenant_runs.current_run_id updates, atomic run_id swap pattern.

Convergence reads DCL-owned tables directly (SELECT only) for report-time metric queries. Convergence never writes to DCL-owned tables. All Convergence triple writes go to convergence_triples. If Convergence entity data appears in DCL's semantic_triples, that is a pipeline routing bug.

## 4.2 Current State

| Metric | Value |
|---|---|
| Total triples (last ingest) | ~23,000 |
| Entities | 1 (AOS entity per tenant) |
| Domains | 17 |
| Periods | 24 |
| AOS engines retained | resolver_v2, ontology, semantic_graph, graph_store, dcl_engine |
| Integration tests passing | 131 (AOS suite) |

**Note:** Stale Convergence data cleanup (meridian, cascadia, combined, UUID-entity rows) is tracked in se_triples_conversion_build_plan_v2.2 §1. If these entities still appear in semantic_triples, that is the pipeline routing bug defined in §4.1.

## 4.3 DCL Table Ownership

| Table | Owner | Who Reads | Who Writes | Migrations Live In |
|---|---|---|---|---|
| semantic_triples | DCL | DCL + Convergence (SELECT only) | DCL only (AOS Farm financial triples via POST /api/dcl/ingest-triples) | dcl/migrations/ |
| convergence_triples | Convergence | Convergence | Convergence only (Farm Convergence triples, COFA mappings, combining output) | convergence/migrations/ |
| dimension_values_v2 | DCL | DCL + Convergence | DCL only | dcl/migrations/ |
| tenant_runs | DCL | DCL + Convergence | DCL only | dcl/migrations/ |
| tenant_registry | DCL | DCL | DCL only | dcl/migrations/ |

Schema contract in SCHEMA_CONTRACT.md (DCL repo). Additive changes are non-breaking. Column renames/type changes/removals are breaking and require Convergence repo coordination before merge. **No automated CI gate currently enforces this contract — paper guard only.**

## 4.4 Farm Configurations

**Transitional.** Current fixture-based configs (farm_config_meridian.yaml, farm_config_cascadia.yaml) are superseded by convergence_transition_master WP2 (Farm industry catalog generalization). These configs will be deleted, not demoted. See convergence_transition_master for the transition plan.

## 4.5 Data Pipeline

**AOS pipeline:** Farm Snapshot → AOD Discovery → Handoff → AAM Inference → Farm Financials → DCL Ingest. Orchestrator calls Farm, then pushes financial triples to DCL via POST /api/dcl/ingest-triples. AOD/AAM direct-PG write code is tech debt (see se_triples_conversion_build_plan_v2.2).

**Convergence pipeline:** Both entities' data flow through separate Farm configs → triple conversion → convergence_triples, tagged by entity_id. Convergence repo runs workflow chain (COFA merge, combining, resolution, bridge, QofE). Farm Convergence push routes to Convergence backend (port 8010), not DCL. Convergence triples never write to DCL's semantic_triples.

---

# 5. Convergence Architecture

Convergence = base AOS plus a bridge where Target pipes join Acquirer pipes into one unified context. Entity is a tag, same engine, no split brain, no query-time composition.

## 5.1 Invariants

Same engine as base AOS. Entity_id is a column, not a separate instance. No new resolution logic beyond what AOS uses. The bridge is the join point, not a fork. Convergence owns its operational workflows end to end.

## 5A. Convergence Service (Separate Repo)

### 5A.1 Service Boundaries

| Attribute | Value |
|---|---|
| Repo | convergence |
| Backend | FastAPI, port 8010 |
| Frontend | React, port 3010 |
| Branch | dev |
| Database | Same Supabase PG instance as DCL (shared store, entity is a tag) |

### 5A.2 Engines and Modules

| Module | Purpose | Status |
|---|---|---|
| model_client | Structured LLM invocation. Anthropic SDK. No agent loop, no tool registry. | ACTIVE (v9) |
| combining_v2 | Combining financial statements (P&L, BS, SOCF) | BUILT |
| ebitda_bridge_v2 | EBITDA bridge with adjustments | BUILT |
| qoe_v2 | Quality of Earnings analysis | BUILT |
| overlap_v2 | Customer/vendor overlap and concentration | BUILT |
| cross_sell_v2 | Cross-sell pipeline generation | ON DOCKET |
| upsell_v2 | Upsell analysis | ON DOCKET |
| entity_resolution_v2 | Cross-entity identity matching | BUILT (fixture-based; transition to resolver in convergence_transition_master) |
| cofa_mapping_writer | COFA triple persistence from workflow output | BUILT |
| what_if_v2 | Scenario modeling | ON DOCKET |
| engagement.py | Engagement lifecycle management | BUILT |
| query_resolver_v2 | Convergence query resolution (forked from DCL) | BUILT |
| revenue_bridge | Convergence financial bridge logic | ON DOCKET |
| dashboards | Convergence-specific dashboards | ON DOCKET |
| identity_resolver_v2 | Real identity resolution with tiers 1-4, HITL | ON DOCKET (convergence_transition_master WP3) |

### 5A.3 Tables Owned by Convergence

| Table | Purpose | Migrations |
|---|---|---|
| resolution_workspaces_v2 | Entity resolution workspace persistence | convergence/migrations/ |
| whatif_scenarios | Saved scenario configurations | convergence/migrations/ |
| engagement_state | Per-deal engagement lifecycle | convergence/migrations/ |
| run_ledger | Workflow run records | convergence/migrations/ |

### 5A.4 API Contracts

**Convergence → DCL (HTTP, read only):**
- GET /api/dcl/semantic-export — Semantic catalog for cross-reference

**DCL → Convergence (HTTP):**
- GET /api/convergence/engagement/active — engagement context

**Workflow endpoints (Convergence internal, called by Convergence frontend):**
- POST /api/convergence/workflow/cofa_merge/run — COFA merge workflow. Synchronous.
- POST /api/convergence/workflow/combine/run — Combining FS (future)
- POST /api/convergence/workflow/resolve_entities/run — Entity resolution (future)
- POST /api/convergence/workflow/review_adjustments/run — QofE adjustment review (future)

**Observability endpoint (read by Mai concierge):**
- GET /api/convergence/runs — Workflow run ledger query.

**Convergence → Convergence PG (internal writes):** All Convergence triple writes go to convergence_triples. DCL is never in the Convergence write path.

**Convergence → DCL PG (direct reads, SELECT only):** SELECT only against semantic_triples, dimension_values_v2, tenant_runs. Read-only cross-reference.

**Callers rerouted (v9):**
- Console: CONVERGENCE_API_URL routes combining/bridge/QoE/overlap/whatif calls
- NLQ: dcl_proxy.py routes reports calls to Convergence
- Convergence frontend: MergePanel calls POST /api/convergence/workflow/cofa_merge/run directly.
- Platform Mai: operational tools removed from Mai's tool registry. Mai reads workflow runs via GET /api/convergence/runs.

### 5A.5 Forked Files

Eight files exist in both DCL and Convergence with dated fork headers. Extraction to aos-common package is cleanup debt.

| File | Convergence Divergence | DCL Copy |
|---|---|---|
| core/db.py | Env vars renamed CONVERGENCE_* | Unchanged |
| core/constants.py | Dated fork header | Unchanged |
| core/security_constraints.py | Dated fork header | Unchanged |
| db/triple_store.py | Dated fork header | Unchanged |
| domain/base.py | Dated fork header | Unchanged |
| utils/log_utils.py | Dated fork header | Unchanged |
| api/routes/v2_helpers.py | Keeps full resolution chain | Drops engagement lookup |
| engine/query_resolver_v2.py | Retains engagement lookup | Drops get_active_engagement |

**No automated divergence detection mechanism exists.** Each month of independent development increases extraction cost. Gate: when a bug fix hits one copy but not the other, this becomes blocking, not cleanup.

## 5.2 Integration Chain

| Step | Workflow | Owner | Output | Gate |
|---|---|---|---|---|
| 1. Dual CoA ingestion | cofa_engine | Convergence | Two sets of COFA triples | Both entities have cofa-domain triples |
| 2. COFA unification | cofa_merge workflow | Convergence | Mapping table, conflict register, unified structure | COFACompletionGate (no orphans) |
| 3. Combining FS | combine workflow | Convergence | P&L, BS, SOCF four-column | DR=CR, revenue identity, balance sheet identity |
| 4. Entity resolution | resolve_entities workflow | Convergence | Resolution workspaces with confidence | Deterministic keys first, LLM for residue |
| 5. Overlap/concentration | overlap_v2 | Convergence | Shared counterparties, risk flags | Data exists for both entities |
| 6. Cross-sell | cross_sell_v2 + LLM | Convergence | Pipeline, propensity, ACV | Overlap complete |
| 7. EBITDA bridge | bridge_v2 | Convergence | Adjustments with confidence grades | Combining FS complete |
| 8. QofE | qoe_v2 + review_adjustments | Convergence | Quality adjustments, trending | EBITDA bridge complete |
| 9. What-if | whatif_v2 | Convergence | Parameterized scenarios | All prior steps complete |

## 5.3 Hard Accounting Gates (Deterministic)

DR = CR. Revenue identity. Asset identity. Balance sheet identity. These are not negotiable. Workflow LLM reasoning cannot override. Gates are deterministic validation code, not LLM-evaluated. **DCL enforces for AOS pipeline. Convergence enforces for Convergence pipeline.** Each product owns its own validation — no cross-service HTTP call required.

## 5.4 Workflow Pattern

Every Convergence operational workflow implements the same shape. Code owns control flow; the LLM is a bounded reasoning step inside one node.

### 5.4.1 Category Rule

- **Agent** (Mai): LLM decides control flow. Concierge scope only.
- **Workflow** (Convergence): code decides control flow; LLM is bounded reasoning step.

Operational tasks route through workflow endpoints on Convergence. Not through Mai. LLM scaffolding is workflow-internal.

### 5.4.2 Shell

| Step | Actor | Responsibility |
|---|---|---|
| 1. Receive request | Workflow handler | Validate engagement_id, tenant_id, entity_ids, lifecycle_stage. 422 on missing. |
| 2. Pre-flight | Code | Fetch source data, load policies, assemble context. |
| 3. LLM reasoning | model_client | Single call. Constrained prompt. Pydantic output. No agent loop. |
| 4. Validate | Code | Deterministic gates. Classify validation failure vs LLM output failure. |
| 5. Persist | Code | Write to convergence_triples or workflow-owned table. |
| 6. Record run | Code | Append run_ledger row. |
| 7. Respond | Workflow handler | Structured JSON response. |

Transport: request/response HTTP. Not SSE.

### 5.4.3 Model Client

Convergence calls the model directly via Anthropic SDK. One `invoke_*` function per workflow. Each owns its prompt, structured output schema, and restricted tool set (typically zero tools). No agent loop. Retries happen at validation step by reissuing with failure context.

### 5.4.4 Relationship to Mai

Workflows do not call Mai. Mai does not call workflows. The surfaces are independent. Mai observes workflow runs via GET /api/convergence/runs.

Operational tools are not registered in Mai's tool registry. They exist inside workflow handlers as internal calls.

## 5.5 COFA Merge Workflow

First instance of the workflow pattern.

### 5.5.1–5.5.10

*(COFA merge workflow sections unchanged from convergence_MA_spec_v8 §5.5.1–5.5.10. Inputs, pre-flight, LLM reasoning, COFACompletionGate validation, domain boundary constraints, known conflict types COFA-001 through COFA-006, persist to convergence_triples, structured response, materiality ranking, no auto-resolution, batch approve, resolution options, audit trail, COFA truth test results STRONG PASS.)*

## 5.6 COFA Merge Tab (Convergence Frontend)

MergePanel calls POST /api/convergence/workflow/cofa_merge/run directly. No SSE. No frame parsing. Structured response renders inline.

---

# 6. Pipeline Identity Architecture

Governed by pipeline_identity_architecture_v1.

## 6.1–6.2

*(Core principles and anti-brittleness rules unchanged from v8.)*

## 6.3 AOS Identifier Registry

AOS pipeline: Farm → AOD → Handoff → AAM → DCL → Verify.

*(Identifier table unchanged from v8 §6.3.)*

## 6.4 Convergence Identifier Registry

Convergence pipeline: Farm (per entity) → Convergence (per entity) → COFA merge → Verify.

| Identifier | Namespace | Function |
|---|---|---|
| run_name | Orchestrator | {engagement_short_name}-{short_hash} |
| pipeline_run_id | Orchestrator | UUID. Correlates all stages. |
| engagement_id | Engagement | Groups the entity pair. Top-level organizing concept. |
| entity_id | Engagement config | From farm_config_{entity}.yaml. Dropdown-selectable. |
| tenant_id | Tenant | Machine-facing UUID. Never displayed. |
| tenant_display_name | Tenant table | Human-readable label. |
| farm_manifest_id | Farm | One per entity per generation. |
| convergence_ingest_id | Convergence | One per entity. Dropdown-selectable as COFA input. |
| cofa_run_id | COFA merge workflow | Declares consumed convergence_ingest_id(s). Same record in run_ledger. |
| workflow_run_id | Any Convergence workflow | Generic run_id for non-COFA workflows. |
| verify_id | Verify | Declares what it checked. |

## 6.5 Operator UX Rules

1. Dropdowns, not text fields.
2. Stage-level status visible without running full pipeline.
3. One run_name visible on every screen.
4. Plain-language run summary at top of every completed run.
5. COFA input is an explicit dropdown of completed convergence_ingest_ids.
6. Parallel entity ingests = 1 step.
7. Verify runs once, after COFA (Convergence) or after DCL (AOS).

---

# 7. Combining Financial Statements (Proforma)

*(§7.1–7.4.9 unchanged from v8 — output format, statement types, EBITDA bridge, QofE two-axis adjustment model, bridge v2 engine changes, Farm generation changes, NLQ frontend wiring, guardrails, build sequence. All references to "DCL" engine locations corrected to "Convergence repo.")*

---

# 8. Functionality Map

125 capabilities across 13 sections (A–M).

| Section | Name | Scope |
|---|---|---|
| A | Discovery (AOD) | Environment scan, asset catalog, SOR authority mapping |
| B | Connection Mapping (AAM) | Connector library, schema extraction, data sampling |
| C | Semantic Layer (DCL Core — AOS only) | Triple store, hierarchy, provenance, entity resolution, domain boundaries |
| D | COFA Unification | Dual CoA ingestion, COFA merge workflow, validation, conflict register |
| E | Combining Financial Statements | Hard accounting gates, combining P&L/BS/SOCF, unified trial balance |
| F | Overlap & Concentration | Customer/vendor overlap, revenue concentration, risk scoring |
| G | Cross-Sell | Pipeline generation, propensity scoring, ACV estimates |
| H | EBITDA Bridge | Adjustment identification, confidence grading, what-if sensitivity |
| I | Quality of Earnings | Recurring/non-recurring, normalization, trending, review_adjustments workflow |
| J | What-If Scenarios | Parameterized modeling, scenario comparison, saved scenarios |
| K | Executive Dashboards | CFO, CRO, CHRO, COO, CTO persona views |
| L | NLQ (Natural Language Query) | Intent recognition, entity detection, query routing, provenance display |
| M | Mai | Concierge agent, constitution, admin tools, chat, observability over workflow runs. Does not execute operational workflows. |

Sections D–J workflow code in Convergence repo. Section C in DCL. Section M in Platform (see mai_blueprint_master).

---

# 9. Build Status & Milestones

## 9.1 Completed

*(Milestone table unchanged from v8 §9.1.)*

## 9.2 Active / Next

| Item | Status | Dependency |
|---|---|---|
| Mai/workflow separation | Active | v9 governs. COFA merge ports to Convergence workflow. |
| Convergence model_client module | Active | Anthropic SDK. Structured output. No agent loop. |
| COFA merge workflow (Convergence) | Active | POST /api/convergence/workflow/cofa_merge/run |
| GET /api/convergence/runs | Active | Observability endpoint. Mai concierge reads this. |
| Mai tool registry audit | Active | Operational tools out. Concierge tools only. |
| Entity policy file relocation | Active | Platform → Convergence (backend/policies/) |
| Pipeline identity cleanup | Active sprint | pipeline_identity_architecture_v1 governs |
| Supervised execution Phase 2 | In progress | tenant_preferences table |
| Convergence fixture eradication | Per convergence_transition_master | WP2: delete Meridian/Cascadia, generalize Farm catalog |
| Identity resolver v2 | Per convergence_transition_master | WP3: real resolution with HITL |
| AOS triples conversion (AOD + AAM) | Build plan v2.2 | Deferred |
| Triple store accumulation cleanup | In progress | semantic_triples accumulation |
| aos-common package extraction | Cleanup debt | Forked files |
| Convergence URL prefix standardization | Cleanup debt | /api/convergence/* vs /api/me/* — bundle with AOS/Convergence terminology rename |
| NLQ 422 fix | Deferred | Blocked on identity cleanup |
| Main/dev reconciliation | Known debt | NLQ, Platform, AOD |
| Combine workflow | Next | Second workflow instance |
| Resolve_entities workflow | Next | Third instance |
| Review_adjustments workflow | Next | Fourth instance |

---

# 10. Advisory Review Rulings

*(Table unchanged from v8 §10, with all v8 rulings included.)*

---

# 11. Guardrails & Anti-Patterns for CC Agents

## 11.1 Universal Rules

No silent fallbacks. No bandaids. No tech debt. No shortcuts. No cheating to pass tests. Preexisting errors found during work must be fixed. Unscoped changes must be reverted. Every CC prompt must reference CLAUDE.md. Self-review against guardrails before presenting. Consequences rule: impact analysis required before changing critical path items.

## 11.2 CLAUDE.md Harness Rules (Sections A–F)

Demo data doesn't count as pass. Source field checked on every test. Pipeline must run before harness. Test what the user sees. Own all repos to fix bugs. Run twice for consistency.

## 11.3 CC Agent Cheat Patterns (Reject)

| Pattern | Description |
|---|---|
| Lightweight/test-only endpoints | Backdoor endpoints only tests use |
| Building test infrastructure without running it | Scaffolding that never executes |
| Testing at wrong abstraction layer | Testing internals instead of user-facing API |
| Manufacturing system state | Inserting test data instead of validating pipeline output |
| Mode-set backdoors | Demo/test mode flags bypassing real logic |
| Silent fallback to defaults | Plausible-looking zeros or empty arrays instead of failing |
| Observability-as-fix | Logging or UI surfacing as substitute for diagnosing actual failure |
| Surface-deletion without consumer trace | Deleting files without grepping consumers |

## 11.4 Workflow vs Agent Category Rule

Operational work is a workflow, not a chat turn. Do not ship operational execution through Mai's chat endpoint. Do not register operational tools in Mai's tool registry.

## 11.5 Self-Review Rule

Check for: silent fallbacks, RACI violations, missing cross-module impact, missing harness reference, open-ended CC judgment calls, temp code, no git discipline, internal contradictions, data loss without stop-gates, workflow-through-chat routing.

---

# 12. Development Environment

| Component | Detail |
|---|---|
| Coding agents | Claude Code CLI (Windows desktop + WSL/Ubuntu laptop), Gemini CLI |
| Repos | dcl, convergence, nlq, farm, platform, aod, aam, console |
| Branch convention | dev is the working branch |
| Production deployment | Render |
| Database | Supabase (Postgres) |
| Founder role | Ilya runs all terminals. Architect, not coder or devops. |
| Active services | AOD, AAM, DCL, NLQ, Farm, Console (six core pipeline services) + Convergence (M&A product) + Platform (Mai host, supervised execution, guided tour, tool executor). All active. |

Platform is active. It is the canonical home for Mai (concierge agent, constitution, admin tool registry, run ledger) and for the supervised execution classification engine. Platform also hosts the guided tour / onboarding walkthrough. Platform is not a transitional artifact and is not sunset.

---

# 13. Open Items

| Item | Priority | Notes |
|---|---|---|
| Mai/workflow separation rollout | Active | v9 canonical. COFA merge first. |
| Pipeline identity cleanup | Active | pipeline_identity_architecture_v1 governs |
| GL ingestion pipeline | High | Intake path for Story 1 |
| Context window sizing validation | High | Validate largest workflow pre-flight fits within Sonnet limits |
| aos-common package extraction | Medium | Eliminate forked files |
| Portfolio-scale blocking/clustering | Deferred | 10+ entities |
| RAG pipeline | Deferred | Context window overflow |
| Automated qualitative document parsing | Deferred | Phase 2 |
| Model routing | Deferred | Cost lever |
| Graph store | Deferred | Pattern detection at scale |
| Tenant registry | Deferred | AWS migration |

---

# 14. Governing Documents

| Document | Scope |
|---|---|
| convergence_blueprint_master.md (this document) | Canonical Convergence product spec |
| mai_blueprint_master.md | Canonical Mai concierge specification |
| convergence_transition_master.md | Fixture eradication, resolver, Convergence pipeline generalization |
| pipeline_identity_architecture_v1 | Pipeline identity, provenance, naming |
| se_triples_conversion_build_plan_v2.2 | AOD + AAM EAV conversion (deferred) |
| aos_production_architecture_blueprint_v1.1.docx | Infrastructure and deployment |
| CLAUDE.md (per-repo) | Agent constitution, harness rules, workflow-vs-agent category rule |
| AOS_MASTER_RACI_v8.6 | Module ownership matrix |

---

# Version History

| Version | Date | Changes |
|---|---|---|
| v5 | Mar 2026 | Initial Mai platform spec |
| v6 | Mar 2026 | Tier definitions, portfolio deferred, COFA spike, run ledger |
| v7.0 | Mar 2026 | Consolidated governing document |
| v7.1 | Mar 2026 | Convergence-Lite input/output spec, Layer 3 entity policies |
| v7.2 | Mar 2026 | Product line alignment, ContextOS → AOS |
| v7.3 | Mar 2026 | Two-Axis QofE Adjustment Model |
| v7.4 | Mar 2026 | AOS/Convergence separation, carveout, pipeline identity, supervised execution |
| v7.4.1 | Apr 2026 | Convergence write path corrections |
| v8 | Apr 2026 | Mai/workflow category separation |
| **v9** | **Apr 2026** | **Document consolidation. Convergence spec extracted from omnibus into convergence_blueprint_master. Mai sections extracted to mai_blueprint_master. Fixture transition extracted to convergence_transition_master. Terminology normalized: AOS/Convergence in specs, SE/ME in code with glossary. Platform status confirmed active. Entity policies canonical location: Convergence repo. Engine status column added to §5A.2. DCL §4.2 current state corrected. Farm §4.4 annotated as transitional. Hard accounting gates: each product enforces its own. RACI bumped to v8.6.** |
