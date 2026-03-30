**AOS Convergence M&A Specification**  
Version 7.4 — March 2026

AutonomOS, Inc.

*Canonical governing document. All build decisions, CC prompts, and GTM materials reference this spec.*

# **1. Product Overview**

AOS (autonomOS) is an enterprise platform that delivers unified context across enterprise systems. The platform has three product lines: AOS (single-entity enterprise intelligence), Convergence (multi-entity integration intelligence), and **Convergence M&A (diligence through post-integration).** All share a common engine.

## **1.1 Product Lines**

**AOS.** Single-entity enterprise intelligence. Full pipeline: AOD (discovery) → AAM (connection mapping) → Farm (financial model generation) → DCL (semantic context layer) → NLQ (natural language query). Customer connects their systems, AOS builds contextual intelligence.

**Convergence.** Multi-entity integration intelligence for organizations with multiple subsidiaries that need contextual integrated information. Ongoing operating rhythm, not deal-driven. Same engine as AOS and Convergence M&A: entity is a tag, same DCL, same resolution. Multiple entities flow through the pipeline tagged by entity_id into one semantic store. Unified reporting, cross-entity analytics, and continuous monitoring across persistent entities.

**Convergence M&A.** Multi-entity M&A integration intelligence. Deal-driven: diligence through post-integration. Acquirer and Target data flow into one DCL. Entity is a tag, not a separate brain. Same engines as base AOS, plus a bridge that joins Target pipes into Acquirer pipes. COFA unification, combining financial statements, entity resolution, overlap/concentration analysis, cross-sell, EBITDA bridge, QofE.

PE-specific portfolio product is deferred. The Convergence product line covers multi-entity operating use cases including fund-level visibility across portfolio companies.

## **1.2 Three User Stories**

| Story | Entry Point | Pipeline | Commercial Path |
| :---- | :---- | :---- | :---- |
| 1. Convergence-Lite | Greenfield M&A, upload-based (GL minimum) | No AOD/AAM. Maestra ingests GLs + CoAs, runs full integration chain. | Lands diligence (Explore) → Resolve |
| 2. AOS Single-Entity | Enterprise connects systems | Full AOD→AAM→Farm→DCL→NLQ | Standalone entry, enables Story 3 |
| 3. AOS→Convergence | Acquirer already on platform | Target onboarded via upload or discovery. Convergence runs across both. | Upsell to Resolve, then Operate |
| 3.5 Don't Migrate, Converge | Post-close, target stays on own systems | Target gets AOS + persistent Convergence replaces system migration | Operate tier (persistent monitoring) |

## **1.3 Convergence-Lite: Input / Output Spec**

GL detail is the minimum input. Every real deal has it — monthly GL for 2-3 years is a standard LOI ask. We do not design, spec, or build for a CoA-only or summary-TB-only scenario.

### **1.3.1 Required Inputs**

| Input | Format | Minimum Scope | What It Enables |
| :---- | :---- | :---- | :---- |
| General Ledger (both entities) | CSV/Excel upload | 8+ quarters monthly detail. Account number, account name, debit, credit, period, department/segment if available. | Full fidelity: line-item combining, trending, variance, QofE, EBITDA bridge with actuals |
| Chart of Accounts (both entities) | CSV/Excel upload or extracted from GL | Account number, account name, account type, hierarchy. Ideally includes grouping/classification. | COFA mapping, domain boundary enforcement, conflict identification |

### **1.3.2 Optional Enrichment Inputs**

| Input | What It Unlocks |
| :---- | :---- |
| Customer sub-ledger or customer list with revenue by customer | Entity resolution, customer overlap/concentration, cross-sell pipeline with named accounts and ACV |
| Vendor sub-ledger or vendor list with spend by vendor | Vendor overlap, procurement synergy identification |
| Employee/headcount data | People overlap, org structure comparison, compensation benchmarking |
| Trial balance (if GL not available at line level) | NOT a substitute for GL. Produces summary-only output. Not the design target. |

### **1.3.3 Output: Diligence Integration Package**

This is the deliverable the customer pays for. Produced from GL + CoA inputs. Replaces 3-4 weeks of associate work.

| # | Deliverable | Content | Source Engine |
| :---- | :---- | :---- | :---- |
| 1 | COFA Mapping Table | Every GL account from both entities mapped to a unified structure. Confidence scores. Mapping basis (exact match, semantic similarity, hierarchy). Entity of origin. | Maestra (LLM-driven) |
| 2 | Conflict Register | Every conflict typed (recognition timing, measurement basis, classification, scope). Severity (high/medium/low). Estimated dollar impact from GL actuals. Resolution status. | Maestra + DCL domain gates |
| 3 | Combining P&L | Four columns: Entity A | Entity B | Adjustments | Combined. Preserves industry-specific line structure. Every adjustment links to a conflict register entry. Quarterly and annual. | combining_v2 |
| 4 | Combining Balance Sheet | Same four-column format. Fair value adjustments in Adjustments column. Balance sheet identity enforced (A = L + E). | combining_v2 |
| 5 | Combining Cash Flow | Operating, investing, financing. Derived from P&L + BS changes. Cash flow identity enforced. | combining_v2 |
| 6 | EBITDA Bridge | Adjustment categories with confidence grades. Dollar impact per adjustment from GL actuals. What-if sensitivity sliders. | bridge_v2 |
| 7 | Quality of Earnings | Recurring vs non-recurring classification. Normalization adjustments. Period-over-period trending. Quality score per earnings line. | qoe_v2 |
| 8 | Entity Resolution (if enrichment data provided) | Cross-entity matches for customers, vendors, people. Confidence scores. Ambiguous pairs surfaced for human review. | entity_resolution |
| 9 | Overlap & Concentration (if enrichment data provided) | Shared counterparties. Revenue concentration by customer. Risk flags for single-customer dependency. | overlap_v2 |
| 10 | Cross-Sell Pipeline (if enrichment data provided) | Named accounts, propensity scores, ACV estimates, direction (A→B and B→A). | Maestra + overlap_v2 |

Deliverables 1-7 are always produced (GL + CoA is sufficient). Deliverables 8-10 require optional enrichment inputs. Maestra tells the customer which deliverables are available based on what was uploaded — no silent omission.

# **2. Architecture Decisions** 

## **2.1 Core Decisions**

| Decision | Ruling | Rationale |
| :---- | :---- | :---- |
| Storage engine | Postgres (Supabase) for MVP | EAV at two-entity scale is fine. Evaluate columnar/graph at 10+ entities. |
| Maestra execution role | Maestra reasons through integration chain. DCL validates outputs. Deterministic gates own replayable/exact checks. | No separate orchestration framework for MVP. Not 'no deterministic anything.' |
| Context management | Staged processing with stored work products between stages | RAG deferred. Each stage independently fits within context window. Full chain is never in one prompt. |
| Workflow engine | Prompt-driven, not LangGraph/Temporal | Discrete steps with stored work products give effective resumability. |
| Entity resolution scale | Deterministic keys first, LLM for fuzzy residue, batched by business impact | O(n²) is a portfolio problem, not MVP. Add blocking/clustering at 10+ entities. |
| COFA accuracy | Validated (100% completeness, 6/6 conflicts, $1.49/engagement) | Gating technical risk retired. LLM can reason about chart of accounts unification. |
| Testing | Two-tier: deterministic harness (Tier 1, built) + LLM-as-Judge (Tier 2, bounded) | CLAUDE.md Sections A–F with 26+ rules. 100% pass or not done. |
| Document ingestion | CSV + Excel for MVP quantitative pipeline. Automated parsing of qualitative PDFs/Word docs is Phase 2. | For MVP, Layer 3 entity policies are manually authored Markdown files stored in the repo and injected at runtime. This is not deferred — it is a different authoring mode. |
| Model routing | Sonnet for everything at MVP. Architecture supports dispatch per interaction type. | Model routing is a cost lever activated when volume justifies it. |
| Convergence architecture | Same engine as base AOS. Entity is a tag. Bridge joins Target + Acquirer pipes into one DCL. | No split brain, no query-time composition, no new resolution logic. |
| Silent fallbacks | Prohibited. Fail loud or not at all. | Hard architectural rule across all repos. |
| No GAAP fallback | Maestra must not infer from general GAAP when entity policy is missing | Output null with flag. LLMs default to GAAP reasoning; must be explicitly blocked. |
| Data claims | Do not claim 'metadata only' or 'we don't touch your data' | Not architecturally validated. |
| Ontology claims | Do not claim AOS delivers ontology | Current: sophisticated semantics. Ontology is aspirational/roadmap. |
| QofE adjustment model | Two-axis: fiscal period attribution + diligence lifecycle stage on every adjustment triple. Triple key becomes (entity_id, concept, property, lifecycle_stage). DISTINCT ON pattern replaced with lifecycle-aware grouping. | v1 QoE had the right schema (diligence_amount, prior_amount, trend) but wrong data source. Formalizing in triples enables H.1, H.5, H.6, G.6, M.14. |
| **SE/ME pipeline separation** | **SE and ME are strict separation of concerns with no shared plumbing. Unit of work is the stage, not the pipeline. Convergence repo owns all ME engines. DCL is SE-only.** | **Shared code paths declared unsustainable after ME pipeline fixes exposed coupling. Pipeline isolation spec v1/v2 produced. Convergence carveout blueprint executed.** |
| **Pipeline identity architecture** | **run_id banned from all API response payloads. Replaced with namespaced stage identifiers (farm_manifest_id, aod_discovery_id, etc). Identity pair (tenant_id + entity_id) required on every stage response. 422 on missing — no silent fallbacks, no identity degradation at service boundaries.** | **Audit of actual pipeline JSON showed run_id collision, orphaned IDs, missing tenant_id, opaque row expansion, and stale source references across both SE and ME modes. Per pipeline_identity_architecture_v1.** |
| **Maestra supervised execution** | **Four-tier model: Auto-execute, Validate with Farm dry-run, Plan Mode, Escalate only. maestra_plans table (19 columns). Classification engine with 15+ test cases 100% pass, 146+ total tests zero regressions.** | **Maestra actions require operator oversight proportional to risk. Console operator feed (card-based, tier badges, 30-second polling) is the review surface.** |

# **3. Maestra**

## **3.1 What Maestra Is**

Maestra is a prompt-engineered persona running on a frontier LLM (Claude) with structured context injection. She is the persistent AI engagement lead across all AOS modules and deployment scenarios. She is not a fine-tuned model, not a separate deployed service (for now), not entangled with NLQ internals. She does not bypass RACI module boundaries.

CSAT is her incentive metric. Architecture sits above NLQ. She is the operational interface to AOS — the way users interact with the platform.

## **3.2 The Runtime Pattern**

| Step | Action | Source |
| :---- | :---- | :---- |
| 1 | Customer sends message via report portal / chat surface | UI layer |
| 2 | Context assembler pulls Maestra constitution (scenario variant) | Static document |
| 3 | Context assembler pulls engagement state for this customer | Supabase: maestra schema |
| 4 | Context assembler pulls live module state (cached, event-driven) | Module REST endpoints via state cache |
| 5 | Assembled prompt sent to LLM with customer message | Claude API (model per routing tier) |
| 6 | LLM responds as Maestra; response may include structured action blocks | LLM output |
| 7 | If action block present: dispatch to module endpoint (read) or generate plan (write) | Action dispatch layer |
| 8 | Update engagement state with interaction record | Supabase: maestra schema |

## **3.3 Three Knowledge Sources**

**The Maestra Constitution (static, versioned).** Her identity, voice, role boundaries, action catalog, scenario variants. Separate constitution variants per scenario type (single entity, multi-entity, M&A, portfolio) sharing a common base.

**Engagement State (persistent, per-customer).** Structured data in Supabase tracking: onboarding steps completed, questions asked, items flagged, outstanding issues. Not conversation history. Structured state that survives across sessions.

**Live Module State (dynamic, cached).** Current state from each AOS module. Modules publish state changes to a cache layer. Maestra reads from cache. Modules push; Maestra never waits.

## **3.4 Constitution Layer Architecture**

Maestra's behavior is governed by a layered constitution. Higher layers cannot contradict lower layers.

| Layer | Name | Content | Loaded When |
| :---- | :---- | :---- | :---- |
| 0 | Accounting Axioms | DR=CR, element boundaries (A/L/E/R/E mutually exclusive), articulation rules. Both prompt axioms AND backend Pydantic validation. | Always, every invocation |
| 1 | P&L Constitution | Temporal/flow logic, stub periods, combining, rev rec delegation. Separate doc: BS Constitution (point-in-time, fair value, equity roll-forward). | Per agent invocation |
| 2 | COFA Ontology | Entity resolution rules, match/probable/no-match taxonomy, overlap classification, conflict register structure. | Convergence engagements only |
| 3 | Entity Policies | Per-entity policy docs (scope/rule/boundary). Explicit gaps section required. No-GAAP-fallback constraint in preamble. | Per engagement, one per entity |
| 4 | Industry Profiles | SaaS, Manufacturing (MVP). CoA expectations, KPIs, classification rules. Authoritative definitions. | Per entity industry tag |
| — | Orchestrator | Top-level persona. Agent sequencing, handshake contract, failure handling, CSAT incentive, flag aggregation. | Always, wraps all invocations |

Implementation model: Layers 0-2 hardcoded into agent system prompts (static templates). Layers 3-4 stored as Markdown files, read and appended to context at runtime. Layer 3 policies are manually authored for MVP (not parsed from uploaded documents — automated parsing is Phase 2). Backend Pydantic validation on every agent output, independent of LLM.

## **3.5 Build Status (Constitution)**

All 6 phases built on branch 'maestra' across Platform repo. Stage 5 COFA truth test: STRONG PASS. Ready for integration testing and merge to dev.

| Phase | Content | Status |
| :---- | :---- | :---- |
| 1 — Validation Layer | Layer 0 Pydantic schema + prompt axioms | Built |
| 2 — P&L Agent | Layer 1 P&L constitution + agent | Built |
| 3 — BS Agent + Handshake | Layer 1 BS constitution, net income handshake | Built |
| 4 — Context Injection | Layer 3 + 4 runtime loading | Built |
| 5 — COFA | Layer 2 COFA unification via Maestra | Built, STRONG PASS |
| 6 — Orchestrator | Top-level persona, agent sequencing, flag aggregation | Built |

## **3.6 Supervised Execution (Phase 1 Complete)**

Maestra actions are classified into four tiers based on risk and reversibility. The classification engine runs in Platform (constitution module, classify_change tool). Console provides the operator review surface.

| Tier | Name | Behavior | Example |
| :---- | :---- | :---- | :---- |
| 1 | Auto-execute | Maestra executes immediately. Logged but no human gate. | Read-only queries, status checks |
| 2 | Validate | Maestra executes with Farm dry-run validation. Operator sees result, can revert. | Triple writes with low blast radius |
| 3 | Plan Mode | Maestra produces a plan. Operator reviews and approves before execution. | COFA unification, combining FS |
| 4 | Escalate Only | Maestra surfaces the issue. No execution proposed. Human decides everything. | Cross-domain reclassification, material adjustments |

**maestra_plans table** (19 columns): stores every proposed plan with classification tier, inputs hash, proposed actions, operator decision, execution result, and audit trail.

**Console operator feed**: card-based UI with tier badges. 30-second polling. Operators see pending plans, approve/reject, and view execution history.

**Phase 2 (in progress)**: tenant_preferences table with CHECK constraint enforcing 12-key registry. Allows per-tenant tier overrides and automation thresholds.

## **3.7 Engagement Run Ledger**

Every Maestra engagement step emits a ledger entry: engagement_id, step_name, inputs (or hash), model_version, constitution_version, intermediate output, validation result, human override (if any), timestamp. Stored in maestra schema. Required for debugging customer incidents.

## **3.8 Human Review Pipeline**

For high-impact decisions (entity resolution involving material revenue, COFA mappings affecting reported EBITDA, cross-domain reclassifications), Maestra prepares a structured recommendation with evidence and business implications. Human confirms. Decision recorded with provenance. This is the primary workflow, not a fallback.

Four-tier classification. Confidence decomposition: compound confidence broken into components, each evaluated independently. Below medium threshold: Maestra must surface the number with plain-language explanation of which input drove it down, link to underlying workspace or mapping, and format the conflict for human review. Maestra does not recommend an accounting resolution — she isolates the variables and presents them. The human decides.

## **3.9 COFA Unification (How Maestra Does It)**

Maestra reads two CoAs. She understands economic substance, not just label matching. She builds a mapping table: source account → unified account, with confidence scores. Where one entity has granularity the other lacks, the unified CoA keeps the granular structure. Where entities use different treatments for the same substance, she flags it as a typed conflict (recognition timing, measurement basis, classification, scope). She asks questions on judgment calls. She writes the mapping, conflict register, and unified structure to DCL as triples.

DCL validates via COFACompletionGate: every source account must appear in the output. If orphaned, DCL rejects and tells Maestra which accounts are missing. Self-correcting loop.

### **3.9.1 Domain Boundary Constraints**

| Constraint | Type | Description |
| :---- | :---- | :---- |
| Asset/Liability/Equity/Revenue/Expense | Hard gate | Mutually exclusive. DCL rejects cross-domain mappings. |
| Revenue cannot map to OpEx | Hard gate | DCL rejects with explanation. Maestra cannot override. |
| COGS/OpEx boundary | Soft gate | Flag + human confirmation instead of hard rejection. |
| Contra-account handling | Rule | Accumulated depreciation handled by parent account's domain (Asset), not by credit sign. |

### **3.9.2 Known Conflict Types (Consulting/BPM Playbook)**

| COFA ID | Conflict | Severity Guidance |
| :---- | :---- | :---- |
| COFA-001 | Revenue gross/net recognition | Revenue diff > 5% combined: HIGH |
| COFA-002 | Benefits loading (COGS vs OpEx) | 8-15% of COGS |
| COFA-003 | S&M bundling | Affects OpEx comparability |
| COFA-004 | Recruiting capitalization vs expense | Affects COGS/OpEx and asset base |
| COFA-005 | Automation capitalization | Same pattern as COFA-004 |
| COFA-006 | Depreciation method (straight-line vs accelerated) | Affects D&A and book value |

### **3.9.3 COFA Truth Test Results**

| Test | Input | Completeness | Conflicts Found | Cost |
| :---- | :---- | :---- | :---- | :---- |
| A (structured × structured) | Meridian + Cascadia full CoAs | 100% | 6/6 correctly typed | $1.49/engagement |
| B (structured × degraded) | Meridian + degraded (no metadata) | 100% | 3 (expected — degraded CoA lacks accounts needed for all 6) | — |

Decision gate result: STRONG PASS. Proceed as designed.

### **3.9.4 Materiality and Conflict Resolution Workflow**

Maestra is a reporter, not an authority. She identifies conflicts, types them, estimates dollar impact from GL data, and ranks them by materiality. She does not resolve them. Humans resolve. The workflow is designed to make that human decision-making efficient at scale.

**Conflict ranking.** Maestra sorts all identified conflicts by estimated annual dollar impact, descending. The CFO sees the $50M revenue recognition difference before the $200K depreciation method difference. This is the primary mechanism for managing volume — at 200 conflicts, the top 10 typically represent 90% of total impact.

**No auto-resolution.** Every conflict routes to human review regardless of materiality. Maestra does not apply a materiality threshold to skip conflicts or silently resolve them. Low-materiality conflicts still appear in the queue — they are ranked last, not hidden. This is a deliberate design choice: no accounting decision, however small, is made by the LLM.

**Batch approve.** The Human Review Queue supports batch actions. After reviewing the top material conflicts individually, the CFO can select all remaining conflicts below a self-determined threshold and apply a bulk resolution (e.g., 'accept acquirer treatment for all selected'). This is a human action with an audit trail — the system records who approved, when, what threshold they applied, and which conflicts were included. Maestra does not set the threshold or suggest the batch action.

**Resolution options per conflict.** For each conflict, the human selects one of: (1) normalize to acquirer treatment, (2) normalize to target treatment, (3) keep both and show adjustment in combining column, (4) flag for post-close harmonization (no adjustment now, tracked as open item). The decision is recorded with reasoning and linked to the conflict register entry. The combining engine reads the resolution and applies it.

**Audit trail.** Every resolution records: conflict_id, decision (which option), decided_by (human:user_id), reasoning (free text), timestamp, materiality at time of decision (dollar impact). Batch approvals record the same fields plus the selection criteria used. This trail is a deliverable — it goes into the Diligence Integration Package as evidence of how each difference was addressed.

Future (Resolve tier): engagement-level materiality threshold set by the CFO at scoping. Conflicts below threshold auto-resolve to a default treatment specified by the CFO, with full audit trail. This is the CFO's policy applied deterministically, not Maestra's judgment. Deferred until batch approve proves the workflow at MVP scale.

## **3.10 Layer 3 Entity Policies (MVP)**

Per §3.4, Layer 3 entity policies are manually authored Markdown files for MVP. Automated parsing of uploaded PDFs/Word docs is Phase 2. Two policy documents exist, stored in Platform at app/maestra/constitution/policies/:

| File | Entity | Sections | Key Policy Elections |
| :---- | :---- | :---- | :---- |
| meridian_policy.md | Meridian Partners (Acquirer) | Revenue recognition, COGS, OpEx, D&A, BS policies, Explicit Gaps | Gross revenue recognition. Benefits in OpEx (not COGS). Recruiting expensed immediately. R&D expensed below $10M threshold. Straight-line depreciation. |
| cascadia_policy.md | Cascadia Process Solutions (Target) | Revenue recognition, COGS, OpEx, Capitalization policy, D&A, BS policies, Explicit Gaps | Net revenue recognition. Benefits in COGS for delivery staff. Recruiting capitalized above $50K/hire. Automation capitalized above $2M/project. Accelerated depreciation for delivery equipment. |

Each document includes an Explicit Gaps section listing items NOT covered. Maestra must output null with a flag for any gap item — she must not infer from general GAAP training data. This is the no-GAAP-fallback constraint enforced at the document level.

For a real customer engagement (Convergence-Lite, §1.2 Story 1), these documents would be authored by the customer's finance team during onboarding or extracted manually from their accounting policy manual. Maestra's scoping conversation can guide the customer through what's needed.

# **4. DCL (Data Context Layer) — SE Only**

As of the Convergence carveout (March 2026), DCL is an SE-only service. All ME engines, engagement lifecycle, and resolution workspace logic have been extracted to the standalone Convergence repo (§5A). DCL retains ownership of the semantic triple store, ontology engine, graph engine, SE query resolution, reconciliation, and triple monitoring.

## **4.1 Semantic Triple Store**

All data in DCL lives as semantic triples: (entity_id, concept, property, value, period). Every triple carries provenance: source_system, source_field, confidence_score, confidence_tier, pipe_id, run_id, created_at. Stored in Postgres (Supabase).

Concepts use dot-separated hierarchical naming (e.g., cofa.automation_capitalization, compensation.base). Domains are the first segment of the concept name.

DCL owns all write-side invariants for the triple store: old-run deactivation, COPY-based bulk ingest, is_active flag management, tenant_runs.current_run_id updates, and the atomic run_id swap pattern (O(1) UPSERT via tenant_runs pointer table, replacing O(n) bulk UPDATE). Post-swap purge step deletes stale rows.

Convergence reads DCL-owned tables directly (SELECT only) for report-time metric queries. Convergence never writes to DCL-owned tables directly — all triple writes go through DCL's POST /api/dcl/ingest-triples.

## **4.2 Current State**

| Metric | Value |
| :---- | :---- |
| Total triples (last ingest) | 18,500 |
| Entities | 2 (Meridian, Cascadia) |
| Domains | 17 |
| Periods | 24 |
| SE engines retained | resolver_v2, ontology, semantic_graph, graph_store, dcl_engine |
| Integration tests passing | 131 (SE suite) |
| Old JSON engines | Present alongside v2 via compat layer |

## **4.3 DCL Table Ownership**

| Table | Owner | Who Reads | Who Writes | Migrations Live In |
| :---- | :---- | :---- | :---- | :---- |
| semantic_triples | DCL | DCL + Convergence | DCL only (via POST /api/dcl/ingest-triples) | dcl/migrations/ |
| dimension_values_v2 | DCL | DCL + Convergence | DCL only | dcl/migrations/ |
| tenant_runs | DCL | DCL + Convergence | DCL only | dcl/migrations/ |
| tenant_registry | DCL | DCL | DCL only | dcl/migrations/ |

Schema contract documented in SCHEMA_CONTRACT.md in DCL repo. Additive changes (new columns with defaults) are non-breaking. Column renames, type changes, or removals are breaking and require Convergence repo coordination before merge.

## **4.4 DCL Restructure (Completed)**

The DCL restructure replaced the old JSON-based in-memory engine stack with a Postgres-backed semantic triple store. Five phases:

| Phase | Content | Status |
| :---- | :---- | :---- |
| 0 — Schema + Farm Triples | PG schema for semantic_triples, Farm outputs triples instead of bespoke JSON | Done |
| 1 — DCL Core | Ingest, query resolver reads from PG, entity resolution in PG | Done |
| 2 — Engine Re-plumb | Each v2 engine reads from triples. NLQ de-hardcoded. | Done |
| 3 — Missing Capabilities | Combining BS/CF, revenue variance bridge, scenario comparison | Done |
| 4 — Maestra Foundation | Engagement lifecycle, constitution, tools, chat, human review | Done |
| 5 — Integration Chain | COFA truth test, combining financials via Maestra | STRONG PASS |
| **6 — Convergence Carveout** | **ME engines, engagement lifecycle, resolution workspaces extracted to Convergence repo. DCL becomes SE-only.** | **Done** |

## **4.5 Farm Configurations**

Two canonical Farm configs. No other configs are valid. fact_base.json and the default $35M toy config have been permanently removed.

| Config | Entity | Revenue | Domains |
| :---- | :---- | :---- | :---- |
| farm_config_meridian.yaml | Meridian (Acquirer) | $5B | 14 financial domains |
| farm_config_cascadia.yaml | Cascadia (Target) | $1B | 14 financial domains |

## **4.6 Data Pipeline**

Single-entity (AOS): AOD → AAM → Farm → triple conversion → PG direct. No DCL pipe ingest (Structure/Dispatch/Content path is deprecated). Only Farm Financials writes triples to DCL via orchestrator. Existing direct-PG write code in AOD/AAM is labeled tech debt, must not be extended.

Multi-entity (Convergence): Both entities' data flow through separate Farm configs → triple conversion → same PG store, tagged by entity_id. Convergence repo (§5A) runs COFA chain, combining engines, and all ME report generation. Farm ME push routes to Convergence, not DCL.

DCL Ingest/Recon tabs are legacy. Triples tab is the active monitoring surface.

# **5. Convergence Architecture**

Convergence = base AOS plus a bridge where Target pipes join Acquirer pipes into one DCL. Entity is a tag, same engine, no split brain, no query-time composition.

## **5.1 Invariants**

Same engine as base AOS. Entity_id is a column, not a separate instance. No new resolution logic beyond what AOS uses. The bridge is the join point, not a fork.

## **5A. Convergence Service (Separate Repo)**

As of March 2026, all ME/M&A engine code, engagement lifecycle, and resolution workspace logic lives in the standalone Convergence repo. This was extracted from DCL via the Convergence Carveout Blueprint.

### **5A.1 Service Boundaries**

| Attribute | Value |
| :---- | :---- |
| Repo | convergence |
| Backend | FastAPI, port 8010 |
| Frontend | React, port 3010 |
| Branch | dev (carveout merged) |
| Database | Same Supabase PG instance as DCL (shared store, entity is a tag) |

### **5A.2 Engines Owned by Convergence**

All ME v2 engines now live in convergence/backend/engine/:

| Engine | Purpose | Key Outputs |
| :---- | :---- | :---- |
| combining_v2 | Combining financial statements (P&L, BS, SOCF) | Four-column format: Entity A | Entity B | Adjustments | Combined |
| ebitda_bridge_v2 | EBITDA bridge with adjustments | Adjustment categories, confidence grades, what-if sliders |
| qoe_v2 | Quality of Earnings analysis | QofE adjustments, trending, recurring vs non-recurring |
| overlap_v2 | Customer/vendor overlap and concentration | Shared counterparties, revenue concentration, risk flags |
| cross_sell_v2 | Cross-sell pipeline generation | Named accounts, propensity, ACV |
| upsell_v2 | Upsell analysis | Expansion opportunities |
| entity_resolution_v2 | Cross-entity identity matching | Resolution workspaces with confidence scores |
| cofa_mapping_writer | COFA generation from Farm output | Chart of accounts triples per entity |
| what_if_v2 | Scenario modeling | Parameterized what-if with stored scenarios |
| engagement.py | Engagement lifecycle management | Active engagement state, entity pair config |
| query_resolver_v2 | ME query resolution (forked from DCL) | Retains engagement lookup; DCL copy drops it |
| revenue_bridge | ME financial bridge logic | Revenue bridge analysis |
| dashboards | ME-specific dashboards | Engagement dashboards |

### **5A.3 Tables Owned by Convergence**

| Table | Purpose | Migrations Live In |
| :---- | :---- | :---- |
| resolution_workspaces_v2 | Entity resolution workspace persistence | convergence/migrations/ |
| whatif_scenarios | Saved scenario configurations | convergence/migrations/ |
| engagement_state | Per-deal engagement lifecycle | convergence/migrations/ |

### **5A.4 API Contracts**

**Convergence → DCL (HTTP):**
- POST /api/dcl/ingest-triples — COFA triple writes (DCL validates, writes)
- GET /api/dcl/semantic-export — Semantic catalog

**DCL → Convergence (HTTP):**
- GET /api/convergence/engagement/active — maestra.py calls this for engagement context (replaces direct import)

**Convergence → PG (Direct):**
SELECT only against semantic_triples, dimension_values_v2, tenant_runs. No HTTP intermediary for report-time metric queries.

**Callers rerouted:**
- Console: CONVERGENCE_API_URL routes combining/bridge/QoE/overlap/whatif calls
- NLQ: dcl_proxy.py routes reports calls to Convergence
- Platform: tool_executor.py routes COFA/merge calls to Convergence

### **5A.5 Forked Files**

These files exist in both DCL and Convergence with dated fork headers. Divergence is tracked. Extraction to aos-common package is post-carveout cleanup debt.

| File | Convergence Divergence | DCL Copy |
| :---- | :---- | :---- |
| core/db.py | Env vars renamed CONVERGENCE_*, separate pool sizing | Unchanged |
| core/constants.py | Dated fork header | Unchanged |
| core/security_constraints.py | Dated fork header | Unchanged |
| db/triple_store.py | Dated fork header | Unchanged |
| domain/base.py | Dated fork header | Unchanged |
| utils/log_utils.py | Dated fork header | Unchanged |
| api/routes/v2_helpers.py | Keeps full resolution chain (explicit param → engagement_state → semantic_triples) | Drops engagement lookup (explicit param → semantic_triples only) |
| engine/query_resolver_v2.py | Retains engagement lookup | Drops get_active_engagement import |

## **5.2 Integration Chain**

| Step | Engine | Owner | Output | Gate |
| :---- | :---- | :---- | :---- | :---- |
| 1. Dual CoA ingestion | cofa_engine | Convergence | Two sets of COFA triples in store | Both entities have cofa-domain triples |
| 2. COFA unification | Maestra (LLM-driven) | Convergence | Mapping table, conflict register, unified structure | COFACompletionGate (no orphans) |
| 3. Combining FS | combining_v2 | Convergence | P&L, BS, SOCF in four-column format | DR=CR, revenue identity, balance sheet identity |
| 4. Entity resolution | entity_resolution | Convergence | Resolution workspaces with confidence | Deterministic keys first, LLM for residue |
| 5. Overlap/concentration | overlap_v2 | Convergence | Shared counterparties, risk flags | Data exists for both entities |
| 6. Cross-sell | Maestra + overlap_v2 | Convergence | Pipeline, propensity, ACV estimates | Overlap step complete |
| 7. EBITDA bridge | bridge_v2 | Convergence | Adjustments with confidence grades | Combining FS step complete |
| 8. QofE | qoe_v2 | Convergence | Quality adjustments, trending | EBITDA bridge complete |
| 9. What-if | whatif_v2 | Convergence | Parameterized scenarios | All prior steps complete |

## **5.3 Hard Accounting Gates (Deterministic, DCL-Enforced)**

DR = CR (trial balance nets to zero pre and post mapping). Revenue identity (combined = sum of standalones). Asset identity (combined = sum ± intercompany). Balance sheet identity (A = L + E for each entity and combined). These are not negotiable. Maestra cannot override. Convergence calls DCL for validation; DCL enforces.

## **5.4 COFA Merge Tab (Convergence Frontend)**

Top-level view in Convergence frontend (port 3010) displaying COFA merge status. Five sections: Merge Overview (entity stats), Side-by-Side COFA Comparison (acquirer left, target right), Account Match Table (resolution data if exists), Unmatched/Orphan Accounts, Raw COFA Triple Browser. Read-only display of what's in the store. Merge engine is Maestra, not a coded pipeline.

# **6. Pipeline Identity Architecture**

Governed by pipeline_identity_architecture_v1. This section summarizes the rules that all services and all CC prompts must follow.

## **6.1 Core Principles**

Seven rules that apply to every service boundary in both SE and ME modes:

1. The word run_id is banned from all response payloads. Every service uses a namespaced identifier.
2. Every stage response carries the identity pair: tenant_id (UUID, machine-only, never displayed) + entity_id (string business key, always displayed).
3. Every stage response declares what it consumed. Provenance is explicit, never inferred.
4. Operators never type or paste an ID. All input selection is via dropdown populated from what exists.
5. One human-readable run_name is visible on every operator surface: stage cards, run history, Slack alerts.
6. Triples are tagged with pipeline_run_id (E2E) or the stage's own namespaced ID (manual run). No separate triples_id concept. No prefix stripping.
7. Expansion is self-documenting: every ingest response includes source_rows, triples_written, and expansion_factor.

## **6.2 Anti-Brittleness Rules (Identity Architecture)**

Six rules locked for all pipeline work:

1. No identity degradation at service boundaries — 422 on missing identity fields, no silent fallback.
2. All write paths produce identical identity pairs — no path produces a different shape.
3. No derivation functions in pipeline path — entity_id is passed through, not computed from tenant_id.
4. One canonical env var: AOS_TENANT_ID — no alternative names.
5. No string mangling — no prefix stripping, no substring extraction, no regex on IDs.
6. Run-level identifiers as separate fields — pipeline_run_id and stage-specific IDs are distinct columns, not overloaded.

## **6.3 SE Identifier Registry**

SE pipeline: Farm → AOD → Handoff → AAM → DCL → Verify. One entity, fixed at tenant creation.

| Identifier | Namespace | Function |
| :---- | :---- | :---- |
| run_name | Orchestrator | {entity_id}-{short_hash}. E.g., BlueLogic-NEQ8-a9ed. |
| pipeline_run_id | Orchestrator | UUID. Correlates all stages. E2E runs only. |
| entity_id | Farm (generated) | Deterministic synthetic name from UUID seed. Business key. |
| tenant_id | Tenant | Source UUID seed. Machine-only. Never displayed. |
| farm_manifest_id | Farm | One per generation. Dropdown-selectable. |
| aod_discovery_id | AOD | Declares consumed_snapshot_id. |
| handoff_id | Handoff | Declares source_aod_discovery_id. |
| aam_inference_id | AAM | Declares source_handoff_id. ledger_id dropped. |
| dcl_ingest_id | DCL | Top-level field. Same name in Verify. |
| verify_id | Verify | Declares which dcl_ingest_id it checked. |

## **6.4 ME Identifier Registry**

ME pipeline: Farm (per entity) → DCL (per entity) → COFA → Verify. Multiple entities, engagement-driven.

| Identifier | Namespace | Function |
| :---- | :---- | :---- |
| run_name | Orchestrator | {engagement_short_name}-{short_hash}. E.g., MerCas-2571. |
| pipeline_run_id | Orchestrator | UUID. Correlates all stages. E2E runs only. |
| engagement_id | Engagement | Groups the entity pair. Top-level organizing concept. |
| entity_id | Engagement config | From farm_config_{entity}.yaml. Operator selects from dropdown. |
| tenant_id | Tenant | Machine-facing UUID. Never displayed. |
| tenant_display_name | Tenant table | Human-readable label. Replaces raw UUID in all surfaces. |
| farm_manifest_id | Farm | One per entity per generation. Dropdown-selectable. |
| dcl_ingest_id | DCL | One per entity. Dropdown-selectable as COFA input. |
| cofa_run_id | COFA | Declares which dcl_ingest_id(s) it consumed. |
| verify_id | Verify | Declares what it checked. Runs after COFA. Explicit DAG dependency. |

## **6.5 Operator UX Rules**

The unit of work is the stage, not the pipeline. E2E is just all stages run in sequence.

1. Dropdowns, not text fields. Entity selection, stage selection, input selection — all dropdowns populated from what actually exists.
2. Stage-level status visible without running full pipeline. Each stage card shows its own last-run state independently.
3. One run_name visible on every screen. Operator never needs to know which namespace they are in.
4. Plain-language run summary at top of every completed run. Not JSON.
5. COFA input is an explicit dropdown selection. Unify against which ingests? — operator picks from list of completed dcl_ingest_ids.
6. Step counting: parallel entity ingests = 1 step. Per-entity detail = tasks within the step.
7. Verify runs once, after COFA (ME) or after DCL (SE), as completeness check. Never timing-dependent.

# **7. Combining Financial Statements (Proforma)**

## **7.1 Output Format**

Four columns: Entity A | Entity B | Adjustments | Combined. Every adjustment links to a conflict register entry. Annual comparisons. Revenue lines preserve industry-specific structure (consulting vs managed services, not generic revenue).

## **7.2 Statement Types**

| Statement | Key Lines | Notes |
| :---- | :---- | :---- |
| Combining P&L | Revenue (by type), COGS (by structure), OpEx (S&M, G&A, R&D), down to EBITDA | COGS lines preserve entity cost structures |
| Combining BS | Assets, Liabilities, Equity. Fair value adjustments in Adjustments column. | Balance sheet identity enforced |
| Combining SOCF | Operating, investing, financing. Derived from P&L + BS changes. | Cash flow identity enforced |
| Unified Trial Balance | Both entities' period balances in unified structure. Adjustment column for reclassifications. | Debits = Credits for each entity and combined |

## **7.3 EBITDA Bridge**

Adjustment categories with confidence grades. What-if sliders for sensitivity. Each adjustment typed and linked to evidence. Grades: high confidence (deterministic calculation), medium (LLM-assisted with supporting data), low (requires human adjudication).

## **7.4 Quality of Earnings (QofE)**

QofE is an ongoing analytical instrument applied to each quarterly and annual report, not a one-time diligence artifact. Each period, the QofE engine runs against the latest financials and produces an updated assessment: is the earnings quality holding, improving, or deteriorating? Are the adjustments from diligence still valid? Have new adjustments emerged?

### **7.4.1 Two-Axis Adjustment Model**

Every adjustment triple must carry two temporal dimensions: a fiscal period the adjustment relates to, and a diligence lifecycle stage at which the estimate was produced. Without both axes, QofE is a static list of numbers. With both, QofE answers the questions that buyers, PE funds, and post-close operators actually ask.

### **7.4.2 Axis 1: Fiscal Period Attribution**

The fiscal period the adjustment relates to. The period property on the triple carries the relevant quarter (e.g., 2025-Q1). An additional property, period_type, classifies the temporal nature of the adjustment:

| period_type | Definition | Example |
| :---- | :---- | :---- |
| **occurred** | The adjustment relates to a specific period in which the event happened. | Non-recurring legal $11M in 2024-Q3 |
| **annualized** | An annualized normalization applied to the assessment period. Not tied to a single quarter. | Owner compensation $30M/yr normalization |
| **run_rate** | A forward-looking projected savings or cost, annualized from the assessment period. | Run rate cost savings $59M projected |
| **synergy** | A post-close integration synergy, projected forward from the deal close. | Facility consolidation $29M post-close |

### **7.4.3 Axis 2: Diligence Lifecycle Stage**

| lifecycle_stage | Definition | Typical Source |
| :---- | :---- | :---- |
| **management** | Management's self-reported adjustment, typically presented at LOI or CIM stage. | CIM, management deck |
| **initial_diligence** | Independent estimate produced during initial due diligence. | DD workstream output |
| **confirmatory** | Refined estimate after confirmatory diligence. Higher evidence quality, tighter range. | Confirmatory DD report |
| **agreed** | Final agreed amount at or near close. Goes into the purchase agreement or closing adjustment. | SPA, closing memo |
| **post_close** | Ongoing quarterly reassessment after close. | Quarterly QofE review |

### **7.4.4 Triple Store Key Model**

Current key: (entity_id, concept, property), disambiguated by created_at DESC. This produces one row per adjustment. New key: (entity_id, concept, property, lifecycle_stage), with period as an additional queryable property. This allows multiple rows per adjustment concept, one per lifecycle stage, enabling temporal comparison.

No schema migration is required. lifecycle_stage, period, and period_type are new property values on adjustment triples that the existing PG triple store already supports.

### **7.4.5 DCL/Convergence Engine Changes**

Bridge v2 engine (ebitda_bridge_v2.py, now in Convergence repo): The DISTINCT ON pattern is replaced, not patched. The new query groups adjustment triples by (entity_id, concept). Within each group, it retrieves rows for all lifecycle stages and pivots:

| Output Column | Source |
| :---- | :---- |
| **Current** | amount from the latest lifecycle_stage present for this concept |
| **Diligence** | amount from lifecycle_stage = management (the management number at LOI) |
| **Prior** | amount from the lifecycle_stage one step before the latest present stage |
| **Trend** | Derived: current > prior = up arrow, current < prior = down, equal = stable. Single stage = neutral. |
| **Conf.** | confidence from the latest lifecycle_stage (unchanged from current behavior) |

QofE combined endpoint (now served by Convergence at /api/convergence/reports/v2/qoe/combined): must additionally return adjustment_lifecycle and sustainability_trend.

### **7.4.6 Farm Generation Changes**

Farm's adjustment triple generator must emit lifecycle_stage, period, and period_type on every adjustment triple. The entity config YAML files must specify period_type and lifecycle_stages per adjustment definition.

### **7.4.7 NLQ Frontend Wiring**

The QofE tab already renders the correct column structure. Diligence column: populate from management lifecycle_stage amount. Prior column: populate from prior lifecycle_stage amount. Trend column: directional arrow from current vs. prior comparison.

### **7.4.8 QofE Guardrails (Locked Decisions)**

No schema migration. lifecycle_stage is required on all adjustment triples going forward. Existing NULL triples are treated as lifecycle_stage = initial_diligence for backward compatibility. period is required on all adjustment triples going forward. EBITDA is still always derived. No auto-resolution of adjustment conflicts across lifecycle stages. The DISTINCT ON pattern in the bridge v2 engine is replaced, not patched.

### **7.4.9 Build Sequence**

Phase 1 (Farm): Update entity config YAMLs. Update adjustment triple generator. Regenerate seed data. Phase 2 (Convergence, formerly DCL): Rewrite bridge v2 engine query. Update QofE combined endpoint. Update integration tests. Phase 3 (NLQ): Wire columns to real data. Add lifecycle timeline to expanded row detail. Phases are sequential.

# **8. Functionality Map**

The canonical functionality map (functionality_map.xlsx) is the official AOS build tracker. 125 capabilities across 13 sections (A–M), organized by pipeline stage. Customer-facing — no internal dev artifacts, no negative framing. Status filled by CC runner audit.

## **8.1 Sections**

| Section | Name | Scope |
| :---- | :---- | :---- |
| A | Discovery (AOD) | Environment scan, asset catalog, SOR authority mapping |
| B | Connection Mapping (AAM) | Connector library, schema extraction, data sampling |
| C | Semantic Layer (DCL Core — SE only) | Triple store, business object hierarchy, provenance, entity resolution, domain boundaries |
| D | COFA Unification | Dual CoA ingestion, account mapping, completeness validation, conflict register, policy flags |
| E | Combining Financial Statements | Hard accounting gates, combining P&L/BS/SOCF, unified trial balance |
| F | Overlap & Concentration | Customer/vendor overlap, revenue concentration, risk scoring |
| G | Cross-Sell | Pipeline generation, propensity scoring, ACV estimates |
| H | EBITDA Bridge | Adjustment identification, confidence grading, what-if sensitivity |
| I | Quality of Earnings | Recurring/non-recurring, normalization, trending |
| J | What-If Scenarios | Parameterized modeling, scenario comparison, saved scenarios |
| K | Executive Dashboards | CFO, CRO, CHRO, COO, CTO persona views |
| L | NLQ (Natural Language Query) | Intent recognition, entity detection, query routing, provenance display |
| M | Maestra | Engagement lifecycle, constitution, tools, chat, human review, run ledger |

Note: Sections D–J engine code now lives in Convergence repo. DCL retains Section C (SE semantic layer).

## **8.2 Status Definitions**

| Status | Meaning |
| :---- | :---- |
| BUILT | Runs against live data for arbitrary entities, harness passes independently |
| PARTIAL | Logic exists with gaps (state exactly what is missing) |
| STUB | Endpoint exists, returns placeholder/mock data |
| MISSING | No code found |
| HARDCODED | Works but only for Meridian/Cascadia specifically, not entity-agnostic |

# **9. Build Status & Milestones**

## **9.1 Completed**

| Milestone | Date | Evidence |
| :---- | :---- | :---- |
| DCL restructure (Phases 0–5) | Mar 2026 | 131 integration tests passing, 8 v2 engines, STRONG PASS |
| Stage 5 COFA truth test | Mar 2026 | PASS, 100% completeness, 6/6 conflicts |
| Maestra constitution (6 phases) | Mar 2026 | Layers 0–4 + Orchestrator built on maestra branch |
| Farm triple conversion | Mar 2026 | Meridian + Cascadia configs output semantic triples |
| NLQ de-hardcoding | Mar 2026 | One query path, no demo/live branching |
| Triple monitoring surfaces | Mar 2026 | DCL Triples tab, Farm Triples tab, Sankey from triples |
| CLAUDE.md (harness rules) | Ongoing | Sections A–F, 26+ rules across all repos |
| Pitch deck + commercial model | Mar 2026 | Four-tier packaging, three user stories |
| **Convergence carveout (DCL Phase 6)** | **Mar 2026** | **ME engines, engagement lifecycle, resolution workspaces extracted to standalone Convergence repo. DCL is SE-only. All callers rerouted (Console, NLQ, Platform). Per CONVERGENCE_CARVEOUT_BLUEPRINT_CANONICAL.** |
| **SE/ME pipeline separation** | **Mar 2026** | **Strict separation of concerns. No shared plumbing. Pipeline isolation spec v1/v2 produced and executed.** |
| **Pipeline identity architecture v1** | **Mar 2026** | **run_id banned. Namespaced stage IDs. Identity pair on every response. Operator dropdown-only input. Per pipeline_identity_architecture_v1.** |
| **Maestra supervised execution (Phase 1)** | **Mar 2026** | **Four-tier model. maestra_plans table (19 columns). Classification engine 15+ test cases 100% pass, 146+ total tests zero regressions. Console operator feed shipped.** |
| **DCL atomic run_id swap** | **Mar 2026** | **tenant_runs pointer table. O(1) UPSERT replaces O(n) bulk UPDATE. Post-swap purge step. COFA exclusion guards audited (Bug A/B/C).** |
| **Console build** | **Mar 2026** | **React 18 + TypeScript + Vite + Tailwind, FastAPI + asyncpg backend. Mode-based structure (SE, MA, ME, ALL). Pipeline orchestrator, entity switcher, sidebar. Production surface replacing Platform.** |
| **ME pipeline fixes** | **Mar 2026** | **Console ME pipeline shipped. Platform engagement ID replaces Console UUID in COFA step. DCL DELETE scoped to entity_id. Farm batch size 5000/concurrency 2. Successful ME run (51 unified accounts, 10 COFA conflicts).** |

## **9.2 Active / Next**

| Item | Status | Dependency |
| :---- | :---- | :---- |
| Pipeline identity cleanup (SE/ME) | Active sprint | Pipeline identity architecture v1 governs. Console tenant_id threading in progress. |
| Maestra supervised execution Phase 2 | In progress | tenant_preferences table with 12-key CHECK constraint |
| SE triples conversion (AOD + AAM) | Build plan v2.1 produced | Deferred from v2.0 — confirmed working SE pipeline is six orchestrated steps with only Farm Financials writing triples to DCL via orchestrator |
| Triple store accumulation cleanup | In progress | semantic_triples table accumulation diagnosed |
| aos-common package extraction | Cleanup debt | Forked infra files in DCL + Convergence with dated headers |
| Convergence URL prefix standardization | Cleanup debt | /api/convergence/* vs /api/me/* |
| NLQ 422 fix (shared run_id from orchestrator) | Identified, deferred | Shared run_id from orchestrator — blocked on identity cleanup |
| Main/dev reconciliation across repos | Known debt | NLQ, Platform, AOD all need reconciliation |
| Farm Period 0 opening balance sheet anchor | Open | Farm config update |
| COFA prefilter relocation per RACI | Open | DCL internal |

# **10. Advisory Review Rulings**

Two rounds of external review (Claude, ChatGPT, Gemini). 15 items debated. Summary of rulings that changed the spec:

| Item | Ruling | Spec Change |
| :---- | :---- | :---- |
| Maestra execution role language | Replace 'no new deterministic engines' with honest description | Maestra reasons; DCL validates; deterministic gates own exact checks |
| COFA spike scope | Include adversarial inputs, define pass criteria before running | Done — degraded CoA test added, rubric defined, STRONG PASS achieved |
| Run ledger | New requirement accepted | engagement_id, step_name, model_version, constitution_version, validation result, timestamp |
| Human review as product | Reframe from fallback to primary workflow | High-impact decisions: Maestra prepares, human confirms, decision recorded |
| Confidence degradation | Add constitution rule for plain-language explanation below medium threshold | Maestra must surface which input drove confidence down, link to evidence, recommend action |
| Cost model | Measure both token consumption and human adjudication time | $1.49/engagement validated for token cost. Human time not yet measured. |
| Scale boundary | Explicit MVP scope: single entity + two-entity Convergence | Portfolio scale (10+ entities) needs blocking/clustering, storage eval, context evolution |

# **11. Guardrails & Anti-Patterns for Claude Code CLI Agents**

## **11.1 Universal Rules**

No silent fallbacks. No bandaids — fundamental, scalable, architecturally sound fixes only. No tech debt. No shortcuts. No cheating to pass tests. Preexisting errors found during work must be fixed. Unscoped agent changes must be reverted. Every CC prompt must reference CLAUDE.md (which contains all harness rules). Self-review every CC prompt against guardrails before presenting. Consequences rule: impact analysis required before changing critical path items.

## **11.2 CLAUDE.md Harness Rules (Sections A–F)**

Demo data doesn't count as pass. Source field checked on every test. Pipeline must run before harness. Test what the user sees, not internal endpoints. Own all repos to fix bugs. Run twice for consistency. Tests must assert positive expected outcome, not just absence of bad outcome.

## **11.3 CC Agent Cheat Patterns (Reject)**

| Pattern | Description |
| :---- | :---- |
| Lightweight/test-only endpoints | Building backdoor endpoints that only the test uses |
| Building test infrastructure without running it | Scaffolding test files that never execute |
| Testing at wrong abstraction layer | Testing internal functions instead of user-facing API |
| Manufacturing system state | Inserting test data directly instead of validating real pipeline output |
| Mode-set backdoors | Adding demo/test mode flags that bypass real logic |
| Fake API key errors | Returning auth errors to avoid running real code |
| In-memory test data | Loading test fixtures instead of querying live store |
| Silent fallback to defaults | Returning plausible-looking zeros or empty arrays instead of failing |

## **11.4 Self-Review Rule**

Every CC prompt must be reviewed against AOS guardrails before presenting. Check for: silent fallbacks, RACI violations, missing cross-module impact, missing harness reference, open-ended CC judgment calls, temp code, no git discipline, internal contradictions, data loss without stop-gates.

# **12. Development Environment**

| Component | Detail |
| :---- | :---- |
| Coding agents | Claude Code CLI (Windows desktop + WSL/Ubuntu laptop), Gemini CLI |
| Repos (7) | dcl, convergence, nlq, farm, platform, aod, aam |
| Console repo | console (React 18 + TypeScript + Vite + Tailwind, FastAPI + asyncpg backend) |
| Branch convention | dev is the working branch. No feature branches unless explicitly stated. |
| Production deployment | Render |
| Database | Supabase (Postgres) |
| Founder role | Ilya runs all terminals. Architect, not coder or devops. |
| Active services (7) | AOD, AAM, DCL, Convergence, NLQ, Farm, Console |
| Sunset | Platform (dev-only transitional artifact, not production) |

# **13. Open Items**

| Item | Priority | Notes |
| :---- | :---- | :---- |
| Pipeline identity cleanup | **Active** | Governed by pipeline_identity_architecture_v1. SE/ME Console tenant_id threading. Farm-minted entity_id persisted into blobs. No silent fallbacks. |
| GL ingestion pipeline (Convergence-Lite) | High | CSV/Excel GL upload → parse → validate → convert to semantic triples → PG. This is the intake path for Story 1. Farm generates synthetic data; this path handles real customer uploads. |
| Context window sizing validation | High | Validate that the largest single stage fits within Sonnet context window limits. |
| aos-common package extraction | Medium | Eliminate forked infra files between DCL and Convergence. |
| Portfolio-scale blocking/clustering | Deferred | At 10+ entities. Not MVP scope. |
| RAG pipeline | Deferred | Only needed when engagement history or entity count exceeds context window. |
| Automated qualitative document parsing | Deferred | Phase 2 — automated extraction from PDFs/contracts. |
| Model routing implementation | Deferred | Dispatch function exists; routing logic deferred. |
| Graph store (Neo4j/AGE) | Deferred | Pattern detection at scale. Not MVP. |
| Tenant registry | Deferred | Deferred to AWS migration. |

# **14. Governing Documents**

| Document | Scope |
| :---- | :---- |
| convergence_MA_spec_v7.4.md (this document) | Canonical M&A spec. All build decisions reference this. |
| pipeline_identity_architecture_v1 | Pipeline identity, provenance, operator-facing naming across SE and ME. |
| CONVERGENCE_CARVEOUT_BLUEPRINT_CANONICAL.md | Agent-executable plan for the Convergence carveout from DCL. Completed. |
| SE_pipeline_modality.docx | SE stage definitions (Farm, AOD, Handoff, AAM, DCL, Verify). |
| se_triples_conversion_build_plan_v2.1 | AOD + AAM EAV conversion work packages. |
| aos_production_architecture_blueprint_v1.1.docx | Infrastructure and deployment. 14 rulings (R1-R14). |
| CLAUDE.md v7.0 (per-repo) | Agent constitution. Merged with HARNESS_RULES_v2. Sections A–F (26 rules: A1-A13, B1-B18, C1-C13, D1-D7) + pipeline identity rules I1–I6. B17 = Playwright. |
| AOS_MASTER_RACI_v8.2.csv | Module ownership matrix. 7 active services + Console. |

# **Version History**

| Version | Date | Changes |
| :---- | :---- | :---- |
| v5 | Mar 2026 | Initial Maestra platform spec. Architecture, capability layers, runtime pattern. |
| v6 | Mar 2026 | Tier definitions enriched per advisor synthesis. Portfolio deferred as standalone product. COFA spike scope amended. Run ledger added. Human review reframed. |
| v7.0 | Mar 2026 | Consolidated governing document. Merged all doctrine into single source of truth. Three contradictions resolved. |
| v7.1 | Mar 2026 | Added §1.3 Convergence-Lite input/output spec. Diligence Integration Package defined (10 deliverables). Added materiality and conflict resolution workflow. Added Layer 3 entity policies. |
| v7.2 | Mar 2026 | Product line alignment: three product lines (AOS, Convergence, Convergence M&A). ContextOS renamed to AOS throughout. |
| v7.3 | Mar 2026 | Two-Axis QofE Adjustment Model. Formalized temporal data model for adjustment triples. Bridge v2 DISTINCT ON replaced with lifecycle-aware grouping. |
| **v7.4** | **Mar 2026** | **SE/ME separation formalized. Convergence carveout complete — new §5A documents standalone Convergence service (repo, engines, tables, API contracts, forked files). DCL §4 updated to SE-only scope. New §6 Pipeline Identity Architecture (identity pairs, namespaced IDs, run_id ban, anti-brittleness rules, SE/ME identifier registries, operator UX rules). §3.6 Supervised Execution added (four-tier model, maestra_plans, operator feed). Integration chain §5.2 updated with engine ownership. §9 milestones updated with carveout, identity architecture, Console build, ME pipeline fixes. §12 Development Environment updated to 7 repos + Console. §14 Governing Documents expanded. RACI bumped to v8.2.** |
