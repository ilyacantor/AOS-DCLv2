**AOS Convergence M\&A Specification**  
Version 7.3 — March 2026

AutonomOS, Inc.

*Canonical governing document. All build decisions, CC prompts, and GTM materials reference this spec.*

# **1\. Product Overview**

AOS (autonomOS) is an enterprise platform that delivers unified context across enterprise systems. The platform has three product lines: AOS (single-entity enterprise intelligence), Convergence (multi-entity integration intelligence), and **Convergence M\&A (diligence through post-integration).** All share a common engine.

## **1.1 Product Lines**

**AOS.** Single-entity enterprise intelligence. Full pipeline: AOD (discovery) → AAM (connection mapping) → Farm (financial model generation) → DCL (semantic context layer) → NLQ (natural language query). Customer connects their systems, AOS builds contextual intelligence.

**Convergence.** Multi-entity integration intelligence for organizations with multiple subsidiaries that need contextual integrated information. Ongoing operating rhythm, not deal-driven. Same engine as AOS and Convergence M\&A: entity is a tag, same DCL, same resolution. Multiple entities flow through the pipeline tagged by entity\_id into one semantic store. Unified reporting, cross-entity analytics, and continuous monitoring across persistent entities.

**Convergence M\&A.** Multi-entity M\&A integration intelligence. Deal-driven: diligence through post-integration. Acquirer and Target data flow into one DCL. Entity is a tag, not a separate brain. Same engines as base AOS, plus a bridge that joins Target pipes into Acquirer pipes. COFA unification, combining financial statements, entity resolution, overlap/concentration analysis, cross-sell, EBITDA bridge, QofE.

PE-specific portfolio product is deferred. The Convergence product line covers multi-entity operating use cases including fund-level visibility across portfolio companies.

## **1.2 Three User Stories**

| Story | Entry Point | Pipeline | Commercial Path |
| :---- | :---- | :---- | :---- |
| 1\. Convergence-Lite | Greenfield M\&A, upload-based (GL minimum) | No AOD/AAM. Maestra ingests GLs \+ CoAs, runs full integration chain. | Lands diligence (Explore) → Resolve |
| 2\. AOS Single-Entity | Enterprise connects systems | Full AOD→AAM→Farm→DCL→NLQ | Standalone entry, enables Story 3 |
| 3\. AOS→Convergence | Acquirer already on platform | Target onboarded via upload or discovery. Convergence runs across both. | Upsell to Resolve, then Operate |
| 3.5 Don't Migrate, Converge | Post-close, target stays on own systems | Target gets AOS \+ persistent Convergence replaces system migration | Operate tier (persistent monitoring) |

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

This is the deliverable the customer pays for. Produced from GL \+ CoA inputs. Replaces 3-4 weeks of associate work.

| \# | Deliverable | Content | Source Engine |
| :---- | :---- | :---- | :---- |
| 1 | COFA Mapping Table | Every GL account from both entities mapped to a unified structure. Confidence scores. Mapping basis (exact match, semantic similarity, hierarchy). Entity of origin. | Maestra (LLM-driven) |
| 2 | Conflict Register | Every conflict typed (recognition timing, measurement basis, classification, scope). Severity (high/medium/low). Estimated dollar impact from GL actuals. Resolution status. | Maestra \+ DCL domain gates |
| 3 | Combining P\&L | Four columns: Entity A | Entity B | Adjustments | Combined. Preserves industry-specific line structure. Every adjustment links to a conflict register entry. Quarterly and annual. | combining\_v2 |
| 4 | Combining Balance Sheet | Same four-column format. Fair value adjustments in Adjustments column. Balance sheet identity enforced (A \= L \+ E). | combining\_v2 |
| 5 | Combining Cash Flow | Operating, investing, financing. Derived from P\&L \+ BS changes. Cash flow identity enforced. | combining\_v2 |
| 6 | EBITDA Bridge | Adjustment categories with confidence grades. Dollar impact per adjustment from GL actuals. What-if sensitivity sliders. | bridge\_v2 |
| 7 | Quality of Earnings | Recurring vs non-recurring classification. Normalization adjustments. Period-over-period trending. Quality score per earnings line. | qoe\_v2 |
| 8 | Entity Resolution (if enrichment data provided) | Cross-entity matches for customers, vendors, people. Confidence scores. Ambiguous pairs surfaced for human review. | entity\_resolution |
| 9 | Overlap & Concentration (if enrichment data provided) | Shared counterparties. Revenue concentration by customer. Risk flags for single-customer dependency. | overlap\_v2 |
| 10 | Cross-Sell Pipeline (if enrichment data provided) | Named accounts, propensity scores, ACV estimates, direction (A→B and B→A). | Maestra \+ overlap\_v2 |

Deliverables 1-7 are always produced (GL \+ CoA is sufficient). Deliverables 8-10 require optional enrichment inputs. Maestra tells the customer which deliverables are available based on what was uploaded — no silent omission.

# **2\. Architecture Decisions** 

## **2.1 Core Decisions**

| Decision | Ruling | Rationale |
| :---- | :---- | :---- |
| Storage engine | Postgres (Supabase) for MVP | EAV at two-entity scale is fine. Evaluate columnar/graph at 10+ entities. |
| Maestra execution role | Maestra reasons through integration chain. DCL validates outputs. Deterministic gates own replayable/exact checks. | No separate orchestration framework for MVP. Not 'no deterministic anything.' |
| Context management | Staged processing with stored work products between stages | RAG deferred. Each stage independently fits within context window. Full chain is never in one prompt. |
| Workflow engine | Prompt-driven, not LangGraph/Temporal | Discrete steps with stored work products give effective resumability. |
| Entity resolution scale | Deterministic keys first, LLM for fuzzy residue, batched by business impact | O(n²) is a portfolio problem, not MVP. Add blocking/clustering at 10+ entities. |
| COFA accuracy | Validated (100% completeness, 6/6 conflicts, $1.49/engagement) | Gating technical risk retired. LLM can reason about chart of accounts unification. |
| Testing | Two-tier: deterministic harness (Tier 1, built) \+ LLM-as-Judge (Tier 2, bounded) | HARNESS\_RULES.md with 16 anti-cheat rules. 100% pass or not done. |
| Document ingestion | CSV \+ Excel for MVP quantitative pipeline. Automated parsing of qualitative PDFs/Word docs is Phase 2\. | For MVP, Layer 3 entity policies are manually authored Markdown files stored in the repo and injected at runtime. This is not deferred — it is a different authoring mode. |
| Model routing | Sonnet for everything at MVP. Architecture supports dispatch per interaction type. | Model routing is a cost lever activated when volume justifies it. |
| Convergence architecture | Same engine as base AOS. Entity is a tag. Bridge joins Target \+ Acquirer pipes into one DCL. | No split brain, no query-time composition, no new resolution logic. |
| Silent fallbacks | Prohibited. Fail loud or not at all. | Hard architectural rule across all repos. |
| No GAAP fallback | Maestra must not infer from general GAAP when entity policy is missing | Output null with flag. LLMs default to GAAP reasoning; must be explicitly blocked. |
| Data claims | Do not claim 'metadata only' or 'we don't touch your data' | Not architecturally validated. |
| Ontology claims | Do not claim AOS delivers ontology | Current: sophisticated semantics. Ontology is aspirational/roadmap. |
| QofE adjustment model | Two-axis: fiscal period attribution \+ diligence lifecycle stage on every adjustment triple. Triple key becomes (entity\_id, concept, property, lifecycle\_stage). DISTINCT ON pattern replaced with lifecycle-aware grouping. | v1 QoE had the right schema (diligence\_amount, prior\_amount, trend) but wrong data source. Formalizing in triples enables H.1, H.5, H.6, G.6, M.14. |

# **3\. Maestra**

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

**The Maestra Constitution (static, versioned).** Her identity, voice, role boundaries, action catalog, scenario variants. Separate constitution variants per scenario type (single entity, multi-entity, M\&A, portfolio) sharing a common base.

**Engagement State (persistent, per-customer).** Structured data in Supabase tracking: onboarding steps completed, questions asked, items flagged, outstanding issues. Not conversation history. Structured state that survives across sessions.

**Live Module State (dynamic, cached).** Current state from each AOS module. Modules publish state changes to a cache layer. Maestra reads from cache. Modules push; Maestra never waits.

## **3.4 Constitution Layer Architecture**

Maestra's behavior is governed by a layered constitution. Higher layers cannot contradict lower layers.

| Layer | Name | Content | Loaded When |
| :---- | :---- | :---- | :---- |
| 0 | Accounting Axioms | DR=CR, element boundaries (A/L/E/R/E mutually exclusive), articulation rules. Both prompt axioms AND backend Pydantic validation. | Always, every invocation |
| 1 | P\&L Constitution | Temporal/flow logic, stub periods, combining, rev rec delegation. Separate doc: BS Constitution (point-in-time, fair value, equity roll-forward). | Per agent invocation |
| 2 | COFA Ontology | Entity resolution rules, match/probable/no-match taxonomy, overlap classification, conflict register structure. | Convergence engagements only |
| 3 | Entity Policies | Per-entity policy docs (scope/rule/boundary). Explicit gaps section required. No-GAAP-fallback constraint in preamble. | Per engagement, one per entity |
| 4 | Industry Profiles | SaaS, Manufacturing (MVP). CoA expectations, KPIs, classification rules. Authoritative definitions. | Per entity industry tag |
| — | Orchestrator | Top-level persona. Agent sequencing, handshake contract, failure handling, CSAT incentive, flag aggregation. | Always, wraps all invocations |

Implementation model: Layers 0-2 hardcoded into agent system prompts (static templates). Layers 3-4 stored as Markdown files, read and appended to context at runtime. Layer 3 policies are manually authored for MVP (not parsed from uploaded documents — automated parsing is Phase 2). Backend Pydantic validation on every agent output, independent of LLM.

## **3.5 Build Status (Constitution)**

All 6 phases built on branch 'maestra' across Platform repo. Stage 5 COFA truth test: STRONG PASS. Ready for integration testing and merge to dev.

| Phase | Content | Status |
| :---- | :---- | :---- |
| 1 — Validation Layer | Layer 0 Pydantic schema \+ prompt axioms | Built |
| 2 — P\&L Agent | Layer 1 P\&L constitution \+ agent | Built |
| 3 — BS Agent \+ Handshake | Layer 1 BS constitution, net income handshake | Built |
| 4 — Context Injection | Layer 3 \+ 4 runtime loading | Built |
| 5 — COFA | Layer 2 COFA unification via Maestra | Built, STRONG PASS |
| 6 — Orchestrator | Top-level persona, agent sequencing, flag aggregation | Built |

## **3.6 Engagement Run Ledger**

Every Maestra engagement step emits a ledger entry: engagement\_id, step\_name, inputs (or hash), model\_version, constitution\_version, intermediate output, validation result, human override (if any), timestamp. Stored in maestra schema. Required for debugging customer incidents.

## **3.7 Human Review Pipeline**

For high-impact decisions (entity resolution involving material revenue, COFA mappings affecting reported EBITDA, cross-domain reclassifications), Maestra prepares a structured recommendation with evidence and business implications. Human confirms. Decision recorded with provenance. This is the primary workflow, not a fallback.

Four-tier classification. Confidence decomposition: compound confidence broken into components, each evaluated independently. Below medium threshold: Maestra must surface the number with plain-language explanation of which input drove it down, link to underlying workspace or mapping, and format the conflict for human review. Maestra does not recommend an accounting resolution — she isolates the variables and presents them. The human decides.

## **3.8 COFA Unification (How Maestra Does It)**

Maestra reads two CoAs. She understands economic substance, not just label matching. She builds a mapping table: source account → unified account, with confidence scores. Where one entity has granularity the other lacks, the unified CoA keeps the granular structure. Where entities use different treatments for the same substance, she flags it as a typed conflict (recognition timing, measurement basis, classification, scope). She asks questions on judgment calls. She writes the mapping, conflict register, and unified structure to DCL as triples.

DCL validates via COFACompletionGate: every source account must appear in the output. If orphaned, DCL rejects and tells Maestra which accounts are missing. Self-correcting loop.

### **3.8.1 Domain Boundary Constraints**

| Constraint | Type | Description |
| :---- | :---- | :---- |
| Asset/Liability/Equity/Revenue/Expense | Hard gate | Mutually exclusive. DCL rejects cross-domain mappings. |
| Revenue cannot map to OpEx | Hard gate | DCL rejects with explanation. Maestra cannot override. |
| COGS/OpEx boundary | Soft gate | Flag \+ human confirmation instead of hard rejection. |
| Contra-account handling | Rule | Accumulated depreciation handled by parent account's domain (Asset), not by credit sign. |

### **3.8.2 Known Conflict Types (Consulting/BPM Playbook)**

| COFA ID | Conflict | Severity Guidance |
| :---- | :---- | :---- |
| COFA-001 | Revenue gross/net recognition | Revenue diff \> 5% combined: HIGH |
| COFA-002 | Benefits loading (COGS vs OpEx) | 8-15% of COGS |
| COFA-003 | S\&M bundling | Affects OpEx comparability |
| COFA-004 | Recruiting capitalization vs expense | Affects COGS/OpEx and asset base |
| COFA-005 | Automation capitalization | Same pattern as COFA-004 |
| COFA-006 | Depreciation method (straight-line vs accelerated) | Affects D\&A and book value |

### **3.8.3 COFA Truth Test Results**

| Test | Input | Completeness | Conflicts Found | Cost |
| :---- | :---- | :---- | :---- | :---- |
| A (structured × structured) | Meridian \+ Cascadia full CoAs | 100% | 6/6 correctly typed | $1.49/engagement |
| B (structured × degraded) | Meridian \+ degraded (no metadata) | 100% | 3 (expected — degraded CoA lacks accounts needed for all 6\) | — |

Decision gate result: STRONG PASS. Proceed as designed.

### **3.8.4 Materiality and Conflict Resolution Workflow**

Maestra is a reporter, not an authority. She identifies conflicts, types them, estimates dollar impact from GL data, and ranks them by materiality. She does not resolve them. Humans resolve. The workflow is designed to make that human decision-making efficient at scale.

**Conflict ranking.** Maestra sorts all identified conflicts by estimated annual dollar impact, descending. The CFO sees the $50M revenue recognition difference before the $200K depreciation method difference. This is the primary mechanism for managing volume — at 200 conflicts, the top 10 typically represent 90% of total impact.

**No auto-resolution.** Every conflict routes to human review regardless of materiality. Maestra does not apply a materiality threshold to skip conflicts or silently resolve them. Low-materiality conflicts still appear in the queue — they are ranked last, not hidden. This is a deliberate design choice: no accounting decision, however small, is made by the LLM.

**Batch approve.** The Human Review Queue supports batch actions. After reviewing the top material conflicts individually, the CFO can select all remaining conflicts below a self-determined threshold and apply a bulk resolution (e.g., 'accept acquirer treatment for all selected'). This is a human action with an audit trail — the system records who approved, when, what threshold they applied, and which conflicts were included. Maestra does not set the threshold or suggest the batch action.

**Resolution options per conflict.** For each conflict, the human selects one of: (1) normalize to acquirer treatment, (2) normalize to target treatment, (3) keep both and show adjustment in combining column, (4) flag for post-close harmonization (no adjustment now, tracked as open item). The decision is recorded with reasoning and linked to the conflict register entry. The combining engine reads the resolution and applies it.

**Audit trail.** Every resolution records: conflict\_id, decision (which option), decided\_by (human:user\_id), reasoning (free text), timestamp, materiality at time of decision (dollar impact). Batch approvals record the same fields plus the selection criteria used. This trail is a deliverable — it goes into the Diligence Integration Package as evidence of how each difference was addressed.

Future (Resolve tier): engagement-level materiality threshold set by the CFO at scoping. Conflicts below threshold auto-resolve to a default treatment specified by the CFO, with full audit trail. This is the CFO's policy applied deterministically, not Maestra's judgment. Deferred until batch approve proves the workflow at MVP scale.

## **3.9 Layer 3 Entity Policies (MVP)**

Per §3.4, Layer 3 entity policies are manually authored Markdown files for MVP. Automated parsing of uploaded PDFs/Word docs is Phase 2\. Two policy documents exist, stored in Platform at app/maestra/constitution/policies/:

| File | Entity | Sections | Key Policy Elections |
| :---- | :---- | :---- | :---- |
| meridian\_policy.md | Meridian Partners (Acquirer) | Revenue recognition, COGS, OpEx, D\&A, BS policies, Explicit Gaps | Gross revenue recognition. Benefits in OpEx (not COGS). Recruiting expensed immediately. R\&D expensed below $10M threshold. Straight-line depreciation. |
| cascadia\_policy.md | Cascadia Process Solutions (Target) | Revenue recognition, COGS, OpEx, Capitalization policy, D\&A, BS policies, Explicit Gaps | Net revenue recognition. Benefits in COGS for delivery staff. Recruiting capitalized above $50K/hire. Automation capitalized above $2M/project. Accelerated depreciation for delivery equipment. |

Each document includes an Explicit Gaps section listing items NOT covered. Maestra must output null with a flag for any gap item — she must not infer from general GAAP training data. This is the no-GAAP-fallback constraint enforced at the document level.

For a real customer engagement (Convergence-Lite, §1.2 Story 1), these documents would be authored by the customer's finance team during onboarding or extracted manually from their accounting policy manual. Maestra's scoping conversation can guide the customer through what's needed.

# **4\. DCL (Data Context Layer)**

## **4.1 Semantic Triple Store**

All data in DCL lives as semantic triples: (entity\_id, concept, property, value, period). Every triple carries provenance: source\_system, source\_field, confidence\_score, confidence\_tier, pipe\_id, run\_id, created\_at. Stored in Postgres (Supabase).

Concepts use dot-separated hierarchical naming (e.g., cofa.automation\_capitalization, compensation.base). Domains are the first segment of the concept name.

## **4.2 Current State**

| Metric | Value |
| :---- | :---- |
| Total triples (last ingest) | 18,500 |
| Entities | 2(Meridian, Cascadia) |
| Domains | 17 |
| Periods | 24 |
| v2 Engines | 8 (resolver, combining, overlap, bridge, whatif, qoe, cofa, entity\_resolution) |
| Integration tests passing | 131 |
| Old JSON engines | Present alongside v2 via compat layer |

## **4.3 DCL Restructure (Completed)**

The DCL restructure replaced the old JSON-based in-memory engine stack with a Postgres-backed semantic triple store. Five phases:

| Phase | Content | Status |
| :---- | :---- | :---- |
| 0 — Schema \+ Farm Triples | PG schema for semantic\_triples, Farm outputs triples instead of bespoke JSON | Done |
| 1 — DCL Core | Ingest, query resolver reads from PG, entity resolution in PG | Done |
| 2 — Engine Re-plumb | Each v2 engine reads from triples. NLQ de-hardcoded. | Done |
| 3 — Missing Capabilities | Combining BS/CF, revenue variance bridge, scenario comparison | Done |
| 4 — Maestra Foundation | Engagement lifecycle, constitution, tools, chat, human review | Done |
| 5 — Integration Chain | COFA truth test, combining financials via Maestra | STRONG PASS |

## **4.4 V2 Engine Stack**

Eight v2 engines, all reading from the semantic triple store:

| Engine | Purpose | Key Outputs |
| :---- | :---- | :---- |
| resolver\_v2 | Query resolution against triples | Structured answers with provenance |
| combining\_v2 | Combining financial statements (P\&L, BS, SOCF) | Four-column format: Entity A | Entity B | Adjustments | Combined |
| overlap\_v2 | Customer/vendor overlap and concentration | Shared counterparties, revenue concentration, risk flags |
| bridge\_v2 | EBITDA bridge with adjustments | Adjustment categories, confidence grades, what-if sliders |
| whatif\_v2 | Scenario modeling | Parameterized what-if with stored scenarios |
| qoe\_v2 | Quality of Earnings analysis | QofE adjustments, trending, recurring vs non-recurring |
| cofa\_engine | COFA generation from Farm output | Chart of accounts triples per entity |
| entity\_resolution | Cross-entity identity matching | Resolution workspaces with confidence scores |

## **4.5 Farm Configurations**

Two canonical Farm configs. No other configs are valid. fact\_base.json and the default $35M toy config have been permanently removed.

| Config | Entity | Revenue | Domains |
| :---- | :---- | :---- | :---- |
| farm\_config\_meridian.yaml | Meridian (Acquirer) | $5B | 14 financial domains |
| farm\_config\_cascadia.yaml | Cascadia (Target) | $1B | 14 financial domains |

## **4.6 Data Pipeline**

Single-entity (AOS): AOD → AAM → Farm → triple conversion → PG direct. No DCL pipe ingest (Structure/Dispatch/Content path is deprecated).

Multi-entity (Convergence): Both entities' data flow through separate Farm configs → triple conversion → same PG store, tagged by entity\_id. Maestra runs COFA chain across both.

DCL Ingest/Recon tabs are legacy. Triples tab is the active monitoring surface.

# **5\. Convergence Architecture**

Convergence \= base AOS plus a bridge where Target pipes join Acquirer pipes into one DCL. Entity is a tag, same engine, no split brain, no query-time composition.

## **5.1 Invariants**

Same engine as base AOS. Entity\_id is a column, not a separate instance. No new resolution logic beyond what AOS uses. The bridge is the join point, not a fork.

## **5.2 Integration Chain**

| Step | Engine | Output | Gate |
| :---- | :---- | :---- | :---- |
| 1\. Dual CoA ingestion | cofa\_engine | Two sets of COFA triples in store | Both entities have cofa-domain triples |
| 2\. COFA unification | Maestra (LLM-driven) | Mapping table, conflict register, unified structure | COFACompletionGate (no orphans) |
| 3\. Combining FS | combining\_v2 | P\&L, BS, SOCF in four-column format | DR=CR, revenue identity, balance sheet identity |
| 4\. Entity resolution | entity\_resolution | Resolution workspaces with confidence | Deterministic keys first, LLM for residue |
| 5\. Overlap/concentration | overlap\_v2 | Shared counterparties, risk flags | Data exists for both entities |
| 6\. Cross-sell | Maestra \+ overlap\_v2 | Pipeline, propensity, ACV estimates | Overlap step complete |
| 7\. EBITDA bridge | bridge\_v2 | Adjustments with confidence grades | Combining FS step complete |
| 8\. QofE | qoe\_v2 | Quality adjustments, trending | EBITDA bridge complete |
| 9\. What-if | whatif\_v2 | Parameterized scenarios | All prior steps complete |

## **5.3 Hard Accounting Gates (Deterministic, DCL-Enforced)**

DR \= CR (trial balance nets to zero pre and post mapping). Revenue identity (combined \= sum of standalones). Asset identity (combined \= sum ± intercompany). Balance sheet identity (A \= L \+ E for each entity and combined). These are not negotiable. Maestra cannot override.

## **5.4 COFA Merge Tab (DCL UI)**

New top-level tab in DCL displaying COFA merge status. Five sections: Merge Overview (entity stats), Side-by-Side COFA Comparison (acquirer left, target right), Account Match Table (resolution data if exists), Unmatched/Orphan Accounts, Raw COFA Triple Browser. Read-only display of what's in the store. Merge engine is Maestra, not a coded pipeline.

# **6\. Combining Financial Statements (Proforma)**

## **6.1 Output Format**

Four columns: Entity A | Entity B | Adjustments | Combined. Every adjustment links to a conflict register entry. Annual comparisons. Revenue lines preserve industry-specific structure (consulting vs managed services, not generic revenue).

## **6.2 Statement Types**

| Statement | Key Lines | Notes |
| :---- | :---- | :---- |
| Combining P\&L | Revenue (by type), COGS (by structure), OpEx (S\&M, G\&A, R\&D), down to EBITDA | COGS lines preserve entity cost structures |
| Combining BS | Assets, Liabilities, Equity. Fair value adjustments in Adjustments column. | Balance sheet identity enforced |
| Combining SOCF | Operating, investing, financing. Derived from P\&L \+ BS changes. | Cash flow identity enforced |
| Unified Trial Balance | Both entities' period balances in unified structure. Adjustment column for reclassifications. | Debits \= Credits for each entity and combined |

## **6.3 EBITDA Bridge**

Adjustment categories with confidence grades. What-if sliders for sensitivity. Each adjustment typed and linked to evidence. Grades: high confidence (deterministic calculation), medium (LLM-assisted with supporting data), low (requires human adjudication).

## **6.4 Quality of Earnings (QofE)**

QofE is an ongoing analytical instrument applied to each quarterly and annual report, not a one-time diligence artifact. Each period, the QofE engine runs against the latest financials and produces an updated assessment: is the earnings quality holding, improving, or deteriorating? Are the adjustments from diligence still valid? Have new adjustments emerged?

The current data model stores adjustment triples with period \= NULL and no lifecycle metadata. The EBITDA bridge v2 engine uses DISTINCT ON (entity\_id, concept, property) ORDER BY created\_at DESC, which returns only the single latest value per adjustment. This makes it impossible to answer three fundamental QofE questions: (1) which fiscal period does this adjustment relate to, (2) how has this adjustment evolved across diligence phases, and (3) is the adjustment improving, stable, or worsening over time. This section formalizes the fix.

### **6.4.1 Two-Axis Adjustment Model**

Every adjustment triple must carry two temporal dimensions: a fiscal period the adjustment relates to, and a diligence lifecycle stage at which the estimate was produced. Without both axes, QofE is a static list of numbers. With both, QofE answers the questions that buyers, PE funds, and post-close operators actually ask.

### **6.4.2 Axis 1: Fiscal Period Attribution**

The fiscal period the adjustment relates to. The period property on the triple carries the relevant quarter (e.g., 2025-Q1). An additional property, period\_type, classifies the temporal nature of the adjustment:

| period\_type | Definition | Example |
| :---- | :---- | :---- |
| **occurred** | The adjustment relates to a specific period in which the event happened. | Non-recurring legal $11M in 2024-Q3 |
| **annualized** | An annualized normalization applied to the assessment period. Not tied to a single quarter. | Owner compensation $30M/yr normalization |
| **run\_rate** | A forward-looking projected savings or cost, annualized from the assessment period. | Run rate cost savings $59M projected |
| **synergy** | A post-close integration synergy, projected forward from the deal close. | Facility consolidation $29M post-close |

Adjustments with period\_type \= occurred must have a period value matching the quarter of occurrence. All other types use the assessment period (the period in which the diligence is being conducted) as their period value.

### **6.4.3 Axis 2: Diligence Lifecycle Stage**

The diligence phase at which this estimate of the adjustment was produced. Each lifecycle stage represents a distinct point of assessment with potentially different amounts, confidence levels, and supporting evidence. Lifecycle stages are ordered: management \< initial\_diligence \< confirmatory \< agreed \< post\_close. Confidence should generally increase as lifecycle advances.

| lifecycle\_stage | Definition | Typical Source |
| :---- | :---- | :---- |
| **management** | Management's self-reported adjustment, typically presented at LOI or CIM stage. | CIM, management deck |
| **initial\_diligence** | Independent estimate produced during initial due diligence. May confirm, adjust, or reject management's number. | DD workstream output |
| **confirmatory** | Refined estimate after confirmatory diligence. Higher evidence quality, tighter range. | Confirmatory DD report |
| **agreed** | Final agreed amount at or near close. Goes into the purchase agreement or closing adjustment. | SPA, closing memo |
| **post\_close** | Ongoing quarterly reassessment after close. Used for QofE trending and synergy materialization tracking. | Quarterly QofE review |

### **6.4.4 Triple Store Key Model**

Current key: (entity\_id, concept, property), disambiguated by created\_at DESC. This produces one row per adjustment. New key: (entity\_id, concept, property, lifecycle\_stage), with period as an additional queryable property. This allows multiple rows per adjustment concept, one per lifecycle stage, enabling temporal comparison.

No schema migration is required. lifecycle\_stage, period, and period\_type are new property values on adjustment triples that the existing PG triple store already supports. The existing properties (amount, amount\_low, amount\_high, confidence, rationale, support\_reference, lever, name, concept) are unchanged.

### **6.4.5 DCL Engine Changes**

Bridge v2 engine (ebitda\_bridge\_v2.py): The DISTINCT ON pattern is replaced, not patched. The new query groups adjustment triples by (entity\_id, concept). Within each group, it retrieves rows for all lifecycle stages and pivots:

| Output Column | Source |
| :---- | :---- |
| **Current** | amount from the latest lifecycle\_stage present for this concept |
| **Diligence** | amount from lifecycle\_stage \= management (the management number at LOI) |
| **Prior** | amount from the lifecycle\_stage one step before the latest present stage |
| **Trend** | Derived: current \> prior \= up arrow, current \< prior \= down, equal \= stable. Single stage \= neutral. |
| **Conf.** | confidence from the latest lifecycle\_stage (unchanged from current behavior) |

When only one lifecycle stage exists, Diligence and Prior show as null (dash in the UI), Trend shows as neutral. This is the current behavior and remains correct for single-stage data.

QofE combined endpoint (/api/dcl/reports/v2/qoe/combined): must additionally return adjustment\_lifecycle (full set of lifecycle stage snapshots per adjustment concept) and sustainability\_trend (sustainability score per assessment period, not just current period).

### **6.4.6 Farm Generation Changes**

Farm's adjustment triple generator must emit lifecycle\_stage, period, and period\_type on every adjustment triple. The entity config YAML files (farm\_config\_meridian.yaml, farm\_config\_cascadia.yaml) must specify period\_type and lifecycle\_stages per adjustment definition, allowing a single adjustment to produce multiple triples.

Seed data requirements: For Meridian and Cascadia, Farm must generate at minimum two lifecycle stages per adjustment. Normalizations: management stage at CIM number, then initial\_diligence with independent estimate (amounts converge). Non-recurring items: management reported amount, then validated/revised initial\_diligence amount. Synergies: management deal thesis number, then initial\_diligence DD estimate (most volatile, should show meaningful differences). Management estimates for synergies tend optimistic; DD estimates for non-recurring tend conservative.

### **6.4.7 NLQ Frontend Wiring**

The QofE tab already renders the correct column structure (Adjustment, Current, Diligence, Prior, Status, Trend, Conf). Diligence column: populate from management lifecycle\_stage amount. Prior column: populate from prior lifecycle\_stage amount. Trend column: directional arrow from current vs. prior comparison. Expanded row detail: show full lifecycle history as a mini-timeline with stage name, amount, confidence, and delta from previous stage.

### **6.4.8 QofE Guardrails (Locked Decisions)**

**No schema migration. The triple store already supports arbitrary properties. lifecycle\_stage, period, and period\_type are new property values, not new columns.**

**lifecycle\_stage is required on all adjustment triples going forward. Existing NULL triples are treated as lifecycle\_stage \= initial\_diligence for backward compatibility.**

**period is required on all adjustment triples going forward. Existing NULL-period adjustment triples are treated as belonging to the assessment period defined in the entity config.**

**EBITDA is still always derived (revenue minus COGS minus opex), never independently stored. Adjustments modify the bridge overlay, not the underlying financial triples.**

**No auto-resolution of adjustment conflicts across lifecycle stages. If management says $29M and DD says $18M, both values are stored and surfaced. Maestra does not pick a winner. Human review resolves. Consistent with Layer 3 manual for MVP.**

**The DISTINCT ON pattern in the bridge v2 engine is replaced, not patched. Any reversion to DISTINCT ON for adjustment triples is a regression.**

### **6.4.9 Build Sequence**

Phase 1 (Farm): Update entity config YAMLs with lifecycle\_stages and period\_type per adjustment. Update adjustment triple generator to emit new properties. Regenerate seed data. Verify triples in PG. Target: dev branch.

Phase 2 (DCL): Rewrite bridge v2 engine query to group by concept and return all lifecycle stages. Update QofE combined endpoint. Update integration tests. Target: dev branch.

Phase 3 (NLQ): Wire Diligence, Prior, and Trend columns to real data. Add lifecycle timeline to expanded row detail. Target: dev branch.

Phases are sequential. Farm must complete and seed data must be verified in PG before DCL work begins. DCL must complete and endpoints must return correct multi-stage data before NLQ wiring begins.

### **6.4.10 Functionality Map Alignment**

| ID | Capability | Enabled By |
| :---- | :---- | :---- |
| **G.6** | Adjustment lifecycle tracking | lifecycle\_stage property on adjustment triples |
| **H.1** | Current period QofE with diligence/prior comparison | Multi-stage query in bridge v2 engine |
| **H.5** | QofE trend over time | period \+ lifecycle\_stage enable quarter-over-quarter trending |
| **H.6** | Adjustment migration tracking | Lifecycle stage progression per adjustment concept |
| **M.14** | QofE view in report portal | All of the above surfaced through NLQ frontend |

# **7\. Functionality Map**

The canonical functionality map (functionality\_map.xlsx) is the official AOS build tracker. 125 capabilities across 13 sections (A–M), organized by pipeline stage. Customer-facing — no internal dev artifacts, no negative framing. Status filled by CC runner audit.

## **7.1 Sections**

| Section | Name | Scope |
| :---- | :---- | :---- |
| A | Discovery (AOD) | Environment scan, asset catalog, SOR authority mapping |
| B | Connection Mapping (AAM) | Connector library, schema extraction, data sampling |
| C | Semantic Layer (DCL Core) | Triple store, business object hierarchy, provenance, entity resolution, domain boundaries |
| D | COFA Unification | Dual CoA ingestion, account mapping, completeness validation, conflict register, policy flags |
| E | Combining Financial Statements | Hard accounting gates, combining P\&L/BS/SOCF, unified trial balance |
| F | Overlap & Concentration | Customer/vendor overlap, revenue concentration, risk scoring |
| G | Cross-Sell | Pipeline generation, propensity scoring, ACV estimates |
| H | EBITDA Bridge | Adjustment identification, confidence grading, what-if sensitivity |
| I | Quality of Earnings | Recurring/non-recurring, normalization, trending |
| J | What-If Scenarios | Parameterized modeling, scenario comparison, saved scenarios |
| K | Executive Dashboards | CFO, CRO, CHRO, COO, CTO persona views |
| L | NLQ (Natural Language Query) | Intent recognition, entity detection, query routing, provenance display |
| M | Maestra | Engagement lifecycle, constitution, tools, chat, human review, run ledger |

## **7.2 Status Definitions**

| Status | Meaning |
| :---- | :---- |
| BUILT | Runs against live data for arbitrary entities, harness passes independently |
| PARTIAL | Logic exists with gaps (state exactly what is missing) |
| STUB | Endpoint exists, returns placeholder/mock data |
| MISSING | No code found |
| HARDCODED | Works but only for Meridian/Cascadia specifically, not entity-agnostic |

Full detail lives in functionality\_map.xlsx. This spec is the governing doctrine; the map is the build tracker.

# **8\. Build Status & Milestones**

## **8.1 Completed**

| Milestone | Date | Evidence |
| :---- | :---- | :---- |
| DCL restructure (Phases 0–4) | Mar 2026 | 131 integration tests passing, 8 v2 engines |
| Stage 5 COFA truth test | Mar 2026 | PASS, 100% completeness, 6/6 conflicts |
| Maestra constitution (6 phases) | Mar 2026 | Layers 0–4 \+ Orchestrator built on maestra branch |
| Farm triple conversion | Mar 2026 | Meridian \+ Cascadia configs output semantic triples |
| NLQ de-hardcoding | Mar 2026 | One query path, no demo/live branching |
| Triple monitoring surfaces | Mar 2026 | DCL Triples tab, Farm Triples tab, Sankey from triples |
| HARNESS\_RULES.md | Ongoing | 16 rules across all repos |
| Pitch deck \+ commercial model | Mar 2026 | Four-tier packaging, three user stories |

## **8.2 Active / Next**

| Item | Status | Dependency |
| :---- | :---- | :---- |
| Maestra branch merge to dev | Audit in progress | Branch state check across all repos |
| COFA Merge tab (DCL UI) | Building | Read-only, no engine work |
| Sankey graph fix (entity as visual dimension) | Needed | Separate from Convergence merge tab |
| Maestra monitoring surface (Platform UI) | Prompt ready | Backend endpoints exist |
| Maestra chat interface (Platform UI) | Not started | After monitoring surface |
| Main/dev reconciliation across repos | Known debt | NLQ, Platform, AOD all need reconciliation |
| DEFAULT\_PERSONA\_CONCEPTS migration | Open | Move from semantic\_export.py to persona\_domains.yaml |
| Farm Period 0 opening balance sheet anchor | Open | Farm config update |
| COFA prefilter relocation per RACI | Open | DCL internal |
| Old NLQ ingest buffer removal | Open | After v2 cutover verified |

# **9\. Advisory Review Rulings**

Two rounds of external review (Claude, ChatGPT, Gemini). 15 items debated. Summary of rulings that changed the spec:

| Item | Ruling | Spec Change |
| :---- | :---- | :---- |
| Maestra execution role language | Replace 'no new deterministic engines' with honest description | Maestra reasons; DCL validates; deterministic gates own exact checks |
| COFA spike scope | Include adversarial inputs, define pass criteria before running | Done — degraded CoA test added, rubric defined, STRONG PASS achieved |
| Run ledger | New requirement accepted | engagement\_id, step\_name, model\_version, constitution\_version, validation result, timestamp |
| Human review as product | Reframe from fallback to primary workflow | High-impact decisions: Maestra prepares, human confirms, decision recorded |
| Confidence degradation | Add constitution rule for plain-language explanation below medium threshold | Maestra must surface which input drove confidence down, link to evidence, recommend action |
| Cost model | Measure both token consumption and human adjudication time | $1.49/engagement validated for token cost. Human time not yet measured. |
| Scale boundary | Explicit MVP scope: single entity \+ two-entity Convergence | Portfolio scale (10+ entities) needs blocking/clustering, storage eval, context evolution |

Items where spec held without change (8): Postgres for triples, context window/RAG, prompt-driven workflow, entity resolution O(n²), testing approach, document ingestion scope, founder knowledge/supportability, RAG pipeline.

# **10\. Guardrails & Anti-Patterns for Claude Code CLI Agents**

## **10.1 Universal Rules**

No silent fallbacks. No bandaids — fundamental fixes only. No tech debt. No cheating to pass tests. Preexisting errors found during work must be fixed. Every CC prompt must reference tests/HARNESS\_RULES.md.

## **10.2 HARNESS\_RULES.md (16 Rules)**

Demo data doesn't count as pass. Source field checked on every test. Pipeline must run before harness. Test what the user sees, not internal endpoints. Own all repos to fix bugs. Run twice for consistency. Tests must assert positive expected outcome, not just absence of bad outcome.

## **10.3 CC Agent Cheat Patterns (Reject)**

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

## **10.4 Self-Review Rule**

Every CC prompt must be reviewed against AOS guardrails before presenting. Check for: silent fallbacks, RACI violations, missing cross-module impact, missing harness reference, open-ended CC judgment calls, temp code, no git discipline, internal contradictions, data loss without stop-gates.

# **11\. Development Environment**

| Component | Detail |
| :---- | :---- |
| Coding agents | Claude Code CLI (Windows desktop \+ WSL/Ubuntu laptop), Gemini CLI |
| Repos | dcl, nlq, farm, platform, aod, aam |
| Branch convention | dev is the working branch. No feature branches unless explicitly stated. |
| Production deployment | Render |
| Database | Supabase (Postgres) |
| Founder role | Ilya runs all terminals. Architect, not coder or devops |

# **12\. Open Items**

| Item | Priority | Notes |
| :---- | :---- | :---- |
| GL ingestion pipeline (Convergence-Lite) | High | CSV/Excel GL upload → parse → validate → convert to semantic triples → PG. This is the intake path for Story 1\. Farm generates synthetic data; this path handles real customer uploads. Must extract: account, period, debit/credit, department/segment. CoA derived from GL or uploaded separately. |
| Context window sizing validation | High | Validate that the largest single stage (likely COFA Unification, which ingests both CoAs simultaneously) fits within Sonnet context window limits. Full chain never needs to fit in one prompt — stages store work products in DCL between steps. RAG trigger is per-stage overflow, not chain-level. |
| Portfolio-scale blocking/clustering | Deferred | At 10+ entities. Not MVP scope. |
| RAG pipeline | Deferred | Only needed when engagement history or entity count exceeds context window. |
| Automated qualitative document parsing | Deferred | Phase 2 — automated extraction from PDFs/contracts. MVP uses manually authored Markdown policies (Layer 3 is active, not deferred). |
| Model routing implementation | Deferred | Opus for everything at MVP. Dispatch function exists; routing logic deferred. |
| **Graph store (Neo4j/AGE)** | **Deferred** | **Pattern detection at scale. Not MVP.** |

# **Version History**

| Version | Date | Changes |
| :---- | :---- | :---- |
| v5 | Mar 2026 | Initial Maestra platform spec. Architecture, capability layers, runtime pattern. |
| v6 | Mar 2026 | Tier definitions enriched per advisor synthesis. Portfolio deferred as standalone product. COFA spike scope amended. Run ledger added. Human review reframed. |
| v7.0 | Mar 2026 | Consolidated governing document. Merged all doctrine into single source of truth. Three contradictions resolved: (1) Layer 3 manually authored for MVP; (2) Maestra does not recommend accounting resolutions; (3) context window \= largest single stage. |
| v7.1 | Mar 2026 | Added §1.3 Convergence-Lite input/output spec: GL is minimum input, no degraded-only design path. Diligence Integration Package defined (10 deliverables). Added §3.8.4 Materiality and Conflict Resolution Workflow: no auto-resolution, materiality-ranked queue, batch approve with audit trail. Added §3.9 Layer 3 Entity Policies: meridian\_policy.md and cascadia\_policy.md with explicit gaps sections. |
| v7.2 | Mar 2026 | Product line alignment: three product lines (AOS, Convergence, Convergence M\&A). ContextOS renamed to AOS throughout. Convergence added as standalone product line for multi-entity operating use cases. PE portfolio product deferred. All ContextOS references updated to AOS. |
| v7.3 | Mar 2026 | Two-Axis QofE Adjustment Model. Formalized temporal data model for adjustment triples: Axis 1 (fiscal period attribution with period\_type taxonomy) and Axis 2 (diligence lifecycle staging: management through post\_close). Triple store key extended to (entity\_id, concept, property, lifecycle\_stage). Bridge v2 engine DISTINCT ON pattern replaced with lifecycle-aware grouping. Farm must emit lifecycle\_stage, period, period\_type on all adjustment triples. Seed data requires minimum two lifecycle stages per adjustment. Six locked guardrails established. Build sequence: Farm then DCL then NLQ, sequential. |

