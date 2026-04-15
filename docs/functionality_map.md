# AOS Platform — Canonical Functionality Map

## How to Read This

Every capability the platform delivers, listed once, in pipeline order. Status reflects what the code does today.

**Status definitions:**
- **BUILT** — runs against live data for arbitrary entities, harness passes independently
- **PARTIAL** — logic exists with gaps (noted in evidence)
- **STUB** — endpoint exists, returns placeholder data
- **MISSING** — capability not yet implemented

---

## A. Discovery

| ID | Capability | Description | Status |
|----|-----------|-------------|--------|
| A.1 | Environment scan | Scan an entity's IT environment. Identify all assets: sources of record, integration layers, data stores, APIs. Classify by type and role. | |
| A.2 | Dual-entity scan | Parallel scans for both entities. Separate inventories, each tagged to its entity of origin. | |
| A.3 | Asset catalog | Structured inventory with classification status, fabric plane assignment, and connection count per asset. | |
| A.4 | Data quality signals | Per-source quality assessment: completeness, freshness, consistency. Available to Mai during context pre-load. | |
| A.5 | Scope assessment | What systems exist, what data is available, what gaps are visible — before any mapping or resolution begins. | |

## B. Connection Mapping

| ID | Capability | Description | Status |
|----|-----------|-------------|--------|
| B.1 | Pipe topology | Map connections between systems. What data flows where, through which fabric plane, in which direction. | |
| B.2 | Gap identification | Surface expected connections that don't exist. Systems with no outbound pipes. Orphaned data stores. | |
| B.3 | Dual-entity connection map | Topology for both entities. Cross-entity connection visibility. | |
| B.4 | Pipe definition export | Connection definitions pushed to the semantic layer for downstream validation. | |

## C. Semantic Layer

### C.1 Core Infrastructure

| ID | Capability | Description | Status |
|----|-----------|-------------|--------|
| C.1.1 | Semantic data model | Business data stored as semantic triples (concept / property / value) with provenance metadata and confidence scores. | |
| C.1.2 | Provenance on every data point | Every value carries: source system, source field, confidence score and tier, mapping status, pipeline run ID. | |
| C.1.3 | Business object hierarchy | Hierarchical concept structure (e.g., entity → entity.office.location → entity.department.name). Drill-down and rollup. | |
| C.1.4 | Entity scoping | Every data point tagged to its entity of origin. Queries scope to individual entities or combined views. | |
| C.1.5 | Materialized query layer | User-facing queries served from pre-indexed structures for performance. | |
| C.1.6 | Confidence metadata | Confidence score (0.0–1.0) and tier (high / medium / low) on all data points. | |

### C.2 Entity Resolution

| ID | Capability | Description | Status |
|----|-----------|-------------|--------|
| C.2.1 | Deterministic matching | Match records across entities on shared keys: Tax ID, DUNS, email domain. | |
| C.2.2 | Parallel matching signals | Trigram name similarity, domain hash matching, shared contacts, address proximity. Pre-built indexes for performance. | |
| C.2.3 | Resolution workspaces | Candidates and evidence packaged into structured workspaces for review. Status: pending / reviewed / resolved. | |
| C.2.4 | Resolution persistence | Decisions survive across sessions, snapshots, and pipeline reruns. Full provenance: who decided, when, reasoning, confidence. | |
| C.2.5 | Human override | Any automated or Mai decision can be overridden by a human. Override logged with new evidence. | |
| C.2.6 | Cross-reference tables | Post-resolution lookup: a single customer record linked to source records from both entities. | |

### C.3 Domain Boundary Constraints

| ID | Capability | Description | Status |
|----|-----------|-------------|--------|
| C.3.1 | Domain classification | Each account classified into its financial statement domain (Asset / Liability / Equity / Revenue / COGS / OpEx) using account type metadata. | |
| C.3.2 | Cross-domain enforcement | Incompatible mappings rejected with explanation. Asset cannot map to Liability. Revenue cannot map to OpEx. | |
| C.3.3 | COGS/OpEx boundary | The one domain boundary where legitimate reclassification occurs frequently. Flag and require human confirmation. | |
| C.3.4 | Contra-account handling | Contra-accounts classified by their parent account's domain (accumulated depreciation is an Asset-domain account). | |

### C.4 COFA Unification

| ID | Capability | Description | Status |
|----|-----------|-------------|--------|
| C.4.1 | Dual CoA ingestion | Read both entities' charts of accounts from CSV/Excel upload or connected ERP. | |
| C.4.2 | Account mapping by economic substance | Map source accounts to a unified structure based on what accounts mean, with confidence scoring. | |
| C.4.3 | Completeness validation | Every GL account in both entities maps to a unified line. Orphan detection. | |
| C.4.4 | Domain constraint compliance | Every proposed mapping validated against domain boundary rules before acceptance. | |

**COFA Unification Outputs:**

| ID | Output | Description | Status |
|----|--------|-------------|--------|
| C.4.5 | Unified account structure | The merged chart of accounts. Every account from both entities mapped to a unified line. | |
| C.4.6 | COFA mapping table | Source account → unified account, with confidence score, mapping basis, entity of origin. | |
| C.4.7 | Conflict register | Every conflict typed: recognition timing, measurement basis, classification, scope. Severity rating. Estimated dollar impact. Resolution status. Link to evidence. | |
| C.4.8 | Policy difference flags | Recognition rule differences identified with type and estimated impact. Full resolution requires Policy Reconciliation Engine. | |

## D. Combining Financial Statements (Proforma)

| ID | Capability | Description | Status |
|----|-----------|-------------|--------|
| D.1 | Accounting identity gates | Trial balance nets to zero pre and post mapping. Combined revenue = sum of standalones. Balance sheet identity (A = L + E) for each entity and combined. | |

**Proforma Outputs:**

| ID | Output | Description | Status |
|----|--------|-------------|--------|
| D.2 | Combining P&L | Four columns: Entity A │ Entity B │ Adjustments │ Combined. Revenue lines (consulting, managed services, reimbursables). COGS (direct labor, subcontractors, delivery). OpEx (S&M, G&A, R&D). Through EBITDA. Every adjustment links to a conflict register entry. | |
| D.3 | Combining Balance Sheet | Four-column format. Current assets, fixed assets, intangibles, current liabilities, long-term debt, equity. Intercompany eliminations in adjustments column. | |
| D.4 | Combining Statement of Cash Flows | Four-column format. Operating, investing, financing activities. | |
| D.5 | Revenue variance bridge | FY prior year → FY current year waterfall. Minimum three driver bars: new logo revenue, expansion revenue, renewal revenue. Churn as negative bar. | |
| D.6 | Single-entity financial statements | Full P&L, BS, SOCF per entity with drill-through to GL detail. | |
| D.7 | Revenue drill-through | Click any revenue line → underlying accounts, source system, confidence. Both entities. | |

## E. Overlap and Concentration Analysis

**Customer Overlap:**

| ID | Output | Description | Status |
|----|--------|-------------|--------|
| E.1.1 | Overlap identification | Customers served by both entities. Named accounts. Match confidence score. | |
| E.1.2 | Revenue concentration | Top customers by combined spend. Concentration risk (% of total revenue from top 5 / 10 / 20). | |
| E.1.3 | Relationship comparison | Same customer, different financials: contract value vs. recognized revenue vs. booked revenue per entity. | |

**Vendor Overlap:**

| ID | Output | Description | Status |
|----|--------|-------------|--------|
| E.2.1 | Vendor overlap identification | Vendors used by both entities. Spend by vendor per entity. | |
| E.2.2 | Consolidation opportunity | Overlapping vendors where combined spend creates leverage. Estimated savings. | |

**People Overlap:**

| ID | Output | Description | Status |
|----|--------|-------------|--------|
| E.3.1 | Corporate function overlap | Headcount by function (Finance, HR, IT, Legal, etc.) per entity. Overlap zone identification. | |
| E.3.2 | Deal model vs. actual comparison | Deal model assumed X headcount. Actual is Y. Variance by function. | |
| E.3.3 | Retention risk flags | Key personnel in overlapping functions with institutional knowledge risk. | |
| E.3.4 | Bench analysis | Bench headcount by segment, bench rate, bench cost, cross-deployment opportunity between entities. | |

**IT Landscape:**

| ID | Output | Description | Status |
|----|--------|-------------|--------|
| E.4.1 | System inventory comparison | Side-by-side: what systems each entity runs, by category. Overlapping platforms identified. | |
| E.4.2 | Integration complexity assessment | Systems requiring integration, consolidation, or decommissioning post-close. | |

## F. Cross-Sell Pipeline

| ID | Output | Description | Status |
|----|--------|-------------|--------|
| F.1 | Bidirectional pipeline | Acquirer services → target clients AND target services → acquirer clients. Both directions scored independently. | |
| F.2 | Named account list | Specific client names with firmographic data. | |
| F.3 | Propensity scoring | Per-candidate score: industry match, size fit, product mix gaps, expansion history, contract status. | |
| F.4 | ACV estimates | Estimated annual contract value per candidate based on comparable engagements in the selling entity. | |
| F.5 | Pipeline summary | Total addressable cross-sell ACV. High / medium / low confidence breakdown. Count by direction. | |
| F.6 | Practice area filtering | Filter candidates by what you're selling (consulting practice area, BPM service line). | |
| F.7 | Deal model comparison | Deal model assumed $Xm in cross-sell synergies. Engine supports Y% with high confidence. Gap analysis. | |

## G. EBITDA Bridge

| ID | Output | Description | Status |
|----|--------|-------------|--------|
| G.1 | Reported EBITDA | Starting point: each entity's reported EBITDA from their own financials. | |
| G.2 | Normalization adjustments | Each adjustment: category, description, amount, confidence grade (high / medium / low), supporting data reference, entity of origin. Categories: above-market compensation, one-time items, related-party transactions, non-recurring revenue, run-rate corrections. | |
| G.3 | Combined normalized EBITDA | Sum of both entities' normalized EBITDA. | |
| G.4 | Synergy adjustments | Revenue synergies (from cross-sell capture at assumed rate). Cost synergies (corporate HC reduction, vendor consolidation, facility rationalization, IT consolidation). Each with estimated dollar impact and confidence. | |
| G.5 | Pro forma EBITDA | Combined normalized + synergies. | |
| G.6 | Adjustment lifecycle tracking | Each adjustment carries status: identified → validated → ongoing → resolved. Updated each reporting period. | |
| G.7 | Confidence grades on every line | High: deterministic or verified. Medium: estimated from available data. Low: assumed or extrapolated. Compound confidence decomposed into components. | |
| G.8 | Supporting data references | Every adjustment links to source data (e.g., above-market exec comp → comp data for named individuals vs. benchmark). | |

## H. QofE (Quality of Earnings)

| ID | Output | Description | Status |
|----|--------|-------------|--------|
| H.1 | Current period QofE | EBITDA bridge applied to current quarter/year. Each adjustment: current value, diligence value, prior quarter value. Green / yellow / red status. | |
| H.2 | Revenue quality metrics | Customer concentration trending. Contract renewal rates. Cohort retention. New logo vs. expansion vs. renewal mix. Recurring vs. non-recurring split. | |
| H.3 | Earnings sustainability score | Percentage of EBITDA from recurring/sustainable sources vs. adjustments and one-time items. Trended over time. | |
| H.4 | Working capital quality | DSO trending (AR growth vs. revenue growth). DPO trending. Inventory days if applicable. Working capital as % of revenue trended. | |
| H.5 | QofE trend over time | EBITDA bridge trended over quarters. Synergy materialization tracking. New adjustment emergence. | |
| H.6 | Adjustment migration tracking | Adjustments flagged at diligence: improving, stable, or worsening quarter over quarter. | |

## I. What-If / Sensitivity Engine

| ID | Output | Description | Status |
|----|--------|-------------|--------|
| I.1 | Preset scenarios | Named scenarios (Conservative, Base Case, Aggressive) with predefined parameter positions. One-click load. | |
| I.2 | User-driven sliders | Adjustable parameters: cross-sell capture rate, bench cross-deployment rate, utilization, offshore mix, corporate HC reduction %, vendor consolidation savings, billing rate adjustment. | |
| I.3 | Real-time financial update | Moving a slider immediately recalculates combining P&L, EBITDA bridge, and implied enterprise value. | |
| I.4 | Scenario comparison | Side-by-side of two or more scenarios with delta on key metrics. | |
| I.5 | Sensitivity attribution | Which lever contributes most to EV change. Parameter sensitivity ranking. | |

## J. Executive Dashboards

| ID | Output | Description | Status |
|----|--------|-------------|--------|
| J.1 | CFO dashboard | Financial summary, EBITDA bridge, QofE status, working capital, key risks. | |
| J.2 | CRO dashboard | Cross-sell pipeline, revenue concentration, customer overlap, ACV opportunity. | |
| J.3 | COO dashboard | People overlap, bench analysis, delivery capacity, IT landscape, facility footprint. | |
| J.4 | CTO dashboard | System inventory, integration complexity, technology stack comparison, shared platforms. | |
| J.5 | CHRO dashboard | Headcount by function, retention risks, comp comparison, org structure overlap, bench deployment. | |

## K. Natural Language Query

| ID | Capability | Description | Status |
|----|-----------|-------------|--------|
| K.1 | Single-entity queries | "What's our revenue?" against one entity's data. | |
| K.2 | Dual-entity queries | "What's combined revenue?" or "Compare revenue across both entities." Entity-scoped routing. | |
| K.3 | Persona detection | Infer executive role from query content. Filter response to relevant metrics and context. | |
| K.4 | Confidence surfacing | Every answer shows confidence tier and source system. | |
| K.5 | Ambiguity resolution | Ambiguous queries trigger clarification rather than guessing. | |

## L. Mai

### L.1 Core Capabilities

| ID | Capability | Description | Status |
|----|-----------|-------------|--------|
| L.1.1 | Context pre-loading | On engagement start, Mai consumes full discovery, connection mapping, and semantic layer state. She enters the first conversation with full company context. | |
| L.1.2 | Constitution | Identity, behavioral constraints, domain playbooks, workflow definitions, quality gates, escalation criteria. | |
| L.1.3 | Query as tool | Mai decides what to query, interprets results in business context, communicates findings. NLQ handles translation and execution. | |
| L.1.4 | Portal navigation | When Mai references a report or data point, the portal navigates to the relevant view. | |
| L.1.5 | Exact figures | Mai's cited numbers match engine outputs. Missing data reported as missing. | |

### L.2 Integration Chain Execution

| ID | Capability | Description | Status |
|----|-----------|-------------|--------|
| L.2.1 | COFA unification reasoning | Reads both CoAs, reasons about economic substance, builds mapping table. Reads accounting policy documents for treatment differences. | |
| L.2.2 | Conflict identification and typing | Flags conflicts with type (recognition timing, measurement basis, classification, scope), severity, estimated dollar impact. | |
| L.2.3 | Resolution workspace adjudication | Consumes entity resolution workspaces, applies business context, produces resolution decisions. Escalates when confidence is insufficient. | |
| L.2.4 | Human escalation | Structured recommendation with evidence bundle, business implications, and clear action for the human. | |

### L.3 Engagement Lifecycle

| ID | Capability | Description | Status |
|----|-----------|-------------|--------|
| L.3.1 | Scoping conversation | Which reconciliation objects are in scope. Entity profiles. System access. Stakeholder mapping. Timeline. | |
| L.3.2 | Execution status tracking | Status per reconciliation object and per conflict. Milestone notifications. | |
| L.3.3 | Deliverable generation | "Generate the combining P&L" triggers report generation from engines. | |
| L.3.4 | Engagement state persistence | Configuration, status, stakeholder assignments, decision history. Survives across sessions. | |

### L.4 Run Ledger and Execution Contract

| ID | Capability | Description | Status |
|----|-----------|-------------|--------|
| L.4.1 | Step status model | Every engagement step: pending → running → complete → failed → stale. Logged in run ledger. | |
| L.4.2 | Idempotency | Every step carries an idempotency key with attempt counter. Accidental retries return existing result. Deliberate re-runs increment counter for fresh inference. | |
| L.4.3 | Retry rules | Read-only steps re-run freely. Steps with side effects require rollback before retry. | |
| L.4.4 | Downstream invalidation | Upstream re-run or override marks downstream steps stale. Operator authorizes cascade re-runs to protect localized human adjustments. | |
| L.4.5 | Run ledger persistence | Every step emits: engagement_id, step_name, status, idempotency_key, inputs_hash, model_version, constitution_version, output, validation_result, human_override, upstream_dependencies, timestamp. | |

### L.5 Human Review

| ID | Capability | Description | Status |
|----|-----------|-------------|--------|
| L.5.1 | Auto-approve with log | Mai decides. Decision logged with full provenance. (Deterministic key matches, high-confidence same-domain mappings.) | |
| L.5.2 | Auto-approve with noted risk | Mai decides. Risk flag visible in review surfaces. (High-confidence fuzzy matches with agreeing evidence.) | |
| L.5.3 | Human confirmation required | Mai prepares structured recommendation. Human approves, modifies, or rejects. (Medium-confidence, conflicting signals, domain boundary classifications.) | |
| L.5.4 | Human decision required | Mai surfaces evidence and options. Human makes the call. (Low-confidence, novel patterns, material impact above threshold.) | |
| L.5.5 | Confidence decomposition | Compound confidence broken into components (field mapping, entity resolution, source data quality). Each component routed to review independently. | |

## M. Report Portal

| ID | Capability | Description | Status |
|----|-----------|-------------|--------|
| M.1 | Entity selector | View toggle across all reports: Entity A, Entity B, Combined. | |
| M.2 | Combining P&L view | Four-column format with drill-through. | |
| M.3 | Combining Balance Sheet view | Four-column format. | |
| M.4 | Combining Cash Flow view | Four-column format. | |
| M.5 | Revenue waterfall | Bridge chart with driver bars. | |
| M.6 | Overlap views | Customer, Vendor, People — named accounts, match confidence, spend comparison. | |
| M.7 | COFA mapping table | Source → unified mapping with confidence. | |
| M.8 | Conflict register | Typed conflicts with severity, dollar impact, resolution status. | |
| M.9 | Cross-sell pipeline view | Pipeline table with sort/filter, summary cards, both directions. | |
| M.10 | EBITDA bridge view | Bridge with expandable adjustment details, confidence grades, supporting references. | |
| M.11 | What-if view | Slider panel with real-time P&L / EBITDA / EV update. Preset scenario buttons. | |
| M.12 | Executive dashboards | Persona selector (CFO / CRO / COO / CTO / CHRO) with five dashboard views. | |
| M.13 | Mai chat interface | Always available. Scripted narration and live Q&A. | |
| M.14 | QofE view | Current period QofE, adjustment lifecycle, revenue quality, sustainability score. | |
| M.15 | IT landscape view | System inventory comparison across entities. | |

---

## Summary

| Section | Capabilities |
|---------|-------------|
| A. Discovery | 5 |
| B. Connection Mapping | 4 |
| C. Semantic Layer | 24 |
| D. Combining FS (Proforma) | 7 |
| E. Overlap / Concentration | 11 |
| F. Cross-Sell Pipeline | 7 |
| G. EBITDA Bridge | 8 |
| H. QofE | 6 |
| I. What-If / Sensitivity | 5 |
| J. Executive Dashboards | 5 |
| K. Natural Language Query | 5 |
| L. Mai | 23 |
| M. Report Portal | 15 |
| **Total** | **125** |
