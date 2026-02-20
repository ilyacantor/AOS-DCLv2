# AOS Data Chain — Landscape Infographic Prompt

## Prompt 1: Operator UX Motion (What You Do, What Happens)

Infographic showing the AutonomOS enterprise data chain from discovery to natural language query. Dark background, flat vector, left-to-right flow across 5 stages.

### Stage 1 — AOD: "Discover"

AOD scans the enterprise observation plane — CMDB records, network traffic logs, IdP/OAuth tokens, finance contracts — and builds an asset catalog. Each discovered system (Salesforce, NetSuite, Workday, Snowflake, etc.) becomes an asset record with classification metadata: vendor, domain, governance status (sanctioned/shadow/zombie), data domain, and System of Record score. AOD detects integration fabric — which iPaaS (Workato, MuleSoft) or API gateway (Kong, Apigee) connects what — and generates evidence leads (connection hints) from observation plane signals. AOD packages each asset as a `ConnectionCandidate` and hands it off to AAM.

Visual: A radar/scanner icon sweeping across enterprise system logos. Output arrow labeled "ConnectionCandidates" with a small card showing: `{vendor: "salesforce", domain: "crm", governance: "sanctioned", fabric_hint: "workato"}`. Caption: "Asset catalog — what exists, where it lives, how it connects."

### Stage 2 — AAM: "Connect & Map"

AAM receives ConnectionCandidates from AOD and does three things in sequence:

**Infer.** For each candidate, AAM infers pipe definitions: entity scope (what data the pipe carries — CRM, ERP, billing), modality (API, event stream, table, file), transport kind (REST, JDBC, webhook), fabric plane (which iPaaS/gateway routes it), identity keys, change semantics, and schema hash. Each pipe gets a UUID `pipe_id`. AAM also runs adapter resolution — determining whether to connect via API Gateway adapter (Kong, Apigee), iPaaS adapter (Workato, MuleSoft), Event Bus adapter (Kafka, EventBridge), or Warehouse adapter (Snowflake, BigQuery). AAM stores all DeclaredPipes in its Pipe Registry.

**Export.** AAM pushes pipe schema blueprints to DCL via `POST /api/dcl/export-pipes`. This registers each UUID `pipe_id` with its schema in DCL's PipeDefinitionStore. This is the Structure Path — DCL now knows what data shapes to expect.

**Dispatch.** AAM constructs a `JobManifest` per pipe and dispatches to Farm via `POST /api/farm/manifest-intake`. Each manifest contains: `source.pipe_id` (UUID), `source.system`, `source.category` (crm, erp, billing, hr, support, devops, observability, infrastructure), `target.dcl_url`, `target.snapshot_name`, `target.tenant_id`, and `run_id`. This is the Content Path trigger.

Visual: Three horizontal swim lanes inside AAM's box — "Infer" (brain icon with pipe definitions fanning out), "Export" (arrow to DCL labeled "Pipe Schemas + UUIDs"), "Dispatch" (arrow to Farm labeled "JobManifests"). Pipe Registry shown as a database icon with UUID keys. Caption: "Connect, map, export structure, dispatch content requests."

### Stage 3 — Farm: "Generate & Push" (Automatic — No Operator Action)

Farm receives JobManifests from AAM. For each manifest, Farm routes to the appropriate source system generator based on `source.category`:

- `crm` → Salesforce archetype (opportunities, accounts, users)
- `erp` → NetSuite archetype (invoices, revenue schedules, GL entries, AR/AP)
- `billing` → Chargebee archetype (subscriptions, invoices, MRR)
- `hr` → Workday archetype (workers, positions, time-off)
- `support` → Zendesk archetype (tickets, organizations)
- `devops` → Jira archetype (issues, sprints)
- `observability` → Datadog archetype (SLOs, incidents)
- `infrastructure` → AWS Cost archetype (line items)

Each generator produces source-system-shaped data (field names match real system conventions — Salesforce PascalCase with `__c` suffixes, NetSuite `snake_case` with `internal_id`, etc.). Farm computes ground truth from the generated data — per-quarter breakdowns of revenue, CAGR, customer counts, churn, expected cross-system conflicts with root causes.

Farm pushes each payload to DCL via `POST /api/dcl/ingest` with headers `x-pipe-id` (the manifest's UUID — never Farm's internal slug names) and `x-run-id` (batch correlation). Ground truth is included in the push.

Full provenance on every push: `run_id` (from AAM), `farm_run_id` (Farm's execution ID), `dcl_run_id` (batch correlation for DCL), `pipe_id` (the join key).

Visual: A factory icon receiving manifest cards on the left, 8 generator modules in the center (each with a source system logo), and outbound arrows on the right labeled with UUID pipe_ids pointing to DCL. A "Ground Truth" badge attached to the outbound arrows. Red text: "No operator buttons. Fully automatic." Caption: "Generate source-shaped data, compute ground truth, push to DCL."

### Stage 4 — DCL: "Validate, Normalize, Map, Publish"

DCL receives two inputs from the chain: pipe schemas from AAM's export (Structure Path) and data rows from Farm's push (Content Path). These meet at the Ingest Guard.

**Validate (Schema-on-Write).** DCL's ingest guard checks every incoming payload's `x-pipe-id` against the PipeDefinitionStore. If the UUID matches a registered schema → 200, accepted. If no match → 422 `NO_MATCHING_PIPE`, rejected. This is the late-binding join — structure and content meet on `pipe_id`.

**Normalize (Source Resolution).** Raw source identifiers (sfdc, ns, d365, wd, chargebee, workday-hcm) resolve to canonical names through 4-level resolution: exact match → alias → pattern → fuzzy. This collapses vendor naming chaos into a clean registry.

**Map (Semantic Mapping).** Field names from each source system (Salesforce `Amount`, NetSuite `amount`, Chargebee `mrr`, Workday `headcount`) map to 16 ontology concepts organized in 5 clusters:

- Finance: revenue, cost, invoice, subscription, currency
- Growth: account, opportunity
- Infra: aws_resource, incident
- Ops: health, usage, ticket, engineering_work, vendor, date
- People: employee

Two mapping modes: Heuristic (~1s, pattern matching) and AI-Powered (~5s, LLM + Pinecone RAG).

**Publish (Semantic Catalog).** DCL builds the full semantic catalog: 37 metrics, 29 dimensions, 5 persona packs (CFO, CRO, COO, CTO, CHRO), 13 bindings. Published via Semantic Export API and visualized as a 4-layer Sankey: L0 Pipeline → L1 Sources → L2 Ontology → L3 Personas. Ground truth from Farm is accessible for verification.

Visual: Large rounded box labeled "DCL Engine" taking ~40% of the image width. Four sequential stages left-to-right inside. JOIN key icon at Validate, funnel at Normalize, colored field-to-concept arrows at Map, Sankey miniature + catalog card at Publish. Output arrow to NLQ. Caption: "Schema-on-Write → Source Resolution → Semantic Mapping → Queryable Catalog."

### Stage 5 — NLQ: "Ask"

NLQ consumes DCL's semantic catalog and exposes a natural language interface. The query engine classifies intent (POINT, COMPARISON, TREND, AGGREGATION, BREAKDOWN), extracts metrics and dimensions with synonym normalization (100+ synonyms → canonical names), resolves temporal references ("last year", "Q3", "trailing 12 months"), and executes against DCL's `POST /api/dcl/query` endpoint.

Query cache (Pinecone vector similarity) avoids redundant LLM calls. Two modes: Static (fast, pattern-based pre-parsing) and AI (Claude API, learns from successful parses). Responses include confidence scores, grounding citations, and visualization (line, bar, area, pie, waterfall, galaxy, dashboards).

NLQ never touches Farm, AAM, or AOD. It only talks to DCL.

Visual: Chat bubble icon with a question "What was Q3 revenue by region?" and an answer card showing "$22.4M" with a breakdown bar chart, confidence badge, and a citation link back to DCL's semantic catalog. Caption: "Natural language → structured query → grounded answer."

### Cross-Chain Elements (Bottom Strip)

**Provenance ribbon** running across the bottom of all 5 stages showing the four correlation keys threading through: `pipe_id` (UUID, the join key — highlighted), `run_id` (AAM's batch), `farm_run_id`, `dcl_run_id`.

**Error callouts** at each boundary:
- AOD → AAM: "Missing classification? AAM triages as deferred."
- AAM → DCL Export: "Schema rejected? DCL returns structured error."
- AAM → Farm Dispatch: "Missing source.category? Farm returns 422 NO_GENERATOR_ROUTE."
- Farm → DCL Ingest: "UUID not registered? DCL returns 422 NO_MATCHING_PIPE. Never retry — config error."
- DCL → NLQ: "Metric not mapped? NLQ returns ambiguity prompt, never raw errors."

**Operator action indicator:** Green "action" badge on AOD (trigger scan) and AAM (trigger pipeline). Grey "read-only" badge on Farm and DCL. Blue "interact" badge on NLQ (ask questions).

---

## Prompt 2: Technical Data Flow (What Crosses Each Boundary)

Infographic showing the AutonomOS data chain as a technical flow diagram. Dark background, flat vector, left-to-right. Focus on API contracts, data shapes, and join keys at each module boundary.

### Left Edge — AOD Output

AOD produces `ConnectionCandidate` objects:

```
{
  vendor: "salesforce",
  display_name: "Salesforce CRM",
  domain: "crm",
  governance_status: "sanctioned",
  sor_score: 0.92,
  fabric_hints: [
    { plane: "workato", evidence: "oauth_token", confidence: 0.87 }
  ],
  cmdb_refs: ["CMDB-4821"],
  idp_signals: ["okta_app_id: 0oa3x..."]
}
```

Arrow labeled `POST /api/aam/candidates` to AAM. Caption: "What exists — raw asset records with connection evidence."

### AAM Processing (3 Internal Stages)

**Stage A — Pipe Inference.** Each candidate becomes one or more `DeclaredPipe`:

```
{
  pipe_id: "a1b2c3d4-e5f6-...",       // UUID assigned by AAM
  source_system: "salesforce",
  category: "crm",
  entity_scope: ["opportunities", "accounts", "users"],
  modality: "API",
  transport_kind: "REST",
  fabric_plane: "workato",
  identity_keys: ["AccountId", "OwnerId"],
  change_semantics: "full_refresh",
  schema_hash: "sha256:9f8e7d..."
}
```

Stored in AAM's Pipe Registry. Schema hash enables drift detection.

**Stage B — Export to DCL.** `POST /api/dcl/export-pipes` with array of DeclaredPipes:

```
Headers: x-run-id: {aam_export_run_id}
Body: {
  pipes: [ ...DeclaredPipe objects... ],
  connections: [
    { system: "salesforce", category: "crm", pipe_ids: ["a1b2c3d4-..."] }
  ],
  snapshot_name: "TenantX-2026Q1"
}
```

DCL registers each `pipe_id` + schema in `PipeDefinitionStore`. This activates the Ingest Guard.

**Stage C — Dispatch to Farm.** `POST /api/farm/manifest-intake` (single) or `/batch` (multiple):

```
{
  source: {
    pipe_id: "a1b2c3d4-...",         // Same UUID exported to DCL
    system: "salesforce",
    category: "crm"                   // Routes Farm's generator
  },
  target: {
    dcl_url: "https://dcl.example.com",
    snapshot_name: "TenantX-2026Q1",
    tenant_id: "tenant_xyz"
  },
  run_id: "aam_run_20260219_142033"
}
```

Critical contract: `source.pipe_id` in the manifest MUST be identical to the `pipe_id` exported to DCL. This is the join key that makes late-binding work.

### Farm Processing

Farm routes by `source.category` to generator archetype. Generator produces source-shaped records:

Salesforce example (CRM generator):
```
[
  { "Id": "006...", "Amount": 125000, "StageName": "Closed Won", "CloseDate": "2025-09-15", "Region__c": "NA-West" },
  { "Id": "006...", "Amount": 89000, "StageName": "Negotiation", "CloseDate": "2025-11-30", "Region__c": "EMEA" }
]
```

NetSuite example (ERP generator):
```
[
  { "internal_id": "INV-4821", "tran_date": "2025-09-30", "amount": 125000, "posting_period": "2025-Q3", "department": "Sales" },
  { "internal_id": "INV-4822", "tran_date": "2025-10-15", "amount": 89000, "posting_period": "2025-Q4", "department": "Sales" }
]
```

Ground truth computed from generated data:
```
{
  "quarter": "2025-Q3",
  "revenue": { "primary_source": "netsuite", "value": 22400000, "corroborating": { "salesforce": 22800000 } },
  "expected_conflict": { "metric": "revenue", "delta": 400000, "root_cause": "timing_cutoff" }
}
```

Farm pushes to DCL:
```
POST /api/dcl/ingest
Headers:
  x-pipe-id: a1b2c3d4-...            // FROM manifest — never Farm's slug
  x-run-id: 457543e9-...             // dcl_run_id generated by Farm
Body: {
  meta: { source_system: "salesforce", pipe_id: "a1b2c3d4-...", run_id: "farm_run_...", snapshot_name: "TenantX-2026Q1" },
  data: [ ...source-shaped records... ],
  ground_truth: { ... }
}
```

### DCL Processing (4 Internal Stages)

**Validate.** Ingest Guard checks `x-pipe-id` against `PipeDefinitionStore`:
- Match → 200, proceed. Response includes `matched_schema` and `schema_fields`.
- No match → 422, rejected. `{ "error": "NO_MATCHING_PIPE", "pipe_id": "...", "registered_pipes": [...] }`
- Store empty → Guard inactive, accept with warning (backward compat).

**Normalize.** Source identifiers resolve through 4 levels:
- Exact: `salesforce` → `Salesforce CRM`
- Alias: `sfdc` → `Salesforce CRM`
- Pattern: `sf-crm-*` → `Salesforce CRM`
- Fuzzy: `sales_force_prod` → `Salesforce CRM` (confidence-scored)

**Map.** Field-to-ontology mapping:
```
salesforce.Amount        → finance.revenue
netsuite.amount          → finance.revenue
chargebee.mrr            → finance.subscription
workday.headcount        → people.employee
jira.story_points        → ops.engineering_work
datadog.slo_target       → ops.health
aws.unblended_cost       → finance.cost
```

16 ontology concepts across 5 clusters. Heuristic mode (~1s) for dev, AI mode (~5s, LLM + Pinecone RAG) for production.

**Publish.** Semantic Catalog:
- 37 metrics (revenue, ARR, churn, pipeline, headcount, CSAT, velocity, SLO attainment, cloud spend...)
- 29 dimensions (region, segment, department, quarter, product, stage, tier, currency...)
- 5 persona packs: CFO (financial metrics), CRO (growth/pipeline), COO (ops/support), CTO (infra/engineering), CHRO (people)
- 13 metric-dimension bindings (which dimensions apply to which metrics)

Exposed via `POST /api/dcl/query` and Semantic Export API.

4-layer Sankey: L0 Pipeline → L1 Sources (Salesforce, NetSuite, Chargebee...) → L2 Ontology (37 metrics) → L3 Personas (CFO, CRO, COO, CTO, CHRO).

### Right Edge — NLQ Consumption

```
POST /api/dcl/query
{
  "question": "What was Q3 revenue by region?",
  "snapshot_name": "TenantX-2026Q1"
}
```

NLQ processing pipeline:
1. Pattern pre-parse (regex for dashboard/viz triggers)
2. Intent classification: POINT | COMPARISON | TREND | AGGREGATION | BREAKDOWN
3. Metric extraction + synonym normalization (100+ synonyms → canonical)
4. Temporal resolution ("Q3" → "2025-Q3")
5. Query cache lookup (Pinecone, EXACT/HIGH/MEDIUM thresholds)
6. If cache miss → LLM parse (Claude API) → structured query → DCL execution
7. Answer assembly: value + unit + period + confidence score + grounding citation
8. Visualization: chart type auto-selected from intent (TREND → line, BREAKDOWN → bar, COMPARISON → grouped bar)

Response:
```
{
  "answer": "$22.4M",
  "breakdown": { "NA-West": 9.2, "NA-East": 6.1, "EMEA": 4.8, "APAC": 2.3 },
  "confidence": 0.94,
  "source": "netsuite (primary), salesforce (corroborating)",
  "period": "2025-Q3",
  "ground_truth_match": true
}
```

### Bottom Strip — Join Key Thread

```
pipe_id: a1b2c3d4-e5f6-...

  AAM assigns it ──► AAM exports it to DCL ──► AAM dispatches it to Farm ──► Farm pushes with it ──► DCL validates it ──► NLQ queries through it

  One UUID. Five modules. Zero ambiguity.
```

### Error Boundary Matrix

| Boundary | Error | HTTP | Meaning | Fix |
|----------|-------|------|---------|-----|
| AOD → AAM | Missing vendor/domain | — | Incomplete candidate | AOD classification gap |
| AAM → DCL | Schema export rejected | 4xx | Malformed pipe definition | Fix AAM inference |
| AAM → Farm | `NO_GENERATOR_ROUTE` | 422 | category missing or unrecognized | AAM must send valid category |
| Farm → DCL | `NO_MATCHING_PIPE` | 422 | pipe_id not in PipeDefinitionStore | Re-run AAM export before Farm push |
| Farm → DCL | `connection_error` | — | target.dcl_url wrong or DCL down | Check manifest dcl_url |
| Farm → DCL | `schema_drift` | 200 | Data shape changed from blueprint | Review generator vs registered schema |
| DCL → NLQ | Unmapped metric | — | Field not in ontology | Extend semantic catalog |
| NLQ → User | Ambiguity | — | Multiple metric matches | NLQ returns clarification prompt |
