# DCL Schema Contract

**Any breaking change to these schemas requires coordination with the convergence repo before merge.**

Additive changes (new columns with defaults, new indexes) are non-breaking.
Column renames, type changes, constraint changes, or column removals are breaking.

---

## `semantic_triples`

Owner: DCL. Convergence reads via SELECT only.

| Column | Type | Nullable | Default | Constraint |
|--------|------|----------|---------|------------|
| `id` | UUID | NOT NULL | `gen_random_uuid()` | PRIMARY KEY |
| `tenant_id` | UUID | NOT NULL | — | — |
| `entity_id` | TEXT | NOT NULL | — | — |
| `concept` | TEXT | NOT NULL | — | — |
| `property` | TEXT | NOT NULL | — | — |
| `value` | JSONB | NOT NULL | — | — |
| `period` | TEXT | NULL | — | — |
| `currency` | TEXT | NULL | `'USD'` | — |
| `unit` | TEXT | NULL | — | — |
| `source_system` | TEXT | NOT NULL | — | — |
| `source_table` | TEXT | NULL | — | — |
| `source_field` | TEXT | NULL | — | — |
| `pipe_id` | UUID | NULL | — | — |
| `run_id` | UUID | NOT NULL | — | — |
| `confidence_score` | NUMERIC(3,2) | NOT NULL | — | `>= 0 AND <= 1` |
| `confidence_tier` | TEXT | NOT NULL | — | `IN ('exact','high','medium','low')` |
| `canonical_id` | UUID | NULL | — | — |
| `resolution_method` | TEXT | NULL | — | `IN ('deterministic','fuzzy','manual') OR NULL` |
| `resolution_confidence` | NUMERIC(3,2) | NULL | — | `>= 0 AND <= 1 OR NULL` |
| `created_at` | TIMESTAMPTZ | NULL | `now()` | — |
| `updated_at` | TIMESTAMPTZ | NULL | `now()` | — |
| `is_active` | BOOLEAN | NULL | `true` | — |
| `source_run_tag` | TEXT | NULL | — | — (added in migration 004) |

### Indexes

| Name | Columns / Expression | Condition |
|------|---------------------|-----------|
| `idx_triples_entity_concept` | `(tenant_id, entity_id, concept)` | — |
| `idx_triples_concept_period` | `(tenant_id, concept, period)` | — |
| `idx_triples_run` | `(run_id)` | — |
| `idx_triples_canonical` | `(canonical_id)` | `WHERE canonical_id IS NOT NULL` |
| `idx_triples_entity_period` | `(tenant_id, entity_id, period)` | — |
| `idx_triples_active` | `(tenant_id, is_active)` | `WHERE is_active = true` |
| `idx_triples_tenant_run` | `(tenant_id, run_id)` | — |
| `idx_triples_source_run_tag` | `(source_run_tag)` | `WHERE source_run_tag IS NOT NULL` |
| `idx_triples_concept_domain` | `(split_part(concept, '.', 1), entity_id)` | `WHERE is_active = true` |
| `idx_triples_canonical_entity` | `(canonical_id, entity_id)` | `WHERE canonical_id IS NOT NULL AND is_active = true` |

---

## `dimension_values_v2`

Owner: DCL. Convergence reads via SELECT only.

| Column | Type | Nullable | Default | Constraint |
|--------|------|----------|---------|------------|
| `id` | UUID | NOT NULL | `gen_random_uuid()` | PRIMARY KEY |
| `tenant_id` | UUID | NOT NULL | — | — |
| `entity_id` | TEXT | NOT NULL | — | — |
| `dimension` | TEXT | NOT NULL | — | — |
| `value` | TEXT | NOT NULL | — | — |
| `parent_id` | UUID | NULL | — | FK → `dimension_values_v2(id)` |
| `depth` | INT | NULL | `0` | — |
| `path` | TEXT | NULL | — | — |
| `run_id` | UUID | NOT NULL | — | — |
| `created_at` | TIMESTAMPTZ | NULL | `now()` | — |

### Indexes

| Name | Columns | Condition |
|------|---------|-----------|
| `idx_dimval_v2_tenant_dim` | `(tenant_id, entity_id, dimension)` | — |
| `idx_dimval_v2_parent` | `(parent_id)` | `WHERE parent_id IS NOT NULL` |

---

## `tenant_runs`

Owner: DCL. Convergence reads via SELECT only.

| Column | Type | Nullable | Default | Constraint |
|--------|------|----------|---------|------------|
| `tenant_id` | UUID | NOT NULL | — | PRIMARY KEY |
| `current_run_id` | UUID | NOT NULL | — | — |
| `previous_run_id` | UUID | NULL | — | — |
| `updated_at` | TIMESTAMPTZ | NOT NULL | `now()` | — |

---

## `tenant_registry`

Owner: DCL. Convergence does not read this table (uses entity_id from triples directly).

| Column | Type | Nullable | Default | Constraint |
|--------|------|----------|---------|------------|
| `entity_id` | TEXT | NOT NULL | — | PRIMARY KEY |
| `tenant_id` | UUID | NOT NULL | — | — |
| `entity_name` | TEXT | NOT NULL | — | — |
| `created_at` | TIMESTAMPTZ | NOT NULL | `now()` | — |

### Indexes

| Name | Columns | Condition |
|------|---------|-----------|
| `idx_tenant_registry_tenant_id` | `(tenant_id)` | — |

---

# Concept Name Registry (Canonical) — the `concept`/`property` contract

The column contract above governs the **table**. This registry governs the **values** of
`concept` + `property` for every metric a dashboard reads. It exists because the same
metric was being emitted under different concept names by different producers (the fabric
path wrote `cloud_spend.*`, the SE path wrote `infrastructure.cloud_spend.*`), so a tile
that passed on a fabric entity went dark on an SE entity. **One canonical name per metric,
everywhere** (I5). Every producer (Farm SE generators + DCL fabric aggregators) AND every
consumer (NLQ `metric_concept_map.yaml`) conforms to the canonical here. Adding a producer
or a consumer for a listed metric under any other name is a contract violation.

### Canonicalization direction rule (how the canonical is chosen)
1. If `dcl/config/ontology_concepts.yaml` declares a root for the concept, that root wins.
   (`cloud_spend` is a declared domain; `infrastructure.cloud_spend` is not — so `cloud_spend.*`
   is canonical and the SE producer conforms to it.)
2. Where the ontology declares neither candidate, the side with the **most aligned producers
   and consumers** wins, so the fewest things move and Convergence's SELECTs stay stable.
   (SE + Convergence + Farm tests all use `customer.count.total`; only the fabric aggregator
   used `customer.total` — so `customer.count.total` is canonical and the fabric aggregator
   conforms.)
3. **Never end in a state where a passing tile breaks.** Conform the producer and the consumer
   in the same change so the metric is never momentarily unresolvable.

### Convergence coordination
The Convergence repo reads `customer.%`, `service.%`, `revenue.total`, `cogs.total`, `opex.total`,
`pnl.ebitda`, `asset.total`, `liability.total`, `equity.total` via SELECT (its QofE, cross-sell,
overlap, and materialized-view engines). The canonical choices below were made to keep **SE emission
of those roots unchanged** — the customer family canonicalizes to the SE-side names, so the
Convergence repo is NOT affected by this reconciliation. Any FUTURE change to a canonical concept
the Convergence repo SELECTs requires coordination per the top of this file.

## Registry

Status: **LOCKED** = conformed + verified; **PENDING** = canonical decided, conform queued;
**NO-PRODUCER** = not name drift — nothing emits it (see register below).

**Progress (2026-06-04):** `cloud_spend` LOCKED (SE conform + re-ingest proof). The 14 SE-only
name-drifts LOCKED — NLQ re-pointed to the canonical (`churn_rate_pct`→`customer.gross_churn_rate`,
`logo_churn_pct`→`customer.logo_churn_rate`, `csat`→`support.csat`, `resolution_hours`/`first_response_hours`
→`support.*`, `tech_debt_pct`/`features_shipped`→`engineering.*`, `ltv_cac`→`customer.ltv_cac_ratio`,
`avg_deal_size`/`quota_attainment_pct`→`sales.*`, `new_logos`→`customer.count.new`,
`selling_expenses`/`g_and_a_expenses`→`opex.sales_marketing`/`opex.general_admin`,
`change_in_deferred_rev`→`…change_in_deferred_rev`) and VERIFIED resolving against SE entity
BlueFlow-I5BQ (ds=dcl_v2). PENDING: `customer_count`+`support_tickets` (coordinated fabric
conform — they have a fabric producer); then the NO-PRODUCER emitters (emit all 14).

### cloud_spend / FinOps  (canonical root `cloud_spend.*` — ontology CSP-001)
| NLQ metric | Canonical `concept` / `property` | Producers | Status |
|---|---|---|---|
| cloud_spend, cloud_spend_monthly_total, cloud_spend_trend | `cloud_spend.summary` / `total_cost` | SE (aws_cost_triples, operational_kpis) + fabric (cloud_spend_aggregator) | **LOCKED** |
| cloud_spend_pct_revenue | derived: `cloud_spend.summary.total_cost ÷ revenue.total.amount` | both | **LOCKED** (was dead `vendor.cloud_spend`) |
| cloud_underutilized_count | `cloud_spend.utilization` / `underutilized_count` | fabric only (SE emits no utilization) | **LOCKED** (fabric-only) |
| cloud_savings_opportunities_count | `cloud_spend.savings` / `opportunity_count` | fabric only | **LOCKED** (fabric-only) |
| cloud_savings_opportunities_amount | `cloud_savings_opportunities_amount.summary` / `amount` | fabric only | **LOCKED** (fabric-only) |
| cloud_spend_by_service | `cloud_spend_by_service.top_service` / `amount` | fabric only (SE has `cloud_spend.by_resource.*` — a different dimension) | **LOCKED** (fabric-only) |
| cloud_spend_by_team | `cloud_spend_by_team.top_team` / `amount` | fabric only | **LOCKED** (fabric-only) |

SE conform applied this session: `infrastructure.cloud_spend.total/amount` → `cloud_spend.summary/total_cost`;
`infrastructure.cloud_spend.pct_revenue/rate` → `cloud_spend.summary/pct_revenue`;
`infrastructure.cloud_spend.by_resource.<t>` → `cloud_spend.by_resource.<t>`. Authority key
`concept_authority.py` `infrastructure.cloud_spend` → `cloud_spend`. Note: SE writes per-period;
fabric writes atemporal (period=NULL). NLQ's period fallback resolves both.

### support  (canonical root `support.*` — ontology CS-012; resolves the `service.*` collision)
| NLQ metric | Canonical `concept` / `property` | Producers | Status |
|---|---|---|---|
| support_tickets | `support.tickets.total` / `count` | SE (operational_kpis) + fabric (records_summary_aggregator: conform `service.support_tickets`→`support.tickets.total`) | PENDING |
| resolution_hours | `support.resolution_time` / `hours` | SE | PENDING (NLQ re-point off `service.*`) |
| first_response_hours | `support.first_response_time` / `hours` | SE | PENDING (NLQ re-point) |

### engineering quality  (canonical root `engineering.*` — ontology PRD-011)
| NLQ metric | Canonical `concept` / `property` | Producers | Status |
|---|---|---|---|
| tech_debt_pct | `engineering.tech_debt_rate` / `rate` | SE | PENDING (NLQ re-point off `service.*`) |
| features_shipped | `engineering.features_shipped` / `count` | SE | PENDING (NLQ re-point) |

### customer / sales  (canonical = SE-side names — keeps Convergence stable)
| NLQ metric | Canonical `concept` / `property` | Producers | Status |
|---|---|---|---|
| customer_count | `customer.count.total` / `count` | SE + fabric (conform `customer.total`→`customer.count.total`) | PENDING |
| churn_rate_pct | `customer.gross_churn_rate` / `rate` | SE | PENDING (NLQ re-point) |
| logo_churn_pct | `customer.logo_churn_rate` / `rate` | SE | PENDING (NLQ re-point) |
| new_logos | `customer.count.new` / `count` | SE | PENDING (NLQ re-point) |
| csat | `support.csat` / `score` | SE | PENDING (NLQ re-point off `customer.*`) |
| avg_deal_size | `sales.avg_deal_size` / `amount` | SE | PENDING (NLQ re-point off `customer.*`) |
| quota_attainment_pct | `sales.quota_attainment` / `rate` | SE | PENDING (NLQ re-point off `customer.*`) |
| ltv_cac | `customer.ltv_cac_ratio` / `rate` | SE | PENDING (NLQ re-point off `service.*`) |

### opex / cash_flow  (canonical = SE-side names)
| NLQ metric | Canonical `concept` / `property` | Producers | Status |
|---|---|---|---|
| selling_expenses | `opex.sales_marketing` / `amount` | SE (financial_statements) | PENDING (NLQ re-point) |
| g_and_a_expenses | `opex.general_admin` / `amount` | SE | PENDING (NLQ re-point) |
| change_in_deferred_rev | `cash_flow.operating.change_in_deferred_rev` / `amount` | SE | PENDING (NLQ re-point `_revenue`→`_rev`) |

### Already aligned (no change) — eng/infra
`uptime_pct`→`uptime_pct.overall`, `deploys_per_week`→`deploy_frequency.quarterly`,
`p1_incidents`→`incident_count.p1`, `uptime_by_service`→`uptime_by_service.<svc>`,
`uptime_trend`→`uptime_trend.quarterly`, `sprint_velocity`→`sprint_velocity.team`,
`mttr_p1_hours`→`infrastructure.mttr.p1`. NLQ already points at the `engineering_metrics`
domain roots SE emits via `engineering_metrics_triples.py`. **ALIGNED.**

### The `service.*` collision — resolution
`service.<practice>` (strategy, finance_accounting, commercial, …) is the **service-catalog
dimension** (ontology OPS-014, emitted by `service_catalogs.py`), NOT a metric. NLQ was
overloading the same root for support/eng metrics. Resolution: pull every METRIC off `service.*`
(→ `support.*` / `engineering.*` / `customer.ltv_cac_ratio` above). After that `service.*` is
unambiguously the catalog dimension — collision gone, no change to `service_catalogs.py`.

## NO-PRODUCER register — NOT name drift; nothing emits these (decision required)
Reconciling names cannot make these populate. The value exists in Farm's ground-truth *oracle*
(`farm/src/generators/ground_truth.py`) for the first group but is **never written as a triple**;
the second group has no data anywhere. Options per metric: **(A) add an SE emitter** (canonical
root in brackets) or **(B) drop from `PERSONA_METRICS`** (nlq `visualization_intent.py`).

**DECISION (Ilya, 2026-06-04): EMIT ALL 14 (option A for every row).** The oracle-backed metrics
get an SE emitter that writes the existing ground-truth value as a triple; the four dataless
metrics (cac_payback, implementation_days, burn_multiple, + its `cash_flow.net_burn` and
`revenue.recurring` components) get a Farm financial-model field + emitter so every persona tile
resolves. None are dropped from `PERSONA_METRICS`; the 4 off-map names are added to NLQ's map at
the canonical concept below.

| Metric (persona) | Oracle data? | Canonical if emitted | Decision |
|---|---|---|---|
| security_vulns (CTO) | yes (`ground_truth.py`) | `engineering.security_vulns` / count | A or B |
| code_coverage_pct (CTO) | yes | `engineering.code_coverage` / rate | A or B |
| bug_escape_rate (CTO) | yes | `engineering.bug_escape_rate` / rate | A or B |
| critical_bugs / open_bugs (CTO) | yes (DCL concept is `critical_bugs`) | `engineering.critical_bugs` / count | A or B (alias open_bugs→critical_bugs) |
| reps_at_quota_pct (CRO) | yes | `sales.reps_at_quota` / rate | A or B |
| offer_acceptance_rate_pct (CHRO) | yes (DCL `pending`) | `workforce.offer_acceptance_rate` / rate | A or B |
| time_to_fill (CHRO) | yes (DCL `pending`) | `workforce.time_to_fill` / days | A or B |
| enps (CHRO) | yes (DCL `pending`) | `workforce.enps` / score | A or B |
| cac_payback_months (COO) | **no** | `customer.cac_payback` / months | B (drop) unless model computes it |
| implementation_days (?) | **no** | — | B (drop) |
| burn_multiple (CFO) | **no** (needs `cash_flow.net_burn` + `revenue.recurring`, neither emitted) | derived | B (drop) or add both components |
| current_liabilities (CFO) | partial (SE emits children + `liability.total`, not the bare current total) | `liability.current` / amount (add rollup) | A (add rollup emitter) |

**CHRO impact:** `offer_acceptance_rate_pct` is CHRO's 4th metric = a KPI tile; until (A) or (B),
the CHRO dashboard has one guaranteed-empty KPI.

## Drift prevention
A contract test should assert that, for a freshly-ingested entity that ran BOTH the SE pipeline
and the fabric planes, every LOCKED metric in this registry resolves to a value (no "no data")
and that no producer emits a listed metric under a non-canonical name. Until that test lands,
this file is the manual gate: any new emitter or NLQ map entry for a listed metric must use the
canonical name here.

---

# SE-path cutover readiness

Rich SE data currently reaches DCL the OLD way: Farm's snapshot_triple_builder →
`POST /api/dcl/ingest-triples` (Farm classifies, DCL stores). The target is to converge on
the records-path the fabric planes already use: source → AAM transport (raw records +
provenance, no mapping in AAM) → `POST /api/dcl/ingest-records` (DCL maps/resolves/classifies).
**Do NOT flip rich SE data from `ingest-triples` to `ingest-records` until ALL readiness gates
below are met.** Re-tenanting the shared SE entities is part of this readiness, not a separate fix.

Identity rule for ContextOS (single-entity): **entity↔tenant is 1:1.** One-to-many (one
tenant, many entities) is Convergence (M&A) only — never the single-entity demo. No
shared-tenant lumping.

## Readiness gates (all required before the flip)
1. **1:1 tenants.** Each SE entity has its own tenant. Today entity→tenant is already 1:1 (no
   entity sits on >1 tenant; the BlueFlow-I5BQ duplicate was removed 2026-06-04), but the
   *reverse* still violates 1:1: ~60 SE demo entities are lumped on the shared tenant
   `69688df3`. Re-tenant them onto dedicated tenants. (FabricDemo `fab1c0de` and the already-
   dedicated SE tenants comply.)
2. **Records-path covers the rich domains.** DCL's record_converter + aggregators must classify
   every SE concept domain (finance/P&L/BS/CF, workforce, customer/sales, support, engineering,
   cloud_spend, …) under the canonical names in the registry above — not just the four fabric
   planes. Measured by gate 4.
3. **is_current scoping.** The multi-entity is_current resolution (dcl #36/#39/#42) is landed so
   a fresh records-path run is naturally the current snapshot for its entity.
4. **SE-parity gate (frozen baseline — not eyeballed).** On the richest SE entity
   (FluxEdge-TMZ8), the records-path (source → transport → DCL-classify) must REPRODUCE the
   current SE dataset at parity: every concept present, under its CANONICAL name, values matching.
   - Baseline captured 2026-06-04: `cutover/se_parity_baseline__FluxEdge-TMZ8.jsonl` (24,165
     active triples, 259 concepts, 31 roots) + `.summary.json`, dumped from the LIVE SE store by
     `cutover/capture_se_parity_baseline.py` (kept local, NOT committed — it reads the pre-mig
     store; see the STORE NOTE below). It DELIBERATELY includes concepts that are dark in NLQ
     today ONLY from name drift (e.g. `infrastructure.cloud_spend.*` 336 triples,
     `customer.count.total`, `support.tickets.total`, `customer.gross_churn_rate`,
     `opex.sales_marketing`) — real data the records-path must emit, mapped to the canonical
     names the registry pins (`cloud_spend.summary.*`, `customer.count.total`,
     `support.tickets.total`, …).
   - The flip is BLOCKED until a records-path run for FluxEdge-TMZ8 diffs clean against this
     baseline (drifted→canonical per the registry): zero missing concepts, values matching.

## STORE LINEAGE — the April 2026 current_triples rebuild (historical), and what is canonical now
Corrected 2026-06-11 after a primary-evidence diagnostic. The `current_triples` store rebuild
(that line's migrations 014/015/016/017: flat live mirror + partitioned archive +
`swap_and_delete`, `is_active` dropped) was REAL production code Apr 13–19 2026 — deployed to
prod via Render, applied to the then-prod store — and was backed out on Apr 19 via a full prod
store reset (every prod table's earliest row is 2026-04-19 21:41Z). The code line survives only
on branch `rollback-backup-apr19`; no environment carries its DDL. The prior wording of this
section ("made current_triples the canonical read") described that deployed-then-reverted era as
if it were current — it is not, and `current_triples` must not be treated as a target.

**Canonical store (Gate 0, ContextOS_Blueprint_v1 §6/§15): `semantic_triples`, bi-temporal.**
Migration `017_bitemporal_store.sql` (applied to aos-dev `shared_gdbmdr` 2026-06-11; prod
application is its own B19-gated gate). Every fact carries two timelines; supersession closes
windows; nothing is deleted on the lifecycle path; hard DELETEs are operator retention tools.

### Temporal Columns v1 (reusable convention — any future fact-bearing table, incl. Gate 1B edges)
| column | type | meaning |
|---|---|---|
| `valid_from` / `valid_to` | timestamptz, NOT NULL/NULL | when the assertion is true in the world (valid_to NULL = still true) |
| `ingested_at` / `superseded_at` | timestamptz, NOT NULL/NULL | when DCL learned it / stopped believing it (superseded_at NULL = live) |
| `is_active` | boolean GENERATED ALWAYS AS (`superseded_at IS NULL`) STORED | compatibility liveness flag — readable everywhere, unwritable by construction |

Rules: lifecycle writes SET `superseded_at = now()` (predicate `is_active = true` for partial-index
match); corrections = new row, same coordinates, same `valid_from`, old row superseded; late-arriving
= new row with past `valid_from`, predecessor gets `valid_to` + `superseded_at`; as-of read =
`ingested_at <= T AND (superseded_at IS NULL OR superseded_at > T)`. The mechanism is key-agnostic —
an edges table carries the same four columns + generated flag unchanged.

### Convergence coordination note (additive schema change, 2026-06-11)
`semantic_triples` gained the four temporal columns; `is_active` was dropped and re-added as the
STORED GENERATED column above — same name, same values, same partial-index predicates. Convergence's
SELECT-only reads (`WHERE is_active = true`) are unaffected. Stability guarantee, grounded in the
Apr 19 history: removing `is_active` was field-tested once and backed out at the cost of a full prod
store reset — under the bi-temporal model the column is definitional (`superseded_at IS NULL`) and
is guaranteed to remain readable; any future change to it requires Convergence coordination here.

### Stage-2 current-state surface: `semantic_triples_current` (VIEW, migration 029)
ContextOS Stage 2 formalizes "current state" as a single LOGICAL view:
`CREATE VIEW semantic_triples_current AS SELECT * FROM semantic_triples WHERE is_active = true`.
Surfacing + conflict + normalization current-state reads route through it so "current state" has
ONE definition; as-of reads bypass it (the parameterized bi-temporal predicate
`ingested_at <= T AND (superseded_at IS NULL OR superseded_at > T)` against the base table, because
point-in-time history lives below the liveness flag). It is read-only by construction — the base
table remains the only write path (supersession via `SET superseded_at = now()`); a Stage-1
normalized value lands in `semantic_triples` and therefore appears in the view automatically.
**Explicitly distinct from the retired materialized `current_triples` table** (the Apr 2026 store
rebuild above — flat live mirror + partitioned archive + `swap_and_delete`, exists in no
environment): `semantic_triples_current` is a logical view over the one bi-temporal substrate, not a
second materialized store, and does not reuse the `current_triples` name. Convergence's SELECT-only
`WHERE is_active = true` reads are unaffected (the view is additive — it neither alters a column nor
changes the predicate Convergence uses).

---

## `entity_edges` (Gate 1B, migration 019)

Owner: DCL. Typed, bi-temporal entity↔entity edges inside one enterprise scope
(`tenant_id` + `entity_id`, the I2 pair). Nodes are `(node_type, node_key)` pairs —
department, service, customer, org_unit, person, … — not separate tables; node values
join from `semantic_triples` at read time. Named `entity_edges` deliberately — AOD owns
a `semantic_edges` table in the aos-dev `dev` schema (dcl_deferred_work.md #57) and a
distinct name keeps cross-schema greps unambiguous. Additive — no existing-table
changes; Convergence is unaffected.

| Column | Type | Nullable | Default | Constraint |
|--------|------|----------|---------|------------|
| `id` | UUID | NOT NULL | `gen_random_uuid()` | PRIMARY KEY |
| `tenant_id` | UUID | NOT NULL | — | — |
| `entity_id` | TEXT | NOT NULL | — | — |
| `src_type` / `src_key` | TEXT | NOT NULL | — | source node |
| `edge_type` | TEXT | NOT NULL | — | registered in `edge_types` |
| `dst_type` / `dst_key` | TEXT | NOT NULL | — | target node |
| `properties` | JSONB | NULL | — | — |
| `source_system` | TEXT | NOT NULL | — | provenance contract, as facts |
| `source_table` / `source_field` | TEXT | NULL | — | — |
| `pipe_id` | UUID | NULL | — | — |
| `run_id` | UUID | NOT NULL | — | exposed as `dcl_ingest_id` (I1) |
| `source_run_tag` | TEXT | NULL | — | — |
| `confidence_score` | NUMERIC(3,2) | NOT NULL | — | `>= 0 AND <= 1` |
| `confidence_tier` | TEXT | NOT NULL | — | `IN ('exact','high','medium','low')` |
| `fabric_plane` / `fabric_product` | TEXT | NULL | — | — |
| `derivation` | TEXT | NOT NULL | — | `IN ('derived','declared')` |
| Temporal Columns v1 | — | — | — | exactly the convention above |
| `created_at` / `updated_at` | TIMESTAMPTZ | NULL | `now()` | — |

Live-edge identity = `(tenant_id, entity_id, src_type, src_key, edge_type, dst_type,
dst_key)`; re-asserting the coordinates supersedes the prior row (corrections, same as
facts). Constraint rules (cardinality + allowed node-type pairs, from `edge_types`) are
enforced in `backend/db/edge_store.py` at the persistence boundary; violating edges are
EXCLUDED from the graph and flagged into `conflict_register`
(`conflict_type='structural'`, classes `edge_cardinality` / `edge_pair_disallowed` /
`edge_type_unregistered`) in the same transaction — register write and graph write
commit or roll back together.

## `edge_types` (Gate 1B, migration 019)

Owner: DCL. `tenant_id '*'` = built-ins (HAS one_to_many, GENERATES many_to_many,
BELONGS_TO many_to_one, REPORTS_TO many_to_one); tenant rows overlay. Cardinality
semantics: `many_to_one` — a src holds ≤1 live edge of the type; `one_to_many` — a dst
is pointed at by ≤1; `one_to_one` — both; `many_to_many` — neither. `allowed_pairs`
JSONB `[[src_type, dst_type], …]`, NULL = unrestricted.

## `concept_hierarchy` (Gate 1B, migration 019)

Owner: DCL. TENANT-DEFINED parent links only — the ontology YAML remains the single
source of the default tree (domain → root, derived at read time in
`backend/registry/concept_hierarchy.py`; dotted children implied by name). PK
`(tenant_id, concept)` — single parent, a tree; cycles rejected at write. Reads
participate via `expand_for_read` (exact concepts + dotted prefixes) used by
`query_triples(include_descendants=true)` and `GET /api/dcl/concepts/hierarchy`.

---

## Gate 3A Change Proposal tables (migration 023)

> **Renamed 2026-06-12 (Dispatch R2):** `alignment_proposals` → `change_proposals`, `alignment_decisions` → `change_proposal_decisions`. The proposal queue is interviewer-agnostic infrastructure (also serves Gate 3B drift findings); the Align service identity was retired into Mai's onboarding scope. Live table names in the DB are the new names; idempotent DDL guards in 023_change_proposals.sql preserve the rename history.

All four tables below are DCL-owned. Convergence does NOT read them (they are proposal-intake only; no `semantic_triples` writes here, so no Convergence coordination required). Additive — no existing table altered except `ALTER TABLE conflict_register ADD COLUMN source_class`.

### `change_proposals`

HITL queue for onboarding-sourced stakeholder elicitation proposals. The third HITL queue in DCL (resolver_hitl_queue is the first; resolution_workspaces is DEFINED-BUT-UNUSED; this is the canonical third). Resolved #45.

| Column | Type | Nullable | Default | Constraint |
|--------|------|----------|---------|------------|
| `id` | UUID | NOT NULL | `gen_random_uuid()` | PRIMARY KEY |
| `tenant_id` | UUID | NOT NULL | — | — |
| `proposal_type` | TEXT | NOT NULL | — | `IN ('authority_map','conflict_candidate','vocabulary_alias','org_hierarchy','management_overlay','priority_query')` |
| `natural_key` | TEXT | NOT NULL | — | — |
| `confidence` | NUMERIC(3,2) | NOT NULL | — | `>= 0 AND <= 1` |
| `provenance_basis` | TEXT | NOT NULL | — | — |
| `provenance_source` | TEXT | NULL | — | — |
| `payload` | JSONB | NOT NULL | — | — |
| `status` | TEXT | NOT NULL | `'pending'` | `IN ('pending','approved','rejected')` |
| `canonical_artifact_id` | UUID | NULL | — | — |
| `created_at` | TIMESTAMPTZ | NOT NULL | `now()` | — |
| `decided_at` | TIMESTAMPTZ | NULL | — | — |

Unique index: `(tenant_id, proposal_type, natural_key) WHERE status = 'pending'` — allows re-proposal after rejection.

### `change_proposal_decisions`

Append-only trace for every approve/reject decision on change_proposals. The 4th branch of the `decision_traces` VIEW (trace_type = `'proposal_decision'`).

| Column | Type | Nullable | Default | Constraint |
|--------|------|----------|---------|------------|
| `id` | UUID | NOT NULL | `gen_random_uuid()` | PRIMARY KEY |
| `proposal_id` | UUID | NOT NULL | — | FK → `change_proposals(id)` |
| `tenant_id` | UUID | NOT NULL | — | — |
| `decision` | TEXT | NOT NULL | — | `IN ('approved','rejected')` |
| `decided_by` | TEXT | NOT NULL | — | — |
| `rationale` | TEXT | NULL | — | — |
| `decided_at` | TIMESTAMPTZ | NOT NULL | `now()` | — |

### `tenant_contour`

Per-tenant org contour (hierarchy, management overlay, priority queries). Split-brain guard: `sor_authority` is NEVER stored here; it is always projected at read time from `tenant_authority_map`.

| Column | Type | Nullable | Default | Constraint |
|--------|------|----------|---------|------------|
| `id` | UUID | NOT NULL | `gen_random_uuid()` | PRIMARY KEY |
| `tenant_id` | UUID | NOT NULL | — | — |
| `contour_type` | TEXT | NOT NULL | — | `IN ('org_hierarchy','management_overlay','priority_query')` |
| `dimension` | TEXT | NOT NULL | — | — |
| `data` | JSONB | NOT NULL | — | — |
| `source_proposal_id` | UUID | NULL | — | — |
| `updated_at` | TIMESTAMPTZ | NOT NULL | `now()` | — |

Unique: `(tenant_id, contour_type, dimension)`.

### `tenant_concept_aliases`

Per-tenant vocabulary aliases (stakeholder shorthand → canonical concept). Wired reader: `GET /api/dcl/align/concept-lookup`.

| Column | Type | Nullable | Default | Constraint |
|--------|------|----------|---------|------------|
| `id` | UUID | NOT NULL | `gen_random_uuid()` | PRIMARY KEY |
| `tenant_id` | UUID | NOT NULL | — | — |
| `alias` | TEXT | NOT NULL | — | — |
| `canonical_concept` | TEXT | NOT NULL | — | — |
| `source_proposal_id` | UUID | NULL | — | — |
| `updated_at` | TIMESTAMPTZ | NOT NULL | `now()` | — |

Unique: `(tenant_id, alias)`.

### `conflict_register.source_class` (additive column, migration 023)

Added: `source_class TEXT NOT NULL DEFAULT 'system_system' CHECK IN ('system_system','stakeholder_system','stakeholder_stakeholder')`. Discriminates conflict origin. Convergence reads `conflict_register` via SELECT — this is an additive column with a default, so it is non-breaking.

---

## Gate 3C MCP agent-identity registry (migration 026)

**DCL-owned. Convergence does NOT read this table.** No `semantic_triples` change — additive new table only. No Convergence coordination required.

### `mcp_agent_identities`

Per-tenant registry of declared agent identities with 3-axis scope. `identity_name` is a stable string key (e.g. `finops-readonly`). Empty arrays on any axis mean unrestricted on that axis — mirrors the token back-compat rule (empty scope = full access). Operators select an identity_name; `scripts/mcp_mint.py` resolves its scopes from this table and embeds them in the HMAC token. Enforcement is self-contained at the MCP boundary (no DB lookup at call time).

| Column | Type | Nullable | Default | Constraint |
|--------|------|----------|---------|------------|
| `id` | UUID | NOT NULL | `gen_random_uuid()` | PRIMARY KEY |
| `tenant_id` | UUID | NOT NULL | — | — |
| `identity_name` | TEXT | NOT NULL | — | — |
| `tool_scope` | TEXT[] | NOT NULL | `'{}'` | — |
| `domain_scope` | TEXT[] | NOT NULL | `'{}'` | — |
| `persona_scope` | TEXT[] | NOT NULL | `'{}'` | — |
| `created_at` | TIMESTAMPTZ | NOT NULL | `now()` | — |

Unique: `(tenant_id, identity_name)`. Index: `idx_mcp_agent_identities_tenant` on `(tenant_id)`.
