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
| support_tickets | `support.tickets` / `total` | SE (operational_kpis) + fabric (records_summary_aggregator: conform `service.support_tickets`→`support.tickets`) | PENDING |
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
