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
