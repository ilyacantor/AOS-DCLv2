-- Migration 019: Entity graph — typed entity↔entity edges + edge-type registry
-- + tenant concept-hierarchy links (ContextOS Gate 1B, Blueprint §7).
--
--   entity_edges      — typed, bi-temporal edges between graph nodes inside one
--                       enterprise (tenant_id + entity_id scope, the I2 pair).
--                       Nodes are (node_type, node_key) pairs — department,
--                       service, customer, org_unit, … — not new tables; node
--                       values come from semantic_triples at read time.
--                       Temporal Columns v1 (SCHEMA_CONTRACT): valid_from/
--                       valid_to + ingested_at/superseded_at; is_active is
--                       STORED GENERATED (superseded_at IS NULL) — lifecycle
--                       writes supersede, never delete; writers of is_active
--                       fail loudly at the database.
--                       NOTE on naming: deliberately NOT `semantic_edges` —
--                       AOD owns a table of that name in the aos-dev `dev`
--                       schema (dcl_deferred_work.md #57); a distinct name
--                       keeps cross-schema greps and monitors unambiguous.
--   edge_types        — built-in + tenant-defined edge types with constraint
--                       rules (cardinality + allowed node-type pairs).
--                       tenant_id '*' = built-ins (same convention as 1A's
--                       tenant_authority_map). Violations of these rules are
--                       flagged into conflict_register (conflict_type=
--                       'structural', migration 018) — never silently dropped.
--   concept_hierarchy — tenant-defined parent links for the concept library.
--                       The ontology YAML remains the source of the default
--                       domain→root tree (derived at read time); rows here
--                       extend or override it per tenant. Single parent per
--                       (tenant_id, concept) — a tree, not a DAG.
--
-- Additive only — no existing table changes. Idempotent — safe to re-run.

BEGIN;

CREATE TABLE IF NOT EXISTS entity_edges (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id        UUID NOT NULL,
    entity_id        TEXT NOT NULL,
    src_type         TEXT NOT NULL,
    src_key          TEXT NOT NULL,
    edge_type        TEXT NOT NULL,
    dst_type         TEXT NOT NULL,
    dst_key          TEXT NOT NULL,
    properties       JSONB,
    -- provenance contract (same fields, same NOT NULLs as semantic_triples)
    source_system    TEXT NOT NULL,
    source_table     TEXT,
    source_field     TEXT,
    pipe_id          UUID,
    run_id           UUID NOT NULL,
    source_run_tag   TEXT,
    confidence_score NUMERIC(3,2) NOT NULL CHECK (confidence_score >= 0 AND confidence_score <= 1),
    confidence_tier  TEXT NOT NULL CHECK (confidence_tier IN ('exact','high','medium','low')),
    fabric_plane     TEXT,
    fabric_product   TEXT,
    derivation       TEXT NOT NULL CHECK (derivation IN ('derived','declared')),
    -- Temporal Columns v1 (SCHEMA_CONTRACT — same convention as semantic_triples)
    valid_from       TIMESTAMPTZ NOT NULL DEFAULT now(),
    valid_to         TIMESTAMPTZ,
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    superseded_at    TIMESTAMPTZ,
    is_active        BOOLEAN GENERATED ALWAYS AS (superseded_at IS NULL) STORED NOT NULL,
    created_at       TIMESTAMPTZ DEFAULT now(),
    updated_at       TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_edges_src
    ON entity_edges (tenant_id, entity_id, src_type, src_key) WHERE is_active = true;
CREATE INDEX IF NOT EXISTS idx_edges_dst
    ON entity_edges (tenant_id, entity_id, dst_type, dst_key) WHERE is_active = true;
CREATE INDEX IF NOT EXISTS idx_edges_type
    ON entity_edges (tenant_id, entity_id, edge_type) WHERE is_active = true;
CREATE INDEX IF NOT EXISTS idx_edges_run
    ON entity_edges (run_id);
-- as-of reads filter on knowledge time and cannot use the partial active indexes
CREATE INDEX IF NOT EXISTS idx_edges_entity_ingested
    ON entity_edges (tenant_id, entity_id, ingested_at);

CREATE TABLE IF NOT EXISTS edge_types (
    tenant_id     TEXT NOT NULL,
    edge_type     TEXT NOT NULL,
    description   TEXT NOT NULL,
    cardinality   TEXT NOT NULL DEFAULT 'many_to_many'
                  CHECK (cardinality IN ('one_to_one','one_to_many','many_to_one','many_to_many')),
    -- [[src_type, dst_type], ...]; NULL = unrestricted
    allowed_pairs JSONB,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, edge_type)
);

-- Built-ins (Blueprint §7). Cardinality semantics (enforced in edge_store):
--   many_to_one  — a src node holds at most ONE live edge of this type
--                  (a department BELONGS_TO one org; a person REPORTS_TO one manager)
--   one_to_many  — a dst node is pointed at by at most ONE live edge of this type
--                  (a service is HAS-owned by one org unit)
--   one_to_one   — both constraints; many_to_many — neither.
INSERT INTO edge_types (tenant_id, edge_type, description, cardinality, allowed_pairs) VALUES
    ('*', 'HAS',        'Structural ownership: parent node has child node',            'one_to_many',  NULL),
    ('*', 'GENERATES',  'Producer relationship: node generates an output node',        'many_to_many', NULL),
    ('*', 'BELONGS_TO', 'Membership: node belongs to exactly one parent',              'many_to_one',  NULL),
    ('*', 'REPORTS_TO', 'Org reporting line: node reports to exactly one node',        'many_to_one',  NULL)
ON CONFLICT (tenant_id, edge_type) DO NOTHING;

CREATE TABLE IF NOT EXISTS concept_hierarchy (
    tenant_id      TEXT NOT NULL,
    concept        TEXT NOT NULL,
    parent_concept TEXT NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, concept)
);

CREATE INDEX IF NOT EXISTS idx_concept_hierarchy_parent
    ON concept_hierarchy (tenant_id, parent_concept);

COMMIT;
