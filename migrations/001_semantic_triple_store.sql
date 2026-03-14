-- Migration 001: Semantic Triple Store
-- Tables: semantic_triples, dimension_values_v2, resolution_workspaces,
--         engagement_state, run_ledger
-- Idempotent: safe to re-run.

-- =============================================================================
-- semantic_triples — core fact store (§3.1)
-- =============================================================================

CREATE TABLE IF NOT EXISTS semantic_triples (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID NOT NULL,
    entity_id       TEXT NOT NULL,
    concept         TEXT NOT NULL,
    property        TEXT NOT NULL,
    value           JSONB NOT NULL,
    period          TEXT,
    currency        TEXT DEFAULT 'USD',
    unit            TEXT,

    -- Provenance (§3.1.2)
    source_system   TEXT NOT NULL,
    source_table    TEXT,
    source_field    TEXT,
    pipe_id         UUID,
    run_id          UUID NOT NULL,
    confidence_score NUMERIC(3,2) NOT NULL CHECK (confidence_score >= 0 AND confidence_score <= 1),
    confidence_tier TEXT NOT NULL CHECK (confidence_tier IN ('exact', 'high', 'medium', 'low')),

    -- Resolution (§3.1.3)
    canonical_id    UUID,
    resolution_method TEXT CHECK (resolution_method IN ('deterministic', 'fuzzy', 'manual') OR resolution_method IS NULL),
    resolution_confidence NUMERIC(3,2) CHECK (resolution_confidence IS NULL OR (resolution_confidence >= 0 AND resolution_confidence <= 1)),

    -- Housekeeping
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now(),
    is_active       BOOLEAN DEFAULT true
);

CREATE INDEX IF NOT EXISTS idx_triples_entity_concept ON semantic_triples (tenant_id, entity_id, concept);
CREATE INDEX IF NOT EXISTS idx_triples_concept_period ON semantic_triples (tenant_id, concept, period);
CREATE INDEX IF NOT EXISTS idx_triples_run ON semantic_triples (run_id);
CREATE INDEX IF NOT EXISTS idx_triples_canonical ON semantic_triples (canonical_id) WHERE canonical_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_triples_entity_period ON semantic_triples (tenant_id, entity_id, period);
CREATE INDEX IF NOT EXISTS idx_triples_active ON semantic_triples (tenant_id, is_active) WHERE is_active = true;

-- =============================================================================
-- dimension_values_v2 — hierarchical dimension store
-- (v1 exists with a different schema; this is the triple-store-aligned version)
-- =============================================================================

CREATE TABLE IF NOT EXISTS dimension_values_v2 (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID NOT NULL,
    entity_id       TEXT NOT NULL,
    dimension       TEXT NOT NULL,
    value           TEXT NOT NULL,
    parent_id       UUID REFERENCES dimension_values_v2(id),
    depth           INT DEFAULT 0,
    path            TEXT,
    run_id          UUID NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dimval_v2_tenant_dim ON dimension_values_v2 (tenant_id, entity_id, dimension);
CREATE INDEX IF NOT EXISTS idx_dimval_v2_parent ON dimension_values_v2 (parent_id) WHERE parent_id IS NOT NULL;

-- =============================================================================
-- resolution_workspaces — HITL entity resolution workspace (§3.1.3)
-- =============================================================================

CREATE TABLE IF NOT EXISTS resolution_workspaces (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID NOT NULL,
    workspace_type  TEXT NOT NULL CHECK (workspace_type IN ('customer', 'vendor', 'employee', 'account')),
    status          TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'in_review', 'resolved', 'escalated')),
    candidates      JSONB NOT NULL,
    evidence        JSONB NOT NULL,
    decision        JSONB,
    decided_by      TEXT,
    decided_at      TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_resws_tenant_status ON resolution_workspaces (tenant_id, status);
CREATE INDEX IF NOT EXISTS idx_resws_type ON resolution_workspaces (tenant_id, workspace_type);

-- =============================================================================
-- engagement_state — engagement lifecycle tracking (§4.3)
-- =============================================================================

CREATE TABLE IF NOT EXISTS engagement_state (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID NOT NULL,
    engagement_id   TEXT NOT NULL UNIQUE,
    entity_a_id     TEXT NOT NULL,
    entity_b_id     TEXT,
    status          TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'paused', 'complete', 'archived')),
    config          JSONB NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_engagement_tenant ON engagement_state (tenant_id);
CREATE INDEX IF NOT EXISTS idx_engagement_eid ON engagement_state (engagement_id);

-- =============================================================================
-- run_ledger — step-level execution tracking (§7.1)
-- =============================================================================

CREATE TABLE IF NOT EXISTS run_ledger (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID NOT NULL,
    engagement_id   TEXT NOT NULL,
    step_name       TEXT NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('pending', 'running', 'complete', 'failed', 'stale')),
    idempotency_key TEXT NOT NULL UNIQUE,
    attempt         INT NOT NULL DEFAULT 1,
    inputs_hash     TEXT,
    outputs_ref     TEXT,
    upstream_deps   TEXT[],
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    error           TEXT,
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_runledger_engagement ON run_ledger (tenant_id, engagement_id);
CREATE INDEX IF NOT EXISTS idx_runledger_idem ON run_ledger (idempotency_key);
CREATE INDEX IF NOT EXISTS idx_runledger_status ON run_ledger (tenant_id, status);
