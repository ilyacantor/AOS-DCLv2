-- Migration 016: SE-path record-identity resolver — canonical registry + HITL queue
-- Tables: canonical_registry, resolver_hitl_queue, resolver_hitl_audit
-- Idempotent: safe to re-run.
--
-- Brings AAM's SE-path identity resolver (fuzzy-match + HITL) into DCL so AAM's
-- copy can be retired (AAM Blueprint v3.1 §3.6 decision (c)). Additive only —
-- no change to semantic_triples, so no Convergence coordination is required
-- (SCHEMA_CONTRACT.md: new tables are non-breaking). These tables are
-- DCL-owned and not read by Convergence.

-- =============================================================================
-- canonical_registry — per-(tenant, domain) canonical entity store
-- Source of truth for tier-1 exact / tier-2 alias resolution and the target
-- of tier-5 discovery mints. Mirrors AAM's canonical_registry contract.
-- =============================================================================

CREATE TABLE IF NOT EXISTS canonical_registry (
    canonical_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id         UUID NOT NULL,
    domain            TEXT NOT NULL,
    normalized_value  TEXT NOT NULL,
    original_value    TEXT NOT NULL,
    aliases_jsonb     JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- One canonical per (tenant, domain, normalized value). Discovery mints
    -- use ON CONFLICT DO NOTHING against this constraint so two concurrent
    -- writers converge on one canonical_id.
    CONSTRAINT uq_canonical_registry UNIQUE (tenant_id, domain, normalized_value)
);

CREATE INDEX IF NOT EXISTS idx_canonical_registry_tenant_domain
    ON canonical_registry (tenant_id, domain);

-- =============================================================================
-- resolver_hitl_queue — record-level identity decisions needing review, plus
-- an audit log of auto-applied matches. Mirrors AAM's resolver_hitl_queue.
--   status = 'pending'      → fuzzy match in [fuzzy_threshold, auto_threshold);
--                             operator-actionable (approve/reject).
--   status = 'auto_applied' → fuzzy match >= auto_threshold; NOT actionable,
--                             surfaced for audit (the resolver already applied it).
--   status = 'approved'/'rejected' → operator decision recorded.
-- =============================================================================

CREATE TABLE IF NOT EXISTS resolver_hitl_queue (
    hitl_queue_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id              UUID NOT NULL,
    entity_id              TEXT NOT NULL,
    domain                 TEXT NOT NULL,
    left_pipe_id           TEXT,
    left_record_key        TEXT,
    left_value             TEXT NOT NULL,
    right_pipe_id          TEXT,
    right_record_key       TEXT,
    right_value            TEXT NOT NULL,
    confidence             NUMERIC(6,4) NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    status                 TEXT NOT NULL DEFAULT 'pending'
                               CHECK (status IN ('pending', 'approved', 'rejected', 'auto_applied')),
    proposed_canonical_id  UUID NOT NULL,
    decided_by             TEXT,
    decided_at             TIMESTAMPTZ,
    audit_id               UUID NOT NULL,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    extra_json             JSONB,
    -- dedup_key collapses (tenant, domain, norm(left), norm(right), status) so a
    -- replayed ingest is a no-op rather than a duplicate row. Built by
    -- resolver_hitl_store._dedup_key(); partial-unique so legacy NULLs are exempt.
    dedup_key              TEXT
);

CREATE INDEX IF NOT EXISTS idx_resolver_hitl_status   ON resolver_hitl_queue (tenant_id, status);
CREATE INDEX IF NOT EXISTS idx_resolver_hitl_entity   ON resolver_hitl_queue (entity_id, domain);
CREATE INDEX IF NOT EXISTS idx_resolver_hitl_created  ON resolver_hitl_queue (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_resolver_hitl_proposed ON resolver_hitl_queue (proposed_canonical_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_resolver_hitl_dedup
    ON resolver_hitl_queue (dedup_key) WHERE dedup_key IS NOT NULL;

-- =============================================================================
-- resolver_hitl_audit — append-only event log per queue row
-- =============================================================================

CREATE TABLE IF NOT EXISTS resolver_hitl_audit (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    audit_id      UUID NOT NULL,
    hitl_queue_id UUID NOT NULL,
    event         TEXT NOT NULL,
    details       JSONB,
    actor         TEXT,
    occurred_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_resolver_hitl_audit_qid ON resolver_hitl_audit (hitl_queue_id);
