-- Migration 018: Conflict Register + per-tenant authority/policy (ContextOS Gate 1A, §8).
--
--   conflict_register      — first-class queryable conflicts: value-level (same entity/
--                            concept/property/period, materially different values across
--                            sources) and structural (multiple sources claiming the same
--                            fact). One register, two classes. Claims carry full provenance
--                            drill (triple ids → bi-temporal rows, never deleted).
--   conflict_dispositions  — APPEND-ONLY decision trace (Gate 2 seed): actor, rationale,
--                            timestamp, winner/losers, superseded triple ids, claims context.
--   tenant_authority_map   — per-tenant source authority (tenant '*' = defaults). Replaces
--                            the hardcoded table in backend/engine/concept_authority.py.
--   tenant_conflict_policy — per-tenant materiality thresholds (abs and/or rel; NULL = off).
--
-- Idempotent — safe to re-run.

BEGIN;

CREATE TABLE IF NOT EXISTS conflict_register (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id               UUID NOT NULL,
    entity_id               TEXT NOT NULL,
    conflict_type           TEXT NOT NULL CHECK (conflict_type IN ('value', 'structural')),
    conflict_class          TEXT NOT NULL,
    concept                 TEXT NOT NULL,
    property                TEXT NOT NULL,
    period                  TEXT,
    dcl_ingest_id           UUID NOT NULL,
    status                  TEXT NOT NULL DEFAULT 'open'
                            CHECK (status IN ('open', 'dispositioned', 'escalated')),
    claims                  JSONB NOT NULL,
    materiality             JSONB,
    recommended             JSONB,
    root_cause_explanation  TEXT,
    root_cause_source       TEXT,
    detected_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One register row per coordinates per detection run; re-detection upserts claims.
CREATE UNIQUE INDEX IF NOT EXISTS uq_conflict_coords_run
    ON conflict_register (tenant_id, entity_id, concept, property,
                          COALESCE(period, ''), dcl_ingest_id);
CREATE INDEX IF NOT EXISTS idx_conflict_status
    ON conflict_register (tenant_id, entity_id, status);
CREATE INDEX IF NOT EXISTS idx_conflict_class
    ON conflict_register (tenant_id, conflict_class, detected_at DESC);

CREATE TABLE IF NOT EXISTS conflict_dispositions (
    id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    conflict_id           UUID NOT NULL REFERENCES conflict_register(id),
    tenant_id             UUID NOT NULL,
    entity_id             TEXT NOT NULL,
    conflict_class        TEXT NOT NULL,
    action                TEXT NOT NULL
                          CHECK (action IN ('accept_a', 'accept_b', 'escalate', 'manual')),
    winner_source         TEXT,
    loser_sources         TEXT[] NOT NULL DEFAULT '{}',
    superseded_triple_ids UUID[] NOT NULL DEFAULT '{}',
    decided_by            TEXT NOT NULL,
    rationale             TEXT NOT NULL,
    decided_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    context               JSONB
);

CREATE INDEX IF NOT EXISTS idx_dispositions_class
    ON conflict_dispositions (tenant_id, conflict_class, decided_at DESC);
CREATE INDEX IF NOT EXISTS idx_dispositions_conflict
    ON conflict_dispositions (conflict_id);

CREATE TABLE IF NOT EXISTS tenant_authority_map (
    tenant_id       TEXT NOT NULL,
    concept_prefix  TEXT NOT NULL,
    ranked_sources  TEXT[] NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, concept_prefix)
);

CREATE TABLE IF NOT EXISTS tenant_conflict_policy (
    tenant_id      TEXT PRIMARY KEY,
    abs_threshold  NUMERIC,
    rel_threshold  NUMERIC,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Seed defaults (tenant '*'): the retired hardcoded authority entries become data,
-- and the default materiality policy is relative-only at 0.5% (abs off — at mixed
-- units an absolute default would flag rounding noise).
INSERT INTO tenant_authority_map (tenant_id, concept_prefix, ranked_sources) VALUES
    ('*', 'engineering',              ARRAY['jira', 'github_actions']),
    ('*', 'infrastructure.incidents', ARRAY['datadog', 'pagerduty']),
    ('*', 'infrastructure.mttr',      ARRAY['datadog', 'pagerduty']),
    ('*', 'infrastructure.uptime',    ARRAY['datadog']),
    ('*', 'infrastructure.downtime',  ARRAY['datadog']),
    ('*', 'cloud_spend',              ARRAY['aws_cost_explorer'])
ON CONFLICT (tenant_id, concept_prefix) DO NOTHING;

INSERT INTO tenant_conflict_policy (tenant_id, abs_threshold, rel_threshold)
VALUES ('*', NULL, 0.005)
ON CONFLICT (tenant_id) DO NOTHING;

COMMIT;
