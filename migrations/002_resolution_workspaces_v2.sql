-- Migration 002: Resolution Workspaces v2
-- Table: resolution_workspaces_v2
-- PG-backed entity resolution workspaces derived from triple overlap.
-- Idempotent: safe to re-run.

CREATE TABLE IF NOT EXISTS resolution_workspaces_v2 (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID NOT NULL,
    run_id          UUID NOT NULL,
    concept         TEXT NOT NULL,
    domain          TEXT NOT NULL CHECK (domain IN ('customer', 'vendor', 'employee')),
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'confirmed', 'rejected', 'escalated')),
    canonical_id    TEXT,
    decided_by      TEXT,
    decided_at      TIMESTAMPTZ,
    escalation_reason TEXT,
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now(),
    UNIQUE(tenant_id, run_id, concept)
);

CREATE INDEX IF NOT EXISTS idx_resws_v2_tenant_status
    ON resolution_workspaces_v2 (tenant_id, run_id, status);
CREATE INDEX IF NOT EXISTS idx_resws_v2_domain
    ON resolution_workspaces_v2 (tenant_id, run_id, domain);
