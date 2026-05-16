-- Tenant registry: single source of truth for tenant identity.
-- entity_id is PK (multiple entities share one tenant_id).
-- entity_name is the human-facing display name, generated once and stored.
CREATE TABLE IF NOT EXISTS tenant_registry (
    entity_id    TEXT PRIMARY KEY,
    tenant_id    UUID NOT NULL,
    entity_name  TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Index for listing all entities under a tenant.
CREATE INDEX IF NOT EXISTS idx_tenant_registry_tenant_id
    ON tenant_registry (tenant_id);

-- Tenant rows are populated by the live pipeline (Farm → DCL ingest),
-- not by migration seeds. Hardcoded entity tags previously seeded here
-- were removed per convergence_transition_master WP2 (fixture-era entity
-- tags eradicated). See ws-1 b5 cleanup.
