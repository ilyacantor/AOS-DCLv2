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

-- Seed Meridian and Cascadia under the shared demo tenant UUID.
-- entity_name values are deterministic: seeded RNG from uuid5(tenant_id, entity_id).
INSERT INTO tenant_registry (entity_id, tenant_id, entity_name) VALUES
    ('meridian', '69688df3-fc8e-51f8-a77c-9c13f9b3a784', 'AeroWave-PD8Q'),
    ('cascadia', '69688df3-fc8e-51f8-a77c-9c13f9b3a784', 'InfoWorks-C5AZ')
ON CONFLICT (entity_id) DO UPDATE
    SET tenant_id = EXCLUDED.tenant_id,
        entity_name = EXCLUDED.entity_name;
