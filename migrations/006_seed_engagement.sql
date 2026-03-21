-- Seed active engagement for deployed environment.
-- tenant_id matches Farm's AOS_DEV_TENANT_ID so that
-- resolve_tenant_and_run() finds this engagement when
-- the tenant's triples are active.
-- Idempotent via ON CONFLICT on engagement_id unique constraint.
INSERT INTO engagement_state (id, tenant_id, engagement_id, entity_a_id, entity_b_id, status, config)
VALUES (
    gen_random_uuid(),
    '69688df3-fc8e-51f8-a77c-9c13f9b3a784',
    'production-001',
    'meridian',
    'cascadia',
    'active',
    '{}'::jsonb
)
ON CONFLICT (engagement_id) DO NOTHING;
