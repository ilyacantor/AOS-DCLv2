-- Migration 026: MCP agent-identity registry (Gate 3C D1).
--
-- Declares per-tenant agent identities with 3-axis scope:
--   tool_scope   — allowed tool names (empty = all tools)
--   domain_scope — allowed concept-root domains (empty = all domains)
--   persona_scope — allowed persona keys (empty = all personas)
--
-- Operators/minters select an identity_name from this registry; mint_token
-- embeds that identity's scopes into the HMAC token so enforcement is
-- self-contained at the MCP boundary (no Platform round-trip — see #18).
--
-- Additive only — no semantic_triples change.
-- No Convergence coordination required (SCHEMA_CONTRACT.md: DCL-owned,
-- no SELECT access granted to Convergence).
-- Apply to aos-dev only via run_migration.py; prod gate at Gate 3C close.

BEGIN;

CREATE TABLE IF NOT EXISTS mcp_agent_identities (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID        NOT NULL,
    identity_name   TEXT        NOT NULL,
    tool_scope      TEXT[]      NOT NULL DEFAULT '{}',
    domain_scope    TEXT[]      NOT NULL DEFAULT '{}',
    persona_scope   TEXT[]      NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, identity_name)
);

COMMENT ON TABLE mcp_agent_identities IS
    'Gate 3C D1: per-tenant declared agent identities. identity_name is a '
    'stable string key (e.g. finops-readonly). Empty arrays mean unrestricted '
    'on that axis — mirrors the token BACK-COMPAT rule (empty scope = '
    'full access). Operators select an identity_name; mint_token resolves '
    'its scopes from this table and embeds them in the HMAC token.';

COMMENT ON COLUMN mcp_agent_identities.tool_scope IS
    'Tool names allowed. Empty = all PUBLIC_TOOLS (unrestricted on tool axis).';
COMMENT ON COLUMN mcp_agent_identities.domain_scope IS
    'Concept-root domains allowed (e.g. cloud_spend, revenue). Empty = all domains.';
COMMENT ON COLUMN mcp_agent_identities.persona_scope IS
    'Persona keys allowed (e.g. CFO, CRO). Empty = all personas.';

CREATE INDEX IF NOT EXISTS idx_mcp_agent_identities_tenant
    ON mcp_agent_identities (tenant_id);

COMMIT;
