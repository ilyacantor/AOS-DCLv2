-- Migration 014: mai_mcp_audit
-- Audit log for every external MCP tool invocation (Plan B WP5, §11.4).
-- Append-only. One row per tool call regardless of outcome.
-- Idempotent: safe to re-run.

CREATE TABLE IF NOT EXISTS mai_mcp_audit (
    audit_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID NOT NULL,
    tool_name       TEXT NOT NULL,
    caller_token_id TEXT NOT NULL,
    arguments_hash  TEXT,
    latency_ms      INT NOT NULL,
    outcome         TEXT NOT NULL,
    error_summary   TEXT,
    transport       TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_mai_mcp_audit_outcome
        CHECK (outcome IN ('success', 'error', 'rate_limited', 'unauthorized'))
);

CREATE INDEX IF NOT EXISTS idx_mai_mcp_audit_tenant_created
    ON mai_mcp_audit(tenant_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_mai_mcp_audit_tool_created
    ON mai_mcp_audit(tool_name, created_at DESC);
