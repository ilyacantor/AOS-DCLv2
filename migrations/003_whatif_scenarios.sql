-- Migration 003: What-if scenarios table
-- Formalizes the table auto-created by WhatIfEngineV2._ensure_scenarios_table()
-- Idempotent: safe to re-run.

CREATE TABLE IF NOT EXISTS whatif_scenarios (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    name TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    period TEXT NOT NULL,
    adjustments JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_whatif_tenant ON whatif_scenarios (tenant_id);
CREATE INDEX IF NOT EXISTS idx_whatif_tenant_run ON whatif_scenarios (tenant_id, run_id);
