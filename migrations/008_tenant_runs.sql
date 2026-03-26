-- Migration 008: tenant_runs — atomic run swap pointer
--
-- Replaces the bulk deactivate_tenant_triples hot path with a single-row
-- pointer swap. Instead of UPDATE-ing 133K+ rows before each ingest,
-- ingest inserts new triples then atomically sets current_run_id here.
-- All financial queries filter by run_id = current_run_id instead of is_active = true.
--
-- Idempotent — safe to run multiple times.

CREATE TABLE IF NOT EXISTS tenant_runs (
    tenant_id       UUID        NOT NULL PRIMARY KEY,
    current_run_id  UUID        NOT NULL,
    previous_run_id UUID,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Composite index for the current-run lookup pattern:
-- WHERE tenant_id = %s AND run_id = (SELECT current_run_id FROM tenant_runs WHERE tenant_id = %s)
-- The inner subquery hits tenant_runs PK. The outer WHERE uses this index.
CREATE INDEX IF NOT EXISTS idx_triples_tenant_run
    ON semantic_triples (tenant_id, run_id);
