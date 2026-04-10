-- Migration 013: per-entity tenant_runs
--
-- tenant_runs was keyed by tenant_id alone (migration 008). One pointer
-- per tenant meant each Farm push (new entity_id) deactivated the previous
-- entity's triples. NLQ could only see the latest entity.
--
-- Fix: key by (tenant_id, entity_id). Each entity gets its own
-- current_run_id pointer. swap_and_deactivate only touches the same
-- entity's previous run.
--
-- Backfills entity_id from the referenced run's actual triples.
-- Idempotent — safe to run multiple times.

-- Step 1: add nullable column
ALTER TABLE tenant_runs ADD COLUMN IF NOT EXISTS entity_id TEXT;

-- Step 2: backfill from the referenced run's triples
UPDATE tenant_runs t
SET entity_id = sub.entity_id
FROM (
    SELECT DISTINCT ON (s.run_id) s.run_id, s.entity_id
    FROM semantic_triples s
    WHERE s.entity_id IS NOT NULL
    ORDER BY s.run_id, s.created_at DESC
) sub
WHERE sub.run_id = t.current_run_id
  AND t.entity_id IS NULL;

-- Step 3: enforce NOT NULL (rows without backfill match = no triples = stale, delete them)
DELETE FROM tenant_runs WHERE entity_id IS NULL;
ALTER TABLE tenant_runs ALTER COLUMN entity_id SET NOT NULL;

-- Step 4: change PK from (tenant_id) to (tenant_id, entity_id)
ALTER TABLE tenant_runs DROP CONSTRAINT IF EXISTS tenant_runs_pkey;
ALTER TABLE tenant_runs ADD PRIMARY KEY (tenant_id, entity_id);
