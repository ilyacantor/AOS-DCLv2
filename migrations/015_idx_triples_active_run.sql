-- Migration 015: partial index on (run_id) WHERE is_active=true
--
-- Background: swap_and_deactivate's UPDATE
--   UPDATE semantic_triples SET is_active=false
--   WHERE run_id = %s AND is_active = true
-- previously planned as a BitmapAnd of idx_triples_run + idx_triples_concept_domain
-- (the latter is partial WHERE is_active=true but keyed on concept-domain/entity_id,
-- so used as a poor proxy for "all active rows globally"). Planner scanned ~668k
-- row-pointers on a 14M-row dev table to filter the run's ~24k active rows.
--
-- A direct partial index on (run_id) WHERE is_active=true collapses the plan to
-- a single index scan returning only the rows that need the UPDATE.
--
-- CONCURRENTLY: creation does not block reads/writes on semantic_triples. Must
-- run outside a transaction block — the migration runner detects CONCURRENTLY
-- and switches to autocommit mode for this file, then runs a post-CONCURRENTLY
-- check that REINDEXes any index left in indisvalid=false state. See
-- run_migration.py:_remediate_invalid_indexes. No manual verification required.
--
-- IF NOT EXISTS: idempotent — safe to re-run.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_triples_active_run
    ON semantic_triples (run_id) WHERE is_active = true;
