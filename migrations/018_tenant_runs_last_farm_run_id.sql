-- Migration 018: record Farm's farm_run_id on tenant_runs.
--
-- Refresh-from-Farm's detection compared Farm's created_at (pipeline start)
-- to DCL's tenant_runs.updated_at (ingest finish). Different events — DCL's
-- timestamp is always seconds later than Farm's for the same run, so the
-- comparison cannot reliably distinguish "already ingested this run" from
-- "this run is older." Switching to farm_run_id identity removes the
-- apples-to-oranges comparison entirely.
--
-- Backfill happens in scripts/backfill_last_farm_run_id.py (requires Farm's
-- /api/runs to join via dcl_run_id). SQL leaves the column NULL; backfill
-- runs after ALTER. Rows that can't be matched (legacy, non-Farm ingest)
-- stay NULL and are treated as candidates on the next Refresh — one
-- idempotent replay sets them.
--
-- Idempotent — safe to run multiple times.

ALTER TABLE tenant_runs
    ADD COLUMN IF NOT EXISTS last_farm_run_id TEXT;
