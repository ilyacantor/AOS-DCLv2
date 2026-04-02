ALTER TABLE tenant_runs
  ADD COLUMN IF NOT EXISTS current_snapshot_name TEXT,
  ADD COLUMN IF NOT EXISTS previous_snapshot_name TEXT;
