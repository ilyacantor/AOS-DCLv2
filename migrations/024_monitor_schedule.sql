-- Migration 024: Monitor schedule table + structural_drift proposal type (Gate 3B D1).
--
-- 1. monitor_schedule — durable per-job scheduler state. The APScheduler
--    AsyncIOScheduler in main.py reads this on boot to re-arm jobs; pause/resume
--    flip `enabled` here so the state survives a backend restart.
--    Seeded with one row for the structural-drift job (enabled=true, 300s interval).
--
-- 2. change_proposals CHECK constraint extended to include 'structural_drift'.
--    The old proposal_type check is found by content (name-independent search)
--    and dropped; the new superset constraint replaces it.  Existing rows are all
--    valid under the extended set — no data rewritten.
--
-- I1: no field named run_id anywhere in this migration.
-- Idempotent — safe to re-run.

BEGIN;

-- =============================================================================
-- 1. monitor_schedule — durable job-state table
-- =============================================================================

CREATE TABLE IF NOT EXISTS monitor_schedule (
    job_name         TEXT PRIMARY KEY,
    interval_seconds INT  NOT NULL CHECK (interval_seconds > 0),
    enabled          BOOL NOT NULL DEFAULT true,
    last_run_at      TIMESTAMPTZ,
    last_status      TEXT,
    last_detail      TEXT,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE monitor_schedule IS
    'ContextOS Gate 3B D1: durable per-job scheduler state. '
    'APScheduler AsyncIOScheduler reads this on boot to re-arm jobs '
    '(enabled=true → add job; enabled=false → omit, job will not fire). '
    'Pause/resume flip enabled so the state survives a backend restart. '
    'I1: no run_id field.';

-- Seed the structural-drift job (idempotent — DO NOTHING on conflict).
INSERT INTO monitor_schedule (job_name, interval_seconds, enabled, last_run_at, last_status, last_detail)
VALUES ('structural_drift', 300, true, NULL, NULL, NULL)
ON CONFLICT (job_name) DO NOTHING;

-- =============================================================================
-- 2. Extend change_proposals CHECK constraint to include 'structural_drift'.
--    Name-independent search: find the proposal_type CHECK by its definition
--    content so the drop succeeds regardless of the auto-generated name.
--    Then add a new, explicitly-named constraint with the full type set.
-- =============================================================================

DO $$
DECLARE
    cname TEXT;
BEGIN
    SELECT conname INTO cname
    FROM pg_constraint
    WHERE conrelid = 'change_proposals'::regclass
      AND contype = 'c'
      AND pg_get_constraintdef(oid) LIKE '%proposal_type%'
    LIMIT 1;
    IF cname IS NOT NULL THEN
        EXECUTE format('ALTER TABLE change_proposals DROP CONSTRAINT %I', cname);
    END IF;
END $$;

ALTER TABLE change_proposals
    ADD CONSTRAINT change_proposals_proposal_type_check
    CHECK (proposal_type IN (
        'authority_map', 'conflict_candidate', 'vocabulary_alias',
        'org_hierarchy', 'management_overlay', 'priority_query',
        'structural_drift'
    ));

COMMENT ON COLUMN change_proposals.proposal_type IS
    'Proposal category. Extended in mig024 (Gate 3B D1) to include '
    '''structural_drift'' for scheduled structural drift detections.';

COMMIT;
