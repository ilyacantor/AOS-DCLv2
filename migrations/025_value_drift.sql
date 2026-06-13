-- Migration 025: value_drift proposal type + value_drift monitor_schedule seed.
-- (Gate 3B D2 — last migration in the Gate 3 campaign reservation.)
--
-- 1. Extend change_proposals CHECK constraint to include 'value_drift'.
--    Same name-independent drop-and-replace pattern as mig024 used for
--    'structural_drift'.  Existing rows are all valid under the extended set.
--
-- 2. Seed value_drift job in monitor_schedule (enabled=false — disabled until
--    the gate-close operator enables it; seeded now so the schedule API can
--    list it and run-now works immediately).
--
-- I1: no field named run_id anywhere in this migration.
-- Idempotent — safe to re-run.

BEGIN;

-- =============================================================================
-- 1. Extend change_proposals CHECK constraint to include 'value_drift'.
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
        'structural_drift',
        'value_drift'
    ));

COMMENT ON COLUMN change_proposals.proposal_type IS
    'Proposal category. Extended in mig024 (structural_drift) and '
    'mig025 (value_drift — Gate 3B D2 scheduled value-conflict sweep).';

-- =============================================================================
-- 2. Seed value_drift job in monitor_schedule.
--    enabled=false: disabled at seed time (operator enables after gate-close).
--    interval_seconds=300: same cadence as structural_drift.
-- =============================================================================

INSERT INTO monitor_schedule (job_name, interval_seconds, enabled, last_run_at, last_status, last_detail)
VALUES ('value_drift', 300, false, NULL, NULL, NULL)
ON CONFLICT (job_name) DO NOTHING;

COMMIT;
