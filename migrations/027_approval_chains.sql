-- Migration 027: Approval chain enforcement (Gate 3C D2).
--
-- Per-tenant approval policy: proposer≠approver enforcement + multi-step chains.
-- Every link in a chain writes a decision trace visible via GET /api/dcl/traces.
-- Back-compat: tenants with no policy row (or defaults) behave exactly as Gate 3A
-- (single approve canonicalizes, no distinct-approver check).
--
-- Tables added:
--   tenant_approval_policy — per-tenant chain config (PK = tenant_id).
--
-- Additive columns:
--   change_proposals.proposer        — identity that created the proposal (nullable;
--                                       drift proposals have no human proposer).
--   change_proposals.steps_approved  — running count of chain steps approved so far.
--   change_proposal_decisions.step_number — which chain link this decision covers.
--
-- CHECK extension on change_proposal_decisions.decision:
--   'denied' added — policy enforcement block, distinct from operator 'reject'.
--   Denial writes a trace without changing proposal status.
--
-- I1: no field named run_id in this migration.
-- Additive only; no semantic_triples change → no Convergence coordination required.
-- SCHEMA_CONTRACT.md updated separately.
-- Idempotent — safe to re-run.

BEGIN;

-- =============================================================================
-- 1. tenant_approval_policy — per-tenant chain config
-- =============================================================================

CREATE TABLE IF NOT EXISTS tenant_approval_policy (
    tenant_id                          UUID PRIMARY KEY,
    require_distinct_proposer_approver BOOLEAN NOT NULL DEFAULT false,
    chain_steps                        INTEGER NOT NULL DEFAULT 1 CHECK (chain_steps >= 1),
    updated_at                         TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE tenant_approval_policy IS
    'Gate 3C D2: per-tenant HITL approval chain configuration. '
    'Absent row = default policy (chain_steps=1, no distinct-approver check). '
    'require_distinct_proposer_approver: decided_by must differ from change_proposals.proposer. '
    'chain_steps: number of distinct approvals required before canonical apply fires. '
    'Step 1..N-1 advance progress without canonicalizing; step N applies. '
    'Rejection at any step = rejected with zero canonical residue (Gate 3A semantics).';

-- =============================================================================
-- 2. change_proposals — add proposer + steps_approved
-- =============================================================================

ALTER TABLE change_proposals
    ADD COLUMN IF NOT EXISTS proposer      TEXT;

ALTER TABLE change_proposals
    ADD COLUMN IF NOT EXISTS steps_approved INTEGER NOT NULL DEFAULT 0;

COMMENT ON COLUMN change_proposals.proposer IS
    'Gate 3C D2: identity (operator ID, session ID, or system name) that created '
    'this proposal. NULL for automated monitors (drift proposals) — the distinct-check '
    'is skipped when proposer is NULL regardless of the policy setting. '
    'Set at intake; never updated after creation.';

COMMENT ON COLUMN change_proposals.steps_approved IS
    'Gate 3C D2: count of chain steps approved so far. Starts at 0. '
    'Increments on each non-final-step approval. Equals chain_steps when approved. '
    'Single-step (default) proposals go 0→1 on the single approve. '
    'Reject sets this column to its current value (no further increment).';

-- =============================================================================
-- 3. change_proposal_decisions — add step_number + extend decision CHECK
-- =============================================================================

ALTER TABLE change_proposal_decisions
    ADD COLUMN IF NOT EXISTS step_number INTEGER;

COMMENT ON COLUMN change_proposal_decisions.step_number IS
    'Gate 3C D2: which chain link this decision row covers (1-based). '
    'NULL on rows written before mig027 (back-compat). '
    'For a chain_steps=2 proposal: step 1 row then step 2 row. '
    'Denied rows use the step number that was attempted.';

-- Extend the decision CHECK to include 'denied'.
-- Name-independent drop: find the constraint by column reference, then add
-- a named replacement with the full value set.
DO $$
DECLARE
    cname TEXT;
BEGIN
    SELECT conname INTO cname
    FROM pg_constraint
    WHERE conrelid = 'change_proposal_decisions'::regclass
      AND contype = 'c'
      AND pg_get_constraintdef(oid) LIKE '%decision%';
    IF cname IS NOT NULL THEN
        EXECUTE 'ALTER TABLE change_proposal_decisions DROP CONSTRAINT '
                || quote_ident(cname);
    END IF;
END $$;

ALTER TABLE change_proposal_decisions
    ADD CONSTRAINT cpd_decision_check
    CHECK (decision IN ('approve', 'reject', 'denied'));

COMMENT ON COLUMN change_proposal_decisions.decision IS
    'approve = step or final approval; reject = operator decline (zero canonical residue); '
    'denied = policy enforcement block (proposer-approver same identity or duplicate '
    'step approver) — proposal stays pending, another approver may try.';

COMMIT;
