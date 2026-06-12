-- Migration 021: Standing rules — provenance-carrying rule registry
-- (ContextOS Blueprint §9, Gate 2A).
--
--   standing_rules           — rules promoted from repeated decisions.
--                              Promotion is PROPOSAL-ONLY without approval,
--                              EVER: rows are born status='proposed'; the
--                              status flips exactly once (proposed→approved
--                              or proposed→rejected), enforced in store code
--                              the same way resolver_hitl_queue enforces its
--                              pending→approved/rejected transition.
--                              Gate 2A binds NO engine behavior to these
--                              rows — this is a provenance-carrying registry,
--                              not a policy engine.
--   standing_rule_provenance — the decision traces that justified each rule.
--                              trace_id resolves through the decision_traces
--                              view (migration 020): (trace_type, trace_id)
--                              identifies the base-table row.
--
-- Additive only — new tables, no existing table changes.
-- Idempotent — safe to re-run.

BEGIN;

CREATE TABLE IF NOT EXISTS standing_rules (
    rule_id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id           UUID NOT NULL,
    -- nullable: rules may be tenant-wide rather than entity-scoped
    entity_id           TEXT,
    rule_scope          TEXT NOT NULL CHECK (rule_scope IN ('conflict_class')),
    conflict_class      TEXT NOT NULL,
    -- e.g. {"action": "accept_a", "winner_source": "..."}
    rule_body           JSONB NOT NULL,
    status              TEXT NOT NULL DEFAULT 'proposed'
                        CHECK (status IN ('proposed', 'approved', 'rejected')),
    proposed_by         TEXT NOT NULL,
    proposed_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    proposal_rationale  TEXT NOT NULL,
    decided_by          TEXT,
    decided_at          TIMESTAMPTZ,
    decision_rationale  TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE standing_rules IS
    'ContextOS §9 Gate 2A: rules promoted from repeated decisions. Promotion is proposal-only without approval, EVER — status flips once (proposed→approved|rejected), enforced in store code like resolver_hitl_queue. Provenance-carrying registry: Gate 2A binds NO engine behavior to these rows.';

CREATE TABLE IF NOT EXISTS standing_rule_provenance (
    rule_id     UUID NOT NULL REFERENCES standing_rules(rule_id),
    trace_type  TEXT NOT NULL,
    trace_id    UUID NOT NULL,
    tenant_id   UUID NOT NULL,
    PRIMARY KEY (rule_id, trace_type, trace_id)
);

COMMENT ON TABLE standing_rule_provenance IS
    'ContextOS §9 Gate 2A: the decision traces that justified the rule. (trace_type, trace_id) resolves through the decision_traces view (migration 020).';

CREATE INDEX IF NOT EXISTS idx_standing_rules_class
    ON standing_rules (tenant_id, conflict_class, status);

CREATE INDEX IF NOT EXISTS idx_standing_rule_prov_trace
    ON standing_rule_provenance (tenant_id, trace_type, trace_id);

COMMIT;
