-- Migration 020: Decision traces — unified read view over the three existing
-- decision stores (ContextOS Blueprint §9, Gate 2A).
--
--   decision_traces (VIEW) — THE unified trace store. One projected shape over:
--       mcp_call             ← mai_mcp_audit          (migration 014)
--       conflict_disposition ← conflict_dispositions ⋈ conflict_register (018)
--       er_confirmation      ← resolver_hitl_audit ⋈ resolver_hitl_queue (016)
--     The three base tables remain the ONLY write paths — no dual-writes, no
--     second system. The view is read-only by construction.
--   mai_mcp_audit gains three nullable go-forward enrichment columns
--     (entity_id, arguments, result_summary). Historical rows stay NULL —
--     knowledge honestly not captured at the time; never backfill fabricated
--     values.
--
-- I1 note: the view exposes no field named run_id.
-- Additive only — no existing column is altered or dropped, no data rewritten.
-- Idempotent — safe to re-run.

BEGIN;

-- =============================================================================
-- 1. mai_mcp_audit go-forward enrichment columns (nullable, never backfilled)
-- =============================================================================

ALTER TABLE mai_mcp_audit
    ADD COLUMN IF NOT EXISTS entity_id      TEXT,
    ADD COLUMN IF NOT EXISTS arguments      JSONB,
    ADD COLUMN IF NOT EXISTS result_summary JSONB;

COMMENT ON COLUMN mai_mcp_audit.entity_id IS
    'Go-forward enrichment (mig 020): entity business key of the call, when known. Historical rows stay NULL — never backfill fabricated values.';
COMMENT ON COLUMN mai_mcp_audit.arguments IS
    'Go-forward enrichment (mig 020): full tool-call arguments. Historical rows stay NULL (only arguments_hash was captured at the time).';
COMMENT ON COLUMN mai_mcp_audit.result_summary IS
    'Go-forward enrichment (mig 020): structured summary of the tool result. Historical rows stay NULL.';

-- =============================================================================
-- 2. decision_traces — unified trace view (UNION ALL, one branch per source)
--    Common shape: trace_id UUID, trace_type TEXT, tenant_id UUID,
--    entity_id TEXT, agent TEXT, decision_type TEXT, concept TEXT,
--    conflict_class TEXT, period TEXT, outcome TEXT, rationale TEXT,
--    payload JSONB, result_summary JSONB, refs JSONB,
--    occurred_at TIMESTAMPTZ, ingested_at TIMESTAMPTZ (= occurred_at),
--    superseded_at TIMESTAMPTZ (= NULL — traces are events, never superseded).
-- =============================================================================

CREATE OR REPLACE VIEW decision_traces AS
SELECT
    m.audit_id            AS trace_id,
    'mcp_call'            AS trace_type,
    m.tenant_id           AS tenant_id,
    m.entity_id           AS entity_id,
    m.caller_token_id     AS agent,
    m.tool_name           AS decision_type,
    NULL::text            AS concept,
    NULL::text            AS conflict_class,
    NULL::text            AS period,
    m.outcome             AS outcome,
    NULL::text            AS rationale,
    m.arguments           AS payload,
    m.result_summary      AS result_summary,
    NULL::jsonb           AS refs,
    m.created_at          AS occurred_at,
    m.created_at          AS ingested_at,
    NULL::timestamptz     AS superseded_at
FROM mai_mcp_audit m

UNION ALL

SELECT
    d.id                     AS trace_id,
    'conflict_disposition'   AS trace_type,
    d.tenant_id              AS tenant_id,
    d.entity_id              AS entity_id,
    d.decided_by             AS agent,
    d.action                 AS decision_type,
    r.concept                AS concept,
    d.conflict_class         AS conflict_class,
    r.period                 AS period,
    d.winner_source          AS outcome,
    d.rationale              AS rationale,
    d.context                AS payload,
    NULL::jsonb              AS result_summary,
    jsonb_build_object(
        'conflict_id',           d.conflict_id,
        'superseded_triple_ids', d.superseded_triple_ids,
        'loser_sources',         d.loser_sources
    )                        AS refs,
    d.decided_at             AS occurred_at,
    d.decided_at             AS ingested_at,
    NULL::timestamptz        AS superseded_at
FROM conflict_dispositions d
JOIN conflict_register r ON d.conflict_id = r.id

UNION ALL

SELECT
    a.id                     AS trace_id,
    'er_confirmation'        AS trace_type,
    q.tenant_id              AS tenant_id,
    q.entity_id              AS entity_id,
    a.actor                  AS agent,  -- NULL = system event (created/auto_applied); '' would pollute the agent axis
    a.event                  AS decision_type,
    q.domain                 AS concept,
    NULL::text               AS conflict_class,
    NULL::text               AS period,
    q.status                 AS outcome,
    NULL::text               AS rationale,
    COALESCE(a.details, '{}'::jsonb) || jsonb_build_object(
        'left_value',  q.left_value,
        'right_value', q.right_value
    )                        AS payload,
    NULL::jsonb              AS result_summary,
    jsonb_build_object(
        'hitl_queue_id',         a.hitl_queue_id,
        'proposed_canonical_id', q.proposed_canonical_id
    )                        AS refs,
    a.occurred_at            AS occurred_at,
    a.occurred_at            AS ingested_at,
    NULL::timestamptz        AS superseded_at
FROM resolver_hitl_audit a
JOIN resolver_hitl_queue q ON a.hitl_queue_id = q.hitl_queue_id;

COMMENT ON VIEW decision_traces IS
    'ContextOS §9 Gate 2A: unified decision-trace store over mai_mcp_audit, conflict_dispositions⋈conflict_register, resolver_hitl_audit⋈resolver_hitl_queue. Read-only; the three base tables remain the only write paths. Exposes no run_id field (I1).';

-- =============================================================================
-- 3. Supporting indexes on the BASE tables for the trace search axes.
--    Already covered (verified in 014/016/018 — not recreated here):
--      idx_dispositions_class        (tenant_id, conflict_class, decided_at DESC)
--      idx_dispositions_conflict     (conflict_id)
--      idx_resolver_hitl_audit_qid   (hitl_queue_id)
--    mai_mcp_audit's existing idx_mai_mcp_audit_tool_created is
--    (tool_name, created_at DESC) — tenant-leading axes below are new.
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_mai_mcp_audit_tenant_entity_created
    ON mai_mcp_audit (tenant_id, entity_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_mai_mcp_audit_tenant_tool_created
    ON mai_mcp_audit (tenant_id, tool_name, created_at DESC);

COMMIT;
