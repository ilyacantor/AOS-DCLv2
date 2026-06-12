-- Migration 023: Change proposal queue + canonical stores (ContextOS Gate 3A §4).
--
-- Renamed 2026-06-12 (Gate 3A→3B re-homing): alignment_proposals → change_proposals,
-- alignment_decisions → change_proposal_decisions, trace_type align_decision →
-- proposal_decision, align_proposal_ids → proposal_ids, align_proposal_id → proposal_id.
-- The only store that ever ran the original text was aos-dev, renamed in the same
-- DDL operation before any code reload. Prod is pre-gate (#85) and will run this
-- edited text at the #70 gate. Fresh stores provision neutral names from birth.
--
-- Third HITL-shaped table in DCL — justified per #45 resolution:
--   resolver_hitl_queue (mig016): pair-match contract, wrong shape.
--   resolution_workspaces (mig001): wrong CHECK constraints, DEFINED-BUT-UNUSED.
--   A constraint-relaxing bandaid on either would violate A2; new queue is correct.
--
-- Tables added (all DCL-owned, no semantic_triples change → no Convergence coordination):
--   change_proposals        — tenant-scoped HITL queue for onboarding-sourced proposals.
--   change_proposal_decisions — append-only decision log; 4th branch in decision_traces VIEW.
--   tenant_contour          — per-tenant approved contour (hierarchy + management_overlay
--                             + priority_queries). sor_authority is NOT stored here —
--                             always projected from tenant_authority_map at read time
--                             (split-brain guard: one source of truth for source authority).
--   tenant_concept_aliases  — per-tenant DB-backed vocabulary aliases.
--
-- Additive column:
--   conflict_register.source_class — stakeholder vs system conflict discriminator.
--   DEFAULT 'system_system' keeps existing rows consistent; no data rewritten.
--
-- decision_traces VIEW extended with a 4th UNION ALL branch over change_proposal_decisions.
--
-- I1: no field named run_id anywhere in this migration.
-- Idempotent — safe to re-run.

BEGIN;

-- =============================================================================
-- 1. change_proposals — tenant-scoped HITL queue
-- =============================================================================

CREATE TABLE IF NOT EXISTS change_proposals (
    proposal_id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id             UUID NOT NULL,
    entity_id             TEXT,
    proposal_type         TEXT NOT NULL
                          CHECK (proposal_type IN (
                              'authority_map', 'conflict_candidate', 'vocabulary_alias',
                              'org_hierarchy', 'management_overlay', 'priority_query'
                          )),
    natural_key           TEXT NOT NULL,
    payload               JSONB NOT NULL,
    confidence            NUMERIC NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    provenance            JSONB NOT NULL,
    status                TEXT NOT NULL DEFAULT 'pending'
                          CHECK (status IN ('pending', 'approved', 'rejected')),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    decided_at            TIMESTAMPTZ,
    decided_by            TEXT,
    decision_note         TEXT,
    canonical_artifact_id TEXT
);

-- Partial unique index: only ONE pending proposal per (tenant, type, natural_key).
-- Allows re-proposal after rejection. Belt-and-suspenders for the application-level
-- duplicate check (which reports 'duplicate of <proposal_id>' explicitly).
CREATE UNIQUE INDEX IF NOT EXISTS uq_change_proposal_pending
    ON change_proposals (tenant_id, proposal_type, natural_key)
    WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_change_proposals_tenant_status
    ON change_proposals (tenant_id, status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_change_proposals_tenant_type
    ON change_proposals (tenant_id, proposal_type, status);

COMMENT ON TABLE change_proposals IS
    'ContextOS Gate 3A: onboarding-sourced HITL proposals (proposal queue, neutral name). '
    'Third HITL-shaped table (#45 resolution). Renamed from alignment_proposals 2026-06-12. '
    'Duplicate detection is explicit at the application layer (never ON CONFLICT DO NOTHING). '
    'Approval applies the canonical artifact in the same transaction as the status flip. '
    'Rejection leaves zero canonical residue.';

-- =============================================================================
-- 2. change_proposal_decisions — append-only decision log
--    Base table for the 4th decision_traces UNION ALL branch.
--    Mirrors the shape of conflict_dispositions (mig018) for the view.
-- =============================================================================

CREATE TABLE IF NOT EXISTS change_proposal_decisions (
    id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id             UUID NOT NULL,
    entity_id             TEXT,
    proposal_id           UUID NOT NULL,
    proposal_type         TEXT NOT NULL,
    decision              TEXT NOT NULL CHECK (decision IN ('approve', 'reject')),
    decided_by            TEXT NOT NULL,
    decision_note         TEXT,
    payload               JSONB,
    canonical_artifact_id TEXT,
    decided_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_change_decisions_tenant_decided
    ON change_proposal_decisions (tenant_id, decided_at DESC);

CREATE INDEX IF NOT EXISTS idx_change_decisions_proposal
    ON change_proposal_decisions (proposal_id);

COMMENT ON TABLE change_proposal_decisions IS
    'ContextOS Gate 3A: append-only change proposal decisions. Base table of the '
    '4th decision_traces UNION ALL branch (trace_type=proposal_decision). '
    'Renamed from alignment_decisions 2026-06-12. No deletions, no updates.';

-- =============================================================================
-- 3. tenant_contour — per-tenant approved org contour
--    hierarchy + management_overlay + priority_queries from proposals.
--    sor_authority is deliberately ABSENT — projected at read time from
--    tenant_authority_map (single source of truth; no split brain).
-- =============================================================================

CREATE TABLE IF NOT EXISTS tenant_contour (
    tenant_id            UUID PRIMARY KEY,
    hierarchy            JSONB NOT NULL DEFAULT '{}'::jsonb,
    management_overlay   JSONB NOT NULL DEFAULT '[]'::jsonb,
    priority_queries     JSONB NOT NULL DEFAULT '[]'::jsonb,
    proposal_ids         TEXT[] NOT NULL DEFAULT '{}',
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE tenant_contour IS
    'ContextOS Gate 3A: per-tenant approved org contour. sor_authority is NOT '
    'stored here — always projected from tenant_authority_map at read time to '
    'prevent split-brain (split-brain guard per the approved-contour endpoint). '
    'Column renamed proposal_ids (was align_proposal_ids) 2026-06-12.';

-- =============================================================================
-- 4. tenant_concept_aliases — per-tenant DB-backed vocabulary aliases
--    ONE real wired reader: GET /api/dcl/concept-lookup
--    (A dead table nothing reads is a forbidden outcome.)
-- =============================================================================

CREATE TABLE IF NOT EXISTS tenant_concept_aliases (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id         UUID NOT NULL,
    concept_id        TEXT NOT NULL,
    alias             TEXT NOT NULL,
    proposal_id       UUID NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_tenant_concept_alias UNIQUE (tenant_id, alias)
);

CREATE INDEX IF NOT EXISTS idx_tenant_concept_aliases_tenant
    ON tenant_concept_aliases (tenant_id, alias);

COMMENT ON TABLE tenant_concept_aliases IS
    'ContextOS Gate 3A: tenant-scoped vocabulary aliases approved via proposals. '
    'Read by GET /api/dcl/concept-lookup (the wired reader). '
    'Column renamed proposal_id (was align_proposal_id) 2026-06-12. '
    'Not backed by get_ontology() — avoids the #79 semantic-depth null bug '
    'and is tenant-scoped unlike the global YAML.';

-- =============================================================================
-- 5. conflict_register — add source_class discriminator (additive, no data lost)
--    Distinguishes stakeholder↔system and stakeholder↔stakeholder conflicts
--    (ContextOS Blueprint §2) from the existing system↔system detections.
--    DEFAULT 'system_system' keeps all existing rows consistent.
-- =============================================================================

ALTER TABLE conflict_register
    ADD COLUMN IF NOT EXISTS source_class TEXT
    DEFAULT 'system_system'
    CHECK (source_class IN ('system_system', 'stakeholder_system', 'stakeholder_stakeholder'));

COMMENT ON COLUMN conflict_register.source_class IS
    'Origin discriminator added in mig023 (Gate 3A). system_system = auto-detected '
    'from ingest (mig018 baseline). stakeholder_system / stakeholder_stakeholder = '
    'proposals from human elicitation.';

-- =============================================================================
-- 6. decision_traces VIEW — extended with 4th UNION ALL branch
--    Replaces the mig020 definition; additive only (no branch removed).
--    4th branch: change_proposal_decisions, trace_type='proposal_decision'
--    (renamed from alignment_decisions / 'align_decision' 2026-06-12).
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
    a.actor                  AS agent,
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
JOIN resolver_hitl_queue q ON a.hitl_queue_id = q.hitl_queue_id

UNION ALL

SELECT
    ad.id                    AS trace_id,
    'proposal_decision'      AS trace_type,
    ad.tenant_id             AS tenant_id,
    ad.entity_id             AS entity_id,
    ad.decided_by            AS agent,
    ad.decision              AS decision_type,
    ad.proposal_type         AS concept,
    NULL::text               AS conflict_class,
    NULL::text               AS period,
    ad.decision              AS outcome,
    ad.decision_note         AS rationale,
    ad.payload               AS payload,
    NULL::jsonb              AS result_summary,
    jsonb_build_object(
        'proposal_id',           ad.proposal_id,
        'canonical_artifact_id', ad.canonical_artifact_id
    )                        AS refs,
    ad.decided_at            AS occurred_at,
    ad.decided_at            AS ingested_at,
    NULL::timestamptz        AS superseded_at
FROM change_proposal_decisions ad;

COMMENT ON VIEW decision_traces IS
    'ContextOS §9 Gate 2A + Gate 3A: unified decision-trace store over mai_mcp_audit, '
    'conflict_dispositions⋈conflict_register, resolver_hitl_audit⋈resolver_hitl_queue, '
    'change_proposal_decisions. Read-only; the four base tables remain the only write paths. '
    'Exposes no run_id field (I1). mig023 adds the 4th branch (proposal_decision). '
    'Renamed from alignment_decisions / align_decision 2026-06-12 (Gate 3A→3B re-homing).';

COMMIT;
