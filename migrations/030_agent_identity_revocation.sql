-- Migration 030: Live agent-identity revocation + narrowing at the MCP boundary
-- (Gate 3C D2 — companion to 026 agent-identity registry).
--
-- Why: a minted MCP token embeds its 3-axis scope at mint time and keeps it
-- until expiry (mcp_auth verifies signature+exp only — it never consults the
-- registry). So today the ONLY way to revoke a live agent is to rotate the
-- global HMAC secret (blast-radius: every token) or wait for expiry. This
-- migration gives the boundary a per-call source of truth so an operator can
-- NARROW or REVOKE one identity and have the NEXT call enforce it.
--
--   1. mcp_agent_identities.revoked_at — NULL = active; non-NULL = revoked.
--      The enforcement path (mcp_server_real._call_tool) reads this live
--      (TTL-cached) and denies-loud when set.
--   2. mai_mcp_audit.identity — go-forward agent-identity NAME on each row so a
--      governance pane shows WHO by identity, not just a token hash. Historical
--      rows stay NULL (knowledge honestly not captured at the time — never
--      backfilled, mirrors the mig-020 enrichment rule).
--   3. decision_traces VIEW — mcp_call branch now projects
--      COALESCE(m.identity, m.caller_token_id) AS agent, so the unified trace
--      surface attributes calls to the identity when known and falls back to the
--      token hash for historical rows. Every other column and the other two
--      UNION branches are byte-identical to migration 020.
--
-- Additive only — no existing column altered or dropped, no data rewritten.
-- Idempotent — safe to re-run.
-- I1 note: the view exposes no field named run_id.
-- Apply to aos-dev ONLY via run_migration.py; prod gate at Gate 3C close
-- (same handling as migration 026).

BEGIN;

-- =============================================================================
-- 1. Revocation flag on the agent-identity registry (NULL = active).
-- =============================================================================

ALTER TABLE mcp_agent_identities
    ADD COLUMN IF NOT EXISTS revoked_at TIMESTAMPTZ;

COMMENT ON COLUMN mcp_agent_identities.revoked_at IS
    'Gate 3C D2: revocation timestamp. NULL = active. When non-NULL the MCP '
    'boundary denies every call by this identity loudly on the NEXT request '
    '(TTL-cached, ~5s) — no HMAC-secret rotation, no waiting for token expiry. '
    'Set via scripts/mcp_revoke.py --revoke; cleared with --restore.';

-- =============================================================================
-- 2. Go-forward agent-identity NAME on each audit row (historical rows NULL).
-- =============================================================================

ALTER TABLE mai_mcp_audit
    ADD COLUMN IF NOT EXISTS identity TEXT;

COMMENT ON COLUMN mai_mcp_audit.identity IS
    'Go-forward enrichment (mig 030): declared agent-identity name of the '
    'caller (e.g. finops-cloud-spend), when the token carried one. Historical '
    'rows and legacy/identity-less tokens stay NULL — never backfilled. A '
    'governance pane reads this to attribute calls by identity, not token hash.';

-- =============================================================================
-- 3. decision_traces — mcp_call branch projects COALESCE(identity, token_id).
--    Every other column + the conflict_disposition and er_confirmation
--    branches are byte-identical to migration 020.
-- =============================================================================

CREATE OR REPLACE VIEW decision_traces AS
SELECT
    m.audit_id            AS trace_id,
    'mcp_call'            AS trace_type,
    m.tenant_id           AS tenant_id,
    m.entity_id           AS entity_id,
    COALESCE(m.identity, m.caller_token_id) AS agent,
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
    'ContextOS §9 Gate 2A: unified decision-trace store over mai_mcp_audit, conflict_dispositions⋈conflict_register, resolver_hitl_audit⋈resolver_hitl_queue. Read-only; the three base tables remain the only write paths. Exposes no run_id field (I1). Mig 030: mcp_call.agent = COALESCE(identity, caller_token_id) — identity name when known, token hash for historical rows.';

COMMIT;
