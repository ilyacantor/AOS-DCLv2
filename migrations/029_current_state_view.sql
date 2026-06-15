-- Migration 029: semantic_triples_current — ContextOS Stage 2 canonical
-- current-state surface (a LOGICAL view over the bi-temporal store).
--
--   semantic_triples_current (VIEW) — the ONE definition of "current state":
--       the rows of semantic_triples whose knowledge window is still open
--       (superseded_at IS NULL, surfaced by the is_active generated column).
--     Surfacing + conflict + normalization current-state reads route through
--     this view so "current state" has a single canonical definition. As-of
--     reads do NOT use it — they carry the parameterized bi-temporal predicate
--     (ingested_at <= T AND (superseded_at IS NULL OR superseded_at > T))
--     against the base table, because point-in-time history lives below the
--     liveness flag.
--
-- This is NOT the reverted materialized `current_triples` table (the Apr 2026
-- store rebuild — flat live mirror + partitioned archive + swap_and_delete,
-- deployed Apr 13–19 then backed out via a full prod store reset; exists in no
-- environment — see SCHEMA_CONTRACT.md "STORE LINEAGE"). It is a read-only
-- logical view over the single bi-temporal substrate; the base table remains
-- the only write path (supersession via SET superseded_at = now()).
--
-- I1 note: the view exposes no field named run_id beyond the base column it
-- inherits; it adds no payload identifiers.
-- Additive only — creates a view, alters no column, rewrites no data.
-- Idempotent — CREATE OR REPLACE, safe to re-run.

BEGIN;

CREATE OR REPLACE VIEW semantic_triples_current AS
    SELECT * FROM semantic_triples WHERE is_active = true;

COMMENT ON VIEW semantic_triples_current IS
    'ContextOS Stage 2 canonical current-state surface: rows where superseded_at IS NULL (is_active generated column). Surfacing + conflict + normalization reads route through this view so "current state" has ONE definition. As-of reads bypass it (parameterized bi-temporal predicate). NOT the reverted materialized current_triples table (see SCHEMA_CONTRACT Store Lineage); this is a logical view over the bi-temporal store.';

COMMIT;
