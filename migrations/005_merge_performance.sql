-- Migration 005: merge_performance (SUPERSEDED — 2026-04)
--
-- Originally created two predicated indexes to speed up COFA merge_overview
-- queries in the legacy DCL-embedded ME engine:
--
--   idx_triples_concept_domain  (split_part(concept,'.',1), entity_id)
--       WHERE is_active = true
--   idx_triples_canonical_entity (canonical_id, entity_id)
--       WHERE canonical_id IS NOT NULL AND is_active = true
--
-- Both callers (_get_cofa_entity_ids, merge_overview, Section 1 overview,
-- Section 4 orphans) were moved out of DCL during the ME carveout
-- (see docs/ME_CARVEOUT_PLAN.md). DCL is SE-only per RACI — COFA and
-- merge_overview are Convergence's concern.
--
-- Both indexes are now maintained, without the is_active predicate, by
-- migration 016 (which also drops the vestigial is_active column).
--
-- This file must stay in the migrations directory as a no-op so
-- run_migration.py's glob picks it up in the expected order and doesn't
-- re-execute dead ME-motivated SQL that would fail after mig016.
DO $$ BEGIN
    RAISE NOTICE '[005] superseded: merge_performance indexes moved to mig016 (ME carveout)';
END $$;
