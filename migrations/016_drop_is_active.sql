-- Migration 016: drop is_active column from semantic_triples.
--
-- Post–store-rebuild the column is vestigial: current_triples is the live
-- slice by construction and semantic_triples_archive holds evicted runs.
-- Nothing reads the column anymore; the three indexes that predicated on it
-- are rebuilt without the WHERE clause so the planner keeps its hot paths.
--
-- Indexes rebuilt in place:
--   idx_triples_active           (tenant_id)
--   idx_triples_concept_domain   (split_part(concept,'.',1), entity_id)
--   idx_triples_canonical_entity (canonical_id, entity_id) WHERE canonical_id IS NOT NULL
--
-- Cross-repo consumer: convergence/backend/engine/cofa_mapping.py
-- must be repointed off `semantic_triples WHERE is_active = true` in the
-- same session — see WP notes.

BEGIN;

DROP INDEX IF EXISTS idx_triples_active;
DROP INDEX IF EXISTS idx_triples_concept_domain;
DROP INDEX IF EXISTS idx_triples_canonical_entity;

ALTER TABLE semantic_triples DROP COLUMN IF EXISTS is_active;

CREATE INDEX IF NOT EXISTS idx_triples_active
  ON semantic_triples (tenant_id);

CREATE INDEX IF NOT EXISTS idx_triples_concept_domain
  ON semantic_triples (split_part(concept, '.', 1), entity_id);

CREATE INDEX IF NOT EXISTS idx_triples_canonical_entity
  ON semantic_triples (canonical_id, entity_id)
  WHERE canonical_id IS NOT NULL;

ANALYZE semantic_triples;

COMMIT;
