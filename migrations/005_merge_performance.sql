-- 005_merge_performance.sql
-- Speed up merge_overview queries:
--   1. Expression index for COFA domain filtering (split_part on concept)
--   2. Composite index for canonical_id self-JOIN in resolution matches

-- Used by _get_cofa_entity_ids, Section 1 overview stats, Section 4 orphans
CREATE INDEX IF NOT EXISTS idx_triples_concept_domain
  ON semantic_triples (split_part(concept, '.', 1), entity_id)
  WHERE is_active = true;

-- Used by Section 3 resolution matches (canonical_id self-JOIN filtered by entity_id)
CREATE INDEX IF NOT EXISTS idx_triples_canonical_entity
  ON semantic_triples (canonical_id, entity_id)
  WHERE canonical_id IS NOT NULL AND is_active = true;
