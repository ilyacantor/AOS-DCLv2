-- Migration 017: bi-temporal triple store (ContextOS Gate 0).
--
-- Every fact gains two timelines:
--   valid_from / valid_to     — when the fact is true in the world
--   ingested_at / superseded_at — when DCL learned it / stopped believing it
--
-- is_active is dropped as a stored flag and re-added as a STORED GENERATED
-- column ≡ (superseded_at IS NULL). Every reader keeps working unchanged
-- (same column name, same values, same partial-index predicates). Any writer
-- still doing `SET is_active = ...` fails loudly at the database — supersession
-- (SET superseded_at = now()) is the only lifecycle write.
--
-- Backfill semantics for pre-migration rows: ingested_at/valid_from come from
-- created_at (insertion was when we learned the fact); superseded_at for
-- already-inactive rows comes from updated_at (the deactivation write was the
-- only updater of that column on the swap path) — a knowledge-time
-- approximation, exact for all rows written after this migration.
--
-- History note: an earlier numbering of migrations 014-017 belonged to the
-- April 2026 current_triples store rebuild, which was deployed to prod
-- Apr 13-19 and backed out via a full store reset (see SCHEMA_CONTRACT.md
-- "Store lineage"). This file is unrelated to that line; bi-temporal
-- supersession supersedes that design.
--
-- Idempotent — safe to re-run (guards on column/index existence).

BEGIN;

SET LOCAL statement_timeout = '300s';

ALTER TABLE semantic_triples
    ADD COLUMN IF NOT EXISTS valid_from    TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS valid_to      TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS ingested_at   TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS superseded_at TIMESTAMPTZ;

UPDATE semantic_triples
   SET ingested_at   = created_at,
       valid_from    = created_at,
       superseded_at = CASE WHEN is_active THEN NULL ELSE updated_at END
 WHERE ingested_at IS NULL;

ALTER TABLE semantic_triples
    ALTER COLUMN ingested_at SET DEFAULT now(),
    ALTER COLUMN valid_from  SET DEFAULT now();
ALTER TABLE semantic_triples
    ALTER COLUMN ingested_at SET NOT NULL,
    ALTER COLUMN valid_from  SET NOT NULL;

-- Swap the stored flag for the generated definition. The four is_active
-- partial indexes drop with the column and are recreated identically below.
DROP INDEX IF EXISTS idx_triples_active;
DROP INDEX IF EXISTS idx_triples_active_run;
DROP INDEX IF EXISTS idx_triples_concept_domain;
DROP INDEX IF EXISTS idx_triples_canonical_entity;

ALTER TABLE semantic_triples DROP COLUMN IF EXISTS is_active;
ALTER TABLE semantic_triples
    ADD COLUMN is_active BOOLEAN GENERATED ALWAYS AS (superseded_at IS NULL) STORED NOT NULL;

CREATE INDEX IF NOT EXISTS idx_triples_active
    ON semantic_triples (tenant_id, is_active) WHERE is_active = true;
CREATE INDEX IF NOT EXISTS idx_triples_active_run
    ON semantic_triples (run_id) WHERE is_active = true;
CREATE INDEX IF NOT EXISTS idx_triples_concept_domain
    ON semantic_triples (split_part(concept, '.', 1), entity_id) WHERE is_active = true;
CREATE INDEX IF NOT EXISTS idx_triples_canonical_entity
    ON semantic_triples (canonical_id, entity_id)
    WHERE canonical_id IS NOT NULL AND is_active = true;

ANALYZE semantic_triples;

COMMIT;
