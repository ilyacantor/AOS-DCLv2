-- Migration 011: Add fabric plane columns to semantic_triples.
--
-- Fabric planes are the architecturally load-bearing layer in AOS
-- (the "4 planes vs 200 connectors" leverage model). These columns
-- denormalize fabric assignment onto each triple at write time.
--
-- Source of truth: Farm SnapshotMeta preset at generation time,
-- AAM pipe metadata at production ingest time.
--
-- Forward migration:
ALTER TABLE semantic_triples ADD COLUMN IF NOT EXISTS fabric_plane TEXT;
ALTER TABLE semantic_triples ADD COLUMN IF NOT EXISTS fabric_product TEXT;

CREATE INDEX IF NOT EXISTS idx_triples_fabric
  ON semantic_triples (entity_id, fabric_plane, fabric_product)
  WHERE fabric_plane IS NOT NULL;

-- Reverse migration (manual, do not auto-run):
-- DROP INDEX IF EXISTS idx_triples_fabric;
-- ALTER TABLE semantic_triples DROP COLUMN IF EXISTS fabric_product;
-- ALTER TABLE semantic_triples DROP COLUMN IF EXISTS fabric_plane;
