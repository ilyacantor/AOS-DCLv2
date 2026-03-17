ALTER TABLE semantic_triples ADD COLUMN IF NOT EXISTS source_run_tag TEXT;
CREATE INDEX IF NOT EXISTS idx_triples_source_run_tag
  ON semantic_triples (source_run_tag) WHERE source_run_tag IS NOT NULL;
