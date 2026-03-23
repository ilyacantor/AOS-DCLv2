-- Ingest activity log: one row per successful ingest-triples call.
-- Captures timing, counts, and rejection metadata for the Ingest monitoring tab.

CREATE TABLE IF NOT EXISTS ingest_log (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id            UUID NOT NULL,
    entity_id         TEXT,
    tenant_id         UUID NOT NULL,
    triples_received  INTEGER NOT NULL,
    triples_written   INTEGER NOT NULL,
    triples_rejected  INTEGER NOT NULL DEFAULT 0,
    rejection_reasons JSONB DEFAULT '[]'::jsonb,
    source_systems    TEXT[] DEFAULT '{}',
    duration_ms       INTEGER NOT NULL,
    created_at        TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ingest_log_run ON ingest_log (run_id);
CREATE INDEX IF NOT EXISTS idx_ingest_log_entity ON ingest_log (entity_id);
CREATE INDEX IF NOT EXISTS idx_ingest_log_created ON ingest_log (created_at DESC);
