-- Migration 014: Store rebuild — schema
--
-- Rebuilds DCL store architecture to eliminate soft-deactivate bloat.
--   semantic_triples_archive : append-only history of displaced runs, monthly partitions
--   current_triples          : flat live mirror, one row per logical triple, no is_active/run_id
--   tenant_runs.run_row_count: precomputed O(1) count per (tenant, entity)
--
-- Data move and count backfill happen in 015. This file is schema only.
-- Idempotent — safe to re-run.

-- =============================================================================
-- semantic_triples_archive — append-only history, partitioned by month
-- =============================================================================

CREATE TABLE IF NOT EXISTS semantic_triples_archive (
    LIKE semantic_triples INCLUDING DEFAULTS
) PARTITION BY RANGE (created_at);

-- Archive has no active/inactive distinction — every archived row is inactive
ALTER TABLE semantic_triples_archive DROP COLUMN IF EXISTS is_active;

CREATE INDEX IF NOT EXISTS idx_archive_tenant_run
    ON semantic_triples_archive (tenant_id, run_id);

-- =============================================================================
-- ensure_archive_partition — on-demand partition creation
-- =============================================================================

CREATE OR REPLACE FUNCTION ensure_archive_partition(ts timestamptz)
RETURNS text AS $$
DECLARE
    partition_name text;
    start_ts timestamptz;
    end_ts timestamptz;
BEGIN
    start_ts := date_trunc('month', ts);
    end_ts := start_ts + interval '1 month';
    partition_name := 'semantic_triples_archive_' || to_char(start_ts, 'YYYY_MM');

    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF semantic_triples_archive '
        'FOR VALUES FROM (%L) TO (%L)',
        partition_name, start_ts, end_ts
    );

    RETURN partition_name;
END;
$$ LANGUAGE plpgsql;

-- Pre-create partitions 2025-01..2026-12 so initial backfill never races
DO $$
DECLARE
    m timestamptz;
BEGIN
    FOR m IN
        SELECT generate_series(
            '2025-01-01'::timestamptz,
            '2026-12-01'::timestamptz,
            '1 month'::interval
        )
    LOOP
        PERFORM ensure_archive_partition(m);
    END LOOP;
END $$;

-- =============================================================================
-- current_triples — live mirror, no is_active / run_id / updated_at
-- =============================================================================

CREATE TABLE IF NOT EXISTS current_triples (
    id                    UUID PRIMARY KEY,
    tenant_id             TEXT NOT NULL,
    entity_id             TEXT NOT NULL,
    concept               TEXT NOT NULL,
    property              TEXT NOT NULL,
    value                 JSONB NOT NULL,
    period                TEXT,
    currency              TEXT DEFAULT 'USD',
    unit                  TEXT,
    source_system         TEXT NOT NULL,
    source_table          TEXT,
    source_field          TEXT,
    pipe_id               UUID,
    source_run_tag        TEXT,
    confidence_score      NUMERIC(3,2) NOT NULL CHECK (confidence_score >= 0 AND confidence_score <= 1),
    confidence_tier       TEXT NOT NULL CHECK (confidence_tier IN ('exact', 'high', 'medium', 'low')),
    canonical_id          UUID,
    resolution_method     TEXT,
    resolution_confidence NUMERIC(3,2),
    fabric_plane          TEXT,
    fabric_product        TEXT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_current_triples_lookup
    ON current_triples (tenant_id, entity_id, concept, property, period);

CREATE INDEX IF NOT EXISTS idx_current_triples_entity
    ON current_triples (tenant_id, entity_id);

CREATE INDEX IF NOT EXISTS idx_current_triples_concept_domain
    ON current_triples (split_part(concept, '.', 1), entity_id);

CREATE INDEX IF NOT EXISTS idx_current_triples_canonical
    ON current_triples (canonical_id, entity_id)
    WHERE canonical_id IS NOT NULL;

-- =============================================================================
-- tenant_runs — add run_row_count and previous_run_row_count
-- =============================================================================

ALTER TABLE tenant_runs
    ADD COLUMN IF NOT EXISTS run_row_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE tenant_runs
    ADD COLUMN IF NOT EXISTS previous_run_row_count INTEGER;
