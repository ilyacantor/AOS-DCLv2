-- Migration 017: denormalize enrichment aggregates into tenant_runs.
--
-- /api/dcl/snapshots must answer in <500ms (B18). Computing DISTINCT/ARRAY_AGG
-- over current_triples for a single tenant scans ~200K rows and takes 330ms+
-- in PG alone — over budget once API overhead is added. tenant_runs is
-- already the per-(tenant, entity) summary row (it precomputes run_row_count),
-- so the per-entity enrichment fields belong there too. Once maintained by
-- the write path, /api/dcl/snapshots becomes a trivial 10-row index scan.
--
-- Idempotent — safe to run multiple times.

-- Step 1: add nullable columns with safe defaults.
ALTER TABLE tenant_runs
    ADD COLUMN IF NOT EXISTS source_systems     TEXT[]      NOT NULL DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS fabric_pairs       TEXT[]      NOT NULL DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS unique_pipes       INTEGER     NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS first_received_at  TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS latest_received_at TIMESTAMPTZ;

-- Step 2: backfill from current_triples. Both tables store tenant_id as TEXT
-- (the pipeline identity contract stores the UUID as a string), so the join
-- is direct.
WITH agg AS (
    SELECT
        ct.tenant_id,
        ct.entity_id,
        COALESCE(
            ARRAY_AGG(DISTINCT ct.source_system) FILTER (WHERE ct.source_system IS NOT NULL),
            ARRAY[]::text[]
        ) AS source_systems,
        COALESCE(
            ARRAY_AGG(DISTINCT ct.fabric_plane || '|' || COALESCE(ct.fabric_product, '') || '|' || COALESCE(ct.source_system, ''))
                FILTER (WHERE ct.fabric_plane IS NOT NULL),
            ARRAY[]::text[]
        ) AS fabric_pairs,
        COUNT(DISTINCT ct.pipe_id) AS unique_pipes,
        MIN(ct.created_at) AS first_received_at,
        MAX(ct.created_at) AS latest_received_at
    FROM current_triples ct
    GROUP BY ct.tenant_id, ct.entity_id
)
UPDATE tenant_runs tr
SET source_systems     = agg.source_systems,
    fabric_pairs       = agg.fabric_pairs,
    unique_pipes       = agg.unique_pipes,
    first_received_at  = agg.first_received_at,
    latest_received_at = agg.latest_received_at
FROM agg
WHERE tr.tenant_id = agg.tenant_id
  AND tr.entity_id = agg.entity_id;
