-- Migration 015: Store rebuild — data backfill (SUPERSEDED)
--
-- The original PL/pgSQL DO block stalled on a catch-all NOT EXISTS
-- anti-join against the full semantic_triples table (~2.5M rows), so the
-- backfill was rewritten in Python and split into two scripts:
--
--   scripts/apply_mig015.py    — Part A (single txn)
--     - load current_triples from tenant_runs
--     - ANALYZE current_triples
--     - backfill tenant_runs.run_row_count + previous_run_row_count
--     - per-entity sweep for known tenant/entity tuples
--     - commit
--
--   scripts/apply_mig015_b.py  — Part B (batched, many small txns)
--     - CTID/id-paged 50k-row batches over semantic_triples
--     - archive orphans whose id is not in current_triples
--     - commit per batch
--
-- For fresh deployments: after mig014 applies, run
--   python scripts/apply_mig015.py
--   python scripts/apply_mig015_b.py
-- once, before bringing any writer online.
--
-- This file is intentionally a no-op so run_migration.py's glob picks
-- it up in the expected order without re-executing the stalled logic.
DO $$ BEGIN
    RAISE NOTICE '[015] superseded by scripts/apply_mig015.py + apply_mig015_b.py';
END $$;
