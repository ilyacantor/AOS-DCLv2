-- Migration 028: Value normalization layer (currency / unit-scale / date).
--
--   semantic_triples.normalization_metadata  — per-row JSONB capturing what the
--                            write-time normalizer changed (raw_value, raw_unit,
--                            raw_currency, raw_period, scale_factor, fx_rate).
--                            NULL when nothing was normalized — the no-op case
--                            stamps no metadata, so the column reads "untouched"
--                            for the overwhelming base/USD majority.
--   tenant_normalization_policy — per-tenant canonical currency + FX rate book
--                            (tenant '*' = default: USD, no rates). The write-time
--                            normalizer loads this once per ingest and converts
--                            every TriplePayload to the tenant canonical BEFORE
--                            conflict detection compares values.
--
-- Normalization happens at the ONE ingest persist chokepoint (raw original
-- preserved in normalization_metadata), so cross-source value conflicts are
-- compared in one canonical shape (USD, base unit, one period representation)
-- instead of being hidden or spuriously flagged by unit/currency/format skew.
--
-- Idempotent — safe to re-run.

BEGIN;

ALTER TABLE semantic_triples ADD COLUMN IF NOT EXISTS normalization_metadata JSONB;

CREATE TABLE IF NOT EXISTS tenant_normalization_policy (
    tenant_id          TEXT PRIMARY KEY,
    canonical_currency TEXT NOT NULL DEFAULT 'USD',
    fx_rates           JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Seed default (tenant '*'): canonical currency USD, no FX rates. A tenant that
-- ingests a non-USD currency with no configured rate fails loud at write time —
-- the default is "USD only", not "assume parity".
INSERT INTO tenant_normalization_policy (tenant_id) VALUES ('*')
ON CONFLICT (tenant_id) DO NOTHING;

COMMIT;
