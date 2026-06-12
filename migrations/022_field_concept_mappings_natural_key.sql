-- Migration 022: field_concept_mappings natural-key uniqueness
-- (deferred #76 — records-path live mapping writer).
--
-- save_mappings() (backend/semantic_mapper/persist_mappings.py) upserts via
--     ON CONFLICT (source_id, table_name, field_name, concept_id)
-- but the table carried only a serial `id` primary key and NO unique index on
-- that natural key, so every upsert raised InvalidColumnReference ("there is no
-- unique or exclusion constraint matching the ON CONFLICT specification"). That
-- is the real reason aos-dev had ZERO mappings and /api/dcl/resolve always
-- ended at "No sources found": the canonical writer could never land a row.
-- This back-fills the natural-key unique index the upsert requires.
--
-- Topology note: on prod / fresh installs field_concept_mappings is a real
-- table. On aos-dev it is an auto-updatable VIEW (public.field_concept_mappings)
-- onto a prod-mirror base table (shared_yuxrdo.field_concept_mappings), reached
-- via search_path. The index must live on the BASE TABLE — that is where the
-- ON CONFLICT arbiter is matched — so this resolves the view to its base table
-- before creating the index.
--
-- Additive only — one unique index, no data change. Idempotent (IF NOT EXISTS).
-- The table is empty in dev, so the build needs no dedupe pass / CONCURRENTLY.

DO $$
DECLARE
    v_target regclass;
    v_kind   "char";
BEGIN
    -- Resolve via search_path first, then public explicitly. to_regclass()
    -- returns NULL instead of raising, so a role that cannot see the
    -- relation (aos-dev dev_user: the view lives in public, the base table
    -- in shared_yuxrdo, neither on its runner search_path — the index there
    -- was applied once via the privileged role, ledger #76) skips with a
    -- NOTICE below instead of aborting every boot-time migration pass on
    -- re-apply. Not-visible-on-re-apply is an environment fact, not a
    -- failure; anywhere the relation IS visible, the index still applies.
    v_target := COALESCE(
        to_regclass('field_concept_mappings'),
        to_regclass('public.field_concept_mappings')
    );

    IF v_target IS NULL THEN
        RAISE NOTICE 'mig022: field_concept_mappings not visible from this role/search_path — natural-key index was applied via the privileged role (ledger #76); nothing to re-apply here.';
        RETURN;
    END IF;

    SELECT relkind INTO v_kind FROM pg_class WHERE oid = v_target;

    IF v_kind = 'v' THEN
        -- Follow the auto-updatable view to its single base table.
        SELECT dep.refobjid::regclass
          INTO v_target
          FROM pg_rewrite rw
          JOIN pg_depend  dep ON dep.objid = rw.oid AND dep.deptype = 'n'
          JOIN pg_class   bc  ON bc.oid = dep.refobjid AND bc.relkind = 'r'
         WHERE rw.ev_class = v_target
         LIMIT 1;
    END IF;

    -- Explicit catalog existence check rather than CREATE ... IF NOT EXISTS:
    -- PG enforces table ownership BEFORE the IF NOT EXISTS short-circuit, so
    -- a restricted role (dev_user) cannot even no-op the re-apply. The
    -- catalog read is permission-free; a TRUE absence still attempts CREATE
    -- and fails loudly when the role lacks rights — absence is never masked.
    IF EXISTS (
        SELECT 1
          FROM pg_index i
          JOIN pg_class ic ON ic.oid = i.indexrelid
         WHERE i.indrelid = v_target
           AND ic.relname = 'field_concept_mappings_natural_key_uq'
    ) THEN
        RAISE NOTICE 'mig022: natural-key index already present on % — nothing to re-apply.', v_target;
        RETURN;
    END IF;

    EXECUTE format(
        'CREATE UNIQUE INDEX field_concept_mappings_natural_key_uq '
        'ON %s (source_id, table_name, field_name, concept_id)',
        v_target
    );
END $$;
