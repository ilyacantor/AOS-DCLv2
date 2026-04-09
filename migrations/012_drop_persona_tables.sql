-- Migration 012: Drop persona_profiles and persona_concept_relevance.
--
-- These tables were populated by backend/utils/config_sync.py from
-- config/persona_profiles.yaml, which used old ontology-style concept
-- IDs (gl_account, invoice) that never matched the triple domain
-- prefixes (gl, revenue) emitted by Farm. The parallel code path in
-- backend/engine/persona_view.py that queried them was dormant only
-- because DATABASE_URL drove _use_defaults back to the YAML mapping
-- — same bug class as the L3 orphan silent-skip fix (plan
-- cosmic-watching-yao).
--
-- Source of truth for persona→domain mapping is now exclusively
-- config/persona_domains.yaml, consumed by PersonaView and enforced
-- at ingest-time and graph-build-time fail-loud checks.
--
-- Forward migration:
DROP TABLE IF EXISTS persona_concept_relevance;
DROP TABLE IF EXISTS persona_profiles;

-- Reverse migration (manual, do not auto-run):
-- CREATE TABLE persona_profiles (...);
-- CREATE TABLE persona_concept_relevance (...);
-- See git history prior to migration 012 for schema.
