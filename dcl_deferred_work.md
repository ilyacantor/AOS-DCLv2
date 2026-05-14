# Deferred Work

Items captured as out-of-scope during active sprints. Each entry names the
originating sprint so future agents can trace the decision.

---

1. **Phase 4 consumer migration** (graph2 L1 sprint) — Secondary/tertiary
   query paths (contextualization_summary, get_persona_domain_stats,
   dashboard_data in triple_monitor.py) do not yet use concept authority
   ranking. Only get_sankey_aggregation path is authority-aware.

2. **Phase 6 Playwright gate** (graph2 L1 sprint) — No Playwright e2e test
   verifying graph2 renders four fabric planes, new SoR nodes, or collisions
   metadata. B17 requires frontend verification.

3. **Test parameterization** (Day 0 sprint) — tests/e2e/operator_e2e.spec.ts
   and tests/test_maestra_status.py hardcode "Meridian"/"Cascadia" entity
   names. Pre-commit hook exempts tests/ from these patterns as a temporary
   measure. Tests predate dynamic entity support.

4. **Migration ownership cleanup** (Day 0 sprint) — engagement_state table
   is defined in DCL migration 001_semantic_triple_store.sql:93 but is
   Convergence-owned per RACI v8.3. Pre-commit hook exempts
   test_s1_dcl.py from this pattern as a marker. The migration itself is
   the constitutional violation, not the test.

5. **HITL collision review UI** (Day 0 sprint) — snapshot.meta.collisions
   carries authority-ranked collision data. Graph2 receives it but no
   rendering badge or operator review surface exists yet.

6. **MCP server for CC log reader** — Spec tracked in separate chat.

7. **Identity generator** (Phase C audit) — Okta SoR resolves via
   SnapshotMeta bridge but no generator emits identity-domain triples.
   Requires ontology design for identity concepts (user_count,
   role_assignments, auth_events, or equivalent). Do not invent concepts;
   surface gap first.

8. **Analytics breadth** (Phase C audit) — Snowflake/Tableau/Looker
   generators. data_warehouse plane populated by AWS Cost Explorer
   (96 triples) is sufficient for fourth-plane objective but thin for
   real analytics coverage.

10. **RAG call instrumentation** (Prod-mode AI/RAG wiring sprint, 2026-04-28) —
    `_apply_prod_mode_ai` (`backend/engine/dcl_engine.py`) reports nothing
    operator-visible about LLM validation activity. No counter for validator
    invocations, no latency histogram, no per-run summary of corrections
    applied vs proposed. Stubbed due to in-flight graph rebuild work that
    consumes the same narration / metrics surface. Until lifted, an operator
    running Prod cannot tell from the UI whether AI fired or how it behaved.

11. **RAG learn instrumentation** (Prod-mode AI/RAG wiring sprint, 2026-04-28) —
    `RAGService.store_mapping_lessons` (`backend/engine/rag_service.py`)
    reports lesson counts via narration only; no operator-visible aggregate
    of Pinecone writes per run, dedupe rate, embedding latency, or vector
    growth over time. Same root cause as #10: instrumentation surface is
    stubbed during graph rebuild. Resolve alongside #10.

9. ~~**Event stream ontology concepts**~~ — **RESOLVED.** event_stream
   concept (IT-012) added to ontology_concepts.yaml in both DCL and
   Convergence. Farm generates 60 event_stream triples per entity
   (topic_count, partition_count, message_throughput, consumer_lag,
   event_volume) via EventStreamTripleGenerator. Routing table maps 6
   streaming vendors to event_bus plane. Graph v2 now renders 4 fabric
   planes (ipaas, api_gateway, data_warehouse, event_bus). Playwright
   verified.

12. ~~**Filename alignment with constitution**~~ — **RESOLVED 2026-05-05.**
    Renamed `DEFERRED.md` → `dcl_deferred_work.md` via `git mv`. Repo grep
    shows no remaining references to the old name except (a) the
    constitution itself forbidding it (CLAUDE.md:550, correct), and (b)
    this entry's own historical text. Pending commit in same session as
    rename.

13. **Split shared secrets between `.env` and `.env.development`** (env config
    session, 2026-05-05) — Redis URL, OpenAI/Anthropic/Pinecone keys, Render
    API key, INTERNAL_SERVICE_KEY, and DCL_INGEST_KEY are byte-identical
    across prod and dev env files. Risks: dev evicting prod Redis keys, no
    cost attribution per env, single revocation kills both, dev token
    unlocks prod services. Defer until a triggering event: multi-user prod
    traffic, cost attribution becomes needed, leak/incident, or a second
    engineer joins. At that point provision dev-scoped keys, rotate prod-
    first to keep prod live, and split. severity: degraded | blocking:
    nothing today.

14. 2026-05-13 | wp2-cloud-spend | tests/test_s1_seed.py:* | seven seed-data
    tests (test_02_all_entities_present, test_03_revenue_positive_entity_0,
    test_05_pl_identity, test_06_bs_identity, test_07_cf_identity,
    test_08_cash_continuity, test_15_period_coverage) assert against
    data/seed_manifest.json which references `ManualProbe-SE01` and farm_run
    `farm_manifest_20260401_161114_461543b5`. The DCL DB has 89 different
    entities (AeroEdge, ApexFlow, etc.) — the manifest is stale relative to
    the active DB. Same 7 failures present before WP2 changes (verified by
    `git stash && pytest tests/test_s1_seed.py`). Root cause is upstream
    seed pipeline drift, not ontology. Resolve by re-running the seed
    pipeline (Farm + AAM + DCL ingest) and writing a fresh
    seed_manifest.json. severity: degraded | blocking: nothing in WP2 scope;
    blocks any future change that requires tests/test_s1_seed.py clean.
    RESOLVED 2026-05-13 (commit 0b15111) — fixed _update_seed_manifest
    producer bug (preserved stale entities/farm_run_id across runs) and
    regenerated the manifest via Farm push-triples; 143 PASS / 0 FAIL.

15. 2026-05-13 | wp2-cloud-spend | config/ontology_concepts.yaml:4180-4250 |
    Five concept entries added in commit 8869f47 reused existing ids
    (`pipeline`, `event`, `datadog`, `aws_cost`, `jira`) with new
    concept_ids (SAL-100, IT-101/102/103, ENG-101). The
    `_load_from_yaml()` enforcer rejected the duplicate ids and the engine
    silently fell back to a 3-entry stub for months. WP2 renamed the five
    later entries to unique ids (`sales_pipeline_metrics`,
    `event_stream_metrics`, `datadog_observability`, `aws_cost_aggregate`,
    `jira_project_metrics`) to unblock the loader. Persona-domains entries
    still reference the canonical root ids (`pipeline`, `event`, `datadog`,
    `aws_cost`, `jira`) which now map to the original PRD-/IT-coded
    entries — the second set is mappable but currently unreferenced by any
    persona. Decide whether to keep the duplicate-info entries, merge their
    example_fields into the canonical entries, or wire the rename into
    persona_domains and Farm generators. severity: degraded | blocking:
    nothing today; the loader works.
