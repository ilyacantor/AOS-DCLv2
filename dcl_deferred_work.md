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
