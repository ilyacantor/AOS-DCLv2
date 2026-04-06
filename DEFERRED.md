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

9. **Event stream ontology concepts** (Phase C audit) — Populating
   event_bus fabric plane requires new ontology concepts for streaming
   infrastructure (candidates: event_stream.message_throughput,
   event_stream.consumer_lag, event_stream.topic_count,
   event_stream.partition_count, event_stream.event_volume). None exist
   in config/ontology_concepts.yaml today. Routing table is ready
   (FABRIC_VENDOR_INFO has confluent/kafka/eventbridge/rabbitmq/pulsar/
   azure_event_hubs); blocker is ontology, not routing. Requires design
   pass on concept names, units, temporal model, and which SoR manifest
   fields feed them. Blocks Phase C event_bus generator.
