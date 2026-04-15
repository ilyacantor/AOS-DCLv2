# Deferred work — dcl

Items migrated from the legacy DEFERRED file on 2026-04-15. Schema fields per
item: number, date, chat-ref, file:line, severity, blocking, reason.

---

## 1. Phase 4 consumer migration
- date: 2026-04-15
- chat-ref: migrated from legacy DEFERRED
- file:line: backend/api/routes/triple_monitor.py (contextualization_summary, get_persona_domain_stats, dashboard_data)
- severity: degraded
- blocking: authority-aware ranking across all DCL query paths
- reason: (graph2 L1 sprint) Secondary/tertiary query paths do not yet use concept authority ranking. Only the get_sankey_aggregation path is authority-aware.

## 2. Phase 6 Playwright gate
- date: 2026-04-15
- chat-ref: migrated from legacy DEFERRED
- file:line: n/a
- severity: blocker
- blocking: B17 compliance for graph2 release
- reason: (graph2 L1 sprint) No Playwright e2e test verifying graph2 renders four fabric planes, new SoR nodes, or collisions metadata. B17 requires frontend verification.

## 3. Test parameterization
- date: 2026-04-15
- chat-ref: migrated from legacy DEFERRED
- file:line: tests/e2e/operator_e2e.spec.ts, tests/test_maestra_status.py
- severity: degraded
- blocking: dynamic entity test support; removal of tests/* hook exemption
- reason: (Day 0 sprint) Tests hardcode the two default entity names from the early demo set. Pre-commit hook exempts tests/ from these patterns as a temporary measure. Tests predate dynamic entity support.

## 4. Migration ownership cleanup
- date: 2026-04-15
- chat-ref: migrated from legacy DEFERRED
- file:line: migrations/001_semantic_triple_store.sql:93
- severity: degraded
- blocking: clean RACI boundary between DCL and Convergence
- reason: (Day 0 sprint) engagement_state table is defined in DCL migration 001_semantic_triple_store.sql:93 but is Convergence-owned per RACI v8.3. Pre-commit hook exempts test_s1_dcl.py from this pattern as a marker. The migration itself is the constitutional violation, not the test.

## 5. HITL collision review UI
- date: 2026-04-15
- chat-ref: migrated from legacy DEFERRED
- file:line: n/a
- severity: degraded
- blocking: operator HITL collision review workflow
- reason: (Day 0 sprint) snapshot.meta.collisions carries authority-ranked collision data. Graph2 receives it but no rendering badge or operator review surface exists yet.

## 6. MCP server for CC log reader
- date: 2026-04-15
- chat-ref: migrated from legacy DEFERRED
- file:line: n/a
- severity: cosmetic
- blocking: CC log visibility tooling
- reason: Spec tracked in a separate chat.

## 7. Identity generator
- date: 2026-04-15
- chat-ref: migrated from legacy DEFERRED
- file:line: n/a
- severity: degraded
- blocking: identity-domain triple coverage
- reason: (Phase C audit) Okta SoR resolves via SnapshotMeta bridge but no generator emits identity-domain triples. Requires ontology design for identity concepts (user_count, role_assignments, auth_events, or equivalent). Do not invent concepts; surface gap first.

## 8. Analytics breadth
- date: 2026-04-15
- chat-ref: migrated from legacy DEFERRED
- file:line: n/a
- severity: degraded
- blocking: production-grade data_warehouse plane coverage
- reason: (Phase C audit) Snowflake/Tableau/Looker generators missing. data_warehouse plane populated by AWS Cost Explorer (96 triples) is sufficient for fourth-plane objective but thin for real analytics coverage.

## 9. Event stream ontology concepts — RESOLVED
- date: 2026-04-15
- chat-ref: migrated from legacy DEFERRED
- file:line: config/ontology_concepts.yaml
- severity: resolved
- blocking: n/a (resolved)
- reason: event_stream concept (IT-012) added to ontology_concepts.yaml in both DCL and Convergence. Farm generates 60 event_stream triples per entity (topic_count, partition_count, message_throughput, consumer_lag, event_volume) via EventStreamTripleGenerator. Routing table maps 6 streaming vendors to event_bus plane. Graph v2 now renders 4 fabric planes (ipaas, api_gateway, data_warehouse, event_bus). Playwright verified.

## 10. Installed pre-commit hook drift
- date: 2026-04-15
- chat-ref: this session
- file:line: .git/hooks/pre-commit
- severity: degraded
- blocking: any commit that trips legacy whitelist patterns until manually re-synced via `cp scripts/precommit.sh .git/hooks/pre-commit`
- reason: installed pre-commit hook drifted from scripts/precommit.sh (4 extra whitelist entries around 188/205/216, still references DEFERRED.md at line 27). Hook installation is manual — needs to become a make target or post-checkout automation so source/installed never diverge.
