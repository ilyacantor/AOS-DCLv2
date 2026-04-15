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
- file:line: tests/e2e/operator_e2e.spec.ts, tests/test_mai_status.py
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

11. 2026-04-14 | https://claude.ai/chat/e6b5a4e3-8e6b-4ddb-a626-2c9b34618784 | scripts/precommit.sh:170 + main.py:549,649 | F1c regex `(^|[^a-z_])"run_id"[[:space:]]*:` only matches JSON-literal key form; misses Pydantic field declarations (`run_id: str`) and kwargs. main.py:549 and main.py:649 contain bare run_id violations that slipped through. Hook itself needs regex broadened to catch Pydantic and kwarg forms. Severity: degraded. Blocking: pipeline identity architecture I1 enforcement is incomplete; future API responses can re-introduce bare run_id without hook catching them.

12. 2026-04-14 | https://claude.ai/chat/e6b5a4e3-8e6b-4ddb-a626-2c9b34618784 | tenant_runs table, tenant 69688df3 | tenant_runs cap is set to 10 but observed 11 rows under canonical tenant 69688df3 — race condition in `swap_and_deactivate` / `upsert_tenant_run` allowing cap overflow. Not investigated further at the time. Severity: degraded. Blocking: cap invariant is not enforced; downstream "latest active run" assumptions can read stale or duplicate state.

13. 2026-04-07 | https://claude.ai/chat/f33e2a8e-1d4c-49d1-b375-336053e80c1c | is_active reader sites (legacy item 23) | separate sprint per Graph2 Phase C standing instruction. Severity: degraded. Blocking: identity layer cleanup.

14. 2026-04-07 | https://claude.ai/chat/f33e2a8e-1d4c-49d1-b375-336053e80c1c | cross-repo | tenant_id/entity_id canonicalization broader pattern across all 11 repos. Phase A entity_id-for-SnapshotMeta-lookup fix was correct local; broader concern open. Severity: degraded. Blocking: identity protection completeness beyond 8-layer architecture in CLAUDE.md.

15. 2026-04-15 | graph-v2 max-width session | tests/e2e/all_tabs_verify.spec.ts:86 (test 3) | dev-server `page.goto(.., { waitUntil: load })` blocks ~190-275s on Vite frontend (confirmed via trace: goto step alone 191s, click 67s, evalOnSelectorAll 22s on a fresh page). Reproduces on unmodified mai-v8-brain tree (stash test) — pre-existing, not caused by the rename / max-width work. Mitigation applied this session: switched to `waitUntil: domcontentloaded` and removed `fullPage: true` from screenshot. Underlying root cause (Vite dev cold-start / load-event hang on a 240+ element SVG page) not investigated. Severity: degraded. Blocking: B17 e2e gate flakiness for graph rendering tests.

16. 2026-04-15 | Brain-A Part 2b rename sweep | SWEEP_REPORT.md:16,29,30 | Brain-A maestra→mai rename sweep could not update SWEEP_REPORT.md because pre-commit hook blocks staging that file due to a pre-existing historical reference on line 54 that includes a Convergence-owned table name (Stage 4 historical prose documenting `_get_latest_tenant()` fallback fix from commit 0a89ca2). SWEEP_REPORT.md is a repo-root historical report file, not in the hook exclusion list (unlike CLAUDE.md, README.md, dcl_deferred_work.md, docs/*, ONGOING_PROMPTS/*). Residual Maestra references preserved verbatim on lines 16 (Sweep 3 row label), 29 and 30 (COFA Readiness item descriptions). Severity: cosmetic. Blocking: hook exclusion list extension for repo-root report files OR prose rewrite of historical line 54.
