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
- TOUCHED 2026-04-15 brain-A: NOT RESOLVED. Plan called for physical drop of engagement_state in Brain-A Part 3 (`dcl/migrations/018_drop_engagement_state_run_ledger.sql`). Pre-audit found engagement_state actively written by `farm/scripts/e2e_convergence.py:241` and seeded by `convergence/migrations/006_seed_engagement.sql`; run_ledger has 12 active rows shared with Convergence reads. Drop would break the demo. import_engagements.py ran (0+2 rows merged into Convergence). Full ownership transfer requires Farm e2e refactor + convergence migration 006 rewrite + DCL test update + hook scope update — see `platform_deferred_work.md` entry #7.

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
- TOUCHED 2026-04-16 refresh-bugfix session: NOT RESOLVED. This session fixed a different tenant_runs race — non-atomic run_row_count update in append_rows_for_entity (see item 18 RESOLVED). The cap-overflow race described here sits in swap_and_delete / upsert_tenant_run on the INSERT path (cap enforcement under concurrent first-time entity inserts), not the run_row_count UPDATE path. Test `test_tenant_runs_per_tenant_cap` currently passes on the live DB. Separate fix required when/if overflow reproduces — add an advisory lock around the cap check + insert, or a pre-insert count under serializable isolation.

13. 2026-04-07 | https://claude.ai/chat/f33e2a8e-1d4c-49d1-b375-336053e80c1c | is_active reader sites (legacy item 23) | separate sprint per Graph2 Phase C standing instruction. Severity: degraded. Blocking: identity layer cleanup.

14. 2026-04-07 | https://claude.ai/chat/f33e2a8e-1d4c-49d1-b375-336053e80c1c | cross-repo | tenant_id/entity_id canonicalization broader pattern across all 11 repos. Phase A entity_id-for-SnapshotMeta-lookup fix was correct local; broader concern open. Severity: degraded. Blocking: identity protection completeness beyond 8-layer architecture in CLAUDE.md.

15. 2026-04-15 | graph-v2 max-width session | tests/e2e/all_tabs_verify.spec.ts:86 (test 3) | dev-server `page.goto(.., { waitUntil: load })` blocks ~190-275s on Vite frontend (confirmed via trace: goto step alone 191s, click 67s, evalOnSelectorAll 22s on a fresh page). Reproduces on unmodified mai-v8-brain tree (stash test) — pre-existing, not caused by the rename / max-width work. Mitigation applied this session: switched to `waitUntil: domcontentloaded` and removed `fullPage: true` from screenshot. Underlying root cause (Vite dev cold-start / load-event hang on a 240+ element SVG page) not investigated. Severity: degraded. Blocking: B17 e2e gate flakiness for graph rendering tests.

16. 2026-04-15 | Brain-A Part 2b rename sweep | SWEEP_REPORT.md:16,29,30 | Brain-A maestra→mai rename sweep could not update SWEEP_REPORT.md because pre-commit hook blocks staging that file due to a pre-existing historical reference on line 54 that includes a Convergence-owned table name (Stage 4 historical prose documenting `_get_latest_tenant()` fallback fix from commit 0a89ca2). SWEEP_REPORT.md is a repo-root historical report file, not in the hook exclusion list (unlike CLAUDE.md, README.md, dcl_deferred_work.md, docs/*, ONGOING_PROMPTS/*). Residual Maestra references preserved verbatim on lines 16 (Sweep 3 row label), 29 and 30 (COFA Readiness item descriptions). Severity: cosmetic. Blocking: hook exclusion list extension for repo-root report files OR prose rewrite of historical line 54.

17. 2026-04-15 | Brain-A Part 2b rename sweep | tests/test_s1_seed.py (11 tests), data/seed_manifest.json | seed_manifest.json points to dcl_ingest_id=e7f9884c-1ace-4c8a-ae05-5cc8dd23e36b (2026-04-14) but semantic_triples current state reflects a different run_id (observed 66c37bd6-7f58-4848-a576-81ca0d7b169e in test stdout). Pre-existing staleness — reproduces on HEAD without working tree changes (verified via git stash before test run). All 11 S1-seed tests fail with "Expected 50 sample triples, got 0" / "No triples found for manifest run_id=e7f9884c" / BS/CF/cash-continuity identity failures. Not caused by maestra→mai rename. Severity: degraded. Blocking: pipeline seed freshness guarantee; harness validity per B15 ("Pipeline must run before the harness").

18. 2026-04-15 | Brain-A Part 2b rename sweep | tests/test_store_invariants.py::test_semantic_equals_sum_run_row_count, tests/test_store_invariants.py::test_per_entity_count_matches_run_row_count | semantic_triples count (471434) != SUM(tenant_runs.run_row_count) (414919), drift = 56515; 10 (tenant, entity) pairs drifted between tenant_runs.run_row_count and current_triples (e.g., CoreLabs-N2UA 15000 vs 20086). Related to open deferred item 12 (tenant_runs cap overflow race). Pre-existing — reproduces on HEAD without working tree changes. Not caused by maestra→mai rename. Severity: degraded. Blocking: store-rebuild pointer-swap invariant; follow-up to item 12. RESOLVED 2026-04-16: root cause was non-atomic read/compute/write of run_row_count in backend/db/triple_store.py::append_rows_for_entity — concurrent append batches under the same (tenant, entity) from Farm's DCLTriplePusher (max_concurrency=2, 5000 rows/batch) read the same prev value, computed prev + batch_size in Python, then overwrote with EXCLUDED.run_row_count, losing one batch's contribution. Fix: removed the pre-read and changed the ON CONFLICT branch to `SET run_row_count = tenant_runs.run_row_count + EXCLUDED.run_row_count` with len(new_rows) as the delta. The UPSERT row lock serializes concurrent increments. All historical drift cleared by re-Refreshing the 9 drifted entities through the fixed path. Post-fix: semantic_triples=456023, SUM(run_row_count)=456023, drift=0; all 6 test_store_invariants.py tests green.

19. 2026-04-16 | Ingest Refresh pull session | backend/api/routes/ingest_triples.py:671 (refresh endpoint), backend/farm/client.py:496 (list_triple_runs) | DCL's `/api/dcl/refresh-from-farm` was implemented against the wrong Farm feed: `GET /api/business-data/triple-runs` is the Convergence-overlay manifest list (always `mode=multi_entity`, routed to Convergence at farm/src/api/business_data.py:1203). The correct SE feed is `GET /api/runs` (Farm port 8003, backed by `manifest_runs` table), which returns `{farm_run_id, run_id, tenant_id, entity_id, dcl_run_id, status, created_at, rows_accepted}` per SE manifest-intake execution. Live query on 2026-04-16: 10 distinct SE entities present in `/api/runs`, including every entity currently in DCL's dropdown. Feature as shipped returns "No Farm runs newer than DCL" on every call because triple-runs has zero SE manifests. Fix is to swap the feed + filter to `/api/runs` and drop the `mode=multi_entity` filter (wrong concern — the SE feed never tags mode). Push path also needs rethink: SE runs originally ingested via Farm manifest-intake → `/api/dcl/ingest` (legacy), not via `/triple-runs/{id}/push-to-dcl` → `/api/dcl/ingest-triples`. Severity: blocker. Blocking: the feature does not achieve its stated goal against real Farm state — Refresh cannot detect or re-ingest newer SE Farm runs. RESOLVED 2026-04-16 (commit 3a2fcca, farm commit fcefae5): Farm now exposes POST /api/runs/{farm_run_id}/push-to-dcl; DCL Refresh consumes GET /api/runs and pushes via that replay endpoint. Live verification: VeloCorp-KY0F refreshed with 20111 triples, dcl_ingest_id=9c427619-50c8-510c-a64d-c569580134b0; second call idempotent. Playwright ingest_refresh_pull.spec.ts: 4/4 pass.

20. 2026-04-16 | refresh-bugfix session | tests/test_s1_seed.py (11 tests), data/seed_manifest.json | Pre-session check: test_s1_seed.py::test_01_triples_exist failed independently of this session's refresh/atomic-count work. seed_manifest.json still points at dcl_ingest_id=e7f9884c-1ace-4c8a-ae05-5cc8dd23e36b (2026-04-14) which is no longer in semantic_triples. Same root cause as deferred #17 (duplicate; captured here for this session's D6 trail, not a new defect). Severity: degraded. Blocking: per-session harness validity. Not resolved this session — seed refresh is out of scope for an Ingest Refresh bugfix.
