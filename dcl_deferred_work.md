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

19. 2026-04-16 | Ingest Refresh pull session | backend/api/routes/ingest_triples.py:671 (refresh endpoint), backend/farm/client.py:496 (list_triple_runs) | DCL's `/api/dcl/refresh-from-farm` was implemented against the wrong Farm feed: `GET /api/business-data/triple-runs` is the Convergence-overlay manifest list (always `mode=multi_entity`, routed to Convergence at farm/src/api/business_data.py:1203). The correct SE feed is `GET /api/runs` (Farm port 8003, backed by `manifest_runs` table), which returns `{farm_run_id, run_id, tenant_id, entity_id, dcl_run_id, status, created_at, rows_accepted}` per SE manifest-intake execution. Live query on 2026-04-16: 10 distinct SE entities present in `/api/runs`, including every entity currently in DCL's dropdown. Feature as shipped returns "No Farm runs newer than DCL" on every call because triple-runs has zero SE manifests. Fix is to swap the feed + filter to `/api/runs` and drop the `mode=multi_entity` filter (wrong concern — the SE feed never tags mode). Push path also needs rethink: SE runs originally ingested via Farm manifest-intake → `/api/dcl/ingest` (legacy), not via `/triple-runs/{id}/push-to-dcl` → `/api/dcl/ingest-triples`. Severity: blocker. Blocking: the feature does not achieve its stated goal against real Farm state — Refresh cannot detect or re-ingest newer SE Farm runs. RESOLVED 2026-04-16 (commit 3a2fcca, farm commit fcefae5): Farm now exposes POST /api/runs/{farm_run_id}/push-to-dcl; DCL Refresh consumes GET /api/runs and pushes via that replay endpoint. Live verification: VeloCorp-KY0F refreshed with 20111 triples, dcl_ingest_id=9c427619-50c8-510c-a64d-c569580134b0; second call idempotent. Playwright ingest_refresh_pull.spec.ts: 4/4 pass. REOPENED 2026-04-16 (same day): the "newer than" check compared Farm's `created_at` (pipeline start) against DCL's `tenant_runs.updated_at` (ingest finish) — two different events. DCL's timestamp was always seconds later than Farm's for the same run, so every post-ingest Refresh silent-fell-back to "No Farm runs newer than DCL" even when a genuinely new Farm run existed. Banner also concatenated every skipped entity's raw timestamp into a multi-line paragraph instead of a terse "No new snapshots found." string. RE-RESOLVED 2026-04-16 (commit 04be984, farm commit 0e8df98): identity-based detection. Migration 018 adds `tenant_runs.last_farm_run_id TEXT NULL`, backfilled by joining `tenant_runs.current_run_id` against Farm's `manifest_runs.dcl_run_id` via `/api/runs`. DCL ingest UPSERTs `last_farm_run_id` from `req.source_farm_manifest_id` (COALESCE so partial batches do not overwrite). Farm's `dcl_triple_pusher.push_triples_batched` now accepts and forwards `source_farm_manifest_id` (= Farm's farm_run_id for the executed run). `_select_newer_farm_runs` picks newest-per-entity from `/api/runs` and compares `farm_run_id` against DCL's `last_farm_run_id`; match = silent skip, differ (incl. NULL) = candidate. `skipped[]` no longer carries "not newer" noise. Empty-candidates response → `message="No new snapshots found."` rendered verbatim by IngestTab. Live verification: 9 entities re-ingested in 48s (≈5.4s/entity, within B18 15s/candidate ceiling); second Refresh 0.4s "No new snapshots found." (B18 2s steady-state ceiling). Playwright ingest_refresh_pull.spec.ts: 4/4 pass (acceptance rules 1+2 — ground truth from Farm at test time, before/after delta). ingest_refresh_atomic_count.spec.ts: 3/3 pass. tests/test_store_invariants.py: 6/6 pass. tests/test_pipeline_identity.py: 22/22 pass.

20. 2026-04-16 | refresh-bugfix session | tests/test_s1_seed.py (11 tests), data/seed_manifest.json | Pre-session check: test_s1_seed.py::test_01_triples_exist failed independently of this session's refresh/atomic-count work. seed_manifest.json still points at dcl_ingest_id=e7f9884c-1ace-4c8a-ae05-5cc8dd23e36b (2026-04-14) which is no longer in semantic_triples. Same root cause as deferred #17 (duplicate; captured here for this session's D6 trail, not a new defect). Severity: degraded. Blocking: per-session harness validity. Not resolved this session — seed refresh is out of scope for an Ingest Refresh bugfix.

21. 2026-04-16 | playwright-acceptance narrowing session | tests/e2e/all_tabs_verify.spec.ts | 7 matches against CLAUDE.md § Playwright Acceptance Rule 3 (context-dependent bucket: bare toBeVisible / toHaveLength(n) / toHaveCount(n) not tied to ground truth). Rewrite file to pull ground truth from Farm or DCL at test time and assert delta per Rules 1 and 2. Severity: cosmetic. Blocking: strict acceptance-test compliance for this spec — the narrowed pre-commit hook does not block existing specs retroactively so this is cleanup, not a regression.

22. 2026-04-16 | playwright-acceptance narrowing session | tests/e2e/graph_v2_no_orphans.spec.ts | 3 matches against CLAUDE.md § Playwright Acceptance Rule 3 (same class as #21). Rewrite file to pull ground truth at test time and assert delta per Rules 1 and 2. Severity: cosmetic. Blocking: as #21.

23. 2026-04-16 | playwright-acceptance narrowing session | tests/e2e/ingest_refresh_pull.spec.ts | 1 match against CLAUDE.md § Playwright Acceptance Rule 3 (same class as #21). Rewrite file to pull ground truth at test time and assert delta per Rules 1 and 2. Severity: cosmetic. Blocking: as #21. RESOLVED 2026-04-16 (commit 04be984): spec rewritten per Acceptance rules 1+2 — Farm `/api/runs` fetched at test time as ground truth, DCL `/api/dcl/entities` filters to tracked set, `computeNewestPerTrackedEntity` computes expected ingested keys, test asserts delta against Refresh response. 4/4 tests pass (contract + first-Refresh identity + steady-state banner + mocked Farm-down regression).

24. 2026-04-16 | playwright-acceptance narrowing session | tests/e2e/operator_e2e.spec.ts | 10 matches against CLAUDE.md § Playwright Acceptance Rule 3 (same class as #21). Rewrite file to pull ground truth at test time and assert delta per Rules 1 and 2. Severity: cosmetic. Blocking: as #21.

25. 2026-04-16 | playwright-acceptance narrowing session | tests/e2e/store_rebuild_acceptance.spec.ts | 14 matches against CLAUDE.md § Playwright Acceptance Rule 3 (including 10 always-wrong toBeTruthy() uses and 4 context-dependent matches). Rewrite file to pull ground truth at test time and assert delta per Rules 1 and 2. Severity: cosmetic. Blocking: as #21.

26. 2026-04-16 | refresh-bugfix session | tests/e2e/graph_v2_render.spec.ts:28 | NetCorp-G19H expected in Graph v2 entity dropdown but absent. Reproduces on HEAD with stash — pre-existing, not caused by mig018 / identity-based Refresh work. Entity likely dropped out of DCL's current_triples set in a prior session. Severity: degraded. Blocking: B17 Playwright gate for Graph v2 render coverage; not addressed this session (out of scope for Ingest Refresh bugfix).

27. 2026-04-16 | refresh-bugfix session | tests/e2e/operator_e2e.spec.ts:90,195,297 | Three failures: (90) App-loads baseline fails on Vite cold start — same class as #15; (195) NLQ revenue question fails because the test's expected NLQ response field names or routing no longer match live NLQ (`/api/v1/*` surface has drifted); (297) NLQ `/api/v1/report-dimensions` returns 404 — endpoint renamed or removed upstream in NLQ repo. Reproduces on HEAD with stash — pre-existing, not caused by this session's DCL refresh fix. Severity: degraded. Blocking: operator e2e regression coverage for Ingest→NLQ handoff; requires NLQ-repo investigation and test re-point, out of scope here.

19 (re-entry). 2026-04-17 | ingest-refresh-task-1+2-session | backend/api/routes/ingest_triples.py:654, src/components/IngestTab.tsx | Deferred #19 REOPENED 2026-04-17 (3rd time) — prior "scope filter" (`if key not in dcl_state: continue` at L654) was a silent-fallback violation (A1): Farm entities not yet in DCL tenant_runs were silently dropped, so Refresh could never bring new entities in. Task 1: filter removed; Refresh now considers every (tenant, entity) key Farm reports as completed. Evictions tracked via `ingested_keys` set and `(pre_keys | ingested_keys) - post_keys` — catches "ingested then evicted by later candidate" cases the naïve `pre - post` diff missed. `RefreshFromFarmResponse` gains `evicted_sample[]` (first 50) + `evicted_total: int`; banner renders ingested / evicted / skipped as three distinct `data-role` sections with entity names. Task 2 (Farm): rename `manifest_runs.dcl_status_code` → `ingestion_status_code` across 18 sites (TRUNCATE, no migration code). Gate verifications (all 4 PASS): (1) Refresh on fresh DCL ingested+evicted sections rendered with entity names; (2) Farm 201 for farm_manifest_20260417_132138_dd6dee8c (GateTwoVerify entity) → matching DCL POST visible in ~/.pm2/logs/dcl-backend-out.log at 13:21:40 UTC within 2s of Farm's 13:21:38 UTC 201, dcl_ingest_id=6d2dd423-572e-558c-9907-c67704d479dd, inserted=2295 — phantom-push alt-hypothesis disproven (real round-trip); (3) tests/e2e/ingest_refresh_pull.spec.ts 4/4 live-services pass; (4) tenant_runs row for GateTwoVerify_20260417_132119 has last_farm_run_id=farm_manifest_20260417_132138_dd6dee8c matching Farm's farm_run_id. RE-RE-RESOLVED 2026-04-17 (commit def6967, farm commit 82e46e9).

28. 2026-04-17 | ingest-refresh-task-1+2-session | backend/api/routes/ingest_triples.py + tenant_runs cap policy | Refresh-replay treadmill: with tenant_runs per-tenant cap=10 and Farm /api/runs reporting >10 unique SE entities (140+ manifest_runs rows accumulated from replays), every Refresh processes (N - cap) candidates because eviction removes entity keys from tenant_runs, so the next Refresh treats them as unknown-to-DCL and re-ingests, evicting another entity. Observed latency 8-9s per Refresh with ~130 candidates at ~60ms/push against live Farm. This is deferred #12 (cap invariant) manifesting as permanent rotation rather than overflow. Cap-policy decision locked into the refresh plan ("Cap=10 stays, accept churn, surface evictions in UI") so not a bug per Ilya's direction — but test/UX invariants that assume steady state are invalid under current Farm entity count. Playwright Test 2 "latency ceiling / steady state" deleted as its premises (≤2s, no ingested/evicted sections) are unreachable under cap-churn reality; Test 4 covers UI↔API banner parity. Severity: degraded. Blocking: (a) steady-state semantics for Ingest tab — operator clicking Refresh repeatedly will always see some ingested/evicted activity; (b) B18 latency ceiling intent — Refresh scales linearly with (Farm-entities - cap); (c) cross-ref deferred #12, revisit cap policy once production telemetry shows real eviction rate.

29. 2026-04-17 | ingest-refresh-task-1+2-session | backend/api/routes/ingest_triples.py::_select_newer_farm_runs, ME_ENTITY_REJECTED path | Persistent ME-entity skip: meridian and cascadia Farm runs (Convergence/ME pipeline) appear as candidates in every Refresh because Farm `/api/runs` returns all completed `manifest_runs` rows without a SE/ME tag. DCL's `/api/dcl/ingest-triples` correctly rejects them with 422 `ME_ENTITY_REJECTED` (RACI boundary: ME ingestion routes through Convergence, not DCL), but nothing in DCL suppresses them from future Refresh candidate selection — so every Refresh call reattempts the push, receives 422, and adds them to `skipped[]` forever. Noise in the banner's "skipped" section; no wasted DCL ingest writes (rejected at validation), but wasted Farm push-to-dcl HTTP round-trips per Refresh. Severity: cosmetic. Blocking: operator-visible Refresh banner clarity; a ME-entity retry-suppression memory (persist rejected farm_run_ids, skip silently on next Refresh) would resolve, but is a new feature (A6) deferred from this session's scope.

30. 2026-04-18 | plan certain-queries-load-a-dynamic-lamport | backend/api/routes/ingest_triples.py:244 (docstring) + farm/src/services/dcl_triple_pusher.py:60 (push_triples_batched) | Atomicity contract is scoped per-request, not per-run — needs to be said out loud. Current ingest_triples.py:244 docstring says "Validates all triples before inserting any (atomic batch)" which is correct for a single `POST /api/dcl/ingest-triples` call but misleading for anyone looking at the system-level guarantee: the default Farm push path (`push_triples_batched`) splits at 5000 triples, sends batch 0 with `?replace=true` (sync), batches 1..N with `?append=true` (concurrent). Per-batch atomicity × concurrent batches × shared `dcl_ingest_id` = per-run cumulative. Bit this session's work on 2017/32017 landing — UNMAPPED_DOMAIN 422 on `account.customer` triples failed the batches that contained them; batches without that concept landed successfully under the same `dcl_ingest_id`, leaving DCL with a partial run. Not a DCL bug; the docstring is load-bearing and correctly scoped — but the next person who reads "atomic batch" will form the wrong system-level model. Fix: (a) amend ingest_triples.py docstring to state scope explicitly — "atomic within a single request; multi-batch callers see per-batch atomicity and must reconcile run-level completeness themselves"; (b) add a docstring block to Farm's `push_triples_batched` noting that append batches commit independently and that a 422 on any batch mid-run leaves prior batches persisted under the same `dcl_ingest_id`. Cross-repo: Farm side is advisory doc; DCL side is the authoritative contract. | severity: cosmetic | blocking: future sessions will re-derive the mental model from the 2017/32017 symptom; documenting the contract prevents the same diagnostic time loss
