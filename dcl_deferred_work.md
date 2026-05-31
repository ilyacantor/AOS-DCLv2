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
    REGRESSED 2026-05-31 (observed during fabric-connect-records session) — the
    same 11 test_s1_seed.py tests fail again: data/seed_manifest.json (committed
    M since a79c2fb "snapshot-grained selector rework") points at dcl_ingest_id
    2ecbada4-966c-4c43-90d9-6f5977f940af, which has 0 triples in the current
    aos-dev DB (DB current_run_id is 6271a019-…). Pure manifest↔DB drift, the
    original #14 class — NOT caused by the fabric-connect work (test_s1_seed.py
    imports none of the new modules; the new /ingest-records tests use entity
    AcmeCo-TEST which is gated out of the manifest by the concept-count guard, so
    the manifest was untouched this session). Remedy unchanged: re-run the Farm SE
    pipeline against aos-dev and regenerate the manifest. Out of scope for the
    fabric-connect server-side build; reopened here so the RESOLVED marker does
    not hide a live red. severity: degraded | blocking: tests/test_s1_seed.py 11/11
    clean (full backend suite this session: 11 failed / 160 passed / 1 skipped —
    all 11 are these).

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
    NOT RESOLVED 2026-05-13 — investigated as a clean-rename caller sweep.
    Findings argue against a sweep: both old and new ids are valid concept
    ids in the post-d91a9e2 ontology. The OLD ids resolve to the canonical
    (earlier) entries with distinct semantics — `pipeline` is PRD-012 CI/CD
    pipeline, `sales_pipeline_metrics` is SAL-100 sales pipeline; `aws_cost`
    is IT-013 single billing line item, `aws_cost_aggregate` is IT-103 roll-
    up; `event` is IT-015 discrete event, `event_stream_metrics` is IT-101
    aggregate metric; `datadog` is IT-014 monitor record, `datadog_observ-
    ability` is IT-102 APM aggregate; `jira` is PRD-013 ticket, `jira_pro-
    ject_metrics` is ENG-101 sprint/backlog roll-up. Production prod-DB
    semantic_triples row counts under each root (gdbmdrouocxjxiohpixr):
    `pipeline.*`=150, `event.*`=75, `datadog.*`=75, `aws_cost.*`=75,
    `jira.*`=75 (total 450 active triples). All 5 new ids have 0 rows.
    Farm generators (src/generators/triples/aws_cost_triples.py and
    siblings), DCL `backend/farm/ingest_bridge.py` source-pipe mapping,
    DCL `config/source_aliases.yaml`, and DCL `config/persona_domains.yaml`
    all reference the OLD ids and emit/validate triples against the
    canonical entries — those references are correct and must not be moved.
    Mai/Convergence/AAM/AOD/NLQ/Platform/Console: no code references to
    either old or new ids outside ontology YAML copies and deferred-work
    entries. No caller sweep performed. Decision required before any code
    change: (a) keep duplicate-info entries dormant (status quo, low risk,
    accepts having two "datadog" concepts in the YAML), (b) merge SAL-100/
    IT-101/IT-102/IT-103/ENG-101 example_fields into the canonical entries
    and delete the renamed duplicate-info concepts (cleanest, but loses the
    aggregate-metrics framing the WP2 entries added), or (c) rewire persona-
    domains + Farm generators to the new ids, then run an UPDATE on
    semantic_triples to migrate 450 prod rows to new concept roots (most
    disruptive, requires a coordinated multi-repo change + production
    migration). The wp2-cloud-spend cloud_spend concept block is unaffected
    by this question.
    RESOLVED 2026-05-14 — Ilya selected path (a): the 5 renamed entries
    (SAL-100, IT-101/102/103, ENG-101) remain dormant in the YAML. No code,
    persona, or Farm generator references them; 0 prod rows. Accepted cost:
    two YAML entries per concept root (canonical + renamed), with the
    renamed set inert. Reopen only if a future caller needs the aggregate-
    metrics framing the renamed entries capture.

16. 2026-05-13 | seed-manifest-regen-session |
    backend/api/routes/ingest_triples.py:_update_seed_manifest |
    seed_manifest.json `total_triples` field is written from the per-batch
    `count` passed by the ingest handler, not the run-cumulative count.
    With Farm's two-batch push (replace=true then append=true), the final
    manifest reports the last batch's count (e.g. 1228) instead of the run
    total (6228). Tests do not assert on this value (test_01 reads from
    the DB), so the failure is cosmetic. Fix: pass the run-cumulative
    count to the function (`_triple_store.count_run_total(run_id)`) or
    drop `total_triples` from the schema since the per-run row count is
    already queryable from semantic_triples. severity: cosmetic |
    blocking: nothing today.

17. 2026-05-13 | dev-prod-separation-bootstrap | conftest.py:6,
    tests/test_s1_dcl.py:21, tests/test_pipeline_identity.py:22,
    tests/test_prod_mode_ingest.py:24, tests/test_cloud_spend_ontology.py:33
    | Five test-bootstrap sites load `.env` (prod Supabase
    `gdbmdrouocxjxiohpixr`) instead of `.env.development` (aos-dev
    `glmeqbnuahlkkbolkent`), violating the dev/prod separation rule in
    CLAUDE.md "Dev/Prod Database Separation". The swap to load_dotenv with
    `.env.development` is straightforward — but blocked because the
    aos-dev Supabase host `db.glmeqbnuahlkkbolkent.supabase.co` resolves
    only to AAAA (IPv6 `2600:1f18:...`) from this WSL2 environment with
    no IPv6 egress; psycopg2 fails with "Network is unreachable". The
    prod host resolves IPv4 (`100.20.171.89`) and works. Same applies to
    Farm: `/home/ilyac/code/farm/.env.development` does not exist at all,
    so even if DCL bootstrap is fixed, the seed manifest cannot be
    regenerated against dev because Farm cannot run against dev to
    produce the upstream triples. Resolution requires either (a) adding
    Supabase Session Pooler URLs (IPv4) to both `.env.development` files
    via the Supabase dashboard, or (b) provisioning IPv6 routing on the
    laptop. Then: create `farm/.env.development`, restart Farm+DCL pm2
    processes against `.env.development`, swap the five bootstrap sites,
    re-run Farm push-triples to regenerate `data/seed_manifest.json`
    against dev, and verify 143/0/1 against dev. NOT auto-resolved this
    session per constitution rule "no fallback `.env.development → .env`".
    No code change was committed because making the bootstrap swap
    without dev DB reachability would break all 143 tests (D6/B7
    violation worse than the underlying separation violation).
    severity: degraded | blocking: real dev/prod separation for the test
    harness; current state lets tests run, but they run against prod.
    RESOLVED 2026-05-14 — pooler URL constructed for aos-dev
    (`dev_user.glmeqbnuahlkkbolkent` on `aws-1-us-east-1.pooler.supabase.com:5432`
    with `?options=-c%20search_path%3Dshared_gdbmdr`; IPv4 reachable from
    WSL2). 5 bootstrap sites swapped to load `.env.development`, plus a
    6th site discovered via C12 audit (`tests/test_s1_seed.py:31-41` was
    parsing `.env` via raw file open, not load_dotenv). Farm
    `.env.development` created pointing at the same pooler; `dev` schema
    created in aos-dev and granted to `dev_user`. DCL migrations 001-014
    applied via Supabase MCP (postgres role has DDL rights; `dev_user`
    only has DML), filling in `mai_mcp_audit`, missing indexes, and
    explicit CHECK/UNIQUE constraints that pre-dated the migration on
    the existing tables. Two hardcoded `schemaname = 'public'` filters
    in `test_s1_dcl.py:test_01/test_02` generalized to
    `current_schema()` so the assertions hold across both topologies
    (prod uses `public`, aos-dev uses `shared_gdbmdr`). Result vs dev:
    154 PASS / 0 FAIL / 1 SKIP, two consecutive identical runs (B14).
    Production runtime `backend/api/main.py:11` retained on bare
    `load_dotenv()` per prompt instruction.

18. 2026-05-14 | wp5-mcp-real | backend/api/mcp_auth.py:* + Platform
    token-issuance gap | The wire-protocol MCP server (Plan B WP5, §11.4)
    currently uses a v1 shim: DCL mints and verifies opaque tokens locally
    with HMAC-SHA256 over DCL_MCP_TOKEN_SECRET. Token issuance is supposed
    to live in Platform (POST /api/mai/mcp-tokens/issue + a
    mai_mcp_tokens table with revocation, scope, audit). Platform has no
    token-issuance infrastructure today — searched `/home/ilyac/code/
    platform` for `issue.*token|mint.*token|tenant.*token|mcp.*token`
    and found only fixture JWTs. Implementing it in Platform was out of
    scope for WP5 (would require new table, migration, two routes, RACI
    re-check, second PR). v2 work: (a) add mai_mcp_tokens table to
    Platform; (b) add issue/verify endpoints; (c) DCL's verify_token()
    becomes an HTTP call to Platform with a 60s cache; (d) revoke the
    shim secret in DCL. severity: degraded | blocking: prod-grade token
    issuance and revocation; current shim cannot revoke individual
    tokens — secret rotation invalidates all.

19. 2026-05-14 | wp5-mcp-real |
    backend/api/mcp_rate_limit.py:TenantRateLimiter | In-memory per-
    process token bucket. Single uvicorn worker today, so the limiter
    holds. If we ever scale DCL horizontally (multiple Render instances
    or multiple uvicorn workers), each process has its own bucket and
    the per-tenant ceiling becomes effectively N×rpm. Resolution: move
    to Redis-backed sliding window (DCL already has Redis as a hard
    dependency for narration). Deferred because v1 deployment is one
    worker. severity: degraded | blocking: horizontal scaling of DCL
    while keeping accurate per-tenant rpm.

20. 2026-05-14 | wp5-mcp-real | tests/test_mcp_wp5.py:test_t4 | Rate-
    limit test drives the in-process MCP server (HTTP+SSE path) rather
    than stdio because the stdio child has its own limiter instance and
    set_tenant_rpm() on the parent doesn't reach it. The test still
    exercises the same Server + limiter wiring used by HTTP+SSE in
    production; the stdio subprocess limiter is exercised implicitly
    via T2/T3 success paths (under their default 60 rpm). severity:
    cosmetic | blocking: nothing — the rate-limit logic is the same
    object in both transports inside a single process.

21. 2026-05-14 | wp5-mcp-real |
    backend/api/mcp_server_real.py:tool_concept_lookup,
    tool_semantic_export | The two ontology-only tools (concept_lookup,
    semantic_export) accept tenant_id='' from the legacy HTTP path so
    Mai's existing calls (which run against a DCL process that does NOT
    set AOS_TENANT_ID) keep working. The real wire-protocol MCP path
    still requires tenant_id (it's bound to the session's token). When
    Mai is migrated to the real MCP transport (per §11.4 last
    paragraph), tighten the tools to reject empty tenant_id everywhere.
    severity: cosmetic | blocking: tightening I2 to "all calls carry
    tenant_id, no exceptions" once Mai uses tokens.

22. 2026-05-15 | wp10-11-finops-farm-session | tests/e2e/test_monitoring_tabs.py:98 | TestCrossTab::test_all_tabs_navigate_without_crash[chromium] expects a "Graph v2" tab label but src/App.tsx:294 renders the tab as just "Graph". Mismatch is pre-existing — predates this session's WP10/11 finops-spend work and is unrelated to the ontology/persona/metric additions. Severity: cosmetic | blocking: e2e UI navigation test green.

23. 2026-05-16 | ws-1 b5 cleanup | dcl repo-wide (19 remaining files) | After Block 5 scrub of demo-critical files (.env.example, migrations/009, schemas/dynamics/contacts.csv, src/components/sankey/data.ts), 19 files retain banned literals in categories (b/c/d): data/service_catalogs/{meridian,cascadia}.json (seed, not loaded by code), data/{combining_statements,customer_profiles,entity_overlap,ebitda_adjustments}.json (fixture JSON loaded by Convergence Reports v2 demo via direct file reads — should migrate to live triple queries; track under convergence repo since DCL owns the table not the file), tests/e2e/operator_e2e.spec.ts (Meridian-tagged Playwright clicks — used by existing operator e2e suite), tests/{test_user_facing,test_cases}.yaml, tests/test_mai_status.py, attached_assets/Pasted-*.txt (archived Replit chat dumps), docs/{DCL_STORAGE_OVERVIEW,DCL_ARCHITECTURE,CONVERGENCE_CARVEOUT_BLUEPRINT_CANONICAL}.md, CLAUDE.md + AOS_MASTER_RACI_v8_6.csv + convergence_{blueprint,transition}_master.md (policy/spec). | severity: degraded | blocking: data/*.json files block convergence demo migration to live triples; tests/e2e/operator_e2e.spec.ts blocks Playwright suite green once Meridian button label disappears from DCL UI

24. 2026-05-16 | ws-1 b2-fabrics-drill | backend/api/routes/triple_monitor.py:541 (triples_browse) | Added `run_id` query param to `/api/dcl/triples/browse` so AAM's WS-1 Fabrics drill page can fetch per-receipt triples (filtered by dcl_ingest_id). UUID-validated (400 on malformed). dcl-dev (:8104) picked up the change via --reload; dcl-backend (:8004) was not restarted in this dispatch — next prod-touch session must `pm2 restart dcl-backend` after pulling, otherwise the Fabrics drill against prod-DCL will return 0 triples even when triples exist. Filter is additive + read-only + safe to deploy. | severity: cosmetic | blocking: AAM Fabrics drill against prod-DCL until pm2 restart

25. 2026-05-20 | identity-remediation R3 | src/components/TriplesPanel.tsx:108 (fetchBrowse) | R3 made `/api/dcl/triples/browse` + `/browse-batch` require a `tenant_id` query param (tenant scoping — every browse is `WHERE tenant_id =`). DCL's own internal triple-monitor UI (TriplesPanel.tsx) calls browse with domain/entity/period/property filters but no tenant_id — it will now get 422. The panel needs a tenant selector/input (mirror the existing `browseEntity` state + input). Not on the demo critical path — the demo surfaces are AAM Fabrics, Console, NLQ dashboards, not DCL's internal monitor UI. AAM + NLQ callers were updated in R3; this DCL-internal frontend is the one caller left. | severity: degraded | blocking: DCL internal TriplesPanel triple-browser tab (not a demo surface)
    RESOLVED 2026-05-20 (commit 211e6cd) — TriplesPanel.fetchBrowse now sends tenant_id; added a Tenant selector dropdown (from the runs list, I4-compliant) + browseTenant state defaulted from the active run. Note found during the fix: TriplesPanel is orphaned — nothing imports it — so this was never a live break. The fix makes the component correct if re-mounted; if it is not meant to be a live surface it should be deleted instead.

26. 2026-05-20 | identity-remediation R3 | backend/api/routes/triple_monitor.py (triples_browse, triples_browse_batch) | R3 added a required `tenant_id` param + unconditional `WHERE tenant_id = %s` to both browse endpoints. This is tenant SCOPING, not authentication — the endpoint does not verify the caller owns the tenant_id it passes. A caller can still pass another tenant's id. Cryptographic auth (token-derived tenant, mirroring the MCP path at backend/api/mcp_auth.py / triple_store.mcp_query_triples) is the real close — deliberately deferred per operator decision (R3 adjusted to "required input, filter, no auth"). Follow-up: add token auth to the plain-HTTP triple read endpoints. | severity: degraded | blocking: true cross-tenant isolation on the plain-HTTP browse endpoints (scoping holds; auth does not)

27. 2026-05-20 | identity-remediation R3/R4 verification | dcl-dev (:8104) aos-dev DB — semantic_triples volume | dcl-dev wedges under burst load: broad queries (/api/dcl/snapshots get_tenant_snapshots, /api/dcl/triples/engagement, the persona browse-batch fan-out) hang past the Postgres statement_timeout (psycopg2.errors.QueryCanceled), the single uvicorn worker stops responding (curl → 000), and httpx.ReadTimeout cascades into NLQ/AAM callers — making R3/R4 verification and test_nlq_v2_client.py runs non-deterministic (B14). CONFIRMED ROOT CAUSE: the aos-dev semantic_triples table is 13.97M rows / 6.35 GB — hundreds of accumulated test-run ingests. NOT an index gap — the table has a rich correct index set (idx_triples_active (tenant_id,is_active) WHERE is_active, idx_triples_tenant_run, idx_triples_entity_concept, idx_triples_entity_period, idx_triples_concept_period, idx_triples_run, ...). NOT a code/query bug — counts_sql tenant-scoping was tried and reverted (A13), it did not help. Pure data volume: the demo tenant 69688df3 alone has 123,934 ACTIVE triples (one FinOps run is ~27k → ~4-5 runs left active), and SELECT count(distinct run_id) for that tenant times out outright. NOT R3/R4-caused. Fix: prune the dev DB — DELETE superseded rows (is_active=false) then keep only the latest run per tenant; ~14M → ~100-150k rows makes every query instant. Destructive op on aos-dev only (data regenerable by a pipeline run) — needs operator authorization. Workaround until then: pm2 restart dcl-dev clears it for ~1-2 min. | severity: blocker | blocking: end-to-end verification of R3 (browse), R4 (persona /resolved), and all of R8 (full pytest, ws5 smoke spec) cannot run against dcl-dev until the dev DB is pruned
    RESOLVED 2026-05-20 — operator-authorized prune of the aos-dev semantic_triples table: staged the 123,934 is_active=true rows, TRUNCATE, reload, VACUUM ANALYZE. Table went 13.97M rows / 6.35 GB → 123,934 rows / 48 MB. Post-prune: /api/dcl/snapshots 200 in 0.49s, /triples/browse 200 in 0.43s (were 30s+ timeouts → 000). dcl-dev no longer wedges. RESIDUAL (not blocking, own follow-up): DCL has no retention policy for superseded is_active=false triples — they accumulate indefinitely on every ingest, so the dev DB will bloat again over time. A periodic prune job or an ingest-time purge of prior-run triples is the durable fix.
    BLOAT REGRESSION 2026-05-27: confirmed bloat is back. `SELECT COUNT(*) FROM semantic_triples` and `SELECT SUM(...)` queries on the table now time out at the 15s statement_timeout — bigger than the post-prune 123,934-row state. Origin reconfirmed: every `swap_and_deactivate` (dcl/backend/db/triple_store.py:282) flips previous-run rows to is_active=false but does not delete them; over a week of operator + test dispatch the inactive accumulation regrew. The ~502 finops-demo-co snapshots tracked in aam_deferred_work.md#37 alone account for ~1.5M rows at ~3k each. This entry is the prime contributor to two downstream symptoms: (a) Phase 5 `count_total_rows` on the ingest hot path (dcl/backend/api/routes/ingest_triples.py:530) — a >200k-row safety warning that runs a full sequential scan on every replace=true ingest, now taking seconds-to-tens-of-seconds because of the bloat; (b) Phase 3 deactivate UPDATE (swap_and_deactivate) scaling cost is acceptable when previous-run is small but compounds as the inactive set grows because the planner's BitmapAnd uses idx_triples_concept_domain (estimating 668,717 rows across the partial-active index) intersected with idx_triples_run — no `(run_id, is_active)` composite exists to cut this short. ABANDONED ROOT-CAUSE FIX (off-branch, history-only): commits 07b96d7 (phases 1-3 store rebuild + migrations 014/015 + current_triples mirror), 07f84fb (phase 4 + mig016), f4e3a97 (phase 5 read-path + mig017) implemented exactly the run-level-pointer redesign that eliminates per-row is_active flipping: a `current_triples` flat mirror + `semantic_triples_archive` + `tenant_runs.run_row_count` + `swap_and_delete` (archive+delete in one txn, single COPY into both stores). git merge-base confirms NONE of those commits are ancestors of HEAD — work was abandoned, not merged forward. The redesign is reasonable on its face and was already implemented through 5 phases; revival is a sequencing question, not a design question. Linkage to current dispatch (2026-05-27): Move (i)' (strip count_total_rows from the ingest hot path) addresses the symptom; the bloat itself still affects Phase 3 cost and every future scan on semantic_triples. Either re-prune + add retention cron, or revive the abandoned store rebuild as the durable fix.
    SHIPPED 2026-05-27: (1) count_total_rows removed from /api/dcl/ingest-triples replace=true path; moved to GET /api/dcl/admin/triple-count (operator/cron polled, no ingest gating). (2) Migration 015 added partial index `idx_triples_active_run ON semantic_triples (run_id) WHERE is_active = true` — direct match for swap_and_deactivate's UPDATE predicate. EXPLAIN ANALYZE on a 24,263-row deactivate: 13,124ms BEFORE (BitmapAnd of idx_triples_run + idx_triples_concept_domain, scanning 1,725,045 index entries) → 1,360ms AFTER (direct Index Scan using idx_triples_active_run, 24ms scan + ~1.3s heap writes for 24k row UPDATEs). 9.6x speedup on the index-scan portion. Note for operator: CONCURRENTLY can leave the index in indisvalid=false state if conflicting writes hit during build (encountered on dev) — verify with `SELECT indisvalid FROM pg_index ...` and remediate with `REINDEX INDEX CONCURRENTLY` if needed. UPDATE 2026-05-28: REINDEX automation now built into run_migration.py (commit 3efbfc3) — no manual verification step needed.
    SHIPPED 2026-05-28 (verification dispatch): (A) get_tenant_snapshots rewritten as single SQL with recursive CTE skip-scan + ROW_NUMBER + LIMIT-N + scalar tenant_runs lookup, mirroring get_all_snapshots's pattern. Closes the gap from the original dispatch (only the no-tenant path was redesigned). Endpoint now bounded at O(N) for the tenant-scoped variant NLQ uses. (B) Operator-authorized prune of aos-dev semantic_triples: SET LOCAL statement_timeout=0 + DELETE WHERE is_active=false + VACUUM (FULL, ANALYZE). Before: 7,069,235 estimated rows / 3,335 MB / `SELECT COUNT(*)` timed out. After: 566,929 active rows / 214 MB / queries return in milliseconds. 15.6× total-size reduction. Verification: /api/dcl/snapshots?tenant_id=X timed out at 60s pre-fix; returns in 0.55-0.93s post-fix. NLQ snapshot-identity twice-identical 12/12 achieved (pre-dispatch baseline was 12/12 then 11/12). AAM Playwright: 37/5 (24.4m) vs pre-dispatch 36/6 (28.0m) — +1 pass, 12% faster suite. The 5 AAM residuals are heavy-ingest end-to-end tests that hit explicit 250s+ test timeouts on the Farm-trigger → AAM-receipt → drill path; NOT the original "Loading snapshots..." pool-load symptom (which is fully resolved). Bloat will reaccumulate but the read paths are now structurally bounded — endpoint stays fast regardless.

28. 2026-05-20 | identity-remediation R8 verification | tests/e2e/test_monitoring_tabs.py + tests/e2e/test_pipeline_quality.py + tests/test_mcp_wp5.py | Full DCL pytest run (R8 verification) = 154 passed, 41 failed, 1 skipped. Root-caused — NEITHER class is caused by the identity-chain remediation (R3 changed only triple_monitor.py browse params; the prune changed only row count): (1) 31 chromium e2e failures (test_monitoring_tabs.py, test_pipeline_quality.py) all fail at `page.goto("http://localhost:3004/", wait_until="networkidle")` with TimeoutError 30000ms — the DCL frontend serves fine (curl localhost:3004 → 200 in 6ms), but the monitoring UI polls so the network never goes idle for 500ms. networkidle is the wrong wait condition for a polling dashboard. Test-design bug, pre-existing. Fix: change wait_until to "domcontentloaded"/"load" across the DCL e2e specs, then re-verify the tab assertions. (2) 8 test_mcp_wp5.py failures (RuntimeError) occur ONLY in the full-suite run — test_mcp_wp5.py passes 11/11 when run in isolation. Test-ordering pollution: an earlier test file leaks process/connection/env state. Pre-existing. Fix: find the leaking fixture and isolate it. | severity: degraded | blocking: D6 100%-pass on the DCL suite; DCL frontend e2e coverage is effectively dark until the networkidle wait is fixed
    CORRECTED + e2e RESOLVED 2026-05-21 (commit 7c096e4) — diagnosis (1) above is WRONG: the DCL frontend does not poll-without-gaps. A network capture showed it fires 39 requests on load and exactly ONE never completes — GET /api/dcl/triples/persona-stats, which took 38.6s on prod (an unbounded whole-store aggregate with no is_active filter + an N+1 per-persona COUNT(DISTINCT) loop). That single perpetually-in-flight request is why networkidle never settled inside the 30s page.goto window — the real cause of all 31 chromium e2e timeouts. Fixed in 7c096e4: persona-stats 38.6s → 19.1s (is_active filter) → 5.9s (collapsed to one query). Re-captured: 39 fired / 39 completed / 0 in-flight — networkidle now reachable, the 31 e2e page.goto timeouts are resolved. Diagnosis (2) — the 8 test_mcp_wp5 failures — is unrelated and REMAINS OPEN: the Playwright e2e tests leave the asyncio runner closed, so the anyio MCP tests then hit RuntimeError: Runner is closed (test_mcp_wp5 passes 11/11 in isolation). Fix for (2): isolate the Playwright suite from the anyio suite.

29. 2026-05-20 | identity-remediation R3 — DCL read-surface audit | backend/api/routes/triple_monitor.py + ingest_triples.py | R3 hard-required tenant_id only on triples/browse + triples/browse-batch. An audit of the rest of DCL's query/read surface found the same unscoped-read hole still open on 6 endpoints: dashboard-data (no tenant_id param at all — filters entity_id/run_id/domain only), ingest-log (no tenant_id — SELECTs the column but does not filter on it), triples/runs (no params — returns every tenant's runs), triples/persona-stats (no params), triples/overview (tenant_id is Optional default None — unscoped when omitted), contextualization-summary (tenant_id Optional default None). The DCL frontend calls dashboard-data / ingest-log / contextualization-summary / overview / runs / persona-stats WITHOUT tenant_id, so each returns cross-tenant or unscoped data — the same I2/I6 leak R3 closed for browse, still open across the entire DCL dashboard/monitoring read path. R3 (per its plan) was deliberately scoped to browse + browse-batch only; this audit shows that scope was too narrow. Fix (its own dispatch, comparable in size to R3): hard-require tenant_id + add WHERE tenant_id on all 6, and give the DCL frontend a tenant-resolution mechanism (it has none today — each panel reads tenant_id off returned data; nothing resolves it before the call). entity_id-only scoping is insufficient per I2/I6 — tenant_id is the isolation key. | severity: blocker | blocking: cross-tenant read isolation on the entire DCL dashboard/monitoring read surface
    REVISED 2026-05-20 (Ilya) — DOWNGRADED to deferred future-work; NOT a demo blocker. The entry text above is wrong and is corrected here: it collapsed two distinct identity axes into one "gap". AOS has two identity axes and they must be kept separate, never conflated:
    (1) ENTITY axis — entity_id — identifies a distinct enterprise environment. The demo is multi-IDENTITY, not single-tenant-hiding-a-gap: it runs multiple Farm-generated snapshots, each acting as a separate enterprise environment, distinguished by entity_id. Entity scoping is the CORRECT identity axis for what the demo demonstrates. DCL's read endpoints (dashboard-data, ingest-log, triples/runs, persona-stats, triples/overview, contextualization-summary) being entity-scoped is correct by design — it is NOT a "gap", NOT a "leak", NOT a "workaround", and entity_id scoping is NOT "insufficient". The audit-entry wording above ("unscoped-read hole", "same I2/I6 leak", "insufficient") mis-framed it by treating tenant_id as the only valid axis.
    (2) TENANT axis — tenant_id — is a SEPARATE isolation layer ABOVE entity scoping: it separates customer tenants and M&A engagement boundaries. The demo does not exercise it. There is no second tenant; nothing leaks.
    The real, deferred item: the tenant axis is simply not yet layered onto these 6 endpoints, and that is fine. Deferred until real multitenancy actually lands — a second tenant is onboarded, OR Convergence M&A needs multiple entities isolated within one engagement. When that happens, ADD tenant scoping as a layer ON TOP of entity scoping (require + filter tenant_id IN ADDITION TO entity_id) — do not replace, remove, or collapse the entity axis. Any new DCL endpoint must keep both axes explicit and distinct. Supersedes "severity: blocker" → deferred future-work; blocks nothing for the demo.

30. 2026-05-21 | demo-pipeline-monitor | backend/api/routes/monitor.py p95_query_ms | GET /api/dcl/monitor/metrics (new, read-only — feeds the AAM-served pipeline monitor at /aam/monitor) reports p95_query_ms as null; the page renders it "n/a". DCL captures no query latency: /api/dcl/query (backend/api/query.py) records no per-call timing, and mai_mcp_audit.latency_ms is MCP-tool-call latency — a different surface (consumer->MCP), and not every MCP tool call is a query. Surfacing it under a field labelled "p95 query" would mislabel it, so the metric was omitted per the build's READ-ONLY guardrail rather than mislabelled or instrumented. To resolve: add per-request latency capture to /api/dcl/query and expose a percentile. | severity: cosmetic | blocking: nothing — the monitor renders "n/a" honestly

31. 2026-05-21 | demo-pipeline-monitor | dcl-backend :8004 prod semantic_triples volume | The monitor build surfaced that dcl-backend :8004 (prod DB) 500s on the EXISTING aggregate-COUNT endpoint /api/dcl/triples/overview — ~27s, then 500; it runs COUNT(*) over semantic_triples. Same class as the dev-side bloat in #27 and persona-stats in #28, on prod this time: prod semantic_triples has accumulated superseded is_active=false rows (no retention policy — see #27 RESIDUAL). NOT caused by the monitor work and NOT in the monitor's own path — /api/dcl/monitor/metrics deliberately reads the small ingest_log table (one row per ingest), not semantic_triples, so it stays fast on :8004 too; logged here only because the monitor build is where the pre-existing triples/overview failure was observed. Out of scope for the monitor build (READ-ONLY — cannot prune prod or change DCL DB config). To resolve: apply the #27 prune to prod semantic_triples, or land the durable #27-RESIDUAL retention job. | severity: degraded | blocking: DCL's own triples/overview + persona-stats monitoring endpoints on :8004 prod

32. 2026-05-21 | snapshot-selector-rework | backend/api/routes/recon_checks.py:278 + triple_monitor.py contextualization-summary | The DCL UI selector was reworked into a follow-latest/pin SNAPSHOT selector (RunSelector.tsx). Per the approved unified model, `*` (the followed default) = the snapshot with the max run_timestamp, computed client-side, deliberately NOT tenant_runs.current_run_id (is_current is verified-unreliable). Consequence: the followed-latest snapshot's run may not be the ACTIVE run (current DB state — tenant 69688df3's newest snapshot VeloCorp-KDDN is is_current=false; current_run_id stale-points at the 32d-old SysEdge-Z321). is_active-scoped tabs then show empty for the followed snapshot: /api/dcl/recon (WHERE is_active=true AND entity_id=%s) returns 0 checks + detail "No active triples found", and contextualization-summary returns domains_populated=0. No red error banner / no crash — the tabs render the empty/FAIL state gracefully — but an operator following the newest snapshot sees no Recon/Context data. Root cause is stale tenant_runs.current_run_id (the ingest write-path is not advancing it), the same data integrity gap the rework was designed around; not a selector bug. Monitoring tests work around it by selecting the is_current snapshot (test_monitoring_tabs.py TestReconTab, test_pipeline_quality.py select_active_snapshot). Proper fix is one of: (a) fix the ingest write-path so current_run_id tracks the newest run, or (b) make is_active-scoped tabs query by the selected snapshot's run_id instead of global is_active. RACI: DCL owns write-side invariants — needs operator decision. | severity: degraded | blocking: Recon + Context tabs show no data for the follow-latest default snapshot whenever tenant_runs.current_run_id lags the newest run

33. 2026-05-21 | snapshot-selector-rework | tests/e2e/all_tabs_verify.spec.ts:86 (test 3 "Graph tab — snapshot selection renders graph") | This test wedges under WSL2 headless Chromium. The DCL app renders correctly — the captured error-context page snapshot shows the Graph tab active with the snapshot dropdown populated and "* VeloCorp-KDDN-c712" selected — but after the test's assertions the browser process goes alive-but-wedged: page.screenshot() neither resolves nor rejects (a guarded .catch() does not help — the call hangs, not throws), so the run consumes the full 180s test timeout. Reproduced in total isolation (npx playwright test -g "Graph tab — snapshot selection" --workers=1), so it is NOT context-teardown churn from sibling tests and NOT backend backlog. Environmental: WSL2 Chromium wedges on this graph-SVG render path — the same class the NLQ snapshot-selector spec documents ("single browser session to avoid WSL2 Chromium --single-process crashes"). NOT caused by the selector rework — the rework's snapshot selector + Graph tab are verified green on the same WSL2 box by tests/e2e/snapshot_selector.spec.ts (the dedicated acceptance test) and the full test_monitoring_tabs.py suite (28/28). all_tabs_verify.spec.ts was also already broken pre-rework (it navigated to a "Graph v2" tab that no longer exists; the rework fixed that to "Graph"). test.describe.serial means this one failure also blocks the 4 sibling tests after it. Fix: restructure all_tabs_verify.spec.ts into a single-browser-session spec (the NLQ mitigation pattern) or run it under a non-WSL Chromium. | severity: degraded | blocking: all_tabs_verify.spec.ts test 3 + the 4 serial siblings after it; not the selector rework's own acceptance

34. 2026-05-21 | snapshot-selector-rework | tests/e2e/se_pipeline_e2e.spec.ts + tests/e2e/provenance_integrity.spec.ts | Both cross-service specs were updated for the snapshot-selector rework: their DCL steps now select snapshots by dcl_ingest_id via #snapshot-selector (resolving the entity from /api/dcl/snapshots) instead of selecting an entity_id on the old entity dropdown. The DCL-side edits mirror the exact pattern verified green in snapshot_selector.spec.ts, snapshot_isolation.spec.ts, graph_v2_render.spec.ts and the 28-test test_monitoring_tabs.py suite. They were NOT executed in this session: each runs a full Console SE pipeline (test.setTimeout 300_000) and so is a ~5-min cross-service run (Console 8009 + NLQ 8005 + DCL), and the WSL2 Chromium graph-render wedge in #33 makes long multi-tab cross-service runs unreliable here. Fix: run both specs end-to-end against live Console/NLQ/DCL on a non-WSL or session-stable Playwright environment and confirm the snapshot-driven DCL steps pass. | severity: degraded | blocking: end-to-end re-verification of the two cross-service specs after the selector rework — DCL-portion changes are pattern-identical to verified specs but not independently run this session

35. 2026-05-27 | snapshots-endpoint-redesign | backend/db/triple_store.py:489 get_all_snapshots + backend/api/main.py:443 /api/dcl/snapshots | Endpoint redesign to fix sustained-load degradation surfaced in cross-repo entry (see aam_deferred_work.md#37). Root cause: get_all_snapshots does N+1 sequential pool borrows — 1 borrow for the tenant list, then a fresh `with get_connection()` block per tenant inside the loop, each running 3 queries (tenant_runs + recursive CTE skip-scan + COUNT batch). POOL_MAX_CONN=20, POOL_GETCONN_TIMEOUT=5.0s. NLQ polls every 12s; Playwright fans out; Farm pushes a new finops-demo-co snapshot every ~5s growing T and per-tenant run history → /api/dcl/snapshots p95 climbs 0.7s → 6–15s mid-suite, cascading 500/503 to NLQ. Implementation: single SQL with ROW_NUMBER() OVER (PARTITION BY tenant_id ORDER BY created_at DESC) WHERE rn <= N joined to tenant_runs for snapshot_name + counts subquery, single pool borrow per request. Endpoint accepts ?limit=N (default 10, max 50) — operator dropdown sees the N most recent snapshots per tenant, not flat-LIMIT 50 that stale finops rows can fill. Index check: confirm or add composite on tenant_runs(tenant_id, created_at DESC) (existing idx set listed in entry #27 — verify against this query plan, EXPLAIN ANALYZE before commit). Preserves snapshot_name derivation (current/previous from tenant_runs, else {entity_id}-{run_id_prefix}). Coupled with farm_deferred_work.md#13 (FARM_DEMO_DISPATCH_ENABLED) — both land same workstream. Pool max_lifetime is a separate long-tail fix (sequenced after this). Async DB migration is a separate workstream (deferred). | severity: degraded | blocking: NLQ snapshot-identity 8-spec suite twice-identical (B14); operator dropdown freshness; any cross-suite Playwright run that exercises /api/dcl/snapshots under load

36. 2026-05-27 | tenant-runs-name-write-site-design | farm/src/api/routes/snapshots.py:165 + farm/src/generators/business_data_orchestrator.py:107/:269 | Write-site fix design pick — required before JOIN ships (see #35 follow-up). Two writers currently produce non-canonical tenant_runs.current_snapshot_name values on the AOS tenant: (a) Farm snapshots.py:165 passes the raw Farm snapshot UUID as snapshot_name (42 polluted rows on dev as of 2026-05-27); (b) Farm business_data_orchestrator.py:107/:269 generates "cloudedge-{4-hex}" via /api/business-data/generate?push_to_dcl=true — currently zero such rows in dev but the writer is active and reaches the AOS tenant via get_tenant_id(). Console SE pipeline already does the right thing via make_run_name() → I5 canonical "{entity_id}-{short_hash}". Two design options for the write-site fix: (X) caller-passes-run_name: every push caller computes the I5 form and passes it explicitly — pushes the canonical-form responsibility upstream, requires symmetric fixes at snapshots.py and business_data_orchestrator and any future caller. (Y) snapshots.py-derives + orchestrator-derives: each push site computes "{entity_id}-{ingest_uuid[:4]}" inline from the local run identity — single-site contract, no caller coordination, but the derivation logic lives in N places. Pick before write-site fix dispatch starts. | severity: degraded | blocking: JOIN dispatch (see #35 follow-up); operator-visible I5 conformance across all push paths
    SHIPPED 2026-05-28 (Option Z hybrid): farm/services/identity.py now exports `derive_run_name(entity_id, ingest_uuid)` and `normalize_run_name(entity_id, ingest_uuid, supplied=None)`. Helper is authoritative on BOTH paths — supplied None → derive; supplied canonical → return unchanged; supplied non-canonical (UUID-shaped, "cloudedge-*", "cloud-spend-*", "cco_summary", any other shape) → raise ValueError. Write sites updated: farm/api/routes/snapshots.py:165 + farm/generators/cloud_spend/orchestrator.py:155 (also closes dcl#37). Defense-in-depth at the DCL persistence boundary: dcl/backend/api/routes/ingest_triples.py rejects non-None non-canonical snapshot_name with 422 NONCANONICAL_SNAPSHOT_NAME regardless of source. business_data_orchestrator.py:107/:269 left untouched — legacy /api/dcl/ingest path, deprecated per CLAUDE.md, DCL-boundary check would catch any attempt. Verified: ZTest-CloudFleet-1 push produces "ZTest-CloudFleet-1-90b6" (canonical); direct UUID push to /api/dcl/ingest-triples returns 422 with canonical-form error. RESOLVED 2026-05-28.

37. 2026-05-27 | cloud-spend-name-form | farm/src/generators/cloud_spend/orchestrator.py:155 | Open question: `cloud-spend-{entity_id}` is canonical-by-design for the cloud_spend ingest path (7 entities on dev DB), distinguishing financial vs. cloud-cost data in the operator dropdown. Strictly violates I5's "{entity_id}-{short_hash}" form. Two paths: (a) amend I5 to allow a domain prefix as the leading token (I5 = "{domain_prefix?}{entity_id}-{short_hash?}"); (b) refactor cloud_spend to encode the domain into entity_id itself (e.g. "cloudfleet-it-bc4224" already carries the disambiguation) and use canonical I5. Neither is urgent — current form is operator-readable and consistent. Decision rides with the write-site fix dispatch (#36) so all write sites move in one motion. SCOPE EXPANSION + RESOLUTION 2026-05-27 (pending dcl#36 dispatch): Deeper trace requested: is the cloud_spend orchestrator demo scaffolding, or production-required? Findings: (1) Orchestrator is production-required source of finops data. Built deliberately in farm commit 33bbc55 (May 14 2026, WP7) as "FinOps cloud_spend persona end-to-end" with 2,293 LOC + 26 tests; not scaffolding-that-ossified. (2) Finops agent (finops/server/recommendations/aosEnrichment.ts + finops/server/mcp/aosMcpClient.ts) queries DCL by concept namespace `cloud_spend.cloud_resource.*` / `cloud_spend.cloud_owner.*` / `cloud_spend.cloud_account.*` per-resource — zero references to `cloud-spend-` snapshot_name or snapshot_id in finops server code. Finops does NOT depend on the snapshot_name shape. (3) Canonical Farm SE path (business_data_orchestrator + aws_cost_triples.py) emits `infrastructure.cloud_spend.{quarter}.{metric}` quarterly aggregates only — not per-resource. Finops needs per-resource granularity canonical doesn't provide; orchestrator can't be deleted. (4) Data shape on dev DB confirms: CloudFleet-IT-bc4224 has 4,217 triples under single `cloud_spend.*` root, source_system=synthetic_warehouse, fabric_plane=warehouse; FluxSystems-CIF5 has 24,267 triples across 30+ concept roots (revenue/gl/invoice/journal_entry/customer/workforce/etc.) — different KINDS of entities, not different labels. Verdict (b): orchestrator stays; snapshot_name moves to canonical I5 `{entity_id}-{short_hash}`; domain encoded by concept prefix (queryable) not snapshot_name (operator-visible label). Folds into dcl#36 — one-line change at orchestrator.py:155. Refactor option (c) (collapse into canonical Farm tier) is possible but template-based per-entity input differs structurally from financial-model-based multi-entity input in BusinessDataOrchestrator; real cost, limited benefit; revisit only on future operator need (e.g., financial + cloud_spend for same entity in one call). RESOLVED 2026-05-27 (pending dcl#36 dispatch execution). | severity: cosmetic | blocking: nothing operationally; I5 strict conformance only
    SHIPPED 2026-05-28 (with dcl#36 dispatch): cloud_spend/orchestrator.py:155 now calls normalize_run_name(entity_id, dcl_ingest_id). The 7 existing `cloud-spend-{entity_id}` rows in dev were backfilled to canonical I5 form via dcl/scripts/backfill_snapshot_names.py (see dcl#39 SHIPPED). cloud_spend domain remains queryable via the `cloud_spend.*` concept namespace — domain encoding moves off the snapshot_name label entirely.

38. 2026-05-27 | aam-relay-null-by-omission | aam/app/ingest/dcl_push.py:103 + dcl/backend/api/routes/ingest_triples.py:86 | AAM's fabric-webhook → DCL push path omits `snapshot_name` from the POST body; DCL's IngestTriplesRequest defaults snapshot_name=None; tenant_runs.current_snapshot_name is written as NULL (4 dev entities: finops-demo-co, techedge-25kh, r6-verify-co, VeloWorks-GQFR partial). Read-time fallback in get_tenant_snapshots derives "{entity_id}-{rid[:4]}" so operator sees a name, but the stored value is NULL — a hybrid column-or-derive contract that is not stated anywhere. Two options: (a) AAM derives and passes "{entity_id}-{dcl_ingest_id[:4]}" at push time — column is authoritative, derive is dead; (b) formalize the hybrid: declare snapshot_name optional, read-side derives when NULL, document the contract in SCHEMA_CONTRACT.md, accept NULL writes as canonical. Until a decision lands, the AAM relay is silently shifting half the identity work onto the read path. | severity: degraded | blocking: AAM-originated entities have null tenant_runs name; operator surface depends on read-side derivation that can drift
    RESOLVED 2026-05-28 (option X — AAM derives and sends): aam/app/ingest/run_name.py adds derive_run_name (byte-identical mirror of farm/services/identity.py per B6 — separate repos, no cross-repo import). aam/app/ingest/dcl_push.py now sets body["snapshot_name"] = derive_run_name(entity_id, dcl_ingest_id) — AAM is a first-class canonical writer; the column is authoritative and DCL's read-side derivation in get_tenant_snapshots demotes to a regression-detector. Verified end-to-end: manual-entry push for ZTest-RelayX-1 wrote current_snapshot_name='ZTest-RelayX-1-f28a' (canonical, not NULL, not read-derived). The 4 NULL rows (finops-demo-co, techedge-25kh, r6-verify-co, VeloWorks-GQFR) backfilled via scripts/backfill_snapshot_names.py (detection broadened: current_run_id present → current name must be canonical, NULL counts as needs-fix; previous canonical IFF previous_run_id present; NULL-aware match via IS NOT DISTINCT FROM). Post-backfill across all 66 rows: 0 current-NULL-with-runid, 0 uuid-shaped, 0 prefix-shaped, 0 previous-NULL-with-runid. Note: VeloWorks-GQFR current_run_id is a synthetic test-fixture marker (00000000-…) predating this work → derived 'VeloWorks-GQFR-0000', a valid canonical shape, harmless dev artifact. Boundary-rejects-NULL hardening (make snapshot_name required at DCL ingest) deferred to dcl#41 — NOT shipped now because the live finops demo runs through the AAM relay and flipping the boundary to required mid-flight risks a loud break there; ship after X is proven in the demo and no other legitimate NULL-writer remains.

39. 2026-05-27 | backfill-uuid-rows | dcl semantic_triples + tenant_runs (aos-dev) | Destructive backfill (B19) needed after the write-site fix in #36 lands: 42 dev tenant_runs rows have UUID-shaped values in current_snapshot_name (full entity list in dispatch report 2026-05-27). Two options: (a) in-place UPDATE — for each polluted row, set current_snapshot_name = entity_id || '-' || substring(current_run_id::text, 1, 4); idempotent, reversible if a backup is taken, touches only the 42 rows. (b) DELETE + re-push — clear the rows and trigger re-ingest from Farm; matches what production would do after the fix lands and a new ingest runs. Required: explicit blast-radius enumeration before execute (tenant_id, count of affected rows, dependent reads — DCL snapshots endpoint, NLQ snapshot list, AAM monitor, Console operator feed). pm2 dcl-dev only; never touch prod tenant_runs via the same script. Operator authorization required before any DELETE/UPDATE runs. | severity: degraded | blocking: clean operator dropdown on dev after write-site fix; will recur in any environment where the UUID-writer ran before being patched
    SHIPPED 2026-05-28 (after dcl#36 dispatch): dcl/scripts/backfill_snapshot_names.py applies the same `derive_run_name` helper from farm/services/identity.py (mirrored byte-identical, no cross-repo import per B6). Two-phase: --audit-only writes blast-radius enumeration to dcl/backfills/YYYY-MM-DD_snapshot_name_backfill.json; --apply runs the UPDATEs with a residual-row check and ROLLBACK-on-residual safety guard. Sequenced per user direction: land Z (write-site + DCL boundary) FIRST → verify new ingests write canonical → snapshot 54 polluted rows pre-backfill → run backfill. Detection broadened from "UUID-shaped" to "any non-canonical for the entity_id" to catch the cloud-spend-* class too (additional 7 rows beyond the original 42 UUID count). Total UPDATEs committed: 62 (54 rows × current/previous columns + 7 cloud-spend, less the rows where one column was already canonical). Final state: 0 non-canonical rows on aos-dev, 4 NULL rows (AAM relay path per dcl#38 open contract). Audit JSON committed for replay/rollback. RESOLVED 2026-05-28.

40. 2026-05-27 | orphan-script-audit | farm/scripts/push_cco_summary.py | Zero in-tree callers found across farm, aam, console, convergence repos. Hardcoded snapshot_name="cco_summary", tenant_id from env/CLI. Likely a dead manual-only script from an earlier WS-5 / FinOps demo; cannot be proven unused without an ops audit of bash history / pm2 logs / personal terminals. Action items: confirm with the operator whether this script is in any documented runbook or scheduled job; if confirmed dead, delete; if alive, fold into the #36 write-site fix design. Until then, treat as a potential third writer to tenant_runs on whichever tenant the operator passes. | severity: cosmetic | blocking: completeness of the write-site writer trace

41. 2026-05-28 | boundary-rejects-null | dcl/backend/api/routes/ingest_triples.py + backend/api/main.py (IngestTriplesRequest) | Follow-on hardening to dcl#38 option X (RESOLVED). Today the DCL ingest boundary rejects non-None non-canonical snapshot_name (422 NONCANONICAL_SNAPSHOT_NAME) but still ACCEPTS None — the AAM relay used to rely on that, and the read-side derivation in get_tenant_snapshots covers NULL. Post-X every writer (Console, Farm SE, cloud_spend, AAM relay) sends a canonical name, so NULL should no longer arrive from any live path. Hardening: make snapshot_name REQUIRED at the ingest boundary (IngestTriplesRequest: snapshot_name: str, not Optional) → any omitter 422s loudly instead of silently writing NULL + leaning on read-side derive. Once shipped, the read-side derivation in get_tenant_snapshots becomes dead code and can be deleted (one source of truth: the column). NOT shipped with the X dispatch (2026-05-28) for sequencing safety: the live finops demo ingests through the AAM relay; flipping the boundary to required before X is confirmed stable in the demo risks a loud break on the demo path. Preconditions before shipping #41: (1) X confirmed live for ≥1 demo cycle with zero NULL writes observed; (2) grep every caller of POST /api/dcl/ingest-triples (Farm dcl_triple_pusher, AAM dcl_push, cloud_spend, any Console path, any test harness) and confirm all send snapshot_name; (3) update IngestTriplesRequest + delete the get_tenant_snapshots read-side derivation fallback in the same PR; (4) negative test asserting 422 on omitted snapshot_name. | severity: cosmetic | blocking: nothing operationally; closes the last NULL-write path and lets the read-side derivation be deleted
    NOTE 2026-05-31 (fabric-connect-records) — precondition (2) now has a new caller: POST /api/dcl/ingest-records (backend/api/routes/ingest_records.py) builds an IngestRequest in-process and calls ingest_triples(); it passes snapshot_name from the records request, which may be None. When #41 ships (snapshot_name required), the records endpoint envelope must derive/require a canonical snapshot_name too. Add it to the caller sweep.

42. 2026-05-31 | fabric-connect-records | docs/INGEST_RECORDS_CONTRACT.md + AAM transport (separate repo) | B17/Playwright UI-acceptance gate for POST /api/dcl/ingest-records is NOT closed by this server-side workstream. The endpoint's only consumer is AAM's fabric transport, which still POSTs pre-converted triples to /api/dcl/ingest-triples today (decision (c) step 4: "re-point the orchestrator's AAM step onto the transport->DCL-ingest path" is AAM-side, not built here). There is no DCL operator UI that renders ingest-records output directly (the resolver HITL surface is API-only; DCL's internal TriplesPanel is orphaned per #25). Server contract verified via live TestClient integration against aos-dev (tests/test_fabric_connect_ingest.py 9/9, twice-identical B14) + the existing ingest/identity suites (26/26, no Farm regression). Unblock: AAM builds the transport to this contract and re-points its orchestrator step; then drive the operator path (AAM Fabrics drill / Console) through real UI events asserting the ingested records' triples + the Acme auto-applied match render. | severity: degraded | blocking: B17 UI-driven acceptance of the records ingest path end-to-end

43. 2026-05-31 | fabric-connect-records | migrations/016_resolver_registry.sql + prod DCL (:8004) | Migration 016 (canonical_registry, resolver_hitl_queue, resolver_hitl_audit) was applied to aos-dev only (Supabase MCP, schema shared_gdbmdr, GRANTed to dev_user) — the env this session built+tested against. It is NOT yet applied to prod. Before /api/dcl/ingest-records works on prod (:8004, .env): (1) apply 016 to the prod project's public schema (run_migration.py with prod DATABASE_URL, or Supabase MCP per the 001-014 pattern); (2) `pm2 restart dcl-backend` to load the new router (import is table-independent, so the restart is safe even pre-migration — existing endpoints keep working, the new one 5xx's until 016 lands). Additive only (new tables; no semantic_triples change) → no Convergence coordination (SCHEMA_CONTRACT.md). | severity: degraded | blocking: records ingest + resolver HITL on prod :8004

44. 2026-05-31 | fabric-connect-records | backend/resolver/record_resolver.py + backend/db/resolver_hitl_store.py | HITL right_value + dedup_key depend on the canonical registry state at resolve time. On a `replace=true` re-ingest of the SAME dcl_ingest_id AFTER the registry changed (a different canonical became the best match for a value), the triples are replaced cleanly but a stale pending/auto_applied HITL row from the prior run can persist alongside the new one (dedup_key differs because right_value differs). Harmless for a single demo run and for byte-identical replays (verified idempotent in test_acme_idempotent_on_replace); only bites when the registry mutated between replays. Fix when it matters: clear pending/auto_applied HITL rows for (tenant_id, entity_id, domain) at the start of a replace ingest, or key dedup on left_value only. | severity: cosmetic | blocking: nothing today; HITL row hygiene across registry-mutating replays

45. 2026-05-31 | fabric-connect-records | migrations/001_semantic_triple_store.sql resolution_workspaces vs migrations/016 resolver_hitl_queue | DCL now has the SE-path resolver HITL queue (resolver_hitl_queue, populated by the records path). The older generic resolution_workspaces table (migration 001, workspace_type customer/vendor/employee/account, status pending/in_review/resolved/escalated, candidates/evidence/decision JSONB) remains DEFINED-BUT-UNUSED — no code populates it (confirmed this session). 016's table was added rather than reused because resolution_workspaces' CHECK constraints (no auto_applied status; workspace_type enum too narrow for arbitrary domains) and JSONB-candidate shape do not fit the pair-match resolver contract without a constraint-relaxing migration (a bandaid). Decide whether to drop resolution_workspaces (dead since 001) or repurpose it; until then DCL carries two HITL-shaped tables, one live (resolver_hitl_queue) one dormant (resolution_workspaces). | severity: cosmetic | blocking: schema tidiness only

42. 2026-05-31 | dcl3-source-scoped-liveness | backend/db/triple_store.py:swap_and_deactivate (282) + replace_tenant_triples (70) + ~14 pointer-based reads (run_id IN current_run_id) | Fix (2) of the dcl3 dispatch — scope is_active liveness by composite key (tenant_id, source_system[, domain]) so independent source feeds don't deactivate each other's active set. Operator decision (2026-05-31): MOVE OFF dcl3 to the cross-source workstream, done there as the full rework (Option 1). NOT a self-contained change. Probe of aos-dev: runs are multi-source PER ENTITY — 58 active (tenant,entity) groups span >1 source_system; 57 (run,entity) groups are themselves multi-source (e.g. AeroHub-6LVL → aws_cost_explorer/datadog/jira/kafka/sap on ONE run_id). Consequences: (a) extending the tenant_runs pointer key to (tenant,entity,source_system) is UNSOUND — a multi-source run can't map to one run per source, and deactivate-by-run nukes siblings; (b) correct shape is row-level is_active scoping in swap_and_deactivate AND replace_tenant_triples (the replace DELETE is source-blind too), but the ~14 reads that resolve liveness via the entity pointer would then disagree with is_active → split-brain unless reconciled onto is_active; (c) tenant_runs + is_active are read SELECT-only by Convergence → SCHEMA_CONTRACT.md coordination gate; (d) overlaps the abandoned #27 store-rebuild (current_triples/swap_and_delete) which already implements run-level liveness — fold source-scoping into that revival rather than reworking is_active flipping twice. NO REGRESSION TODAY: current multi-source entities arrive on a single run, so run-level liveness covers them; the clobber only manifests once two feeds refresh a shared entity independently, which lands with cross-source. Done criterion when built: push source A then source B (separate runs) for one shared entity → both stay is_active and queryable through every liveness read, no split-brain. severity: degraded | blocking: independent per-source feed refresh on a shared entity (cross-source workstream); nothing in dcl3 or the single-source cloud_spend path today.
