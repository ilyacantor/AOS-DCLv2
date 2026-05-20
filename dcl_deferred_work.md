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

26. 2026-05-20 | identity-remediation R3 | backend/api/routes/triple_monitor.py (triples_browse, triples_browse_batch) | R3 added a required `tenant_id` param + unconditional `WHERE tenant_id = %s` to both browse endpoints. This is tenant SCOPING, not authentication — the endpoint does not verify the caller owns the tenant_id it passes. A caller can still pass another tenant's id. Cryptographic auth (token-derived tenant, mirroring the MCP path at backend/api/mcp_auth.py / triple_store.mcp_query_triples) is the real close — deliberately deferred per operator decision (R3 adjusted to "required input, filter, no auth"). Follow-up: add token auth to the plain-HTTP triple read endpoints. | severity: degraded | blocking: true cross-tenant isolation on the plain-HTTP browse endpoints (scoping holds; auth does not)

27. 2026-05-20 | identity-remediation R3 verification | dcl-dev (:8104) — backend/db/triple_store.py get_tenant_snapshots (counts_sql) + broad triples/browse | dcl-dev intermittently wedges under burst load: /api/health, /api/dcl/snapshots and broad triples/browse queries hang past the Postgres statement_timeout (psycopg2.errors.QueryCanceled observed at triple_store.py:460 counts_sql), the single uvicorn worker stops responding (curl → 000), and httpx.ReadTimeout cascades into NLQ callers — making test_nlq_v2_client.py runs non-deterministic (B14). counts_sql (SELECT run_id, COUNT(*) ... WHERE run_id = ANY(%s::uuid[]) GROUP BY run_id) lacks a tenant_id predicate and cannot use the (tenant_id, run_id) index. Tenant-scoping counts_sql was TRIED this session (add tenant_id = %s; run_ids already all belong to the tenant, zero behavior change) and did NOT clear the wedge — reverted under A13 — so the root cause is broader than that one query. Likely: dev semantic_triples has grown large across accumulated test runs and/or lacks indexes for the broad scans, possibly with Supabase dev-pooler contention. NOT R3-caused — R3 only touched triple_monitor.py browse endpoints; selective triples/browse?tenant_id=...&domain=...&entity_id=... returns 200 OK fast in the same logs. Needs a proper DCL dev-DB perf pass: EXPLAIN the snapshots + broad-browse queries, add/verify indexes on semantic_triples, prune stale dev run data. Workaround: pm2 restart dcl-dev clears it transiently. | severity: degraded | blocking: dcl-dev reliability under load; non-deterministic NLQ test runs until the dev-DB perf pass lands
