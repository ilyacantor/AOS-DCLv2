# ME Carve-Out Plan: Move ME from DCL to a standalone repo

## Context

DCL hosts two products in one codebase: **SE** (Single Entity — semantic triple store, graph engine, ontology, reconciliation) and **ME** (Multi-Entity — COFA mapping, entity resolution, combining financials, EBITDA bridge, overlap, what-if). These are separate concerns in the RACI and belong in separate repos. The split brain is most acute in DCL: they share a FastAPI app, the same frontend, the same test suite, and no feature flags separating them.

Goal: new `~/code/me` repo at port 8007/3007, DCL reduced to SE only, three callers (Platform, NLQ, Console) updated to route ME requests to the new service.

---

## Architectural decisions

| Decision | Choice |
|----------|--------|
| URL prefix | ME uses `/api/me/*`. All callers updated. |
| compat.py | Deleted from DCL. Callers updated to call ME directly. |
| Data access | ME gets direct Supabase PG connection (same DATABASE_URL as DCL). ME writes only `cofa.*`-prefixed triples and `resolution_workspaces_v2`. |
| Shared infra | Files copied at fork time with dated headers. No `aos-common` package this sprint. |

---

## New repo: `~/code/me` (port 8007 / frontend 3007)

```
me/
├── backend/
│   ├── api/
│   │   ├── main.py                        # FastAPI app, ME routers only
│   │   └── routes/
│   │       ├── merge_overview.py          # moved from DCL
│   │       ├── merge_conflicts.py
│   │       ├── cofa_mapping.py
│   │       ├── cofa_validation.py
│   │       ├── resolution_v2.py
│   │       ├── reports_combining_v2.py
│   │       ├── reports_bridge_v2.py
│   │       ├── reports_overlap_v2.py
│   │       ├── reports_whatif_v2.py
│   │       └── v2_helpers.py              # moved from DCL (resolve_tenant_and_run)
│   ├── engine/
│   │   ├── combining_v2.py
│   │   ├── cross_sell_v2.py
│   │   ├── ebitda_bridge_v2.py
│   │   ├── overlap_v2.py
│   │   ├── qoe_v2.py
│   │   ├── upsell_v2.py
│   │   ├── entity_resolution_v2.py
│   │   ├── cofa_mapping_writer.py
│   │   ├── query_resolver_v2.py           # moved from DCL (only ME uses it)
│   │   └── what_if_v2.py
│   ├── core/
│   │   ├── db.py                          # copied from DCL; env vars renamed ME_*
│   │   ├── constants.py
│   │   └── security_constraints.py
│   ├── db/
│   │   └── triple_store.py               # copied from DCL, unchanged
│   ├── domain/
│   │   └── base.py
│   └── utils/
│       └── log_utils.py
├── src/
│   ├── main.tsx
│   ├── App.tsx                            # ME app shell (Merge as sole view)
│   └── components/
│       └── MergePanel.tsx                 # moved from DCL
├── config/
│   ├── ontology_concepts.yaml
│   └── source_aliases.yaml
├── data/
│   └── engagements/
├── migrations/
│   └── run_migration.py                   # asserts tables exist; no schema creation
├── tests/
│   ├── conftest.py
│   ├── test_3a_query_resolver.py
│   ├── test_3b_entity_resolution.py
│   ├── test_3c_combining.py
│   ├── test_3d_overlap.py
│   ├── test_3e_ebitda_qoe.py
│   ├── test_3f_whatif.py
│   └── test_cofa_gate.py
├── requirements.txt
├── vite.config.ts
├── package.json
├── render.yaml                            # port 8007
└── CLAUDE.md
```

---

## Files removed from DCL

### `backend/api/main.py`
Remove import lines 79–88 (10 ME routers: resolution_v2, reports_combining_v2, reports_overlap_v2, reports_bridge_v2, reports_whatif_v2, cofa_validation, cofa_mapping, merge_overview, merge_conflicts, compat) plus compat_router include_router call. Remove LEGACY_JSON_LOAD block (lines 361–365) and reports_router import (line 75).

### `backend/api/routes/` — delete
`merge_overview.py`, `merge_conflicts.py`, `cofa_mapping.py`, `cofa_validation.py`, `resolution_v2.py`, `reports_combining_v2.py`, `reports_bridge_v2.py`, `reports_overlap_v2.py`, `reports_whatif_v2.py`, `compat.py`, `v2_helpers.py`, `reports.py`

### `backend/engine/` — delete
`combining_v2.py`, `cross_sell_v2.py`, `ebitda_bridge_v2.py`, `overlap_v2.py`, `qoe_v2.py`, `upsell_v2.py`, `entity_resolution_v2.py`, `cofa_mapping_writer.py`, `query_resolver_v2.py`, `what_if_v2.py`

### `src/`
Delete `src/components/MergePanel.tsx`. In `src/App.tsx`: remove `'merge'` from `MainView` type, remove from `navTabs` (line 298), remove `MergePanel` import and render block.

### `tests/` — move to ME, delete from DCL
`test_3a_query_resolver.py`, `test_3b_entity_resolution.py`, `test_3c_combining.py`, `test_3d_overlap.py`, `test_3e_ebitda_qoe.py`, `test_3f_whatif.py`, `test_cofa_gate.py`

### `backend/engine/engagement.py`
Inline the engagement JSON read (5 lines) directly into `triple_monitor.py`, then delete.

---

## Caller repo changes

### Platform (`~/code/platform`)
- Add `ME_API_URL` env var
- `app/maestra/tool_executor.py`: route merge/COFA calls to `ME_API_URL` + `/api/me/*`
- `app/maestra/routes.py`: update proxy routes for `/api/dcl/merge/*` or `/api/dcl/cofa/*`
- Playwright E2E `e2e/cofa-merge.spec.ts`: update base URLs

### NLQ (`~/code/nlq`)
- `dcl_proxy.py`: overlap, cross-sell, upsell, bridge calls → `ME_API_URL` + `/api/me/reports/*`
- Add `ME_API_URL` env var

### Console (`~/code/console`)
- `client.ts` (or equivalent): combining/bridge calls → `ME_API_URL` + `/api/me/reports/*`
- Proxy layer: route `/api/proxy/dcl/merge/*` and `/api/proxy/dcl/reports/combining*` → ME
- Add `ME_API_URL` env var

---

## ME environment variables

```
DATABASE_URL         # same Supabase PG URL as DCL
ME_POOL_MAX_CONN     # 10
ME_POOL_MIN_CONN     # 2
ME_ENV               # production / dev
FARM_API_URL         # same value as DCL (ME tests use Farm ground truth)
CORS_ORIGINS         # ME domain
BACKEND_PORT         # 8007
```

---

## Pre-execution: Branch setup

Create branch `convergence-sonnet` off `dev` in every affected repo before any code changes:

```
~/code/dcl       git checkout dev && git checkout -b convergence-sonnet
~/code/platform  git checkout dev && git checkout -b convergence-sonnet
~/code/nlq       git checkout dev && git checkout -b convergence-sonnet
~/code/console   git checkout dev && git checkout -b convergence-sonnet
~/code/me        (new repo — initialized directly on convergence-sonnet)
```

All work happens on `convergence-sonnet`. PRs merge to `dev` when each phase is verified.

---

## Migration sequence

**Phase 1 — Build ME repo (DCL untouched)**
1. Create `~/code/me` from scratch
2. Copy shared infra files with fork-date comment headers
3. Copy ME engines, routes, MergePanel, tests; update import paths
4. Create `me/backend/api/main.py` — ME routers only, no DCLEngine, no SemanticMapper
5. Create ME frontend (Vite app, MergePanel as sole view)
6. Write `me/migrations/run_migration.py` — assert-only
7. `pytest tests/` in ME — all 7 test files pass before continuing

**Phase 2 — Deploy ME in parallel (dark)**
1. Deploy ME to Render; DCL unchanged
2. Validate ME health and all routes against shared Supabase DB
3. No traffic routed to ME yet

**Phase 3 — Update callers (one at a time)**
1. Platform: add ME_API_URL, update tool_executor.py and proxy routes, run 146+ tests
2. NLQ: update dcl_proxy.py, run NLQ tests
3. Console: update client.ts and proxy layer, run Playwright suite

**Phase 4 — Strip DCL (only after callers confirmed in prod)**
1. Delete ME files from DCL (engines, routes, compat.py, reports.py, MergePanel)
2. Remove ME route registrations from main.py
3. Remove merge tab from App.tsx
4. Delete ME test files from DCL tests/
5. Inline engagement.py into triple_monitor.py, delete it
6. `pytest tests/` in DCL — all remaining SE tests pass
7. DCL frontend: no Merge tab, no broken imports

---

## Consequences analysis

### DCL (SE after split)

**Gains:** ~20 fewer files, ~35% fewer module imports at startup, more connection pool headroom, legible as single-concern semantic store.

**Risks during migration:**
- compat.py currently provides `/api/reports/*`. If deleted before NLQ/Console are updated, those callers 404 immediately. Phase ordering is strict: callers updated in Phase 3, compat.py deleted in Phase 4.
- `engagement.py` import in `triple_monitor.py` causes startup failure if deleted before `triple_monitor.py` is patched. Must be done atomically.
- DCL test files that import ME engines break at import time if engine files are deleted first. Delete order: ME test files from DCL → then ME engine files.

**What DCL callers notice:** Nothing. DCL's SE routes (`/api/dcl/graph/*`, `/api/dcl/query`, `/api/dcl/entities/*`, `/api/dcl/triples/*`, `/api/dcl/recon/*`) are unchanged.

---

### ME (new service)

**Gains:** Independent deployment, scaling, versioning. Own port visible in aos-launch.sh. ME frontend can evolve without touching DCL's React app.

**Risks:**
- New PG connection pool adds up to 10 connections. **Verify Supabase connection limit before Phase 2 deploy.** Current usage: DCL 20, ME 10 = 30 minimum required.
- ME is a new Render service with its own cold start. If ME is down, all ME features unavailable. DCL SE features unaffected.
- `run_migration.py` is assert-only. If it runs before DCL has migrated a fresh database (staging reset), ME fails startup loudly. Correct behavior per AOS rules.
- Fork drift on `db.py`, `constants.py`, `triple_store.py`. Mitigated by pinned fork-date headers and a test checking key constants match.

---

### Platform

**Callers affected:** Maestra tool calls to COFA mapping, merge overview, conflict resolution, combining statements — currently hitting `DCL_BASE_URL + /api/dcl/cofa/*` and `/api/dcl/merge/*`.

**Consequence of delay:** If Platform is not updated before Phase 4 DCL strip, Maestra's COFA and merge tools 404. No silent failure — AOS rules prohibit swallowing errors.

**Risk:** `tool_executor.py` may have ME-bound calls beyond the obvious merge/COFA routes (e.g., overlap or cross-sell). Full audit required before Phase 3.

---

### NLQ

**Callers affected:** `dcl_proxy.py` routes all `/api/reports/*` to DCL. After DCL drops compat.py, those 404. Reports affected: combining IS, entity overlap, cross-sell, EBITDA bridge, QoE, what-if, persona dashboard.

**Consequence of delay:** NLQ Reports portal loses all ME-derived reports. SE-derived reports (revenue trends, entity stats) unaffected.

**Risk:** NLQ may have additional calls to DCL for ME data beyond dcl_proxy.py. Audit `nlq/` for any hardcoded `/api/dcl/reports/` or `/api/dcl/merge/` references.

---

### Console

**Callers affected:** Combining statements view (P&L, BS, CF four-column), EBITDA bridge report, convergence workflow UI.

**Consequence of delay:** Console's convergence UI goes blank or errors. Operator-facing features impacted. SE views (entity pipeline status, triple counts, graph health) unaffected.

**Risk:** Console may proxy ME calls through its own backend (`/api/proxy/dcl/*`). Proxy layer must be updated to route ME paths to ME before Phase 4.

---

### Farm, AOD, AAM

No dependencies on ME. No changes required.

---

### Data

No data migration. All existing COFA triples, resolution workspaces, and engagement state stay in Supabase PG unchanged.

---

### Local development

`aos-launch.sh` must add ME: `cd ~/code/me && uvicorn backend.api.main:app --port 8007 &`. ME frontend: `npm run dev -- --port 3007`. Add `ME_API_URL=http://localhost:8007` to Platform, NLQ, and Console `.env` files.

---

### RACI update required

Add ME module row in `ONGOING_PROMPTS/AOS_MASTER_RACIv8.csv`:
- **ME owns:** COFA unification, combining statements, cross-entity resolution, overlap analysis, EBITDA bridge, what-if scenarios, conflict register
- **DCL retains:** Semantic triple store, ontology, schema-on-write, single-entity resolution, graph engine, RAG, MCP server

---

## Critical files

| File | Change |
|------|--------|
| `dcl/backend/api/main.py:66-89` | Remove 10 ME router imports + include_router calls |
| `dcl/backend/api/routes/compat.py` | Delete |
| `dcl/src/App.tsx:292-299` | Remove 'merge' from navTabs |
| `dcl/src/components/MergePanel.tsx` | Delete; move to me/src/components/ |
| `dcl/backend/engine/combining_v2.py` | Move to me/backend/engine/ |
| `dcl/backend/api/routes/merge_overview.py` | Move to me/backend/api/routes/ |
| `platform/app/maestra/tool_executor.py` | Add ME_API_URL routing for merge/COFA |
| `nlq/dcl_proxy.py` | Route /api/reports/* calls to ME_API_URL |
| `console/client.ts` (or equiv) | Route combining/bridge calls to ME_API_URL |
| `aos-launch.sh` | Add ME process (port 8007/3007) |
| `ONGOING_PROMPTS/AOS_MASTER_RACIv8.csv` | Add ME module row |

---

## Verification

- `pytest tests/` in `~/code/me` — all 7 ME test files pass
- `pytest tests/` in `~/code/dcl` — all remaining SE tests pass; zero ME engine imports in collection
- DCL frontend: no Merge tab; all other tabs functional
- ME frontend: MergePanel loads; COFA data visible for ME engagements
- Platform Playwright: cofa-merge.spec.ts passes against ME's new URL
- NLQ: overlap/bridge reports resolve correctly via ME
- Console: combining statements render correctly via ME
- `grep -r "combining_v2\|entity_resolution_v2\|cofa_mapping_writer\|OverlapEngineV2" ~/code/dcl/` returns zero results
