# Canonical Convergence Carve-Out Blueprint

**Purpose:** Agent-executable plan to extract ME (Multi-Entity / Convergence M&A) from DCL into a standalone `convergence` repo. DCL becomes SE-only. Three callers (Platform, NLQ, Console) rerouted. Zero downtime via proxy phase.

**Repo:** `~/code/convergence`
**Ports:** Backend 8010 / Frontend 3010
**Branch:** `convergence-opus` off `dev` in all affected repos (dcl, console, platform, nlq)

---

## 1. Database Strategy

Both services share the same Supabase Postgres instance. The architectural invariant is "entity is a tag in one store" — ME reports (combining, bridge, QoE) SELECT across entity_a and entity_b triples in a single query. Separate databases would require cross-database joins, which is prohibited.

### Table Ownership Matrix

| Table | Owner | Who Reads | Who Writes | Migrations Live In |
|-------|-------|-----------|------------|--------------------|
| `semantic_triples` | DCL | DCL + Convergence | DCL only (via `POST /api/dcl/ingest-triples`) | `dcl/migrations/` |
| `dimension_values_v2` | DCL | DCL + Convergence | DCL only | `dcl/migrations/` |
| `tenant_runs` | DCL | DCL + Convergence | DCL only | `dcl/migrations/` |
| `tenant_registry` | DCL | DCL | DCL only | `dcl/migrations/` |
| `resolution_workspaces_v2` | Convergence | Convergence | Convergence only | `convergence/migrations/` |
| `whatif_scenarios` | Convergence | Convergence | Convergence only | `convergence/migrations/` |
| `engagement_state` | Convergence | Convergence | Convergence only | `convergence/migrations/` |

### Read/Write Contracts

- Convergence reads DCL-owned tables directly (SELECT only). ME report engines issue dozens of metric queries per report — HTTP round-trips per metric would blow latency ceilings. Direct PG reads from the same database are sub-millisecond.
- Convergence never writes to DCL-owned tables directly. All triple writes go through DCL's `POST /api/dcl/ingest-triples`. This preserves DCL's write-side invariants: old-run deactivation, COPY-based bulk ingest, `is_active` flag management, `tenant_runs.current_run_id` updates.
- DCL never reads or writes convergence-owned tables. If DCL needs engagement context (e.g., `mai.py`), it calls convergence over HTTP.

### Pool Sizing

| Service | Env Var | Value |
|---------|---------|-------|
| DCL | `POOL_MAX_CONN` | 15 |
| Convergence | `CONVERGENCE_POOL_MAX_CONN` | 10 |

Combined total: 25. Must stay within Supabase connection limit (60 free tier, higher on paid). Remaining headroom for Supabase pooler, monitoring, migrations.

Convergence report queries (combining, bridge) aggregate across large triple sets and may need longer timeouts. Set `CONVERGENCE_STATEMENT_TIMEOUT` independently in `convergence/backend/core/db.py` via `SET statement_timeout` in the connection pool initialization.

### Schema Contract

Create `SCHEMA_CONTRACT.md` in the DCL repo documenting the `semantic_triples` column layout (columns, types, indexes, constraints). Rules:
- Additive changes (new columns with defaults) are non-breaking.
- Column renames, type changes, or removals are breaking and require convergence repo coordination before merge.

---

## 2. API Boundary

### Convergence → DCL (HTTP)

| Endpoint | Purpose | Already Exists |
|----------|---------|----------------|
| `POST /api/dcl/ingest-triples` | COFA triple writes | Yes |
| `GET /api/dcl/semantic-export` | Semantic catalog | Yes |

### DCL → Convergence (HTTP)

| Endpoint | Purpose | New |
|----------|---------|-----|
| `GET /api/convergence/engagement/active` | `mai.py` calls this instead of importing `engagement.py` | Yes (Phase 2) |

### Convergence → PG (Direct)

SELECT only against `semantic_triples`, `dimension_values_v2`, `tenant_runs`. No HTTP intermediary for report-time metric queries.

---

## 3. File Inventory

### Positive Inventory — Move to Convergence

**Routes (9 files):**
- `backend/api/routes/resolution_v2.py`
- `backend/api/routes/merge_overview.py`
- `backend/api/routes/merge_conflicts.py`
- `backend/api/routes/cofa_mapping.py`
- `backend/api/routes/cofa_validation.py`
- `backend/api/routes/reports_combining_v2.py`
- `backend/api/routes/reports_bridge_v2.py`
- `backend/api/routes/reports_overlap_v2.py`
- `backend/api/routes/reports_whatif_v2.py`

**Engines (10 files):**
- `backend/engine/combining_v2.py`
- `backend/engine/ebitda_bridge_v2.py`
- `backend/engine/qoe_v2.py`
- `backend/engine/overlap_v2.py`
- `backend/engine/cross_sell_v2.py`
- `backend/engine/upsell_v2.py`
- `backend/engine/entity_resolution_v2.py`
- `backend/engine/cofa_mapping_writer.py`
- `backend/engine/what_if_v2.py`
- `backend/engine/engagement.py` (contains `get_active_engagement()` — ME engines import this)

**Support (verify in Phase 0 audit, move if imported by ME engines):**
- `backend/engine/engagement_config.py`
- `backend/engine/_engine_cache.py` (caches cross_sell, ebitda_bridge, qoe results)
- `backend/engine/dashboards.py` (imports `get_active_engagement`, ME-specific dashboards)
- `backend/engine/revenue_bridge.py` (ME financial bridge logic)
- `backend/db/engagement_store.py` (engagement lifecycle persistence)
- `backend/db/resolution_store.py` (resolution workspace persistence)

**Data:**
- `data/engagements/demo-001.json` (engagement config file loaded by `engagement.py`)

**Frontend (1 file):**
- `src/components/MergePanel.tsx` (zero-prop, self-contained)

**Tests (7 files):**
- `tests/test_3a_query_resolver.py`
- `tests/test_3b_entity_resolution.py`
- `tests/test_3c_combining.py`
- `tests/test_3d_overlap.py`
- `tests/test_3e_ebitda_qoe.py`
- `tests/test_3f_whatif.py`
- `tests/test_cofa_gate.py`

**Migrations (ME-owned tables):**
- `migrations/002_resolution_workspaces_v2.sql`
- `migrations/003_whatif_scenarios.sql`
- `migrations/006_seed_engagement.sql`

### Fork Inventory — Both repos get a copy

| File | Convergence Copy | DCL Copy |
|------|-----------------|----------|
| `backend/engine/query_resolver_v2.py` | Retains engagement lookup | Drops `get_active_engagement` import (engagement context arrives as constructor params) |
| `backend/api/routes/v2_helpers.py` | Keeps full resolution chain (explicit param → engagement_state → semantic_triples) | Drops engagement lookup (explicit param → semantic_triples only) |
| `backend/core/db.py` | Copied with dated fork header; env vars renamed to `CONVERGENCE_*` | Unchanged |
| `backend/core/constants.py` | Copied with dated fork header | Unchanged |
| `backend/db/triple_store.py` | Copied with dated fork header | Unchanged |
| `backend/core/security_constraints.py` | Copied with dated fork header | Unchanged |
| `backend/domain/base.py` | Copied with dated fork header | Unchanged |
| `backend/utils/log_utils.py` | Copied with dated fork header | Unchanged |

Fork header format:
```python
# FORKED from dcl/backend/core/db.py on YYYY-MM-DD
# Changes from DCL original: [describe divergence]
# aos-common extraction planned post-carveout
```

### Negative Inventory — STAYS in DCL, DO NOT MOVE

**Routes:**
- `backend/api/routes/entities.py` — serves SE entity browsing
- `backend/api/routes/ingest.py`, `ingest_triples.py` — triple ingestion (SE core)
- `backend/api/routes/graph_traversal.py` — graph queries
- `backend/api/routes/triple_monitor.py` — triple monitoring (patched in Phase 3, not moved)
- `backend/api/routes/reconciliation.py`, `recon_checks.py` — source reconciliation
- `backend/api/routes/temporal.py` — temporal versioning
- `backend/api/routes/mai.py` — Mai MCP (rewired in Phase 3, not moved)
- `backend/api/routes/export_pipes.py` — semantic export
- `backend/api/routes/deprecated.py` — deprecated routes (SE)
- `backend/api/routes/compat.py` — deleted in Phase 5, NOT moved to convergence
- `backend/api/routes/reports.py` — deleted in Phase 5

**Engines:**
- `backend/engine/dcl_engine.py` — SE orchestrator
- `backend/engine/semantic_graph.py` — in-memory graph
- `backend/engine/query_resolver.py` — v1 NLQ query resolution (SE)
- `backend/engine/ontology.py` — concept definitions
- `backend/engine/graph_store.py` — graph singleton
- `backend/engine/mai.py` — Mai tools (rewired in Phase 3)
- `backend/engine/narration_service.py`, `rag_service.py`, `persona_view.py`
- `backend/engine/schema_loader.py`, `mapping_service.py`, `source_normalizer.py`
- `backend/engine/edge_index.py`, `graph_types.py`
- `backend/engine/materialized_views.py`, `metric_materializer.py`
- `backend/engine/provenance_service.py`, `temporal_versioning.py`
- `semantic_mapper/` — field-to-concept mapping (SE only)

**Frontend:**
- `SankeyGraph`, `DashboardTab`, `ContextTab`, `IngestTab`, `ReconTab`, `TriplesPanel`, `SnapshotPanel`, `MonitorPanel`
- All shared UI components, hooks, styles

---

## 4. Convergence Repo Structure

```
convergence/
├── backend/
│   ├── api/
│   │   ├── main.py                        # FastAPI app, convergence routers only, port 8010
│   │   └── routes/
│   │       ├── merge_overview.py
│   │       ├── merge_conflicts.py
│   │       ├── cofa_mapping.py
│   │       ├── cofa_validation.py
│   │       ├── resolution_v2.py
│   │       ├── reports_combining_v2.py
│   │       ├── reports_bridge_v2.py
│   │       ├── reports_overlap_v2.py
│   │       ├── reports_whatif_v2.py
│   │       ├── v2_helpers.py              # forked: keeps engagement lookup
│   │       └── engagement_api.py          # NEW: GET /api/convergence/engagement/active
│   ├── engine/
│   │   ├── combining_v2.py
│   │   ├── cross_sell_v2.py
│   │   ├── ebitda_bridge_v2.py
│   │   ├── overlap_v2.py
│   │   ├── qoe_v2.py
│   │   ├── upsell_v2.py
│   │   ├── entity_resolution_v2.py
│   │   ├── cofa_mapping_writer.py
│   │   ├── query_resolver_v2.py           # forked: keeps engagement lookup
│   │   ├── what_if_v2.py
│   │   ├── engagement.py                  # moved from DCL
│   │   ├── engagement_config.py           # moved from DCL
│   │   ├── _engine_cache.py              # moved from DCL (verify in Phase 0)
│   │   ├── dashboards.py                 # moved from DCL (verify in Phase 0)
│   │   └── revenue_bridge.py             # moved from DCL (verify in Phase 0)
│   ├── core/
│   │   ├── db.py                          # forked: env vars renamed CONVERGENCE_*
│   │   ├── constants.py                   # forked with dated header
│   │   └── security_constraints.py        # forked with dated header
│   ├── db/
│   │   ├── triple_store.py               # forked with dated header
│   │   ├── engagement_store.py            # moved from DCL
│   │   └── resolution_store.py            # moved from DCL
│   ├── domain/
│   │   └── base.py                        # forked with dated header
│   └── utils/
│       └── log_utils.py                   # forked with dated header
├── src/
│   ├── main.tsx                           # React entry point
│   ├── App.tsx                            # Convergence app shell, MergePanel as sole view
│   └── components/
│       └── MergePanel.tsx                 # moved from DCL
├── config/
│   ├── ontology_concepts.yaml             # copied (ME engines reference concepts)
│   └── source_aliases.yaml                # copied
├── data/
│   └── engagements/
│       └── demo-001.json                  # moved from DCL
├── migrations/
│   ├── 002_resolution_workspaces_v2.sql
│   ├── 003_whatif_scenarios.sql
│   ├── 006_seed_engagement.sql
│   └── run_migration.py                   # assert-only: verifies tables exist, no schema creation
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
├── render.yaml                            # Render service config, port 8010
└── CLAUDE.md
```

---

## 5. Environment Variables

### Convergence Service

```
DATABASE_URL                   # same Supabase PG URL as DCL
CONVERGENCE_POOL_MAX_CONN=10
CONVERGENCE_POOL_MIN_CONN=2
CONVERGENCE_STATEMENT_TIMEOUT=60000   # ms, longer than DCL default for report aggregation
CONVERGENCE_ENV=production     # or dev
FARM_API_URL                   # same value as DCL (convergence tests use Farm ground truth)
DCL_API_URL=http://localhost:8004     # for triple writes via POST /api/dcl/ingest-triples
CORS_ORIGINS                   # convergence domain + console domain
BACKEND_PORT=8007              # note: internal port, Render maps externally
```

### Added to DCL

```
CONVERGENCE_API_URL=http://localhost:8010   # mai.py HTTP calls to engagement endpoint
```

### Added to Platform

```
CONVERGENCE_API_URL=http://localhost:8010   # tool_executor.py routes COFA/merge calls
```

### Added to NLQ

```
CONVERGENCE_API_URL=http://localhost:8010   # dcl_proxy.py routes reports calls
```

### Added to Console

```
CONVERGENCE_API_URL=http://localhost:8010   # client.ts and proxy layer route combining/bridge calls
```

---

## 6. Migration Phases

---

### Phase 0: Pre-Flight Audit & Decision Log

**Goal:** Mechanically verify the dependency graph before any code moves. No code changes in this phase.

**Action 1 — Branching:**
```bash
cd ~/code/dcl       && git checkout dev && git checkout -b convergence-opus
cd ~/code/platform  && git checkout dev && git checkout -b convergence-opus
cd ~/code/nlq       && git checkout dev && git checkout -b convergence-opus
cd ~/code/console   && git checkout dev && git checkout -b convergence-opus
# convergence repo initialized directly on convergence-opus
mkdir -p ~/code/convergence && cd ~/code/convergence && git init && git checkout -b convergence-opus
```

**Action 2 — Import audit script:**
Run the following from `~/code/dcl/`. Pipe output to `~/code/convergence/PHASE0_AUDIT.txt`.

```bash
echo "=== POSITIVE INVENTORY: WHO IMPORTS THESE FILES ==="
for module in \
  combining_v2 ebitda_bridge_v2 qoe_v2 overlap_v2 cross_sell_v2 \
  upsell_v2 entity_resolution_v2 cofa_mapping_writer what_if_v2 \
  engagement engagement_config _engine_cache dashboards revenue_bridge \
  engagement_store resolution_store cofa_mapping cofa_validation \
  resolution_v2 merge_overview merge_conflicts \
  reports_combining_v2 reports_bridge_v2 reports_overlap_v2 reports_whatif_v2 \
  v2_helpers query_resolver_v2; do
  echo "--- $module ---"
  grep -rn "import.*${module}\|from.*${module}" --include="*.py" .
done

echo ""
echo "=== V1 ENGINE FILES: DEAD OR ALIVE ==="
for module in \
  entity_resolution ebitda_bridge qoe cross_sell what_if; do
  echo "--- v1: $module ---"
  grep -rn "import.*\b${module}\b\|from.*\b${module}\b" --include="*.py" . \
    | grep -v "_v2"
done

echo ""
echo "=== NEGATIVE INVENTORY: VERIFY THESE HAVE NO ME COUPLING ==="
for module in \
  dcl_engine semantic_graph ontology graph_store mai \
  narration_service rag_service persona_view schema_loader \
  mapping_service source_normalizer; do
  echo "--- $module ---"
  grep -rn "import.*combining_v2\|import.*entity_resolution_v2\|import.*cofa_mapping_writer\|import.*overlap_v2\|import.*ebitda_bridge_v2\|import.*qoe_v2\|import.*cross_sell_v2\|import.*what_if_v2" --include="*.py" "backend/engine/${module}.py" 2>/dev/null || echo "(file not found or no hits)"
done

echo ""
echo "=== COMPAT.PY CONSUMERS ==="
grep -rn "api/dcl/reports/\|api/reports/" --include="*.py" --include="*.ts" --include="*.tsx" \
  ~/code/nlq/ ~/code/console/ ~/code/platform/ 2>/dev/null

echo ""
echo "=== ENGAGEMENT.PY FULL BLAST RADIUS ==="
grep -rn "get_active_engagement\|from.*engagement import\|from.*engagement_store" \
  --include="*.py" .
```

**Action 3 — Decision log:**
Create `~/code/convergence/PHASE0_DECISION_LOG.md` with this template:

```markdown
# Phase 0 Decision Log

## v1 Engine Files

| File | Imported By | Decision | Rationale |
|------|-------------|----------|-----------|
| engine/entity_resolution.py | (from audit) | Move / Delete / Defer | |
| engine/ebitda_bridge.py | (from audit) | Move / Delete / Defer | |
| engine/qoe.py | (from audit) | Move / Delete / Defer | |
| engine/cross_sell.py | (from audit) | Move / Delete / Defer | |
| engine/what_if.py | (from audit) | Move / Delete / Defer | |

## Support Files (verify needed)

| File | Imported By | Decision | Rationale |
|------|-------------|----------|-----------|
| engine/_engine_cache.py | (from audit) | Move / Skip | |
| engine/dashboards.py | (from audit) | Move / Skip | |
| engine/revenue_bridge.py | (from audit) | Move / Skip | |

## Unexpected Dependencies Found

| File | Unexpected Import | Resolution |
|------|-------------------|------------|
| (from audit) | | |
```

**Action 4 — Execute v1 decisions:**
If a v1 file has zero importers, delete it from DCL immediately. Commit to `convergence-opus` branch in DCL. If a v1 file is actively imported, add it to the positive inventory with a note on which files import it.

**Gate:** Phase 0 is complete when:
- `PHASE0_AUDIT.txt` exists and has been reviewed
- `PHASE0_DECISION_LOG.md` is filled in with decisions for every v1 and support file
- All v1 dead code is deleted from DCL
- All decisions committed to `convergence-opus` in DCL

---

### Phase 1: Convergence Skeleton (Local Build)

**Goal:** Build the convergence repo from scratch and pass all ME tests locally. DCL is untouched except for the Phase 0 cleanup already committed.

**Action 1 — FastAPI skeleton:**
Create `convergence/backend/api/main.py` with:
- FastAPI app on port 8010
- Health check at `GET /api/health`
- CORS configuration
- All 9 ME route routers mounted under `/api/convergence/`
- No DCLEngine, no SemanticMapper, no SE imports

**Action 2 — Copy positive inventory:**
Copy all files from the positive inventory (Section 3) to their corresponding paths in the convergence repo structure (Section 4). Update all internal import paths from `backend.engine.X` / `backend.api.routes.X` to match the new repo structure. Verify no import references `dcl` or any DCL-specific path.

**Action 3 — Fork shared files:**
Create convergence copies of all files in the Fork Inventory (Section 3):
- `db.py`: rename env vars to `CONVERGENCE_*`, add fork header
- `constants.py`, `triple_store.py`, `security_constraints.py`, `base.py`, `log_utils.py`: copy with fork headers
- `query_resolver_v2.py`: convergence copy retains `get_active_engagement` import
- `v2_helpers.py`: convergence copy retains full resolution chain (explicit param → engagement_state → semantic_triples)

**Action 4 — Frontend shell:**
Create the React app shell for convergence:
- `vite.config.ts` with proxy to port 8010 and dev server on port 3010
- `package.json` with dependencies matching DCL's current React/Vite/Tailwind setup
- `src/main.tsx` as entry point
- `src/App.tsx` with MergePanel as the sole view (no tabs, no navigation — MergePanel is the app)
- Copy any shared UI primitives MergePanel imports (`ui/tabs.tsx`, `ui/resizable.tsx`, `ui/toaster.tsx`, etc.)

**Action 5 — Migrations:**
Copy `002`, `003`, `006` migration files. Create `run_migration.py` as assert-only — it verifies the tables exist in PG and fails loudly if they don't. No `CREATE TABLE` statements.

**Action 6 — Schema contract:**
Create `~/code/dcl/SCHEMA_CONTRACT.md` documenting:
- `semantic_triples` column names, types, nullable flags, defaults
- All indexes on `semantic_triples` (including compound indexes on `(tenant_id, entity_id)`)
- `dimension_values_v2` column layout
- `tenant_runs` column layout
- Header: "Any breaking change to these schemas requires coordination with the convergence repo before merge."

**Action 7 — Config files:**
Copy `config/ontology_concepts.yaml` and `config/source_aliases.yaml` to convergence. ME engines reference concept definitions during report generation.

**Action 8 — CLAUDE.md:**
Create `convergence/CLAUDE.md` defining:
- Service identity: Convergence (ME) — entity resolution, COFA, combining financials, EBITDA bridge, QoE, cross-sell, overlap, what-if
- Port: 8010/3010
- Database: shared PG, reads `semantic_triples` (SELECT only), writes via DCL HTTP
- Table ownership: `resolution_workspaces_v2`, `whatif_scenarios`, `engagement_state`
- Convergence does NOT own: triple store, ontology, semantic graph, query resolution, visualization

**Gate:**
```bash
cd ~/code/convergence && pytest tests/
# All 7 ME test files must pass
# Zero import errors at collection time
```

**Rollback:** Delete the convergence repo contents and restart Phase 1. DCL is unmodified.

---

### Phase 2: Dark Deploy & DCL Proxy

**Goal:** Deploy convergence to production infrastructure. Configure DCL to proxy ME traffic to convergence. Zero consumer changes.

**Action 1 — Render deploy:**
Create `convergence/render.yaml` for the new Render web service. Deploy. Validate:
```bash
curl https://<convergence-render-url>/api/health
# Must return 200
```
Verify DB connectivity by running a test query against `semantic_triples` through the convergence health check.

**Action 2 — DCL proxy configuration:**
In DCL's `backend/api/main.py`, add proxy routes that forward ME traffic to convergence:
- `/api/dcl/reports/v2/combining/*` → `CONVERGENCE_API_URL + /api/convergence/reports/v2/combining/*`
- `/api/dcl/reports/v2/overlap/*` → `CONVERGENCE_API_URL + /api/convergence/reports/v2/overlap/*`
- `/api/dcl/reports/v2/bridge/*` → `CONVERGENCE_API_URL + /api/convergence/reports/v2/bridge/*`
- `/api/dcl/reports/v2/whatif/*` → `CONVERGENCE_API_URL + /api/convergence/reports/v2/whatif/*`
- `/api/dcl/merge/*` → `CONVERGENCE_API_URL + /api/convergence/merge/*`
- `/api/dcl/cofa-mapping` → `CONVERGENCE_API_URL + /api/convergence/cofa-mapping`
- `/api/dcl/cofa-validation` → `CONVERGENCE_API_URL + /api/convergence/cofa-validation`

Pattern: use the same proxy pattern already present in DCL's `main.py` for `/api/platform/{path}`.

**Action 3 — Engagement endpoint:**
Create `convergence/backend/api/routes/engagement_api.py`:
- `GET /api/convergence/engagement/active` → returns `EngagementConfig` JSON from `engagement.py`
- This is what DCL's `mai.py` will call in Phase 3

**Action 4 — aos-launch.sh:**
Add convergence to the local launch script:
```bash
# In aos-launch.sh, add:
cd ~/code/convergence && uvicorn backend.api.main:app --port 8010 &
cd ~/code/convergence && npm run dev -- --port 3010 &
```

**Action 5 — Local env files:**
Add `CONVERGENCE_API_URL=http://localhost:8010` to `.env` files in:
- `~/code/dcl/`
- `~/code/platform/`
- `~/code/nlq/`
- `~/code/console/`

**Gate:**
- All existing consumer traffic flows through DCL proxy to convergence with no behavior change
- Proxy latency does not exceed baseline by more than 2x
- `curl http://localhost:8004/api/dcl/reports/v2/combining/pnl` returns data (proxied through to convergence)
- `curl http://localhost:8010/api/convergence/engagement/active` returns valid EngagementConfig JSON

**Rollback:** Remove proxy routes from DCL `main.py`. Convergence Render service can remain deployed but receives no traffic.

---

### Phase 3: DCL Internal Decoupling

**Goal:** Break DCL's internal dependencies on engagement logic. This is DCL-internal work only — no caller changes.

**Action 1 — mai.py rewire:**
In `~/code/dcl/backend/engine/mai.py`:
- Remove all `from backend.engine.engagement import get_active_engagement` imports
- Remove all `from backend.db.engagement_store import EngagementStore` imports
- Add an HTTP client function:
```python
import httpx
import os

CONVERGENCE_API_URL = os.environ.get("CONVERGENCE_API_URL", "http://localhost:8010")

def get_active_engagement_http():
    """Fetch engagement config from convergence service. Fails loudly if unreachable."""
    try:
        resp = httpx.get(f"{CONVERGENCE_API_URL}/api/convergence/engagement/active", timeout=5.0)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        raise RuntimeError(f"Convergence service unreachable at {CONVERGENCE_API_URL}: {e}") from e
```
- Replace all 12 `get_active_engagement()` call sites with `get_active_engagement_http()`
- **Hard rule:** If convergence is unreachable, the function raises `RuntimeError`. No silent fallback. No empty dict. No None.

**Action 2 — triple_monitor.py patch:**
In `~/code/dcl/backend/api/routes/triple_monitor.py`:
- Inline the 5 lines from `engagement.py` that `triple_monitor.py` actually uses (the engagement JSON read for the `/api/dcl/triples/engagement` endpoint)
- Remove `from backend.engine.engagement import ...`
- Remove `from backend.db.engagement_store import ...`
- Remove the engagement-view route if it exposes engagement state that now belongs to convergence

**Action 3 — v2_helpers.py cleanup (DCL copy):**
In `~/code/dcl/backend/api/routes/v2_helpers.py`:
- Remove the `_get_active_engagement()` function
- Remove the engagement_state fallback from `resolve_tenant_id()` 
- Resolution chain becomes: explicit param → semantic_triples directly
- Do not touch the convergence copy (it retains full resolution chain)

**Action 4 — query_resolver_v2.py cleanup (DCL copy):**
In `~/code/dcl/backend/engine/query_resolver_v2.py`:
- Remove `from backend.engine.engagement import get_active_engagement` (line ~11)
- Remove any usage of `get_active_engagement` in the class body (line ~531)
- Engagement context (tenant_id, run_id) is already passed as constructor params — this is a cleanup, not a behavior change
- Do not touch the convergence copy

**Gate:**
```bash
cd ~/code/dcl && pytest tests/
# All remaining SE tests must pass
# Zero engagement imports remain:
grep -rn "from backend.engine.engagement" ~/code/dcl/backend/
grep -rn "from backend.db.engagement_store" ~/code/dcl/backend/
grep -rn "get_active_engagement" ~/code/dcl/backend/
# All three greps must return zero hits
```

**Rollback:** Revert all Phase 3 changes in DCL. The proxy from Phase 2 continues to work regardless.

---

### Phase 4: External Caller Migration

**Goal:** Update Platform, NLQ, and Console to call convergence directly instead of through DCL proxy.

**Action 1 — Platform:**
In `~/code/platform`:
- `app/mai/tool_executor.py`: route all merge/COFA tool calls to `CONVERGENCE_API_URL + /api/convergence/*`
- `app/mai/routes.py`: update any proxy routes for `/api/dcl/merge/*` or `/api/dcl/cofa/*` to point to convergence
- Verify: run Platform's test suite (146+ tests)
- Verify: Playwright `e2e/cofa-merge.spec.ts` passes against convergence URLs

**Action 2 — NLQ:**
In `~/code/nlq`:
- `dcl_proxy.py`: route overlap, cross-sell, upsell, bridge, combining, QoE, what-if calls to `CONVERGENCE_API_URL + /api/convergence/reports/*`
- Remove any references to `compat.py` routes (`/api/reports/*`, `/api/dcl/reports/*`)
- Verify: run NLQ test suite

**Action 3 — Console:**
In `~/code/console`:
- `client.ts` (or equivalent): route combining/bridge/overlap/what-if API calls to `CONVERGENCE_API_URL + /api/convergence/reports/*`
- Proxy layer: route `/api/proxy/dcl/merge/*` and `/api/proxy/dcl/reports/combining*` to convergence at 8010
- If Console embeds DCL's Merge tab via iframe, update URL from 3004 to 3010
- Entity switcher: if it calls a DCL endpoint for entity list in ME mode, route to convergence
- Verify: run Console Playwright suite

**Action 4 — compat.py consumer verification:**
```bash
# Verify NO caller still hits compat.py routes
grep -rn "api/dcl/reports/" ~/code/nlq/ ~/code/console/ ~/code/platform/
grep -rn "api/reports/" ~/code/nlq/ ~/code/console/ ~/code/platform/ | grep -v convergence
# Both must return zero hits (or only hits pointing at convergence)
```

**Gate:** Each caller tested independently. Revert per-caller if their test suite regresses. Do not proceed to Phase 5 until all three callers are confirmed.

---

### Phase 5: Dead Code Strip in DCL

**Goal:** Remove all ME code, proxy routes, and UI components from DCL. **Execute in this exact order to prevent import-time crashes.**

**Action 1 — Delete ME test files from DCL:**
```
tests/test_3a_query_resolver.py
tests/test_3b_entity_resolution.py
tests/test_3c_combining.py
tests/test_3d_overlap.py
tests/test_3e_ebitda_qoe.py
tests/test_3f_whatif.py
tests/test_cofa_gate.py
```
(Also `tests/test_entities_recon.py` if it imports ME engines — verify in Phase 0 audit)

**Action 2 — Delete ME engine files from DCL:**
```
backend/engine/combining_v2.py
backend/engine/ebitda_bridge_v2.py
backend/engine/qoe_v2.py
backend/engine/overlap_v2.py
backend/engine/cross_sell_v2.py
backend/engine/upsell_v2.py
backend/engine/entity_resolution_v2.py
backend/engine/cofa_mapping_writer.py
backend/engine/what_if_v2.py
backend/engine/engagement.py
backend/engine/engagement_config.py
backend/engine/_engine_cache.py      (if moved in Phase 1)
backend/engine/dashboards.py         (if moved in Phase 1)
backend/engine/revenue_bridge.py     (if moved in Phase 1)
backend/db/engagement_store.py
backend/db/resolution_store.py
data/engagements/demo-001.json
```
Also delete any v1 files that were kept in Phase 0 because they had importers that are now in convergence.

**Action 3 — Delete ME route files from DCL:**
```
backend/api/routes/merge_overview.py
backend/api/routes/merge_conflicts.py
backend/api/routes/cofa_mapping.py
backend/api/routes/cofa_validation.py
backend/api/routes/resolution_v2.py
backend/api/routes/reports_combining_v2.py
backend/api/routes/reports_bridge_v2.py
backend/api/routes/reports_overlap_v2.py
backend/api/routes/reports_whatif_v2.py
backend/api/routes/compat.py
backend/api/routes/reports.py
```

**Action 4 — Clean DCL main.py:**
Remove all ME router imports (lines ~79-88 in current main.py):
- `resolution_v2_router`
- `reports_combining_v2_router`
- `reports_overlap_v2_router`
- `reports_bridge_v2_router`
- `reports_whatif_v2_router`
- `cofa_validation_router`
- `cofa_mapping_router`
- `merge_overview_router`
- `merge_conflicts_router`
- `compat_router`
- `reports_router`

Remove all corresponding `app.include_router()` calls.
Remove the LEGACY_JSON_LOAD block (lines ~361-365).
Remove the Phase 2 proxy routes.

**Action 5 — Clean DCL frontend:**
In `src/App.tsx`:
- Remove `'merge'` from the `MainView` type union
- Remove `'merge'` from `navTabs` array (line ~298)
- Remove `MergePanel` import (line ~17)
- Remove MergePanel render block (lines ~425-426)
- Remove `MappingsPanel` and `EnterpriseDashboard` imports if present

**Action 6 — Delete ME migrations from DCL:**
```
migrations/002_resolution_workspaces_v2.sql
migrations/003_whatif_scenarios.sql
migrations/006_seed_engagement.sql
```
These now live in convergence. DCL retains only SE migrations.

**Gate:**
```bash
cd ~/code/dcl && pytest tests/
# All remaining SE tests pass
# Zero ME imports in test collection
```

**Rollback:** Revert all Phase 5 deletions. The proxy is already removed in Action 4, so re-add it if reverting mid-phase. Convergence continues to serve traffic directly from Phase 4.

---

### Phase 6: Comprehensive Verification

**Goal:** Mechanically confirm clean separation across code, runtime, and data.

#### Static verification (code separation)

```bash
echo "=== Zero engagement coupling in DCL ==="
grep -r "from backend.engine.engagement" ~/code/dcl/backend/
# Expect: 0 hits
grep -r "from backend.db.engagement_store" ~/code/dcl/backend/
# Expect: 0 hits
grep -r "get_active_engagement" ~/code/dcl/backend/
# Expect: 0 hits

echo "=== Zero ME engine code in DCL ==="
grep -r "combining_v2\|entity_resolution_v2\|cofa_mapping_writer\|OverlapEngineV2\|EbitdaBridgeV2\|QoeEngine\|CrossSellEngine\|UpsellEngine\|WhatIfEngine" ~/code/dcl/
# Expect: 0 hits (except possibly CHANGELOG or SCHEMA_CONTRACT references)

echo "=== Zero cross-repo imports ==="
grep -r "from dcl\.\|import dcl\." ~/code/convergence/
# Expect: 0 hits
grep -r "from convergence\.\|import convergence\." ~/code/dcl/
# Expect: 0 hits

echo "=== Zero compat.py consumers ==="
grep -rn "api/dcl/reports/" ~/code/nlq/ ~/code/console/ ~/code/platform/ | grep -v convergence
# Expect: 0 hits
```

#### Test verification

```bash
cd ~/code/dcl && pytest tests/
# 100% pass, zero ME test files present

cd ~/code/convergence && pytest tests/
# 100% pass, all 7 ME test files present and passing

cd ~/code/platform && pytest
# 146+ tests pass

cd ~/code/console && npx playwright test
# All Playwright gates pass
```

#### Runtime verification

```bash
echo "=== Routing ==="
curl http://localhost:8004/api/dcl/reports/v2/combining/pnl
# Expect: 404 (ME routes gone from DCL)

curl http://localhost:8010/api/convergence/reports/v2/combining/pnl
# Expect: 200 with data

echo "=== Health ==="
curl http://localhost:8004/api/health
# Expect: 200 (DCL SE healthy)

curl http://localhost:8010/api/health
# Expect: 200 (Convergence healthy)

echo "=== HTTP Contract ==="
curl http://localhost:8010/api/convergence/engagement/active
# Expect: 200 with valid EngagementConfig JSON

echo "=== Mai degradation ==="
# Stop convergence, then verify mai fails loudly:
# mai engagement tools should return RuntimeError, not empty/None
# SE tools (graph metrics, triple counts) should still work
```

#### Frontend verification

- `http://localhost:3004` (DCL): No Merge tab in navigation. All other tabs functional (Sankey, Dashboard, Context, Ingest, Recon).
- `http://localhost:3010` (Convergence): MergePanel renders. COFA data visible. Conflict resolution works.

#### Database verification

- Convergence application logs show zero `INSERT/UPDATE/DELETE` statements against `semantic_triples`, `dimension_values_v2`, `tenant_runs`, or `tenant_registry`
- DCL application logs show zero queries against `resolution_workspaces_v2`, `whatif_scenarios`, or `engagement_state`
- `SELECT count(*) FROM pg_stat_activity` shows combined connection count within Supabase limit

#### Latency verification

Measure before (pre-carveout baseline) and after:
- Convergence report endpoints (combining, bridge, QoE, overlap, what-if) complete within the same latency ceilings they had when they lived in DCL
- DCL's SE endpoints (semantic-export, query, graph build) show no regression

---

## 7. RACI Update

After Phase 6 verification passes, update `ONGOING_PROMPTS/AOS_MASTER_RACIv8.csv`:

**New "Convergence" module rows (19 ME-only capabilities):**
- COFA unification
- Combining financial statements
- Cross-entity resolution
- Overlap / concentration analysis
- EBITDA bridge
- QoE (Quality of Earnings)
- Cross-sell analysis
- Upsell analysis
- What-if scenario analysis
- Merge conflict detection
- Merge conflict resolution
- Engagement lifecycle management
- Entity resolution workspaces
- COFA validation gate
- Merge overview
- Combining P&L / BS / CF
- Revenue bridge (ME)
- Engagement dashboards (ME)
- ME-specific engine caching

**DCL module rows updated:** Reflect SE-only ownership:
- Semantic triple store
- Ontology engine
- Schema-on-write
- Single-entity query resolution
- Graph engine
- RAG service
- MCP server
- Reconciliation
- Visualization (Sankey, Dashboard, Context)
- Triple monitoring / ingest

**CLAUDE.md updates:** Both repos' CLAUDE.md files reflect the split and cross-reference each other.

---

## 8. Cleanup Debt (Post-Carveout)

Add these to the existing cleanup debt list:

1. Extract `aos-common` package from forked infra files (`db.py`, `constants.py`, `triple_store.py`, `security_constraints.py`, `base.py`, `log_utils.py`). Fork-date headers track divergence.
2. Convergence URL prefix standardization: decide whether routes are `/api/convergence/*` or `/api/me/*` and make consistent.
3. DCL CI schema contract check: automated diff of current `semantic_triples` schema against `SCHEMA_CONTRACT.md`, fail on breaking changes.
4. Convergence connection pool tuning: monitor `pg_stat_activity` under load and adjust `CONVERGENCE_POOL_MAX_CONN` and `CONVERGENCE_STATEMENT_TIMEOUT`.
5. Mai HTTP client resilience: add retry with backoff for convergence calls (not in initial carveout — keep it simple, fail loudly first).

---

## 9. Consequences Analysis

### DCL (direct impact)

| Area | Consequence | Severity |
|------|------------|----------|
| **Codebase size** | Loses ~30 backend files, 3 frontend components, 7 test files, 3 migrations. Significant reduction in surface area. | Positive |
| **main.py** | Drops 12 router imports and mounts. Clearer separation of what DCL actually owns. | Positive |
| **Connection pool** | ME queries no longer compete for DCL's pool. Pool sizing can be tightened. | Positive |
| **mai.py** | 12 calls to `get_active_engagement()` become HTTP calls to convergence. New failure mode: if convergence is down, Mai's engagement-specific tools fail. Must fail loudly per AOS rules (no silent fallback). | Medium risk |
| **triple_monitor.py** | Loses engagement-view endpoint. Entity browsing that shows engagement context must either proxy to convergence or drop the engagement decorator. | Low risk |
| **v2_helpers.py** | Simplified: drops engagement_state lookup. Tenant resolution becomes direct-to-PG only. Faster, fewer moving parts. | Positive |
| **query_resolver_v2.py** | Drops engagement import. Engagement context already arrives as params — this is cleanup, not a behavior change. | Positive |
| **Startup time** | Faster — fewer modules to import, no engagement JSON to load. | Positive |
| **Test suite** | Shrinks by 7 test files. Remaining tests are pure SE. Cleaner pass/fail signal. | Positive |

### Console (pipeline orchestration)

| Area | Consequence | Severity |
|------|------------|----------|
| **New service dependency** | Console must know about convergence at 8010. New env var `CONVERGENCE_API_URL`. | Required change |
| **Pipeline orchestration** | `POST /api/pipeline/run` currently calls DCL for everything. Post-carve-out, ME operations (entity resolution, COFA, reports) must route to convergence. Console's pipeline runner needs a conditional: SE ops → DCL, ME ops → convergence. | Medium effort |
| **Entity switcher** | Console shows/hides entity switcher based on SE/ME mode. The entity list endpoint (`GET /api/dcl/entities`) moves to convergence. Console must call convergence for this. | Required change |
| **Operating mode** | Console displays SYNTHETIC / PRODUCTION_SE / PRODUCTION_ME. Mode detection may need to query convergence for ME health. | Low effort |
| **Merge tab embedding** | If Console embeds DCL's Merge tab via iframe or micro-frontend, the URL changes from 3004 to 3010. | Required change |

### NLQ (query resolution)

| Area | Consequence | Severity |
|------|------------|----------|
| **Core query path** | `GET /api/dcl/semantic-export` and `POST /api/dcl/query` stay in DCL. Zero change for the primary NLQ flow. | No impact |
| **Entity resolution queries** | If NLQ asks "which entities overlap on customer X?", that resolution logic is now in convergence. NLQ would need to call convergence, not DCL. | Low risk (likely unused today) |
| **Conflict-aware answers** | If NLQ surfaces M&A conflict data in natural language answers, the conflict detection endpoint moves to convergence. | Low risk |

### Platform / Mai

| Area | Consequence | Severity |
|------|------------|----------|
| **Mai MCP tools** | DCL's `mai.py` provides 10+ tools to Mai. Tools that return engagement context (deal status, synergy tracker, workstream overview, milestones, entity metadata) now depend on convergence being up. | Medium risk |
| **COFA chat** | Mai's COFA mapping submission (`POST /api/dcl/cofa-mapping`) moves to convergence. Platform must update the endpoint URL. | Required change |
| **Degraded mode** | If convergence is down but DCL is up, Mai's SE tools (graph metrics, triple counts, ontology info) still work. Only engagement-specific tools fail. This is correct behavior — partial degradation, not total failure. | Acceptable |
| **Constitution modules** | Platform's `constitution/modules/dcl.md` describes both SE and ME capabilities. Needs updating to reflect the split: DCL module doc covers SE, new convergence module doc covers ME. | Required change |

### Farm

| Area | Consequence | Severity |
|------|------------|----------|
| **Triple push** | Farm pushes triples to `POST /api/dcl/ingest-triples`. This stays in DCL. Zero change. | No impact |
| **Farm configs** | `farm_config_meridian.yaml` and `farm_config_cascadia.yaml` unchanged. | No impact |

### AAM

| Area | Consequence | Severity |
|------|------------|----------|
| **Schema exports** | AAM exports schemas to DCL. SchemaLoader stays in DCL. Zero change. | No impact |
| **Reconciliation** | `routes/reconciliation.py` stays in DCL (source comparison is SE). Zero change. | No impact |

### Database

| Area | Consequence | Severity |
|------|------------|----------|
| **Pool contention** | Two connection pools against same PG. Need to size: DCL pool (15) + convergence pool (10) ≤ Supabase connection limit. | Requires tuning |
| **Schema drift** | Convergence reads `semantic_triples` directly. If DCL adds/renames columns, convergence breaks silently. Need a contract: DCL documents the `semantic_triples` schema as a public API. Any breaking change requires convergence coordination. | High risk |
| **Migration coordination** | Migrations split across repos. Shared tables (semantic_triples, tenant_runs) are DCL-owned — only DCL runs migrations on them. Convergence only runs migrations on its own tables. | Requires discipline |
| **Write contention** | Convergence writes COFA triples via DCL's HTTP endpoint (serialized). Convergence direct writes go only to its own tables (resolution_workspaces_v2, etc.) — no conflict. | No risk |

### Operations

| Area | Consequence | Severity |
|------|------------|----------|
| **pm2 / aos-launch.sh** | New pm2 process for convergence backend + frontend. `aos-launch.sh` (laptop) and `aos-start` (desktop) need entries for port 8010/3010. | Required change |
| **Render deployment** | New web service in `render.yaml` for convergence. Own build, own health check, own env vars. | Required change |
| **Developer workflow** | Developers working on ME features must run convergence alongside DCL. SE-only work can skip convergence. | Minor friction |
| **Monitoring** | New service to monitor. Health check at `GET /api/health` on 8010. | Required change |
| **CORS** | Convergence frontend at 3010 calls convergence backend at 8010 — needs its own CORS config. DCL's CORS must allow 3010 if any direct calls are needed. | Required change |

### Risks

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| **Convergence down → Mai engagement tools fail** | Medium (new service = new failure point) | Medium (SE tools still work) | Mai HTTP client fails loudly: "Convergence service at 8010 unreachable — engagement tools unavailable" |
| **Schema drift on semantic_triples** | Low (stable schema) | High (silent breakage in convergence reports) | SCHEMA_CONTRACT.md as versioned API contract. DCL CI checks for breaking changes. |
| **Pool exhaustion from two services** | Low (can tune) | High (query failures) | Size pools conservatively. Monitor with `pg_stat_activity`. |
| **Dual-running period confusion** | Medium (Phase 1-2) | Low (no behavior change) | Clear ONGOING_PROMPTS doc stating which repo is authoritative during transition. |
| **Consumer migration incomplete** | Medium | High (mixed routing, debugging confusion) | Phase 2 proxy ensures nothing breaks. Phase 5 removal surfaces any missed migration as 404. |
| **Test coverage gaps** | Low | Medium | Both repos pass 100% independently (D6). Integration tests verify cross-service calls. |
