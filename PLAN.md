# Tech Debt Reduction Plan — DCL Engine

## Audit Summary

**Codebase scanned**: 48 endpoints, ~8,000 LoC backend, ~3,500 LoC frontend
**Issues found**: 12 critical, 18 high, 50+ medium
**Root cause**: Multiple contributors, no enforced structure, everything lands in `main.py`

---

## Change 1: Break up the `main.py` monolith (1,650 lines, 48 endpoints)

**Problem**: `main.py` is a single 1,650-line file containing every endpoint, all request/response models, inline business logic, and three reconciliation god-functions. Nobody can find anything, merge conflicts are guaranteed, and a typo anywhere breaks everything.

**What to extract**:

| New file | What moves there | Lines saved from main.py |
|----------|------------------|--------------------------|
| `backend/api/routes/ingest.py` | All `/api/dcl/ingest/*` endpoints (GET ping, POST ingest, runs, batches, drift, stats) + the 27-line field normalizer | ~250 |
| `backend/api/routes/reconciliation.py` | `/api/dcl/reconciliation`, `/api/dcl/reconciliation/sor`, `/api/reconcile` + the three god-functions (`_farm_reconciliation`, `_aam_reconciliation`, `get_sor_reconciliation`) | ~450 |
| `backend/api/routes/topology.py` | `/api/topology/*` endpoints | ~50 |
| `backend/api/routes/entities.py` | `/api/dcl/entities/*` + `/api/dcl/conflicts/*` endpoints | ~120 |
| `backend/api/routes/temporal.py` | `/api/dcl/temporal/*` + `/api/dcl/provenance/*` + `/api/dcl/persona-definitions/*` | ~100 |
| `backend/api/routes/deprecated.py` | All 410 GONE stubs (ingest/provision, ingest/config, ingest/telemetry, nlq/*, bll/*, execute) | ~50 |

**How**: Use FastAPI `APIRouter`. Main.py becomes a ~200-line app factory that mounts routers. Each router file owns its request/response models.

---

## Change 2: Kill the god-functions

Three functions are each 100-200 lines doing 5+ things:

**`dcl_ingest()` (162 lines, main.py:426-587)**:
- Extract `_normalize_ingest_body(raw_body: dict) -> dict` — the 27-line camelCase→snake_case field remapper
- Extract `_validate_pipe_guard(pipe_id: str) -> Optional[PipeDefinition]` — the schema-on-write gate
- Keep orchestration in the endpoint handler (~40 lines)

**`_farm_reconciliation()` (145 lines)** and **`_aam_reconciliation()` (75 lines)**:
- Extract `_build_normalized_pipes(receipts) -> List[NormalizedPipe]` — shared pipe construction
- Extract `_build_reconciliation_metadata(result, pipes) -> dict` — shared response assembly
- Both call `reconcile()` with slightly different pipe prep — unify the prep

**`reconcile_aam()` (129 lines, main.py:1451-1579)**:
- Extract `_discover_expected_sources()` and `_build_verdict()` — the 7-way decision tree is unreadable inline

---

## Change 3: Fix silent killer fallbacks

These are the ones that will bite you in production — errors that get swallowed, leaving you blind when things go wrong.

| Location | What happens | Fix |
|----------|-------------|-----|
| `main.py:90-94` | `assert_metadata_only_mode()` failure caught and logged as WARNING, execution continues | **Re-raise** in production. A security constraint violation is not a warning. |
| `main.py:126-131` | Mapping cache clear failure swallowed | Log as ERROR with `exc_info=True`, add metric counter |
| `main.py:1468-1475` | Push history fetch fails with bare `except: pass` — **zero logging** | Add `logger.warning()` at minimum. This is a data-loss blind spot. |
| `schema_loader.py:104-106` | CSV load exception caught, returns empty list, demo mode silently has no data | Log ERROR, consider raising if zero sources loaded |
| `source_normalizer.py:300,326` | `_create_fallback_canonical()` silently manufactures a canonical ID when registry unavailable | Log WARNING with context so operators know registry is down |
| `runner.py:70-72` | Mapping save failure caught, downgrades to in-memory without user notification | Log ERROR, set a flag on the response so UI can show degraded state |
| `rag_service.py:187-193` | OpenAI embedding error falls back to mock embeddings silently | Log ERROR with the original exception. Mock embeddings in prod = garbage RAG results. |
| `IngestionPanel.tsx:71` | `catch {}` — literally swallows all errors with no logging | At minimum `console.error(err)`, ideally show toast |
| `App.tsx:40-43` | `.catch(() => {})` on ingest stats poll | Log the error or decrement a health counter |

---

## Change 4: Externalize hardcoded config

**Most dangerous hardcoded values** (things that will break when environments change):

| Value | Location | Extract to |
|-------|----------|-----------|
| Farm URL `"https://autonomos.farm"` | `schema_loader.py:142`, `source_normalizer.py:198` (duplicated) | `FARM_API_URL` env var (already in CLAUDE.md but not used consistently) |
| 60+ alias mappings in `ALIAS_MAP` | `source_normalizer.py:65-124` | `config/source_aliases.yaml` |
| 30+ regex patterns | `source_normalizer.py:126-158` | `config/source_patterns.yaml` |
| Confidence thresholds `0.95, 0.90, 0.75, 0.70, 0.65, 0.60` | `heuristic_mapper.py:190-252`, `mapping_service.py:98-106` | `backend/core/constants.py` (file exists, just not used here) |
| `CORE_ONTOLOGY` Python list | `ontology.py:5-107` | Already have `config/ontology_concepts.yaml` — wire it up instead of duplicating |
| Default personas `[CFO, CRO, COO, CTO]` | `main.py:190` | Load from `persona_profiles.yaml` |
| API version `"2.0.0"` | `main.py:174` | `backend/core/constants.py` or `VERSION` file |
| Trust scores `85`, `70`, `60`, `50` | `ingress.py:202-203`, `main.py:1230,1232` | Named constants in `constants.py` |

---

## Change 5: Frontend monolith extraction

**ReconciliationPanel.tsx (819 lines)** — worst offender:
- Extract `AamReconciliationTab.tsx` (lines 266-504, 238 lines)
- Extract `SorReconciliationTab.tsx` (lines 506-753, 247 lines)
- Extract `ReconciliationDetailModal.tsx` (lines 159-265)
- Parent becomes ~100 lines of tab switching

**MonitorPanel.tsx (698 lines)**:
- Extract `OntologyDetailPanel.tsx` (the modal, lines 159-327)
- Extract `PersonaConnectionsView.tsx` (the 200-line inline IIFE, lines 383-606)

**Shared frontend concerns**:
- Create `src/constants/api.ts` — all API paths in one place (currently hardcoded in 6 components)
- Create `src/hooks/usePoll.ts` — deduplicate the 5 identical polling-with-cleanup patterns
- Fix NarrationPanel polling at **500ms** — absurdly aggressive, change to 2000ms

---

## Change 6: Fix timestamp and response inconsistencies

**Two different datetime formats in use**:
- `datetime.now(timezone.utc).isoformat()` → `2026-02-16T19:51:18.798000+00:00`
- `time.strftime("%Y-%m-%dT%H:%M:%SZ")` → `2026-02-16T19:51:18Z`

Different endpoints return different formats. Create one `utc_now() -> str` helper and use it everywhere.

**Error response structure inconsistent**:
- Some endpoints: `HTTPException(422, detail="string message")`
- Others: `HTTPException(422, detail={"error": "CODE", "message": "..."})`

Standardize on the structured format — clients can't reliably parse errors otherwise.

---

## Change 7: Fix the `ontology.py` lie

`CLAUDE.md` says ontology is YAML-driven. Reality: `ontology.py` has a hardcoded Python list of 8 concepts. `config/ontology_concepts.yaml` exists with 13 concepts. They're **not the same list** — the YAML has 5 concepts the Python code doesn't know about.

**Fix**: Make `get_ontology()` load from YAML (with the Python list as fallback-of-last-resort, not primary). The `config_sync.py` module already knows how to read the YAML — just needs to be wired into `ontology.py`.

---

## Execution order

1. **Change 3** (silent fallbacks) — highest risk, lowest effort, zero refactoring
2. **Change 4** (externalize config) — constants.py already exists, just populate it
3. **Change 7** (ontology.py) — small, surgical, fixes a data correctness bug
4. **Change 6** (timestamps + error format) — standardization pass
5. **Change 2** (god-functions) — extract helpers, keep endpoints as orchestrators
6. **Change 1** (main.py split) — biggest change, do last when helpers are stable
7. **Change 5** (frontend) — independent of backend, can parallel with 1-4

---

## Out of scope (noted but not fixing now)

- No Dockerfile / CI/CD pipeline (infrastructure, not code quality)
- CORS wildcard default (already logged as warning, env-var configurable)
- Thread safety in singletons (acceptable for single-worker uvicorn)
- Duplicate `pyhumps` in requirements.txt (trivial, fix in passing)
- `schema_loader.py` N+1 query in `load_stream_sources()` (perf optimization, not debt)
