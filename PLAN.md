# DCL Engine Code Quality Audit v2 — Plan

## Context

This is the second audit pass. The prior audit (v1) already completed:
- Broke up `main.py` monolith into route modules (done)
- Centralized constants into `backend/core/constants.py` (done)
- Fixed connection pool retry bug (done)
- Fixed RAG truthful counts (done)
- Fixed cache invalidation logging (done)
- Fixed source tracking display-mode independence (done)
- Fixed `utc_now()` helper (done, exists in constants.py)
- Existing regression tests: `tests/test_code_quality.py` (11 passing tests)

This audit focuses on **remaining issues** that v1 did not address.

---

## PART 1: AUDIT FINDINGS — REMAINING ISSUES

### A. Hardcoded Values (Bad-Form)

| # | File:Line | What | Why Bad | Impact |
|---|-----------|------|---------|--------|
| H1 | `dcl_engine.py:199` | `metrics.rag_reads = 3` | Fabricated number. No actual RAG reads happen here. | Dashboard lies about RAG activity |
| H2 | `dcl_engine.py:202` | `metrics.rag_reads = 0` | Inconsistent with H1. Neither is measured. | Unreliable telemetry |
| H3 | `source_normalizer.py:65-124` | 50+ vendor aliases in `ALIAS_MAP` class dict | Every new vendor = code deploy. Not configurable. | Vendor onboarding bottleneck |
| H4 | `source_normalizer.py:126-158` | 30+ regex rules in `PATTERN_RULES` | Cannot extend matching without code change | Blocks runtime extensibility |
| H5 | `source_normalizer.py:160-175` | `CATEGORY_PATTERNS` dict | New categories require code change | Blocks AOD category expansion (per RACI) |
| H6 | `source_normalizer.py:178` | `_CB_COOLDOWN = 120` | Not env-configurable | Production tuning needs code deploy |
| H7 | `rag_service.py:210` | `range(1536)` in mock embeddings | `PINECONE_DIMENSION` exists but not used here | Dimension mismatch if config changes |
| H8 | `rag_service.py:168` | `model="text-embedding-3-small"` | Not configurable via env or constants | Model change needs code deploy |
| H9 | `persist_mappings.py:29-31` | `POOL_MIN_CONN=1, POOL_MAX_CONN=5, CONNECT_TIMEOUT=5` | Not env-configurable | Cannot tune DB pool per environment |
| H10 | `main.py:567` | `port=5000` in `__main__` block | Conflicts with frontend Vite dev on 5000 | Port collision in dev |
| H11 | `App.tsx:18` | `ALL_PERSONAS = ['CFO','CRO','COO','CTO']` | RACI+data flow doc define 5 personas (CHRO) | CHRO unreachable in UI |
| H12 | `App.tsx:53-65` | `personaOntologies` duplicates backend mapping | Backend has canonical mapping in `persona_profiles.yaml` | Frontend/backend drift |
| H13 | `dcl_engine.py:227` | `time.strftime("%Y-%m-%dT%H:%M:%SZ")` | Uses local time. `utc_now()` exists but unused here | Timezone-dependent meta.generated_at |
| H14 | `source_normalizer.py:203` | `timeout=5.0` hardcoded | Not env-configurable | Cannot adjust for slow networks |

### B. Silent Fallbacks (Bad-Form)

| # | File:Line | What | Why Bad | Impact |
|---|-----------|------|---------|--------|
| F1 | `dcl_engine.py:90-95` | DB `get_all_mappings_grouped` failure → silent `{}` | Engine silently regenerates all mappings. User never knows DB is down. | Quality degradation, doubled latency |
| F2 | `dcl_engine.py:182-184` | LLM validation failure → silent heuristic fallback | Prod mode silently returns Dev-quality results | Users think they got Prod quality |
| F3 | `rag_service.py:195-203` | OpenAI embedding error → random mock vectors | Mock embeddings corrupt Pinecone index permanently | RAG lookup returns garbage |
| F4 | `source_normalizer.py:239-247` | Registry failure → `_registry_loaded=True` | Marks loaded despite having no data. Suppresses retries. | Unknown sources get wrong IDs |
| F5 | `main.py:235-236` | AAM pull recording failure → caught, warned | Loss of provenance audit trail | Missing data lineage |
| F6 | `App.tsx:117` | Auto-load `.catch(warn)` | User sees blank graph with no error | Confusing blank-screen UX |
| F7 | `persist_mappings.py:82-85` | `putconn` failure in finally → warning only | Connection not returned AND not closed | Gradual connection pool exhaustion |

### C. Architectural Issues (Remaining)

| # | File | Issue | Impact |
|---|------|-------|--------|
| A1 | `MonitorPanel.tsx` (698L), `ReconciliationPanel.tsx` (831L) | Monolithic components | Hard to test, review, maintain |
| A2 | `dcl_engine.py:28-252` | `build_graph_snapshot` = 225-line god-method | Untestable without full integration |
| A3 | `source_normalizer.py` | Alias/pattern/category data in code, not config | Cannot extend without deploys |
| A4 | `main.py:113-114` | `app.state.loaded_sources` mutable global | Race conditions under concurrency |
| A5 | `App.tsx:43-92` | `generatePersonaViews` duplicates backend logic | Violated single source of truth |

---

## PART 2: FIX PLAN

### Phase 1: Critical Fixes (Stop the Bleeding)

**1.1 — Eliminate fabricated RAG metrics (H1, H2)**
- File: `backend/engine/dcl_engine.py`
- Change: Remove lines setting `rag_reads` to fabricated values. Only set from actual measured reads.
- Test: `assert metrics.rag_reads == 0` when no Pinecone reads occur.

**1.2 — Stop embedding corruption on failure (F3)**
- File: `backend/engine/rag_service.py:195-203`
- Change: On OpenAI failure, return 0 stored (don't fall back to random vectors). Log at ERROR.
- Test: Mock OpenAI failure → assert 0 vectors upserted, no `_create_mock_embeddings` called in Prod.

**1.3 — Use UTC timestamp in graph meta (H13)**
- File: `backend/engine/dcl_engine.py:227`
- Change: Replace `time.strftime(...)` with `utc_now()` from constants.
- Test: Assert `generated_at` matches UTC format.

**1.4 — Fix connection pool leak (F7)**
- File: `backend/semantic_mapper/persist_mappings.py:82-85`
- Change: If `putconn()` raises, call `conn.close()` explicitly.
- Test: Simulate `putconn` failure → assert `conn.close()` called.

**1.5 — Fix blank-screen on auto-load failure (F6)**
- File: `src/App.tsx:117`
- Change: Set error state on catch. Render error message instead of blank.
- Test: Frontend shows message when backend unreachable.

### Phase 2: Configuration Externalization

**2.1 — Source normalizer data to YAML (H3, H4, H5, A3)**
- Create: `config/source_aliases.yaml` with alias_map, pattern_rules, category_patterns
- Change: `SourceNormalizer.__init__` loads YAML at init, hardcoded data becomes fallback
- Test: Add new alias to YAML → assert resolution works without code change.

**2.2 — Add missing constants (H6, H8, H9, H14)**
- File: `backend/core/constants.py`
- Add: `CB_COOLDOWN`, `OPENAI_EMBEDDING_MODEL`, `POOL_MIN_CONN`, `POOL_MAX_CONN`, `DB_CONNECT_TIMEOUT`, `FARM_REGISTRY_TIMEOUT`
- Wire up consumers to use these constants.
- Test: `test_code_quality.py` validates all constants exist.

**2.3 — Fix mock embedding dimension (H7)**
- File: `backend/engine/rag_service.py:210`
- Change: `range(PINECONE_DIMENSION)` instead of `range(1536)`
- Test: Assert mock vector length == `PINECONE_DIMENSION`.

**2.4 — Externalize embedding model (H8)**
- File: `backend/engine/rag_service.py:168`
- Change: Use `OPENAI_EMBEDDING_MODEL` constant.
- Test: Inspect source for hardcoded model string.

**2.5 — Fix dev server port (H10)**
- File: `backend/api/main.py:567`
- Change: `port=int(os.getenv("BACKEND_PORT", "8000"))` (match CLAUDE.md docs)
- Test: `__main__` uses 8000 by default.

### Phase 3: Fallback Transparency

**3.1 — DB fallback flag (F1)**
- Add `db_fallback: bool = False` to `RunMetrics`
- Set `True` when DB unavailable in `build_graph_snapshot`
- Test: DB down → `run_metrics.db_fallback == True`

**3.2 — LLM fallback flag (F2)**
- Add `llm_fallback: bool = False` to `RunMetrics`
- Set `True` when LLM validation fails in Prod mode
- Test: LLM failure → `run_metrics.llm_fallback == True`

**3.3 — Persona alignment (H11, H12)**
- Add `CHRO` to frontend persona list
- Fetch persona list from backend `/api/dcl/semantic-export` on mount instead of hardcoding
- Test: Frontend shows all personas from backend.

**3.4 — Global state removal (A4)**
- Replace `app.state.loaded_sources` with per-request context or response-only data
- Test: Concurrent runs don't cross-contaminate.

### Phase 4: Self-Running Test Harness

**4.1 — `tests/test_audit_v2.py`** — Pytest file validating all Phase 1-3 fixes:
- `TestFabricatedMetrics` — no fabricated rag_reads
- `TestEmbeddingCorruption` — no mock fallback in Prod on failure
- `TestTimestamps` — UTC in generated_at
- `TestPoolLeak` — connection closed on putconn failure
- `TestConstants` — all new constants exist and are used
- `TestFallbackFlags` — db_fallback and llm_fallback populated correctly

**4.2 — `scripts/audit_v2_loop.sh`** — Run all quality tests:
```bash
pytest tests/test_code_quality.py tests/test_audit_v2.py -v --tb=short
```

**4.3 — Verification grep checks** (in test):
- `grep -r "rag_reads = 3" backend/` → 0 matches
- `grep -r "range(1536)" backend/` → 0 matches
- `grep -r '"text-embedding-3-small"' backend/` → 0 matches
- `grep -r "time.strftime" backend/engine/dcl_engine.py` → 0 matches

---

## PART 3: WHAT'S GOOD (Preserve)

| Area | Status | Evidence |
|------|--------|----------|
| Constants centralization | Done (v1) | `backend/core/constants.py` — env-var driven, tested |
| Connection pool retry | Done (v1) | Cooldown prevents hammering, tested in `test_code_quality.py` |
| RAG truthful counts | Done (v1) | Returns 0 on failure, tested |
| Cache invalidation logging | Done (v1) | Logs errors, tested |
| Source tracking independence | Done (v1) | Uses meta.source_names, tested |
| Route module extraction | Done (v1) | `backend/api/routes/` — 7 clean modules |
| Security constraints | Good | Zero-trust check at startup |
| Ingest guard | Good | Schema-on-write with pipe_id validation |
| Heuristic mapper | Good | Negative patterns prevent false positives |
| Domain models | Good | Pydantic V2, proper validation |
| Narration service | Good | Excellent operator observability |
| RACI compliance | Good | DCL owns its designated capabilities per RACI |

---

## PART 4: EXECUTION ORDER

```
Phase 1 (Critical)  → Phase 2 (Config)     → Phase 3 (Transparency) → Phase 4 (Harness)
  1.1 rag metrics      2.1 source YAML        3.1 db fallback flag     4.1 test file
  1.2 embedding fix    2.2 new constants       3.2 llm fallback flag    4.2 loop script
  1.3 UTC timestamp    2.3 mock dimension      3.3 persona alignment    4.3 grep checks
  1.4 pool leak fix    2.4 embedding model     3.4 global state
  1.5 blank screen     2.5 dev port
```

All changes within DCL. No external repo dependencies.

---

## PART 5: SUCCESS CRITERIA

1. `pytest tests/test_code_quality.py tests/test_audit_v2.py -v` — 100% pass
2. No fabricated metrics in codebase (verified by grep)
3. No hardcoded embedding dimensions or model names
4. All fallbacks set explicit flags on `RunMetrics`
5. Frontend builds clean: `npm run build` succeeds
6. All backend modules import without error
7. Existing test harness (`test_harness.py`) still passes against live backend
