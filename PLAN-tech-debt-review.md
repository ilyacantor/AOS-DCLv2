# Tech Debt Review & Remediation Plan

**Date:** 2026-02-13
**Scope:** Full codebase review of AOS-DCLv2
**Focus areas:** Reconciliation failures, pipe count mismatches, regression-prone code, hardcoded cheats

---

## Executive Summary

Three systemic issues explain the reported symptoms:

1. **Invalid Reconciliation Results** — The reconciliation path uses three different normalization strategies (AAM client naive `.lower().replace()`, source normalizer alias resolution, and raw `display_name` matching) that never agree on source identity. Pipe counts computed *after* `source_limit` truncation make reconciliation report phantom drifts.

2. **Frequent Reversions** — Mutable global state (`_last_aam_run`, `app.state.loaded_sources`, singleton caches on class variables) means every `/api/dcl/run` call overwrites the previous run's context. A Demo run silently corrupts the AAM reconciliation state. There are no request-scoped boundaries.

3. **Incorrect Pipe Counts from AAM** — The KPI `"pipes"` field actually counts *sources-with-fields*, not pipes. `source_limit` truncates the source list before KPI calculation, so the count never matches what AAM reported. The AAM `total_connections` value is fetched but never surfaced.

---

## Part 1: Root Cause Analysis — Reconciliation Failures

### RC-1: Three Incompatible Normalization Paths

| Location | Normalization Method | Example: "Salesforce CRM" |
|----------|---------------------|---------------------------|
| `schema_loader.py:345` | `source_name.lower().replace(" ","_").replace("-","_")` | `salesforce_crm` |
| `main.py:908` (POST /api/reconcile) | Same naive `.lower().replace()` | `salesforce_crm` |
| `reconciliation.py:13,21` | `s.lower().strip()` on DCL loaded names | `salesforce crm` (no underscore!) |
| `source_normalizer.py` (Demo/Farm) | Alias registry with canonical IDs | `salesforce` |

The `reconcile()` function at `reconciliation.py:44` compares AAM's `normalized` (lowered+stripped display_name, spaces preserved) against DCL's `dcl_source_set` (also lowered+stripped from `app.state.loaded_sources` labels). But the DCL loaded source *labels* come from `node.label` (`main.py:216`), which is the display name set in the graph builder — and that display name may differ from what AAM sent depending on whether the source normalizer or the AAM fast-path was used.

**Result:** Reconciliation reports "IN_AAM_NOT_DCL" for sources that DCL actually loaded, because the names don't match after inconsistent normalization.

### RC-2: Pipe Count Computed After source_limit Truncation

`schema_loader.py:383-403`:
```
sources = sources[:source_limit]   # line 386 — truncates list
...
kpis = {
    "pipes": sum(1 for s in sources if ...),  # line 401 — counts TRUNCATED list
}
```

If AAM reports 15 connections but `source_limit=5`, the KPI says `"pipes": 5`. The reconciliation endpoint then compares AAM's 15 against DCL's 5 and reports 10 missing.

### RC-3: Global `_last_aam_run` State is Fragile

`main.py:107`: `_last_aam_run: Dict[str, Any] = {}` — a module-level mutable dict.

- Overwritten by every AAM run (`main.py:220-231`)
- Read by reconciliation endpoints (`main.py:745,805,931,995`) at arbitrary later times
- Never cleared between modes — a Demo run doesn't reset it, leaving stale AAM data
- No TTL or staleness check — reconciliation can compare 2-hour-old DCL state against fresh AAM fetch
- `_last_aam_run["sources"]` stores serialized `GraphNode` dicts, not original AAM pipe objects

### RC-4: `app.state.loaded_sources` Stores Labels, Not Canonical IDs

`main.py:213-217`:
```python
source_names = []
for node in snapshot.nodes:
    if node.kind == "source":
        source_names.append(node.label)   # Display label, not canonical_id!
app.state.loaded_sources = source_names
```

Then at `main.py:746`: `dcl_loaded_sources = list(app.state.loaded_sources)` — feeds display labels into reconciliation, which tries to match them against AAM-normalized pipe names.

### RC-5: POST `/api/reconcile` vs GET `/api/dcl/reconciliation` — Two Reconciliation Endpoints, Different Logic

| Aspect | GET `/api/dcl/reconciliation` (line 738) | POST `/api/reconcile` (line 883) |
|--------|-------|------|
| AAM source | Fetches live from AAM via `get_pipes()` | Fetches live OR uses caller-supplied IDs |
| DCL source | `app.state.loaded_sources` (labels) | `_last_aam_run["sources"]` (serialized L1 nodes) |
| Normalization | `reconcile()` uses `.lower().strip()` | Inline `.lower().replace(" ","_").replace("-","_")` |
| Node ID parsing | N/A | `nid.replace("source_", "").replace("fabric_", "")` (line 935) |
| Result format | `{status, summary, diffCauses, fabricBreakdown}` | `{status, matched, missing_in_dcl, extra_in_dcl}` |

Two reconciliation endpoints with different normalization, different data sources, and different output formats. They will almost never agree.

---

## Part 2: Root Cause Analysis — Pipe Count Mismatches

### PC-1: "Pipes" KPI Counts Sources, Not Pipes

`schema_loader.py:401`:
```python
"pipes": sum(1 for s in sources if any(len(t.fields) > 0 for t in s.tables))
```

This counts *how many SourceSystem objects have at least one table with fields*. But in AAM's model, a single source can have multiple pipes (connections across different fabric planes). The AAM `total_connections` value (`schema_loader.py:290`) is logged but never returned in KPIs.

### PC-2: AAM Connections are Flattened to 1:1 Source:Table

`schema_loader.py:334-342`: Each AAM connection creates exactly one `TableSchema` named `{plane_type}_data`. If the same source appears in two fabric planes (e.g., Salesforce in both iPaaS and Warehouse), it creates two separate `SourceSystem` objects with different `source_id`s — but the reconciliation treats them as distinct sources while AAM considers them one source with two pipes.

### PC-3: source_limit Applied Before KPI Calculation

As noted in RC-2, `source_limit` truncates the list at line 386, then KPIs are computed at line 399-404 on the truncated list. The frontend displays the truncated counts as if they represent the full AAM payload.

---

## Part 3: Regression-Prone Architecture (Why Reversions Happen)

### REG-1: Mutable Global Singletons

| Singleton | Location | Risk |
|-----------|----------|------|
| `_last_aam_run` | `main.py:107` | Overwritten per-run, no isolation |
| `app.state.loaded_sources` | `main.py:104` | Overwritten per-run |
| `_normalizer_instance` | `source_normalizer.py:412-419` | Never cleared, accumulates state across runs |
| `SchemaLoader._demo_cache` | `schema_loader.py:17` | Class-level, shared across all instances |
| `SchemaLoader._stream_cache` | `schema_loader.py:18` | Same |
| `PersonaView._concepts_cache` | `persona_view.py:~20` | Same pattern |
| `_aam_client` | `aam/client.py:141` | Singleton, no per-request scope |
| `MappingPersistence._pool` | `persist_mappings.py:35-51` | Sets `_pool_initialized=True` even on failure |

A change to any code path that touches these globals can silently break other code paths. There is no request boundary.

### REG-2: Monolithic `_build_graph()` (dcl_engine.py:223-529)

307 lines with two completely separate code paths for AAM vs non-AAM mode. Fabric aggregation (lines 260-375) duplicates logic from the normal path (lines 376-459). Any change to node/link creation in one path must be mirrored in the other — and often isn't.

### REG-3: Monolithic `main.py` (1,075 lines)

Single file contains:
- FastAPI app initialization
- All endpoint definitions
- AAM reconciliation state management
- Cache invalidation logic
- Inline imports (`import time as _time`, `import hashlib as _hashlib`)
- Two different reconciliation endpoints with different logic
- Deprecated endpoint stubs

### REG-4: Non-Deterministic Graph IDs

`dcl_engine.py:431`:
```python
link_id = f"link_{mapping.source_system}_{mapping.ontology_concept}_{uuid.uuid4().hex[:8]}"
```

Every graph build produces different IDs. Cannot diff two runs. Cannot reproduce bugs. Frontend cannot maintain stable references.

### REG-5: Silent Exception Swallowing

| Location | What's swallowed | Fallback |
|----------|------------------|----------|
| `dcl_engine.py:66-71` | DB mapping load failure | Empty mappings `{}` |
| `dcl_engine.py:158-165` | LLM validation failure | Unvalidated mappings used |
| `main.py:117-118,127-128` | Cache invalidation failures | `pass` |
| `persist_mappings.py:48-51` | Connection pool init failure | `_pool_initialized=True` (prevents retry!) |
| `runner.py:19-24` | DB persistence init failure | In-memory only |
| `narration_service.py:24-26` | Redis connection failure | In-memory only |

When things fail silently, the system appears to work but produces degraded results. Subsequent code changes that depend on the "happy path" break in ways that appear as regressions.

---

## Part 4: Hardcoded Cheats & Magic Values

### HC-1: Hardcoded Trust/Quality Scores
`schema_loader.py:346-347`:
```python
trust_score = 85 if governance == "governed" else 60
data_quality_score = 80 if governance == "governed" else 50
```

### HC-2: Hardcoded Fabric Type Labels and Default
`dcl_engine.py:271-288`:
```python
if tag in ["ipaas", "warehouse", "gateway", "eventbus"]:  # hardcoded list
    fabric_type = tag
if not fabric_type:
    fabric_type = "ipaas"  # silent default
fabric_labels = {"ipaas": "iPaaS", "warehouse": "Warehouse", ...}  # hardcoded
```

### HC-3: Hardcoded Persona Concepts (Fallback)
`persona_view.py:9-15`:
```python
DEFAULT_PERSONA_CONCEPTS = {
    "CFO": ["Revenue", "Cost", "Margin", "CustomerValue", "ARR", "Churn"],
    ...
}
```
These concept names don't match the ontology concept IDs (e.g., "Revenue" vs "revenue").

### HC-4: Missing Ontology Concepts Referenced in Code
`heuristic_mapper.py` and `mapping_validator.py` reference `gl_account` and `currency` concepts that don't exist in `config/ontology_concepts.yaml`. The LLM validation prompt tells the model to map GL fields to "gl_account" — a concept the system doesn't have.

### HC-5: Hardcoded Browser Endpoints for Farm Mode
`schema_loader.py:146-152`: Five hardcoded API endpoints. No configuration, no extensibility.

### HC-6: Hardcoded LLM Model
`mapping_validator.py:122`: `model="gpt-4o-mini"` — no env var fallback.

### HC-7: Hardcoded Pinecone Configuration
`rag_service.py:97-110`: Index name `"dcl-mapping-lessons"`, dimension `1536`, region `us-east-1` all hardcoded.

### HC-8: Hardcoded Confidence Thresholds
`heuristic_mapper.py`: 7 different magic numbers (0.95, 0.80, 0.75, 0.70, 0.65, 0.60, 0.05) scattered across 50 lines with no configuration.

### HC-9: CHRO Persona Defined in Enum but Missing from Config
`models.py:14` defines `CHRO` in the Persona enum. `persona_profiles.yaml` has no CHRO section. No concept relevance exists for CHRO.

### HC-10: Hardcoded Provenance Data with Stale Dates
`provenance_service.py:57-101`: Source system names, quality scores, and dates all hardcoded inline.

### HC-11: Field Type Always "string"
`schema_loader.py:324`: `type="string"` for every AAM field. No type inference.

### HC-12: Mapping Status Always "ok" After LLM Validation
`dcl_engine.py:155`: `status="ok"` hardcoded, masking any issues found by LLM.

---

## Part 5: Frontend Issues Contributing to Symptoms

### FE-1: Persona Views Generated with Empty Array on Load
`App.tsx:114`: `generatePersonaViews(data.graph, [])` — initial load passes empty personas, producing incorrect views.

### FE-2: Pipe Count Display Unvalidated
`App.tsx:227`: `{graphData.meta.runMetrics.payloadKpis.pipes}P` — no null check, will display "undefinedP" if KPIs missing.

### FE-3: Backend Schema Mismatch Patched with Defensive Coding
`EnterpriseDashboard.tsx:99-102`: `metrics?.trustScore ?? metrics?.trust_score ?? 50` — frontend guesses between camelCase and snake_case because backend is inconsistent.

### FE-4: MonitorPanel Type Casts Unsafe
`MonitorPanel.tsx:77`: `node.metrics?.source_hierarchy as unknown as SourceHierarchy` — double cast indicates type mismatch between backend and frontend.

### FE-5: Hardcoded Persona Definitions Duplicate Backend
`App.tsx:49-68`: `personaTitles`, `personaFocusAreas`, `personaOntologies` all hardcoded in frontend, duplicating and potentially contradicting backend config.

### FE-6: ReconciliationPanel is 753 Lines
`ReconciliationPanel.tsx`: Monolithic component handling all reconciliation display, comparison, fabric breakdown, and diff visualization in a single file.

---

## Part 6: Remediation Plan

### Phase 1: Fix Reconciliation (Addresses all three reported symptoms)

#### 1.1 Unify Source Identity
- Create a single `normalize_source_id(name: str) -> str` function in a shared utility
- Use it in: `schema_loader.py`, `reconciliation.py`, `main.py` (both reconcile endpoints)
- Store `canonical_id` (not `label`) in `app.state.loaded_sources`
- Key change: `reconciliation.py:13` and `main.py:908` must use the same normalization

#### 1.2 Fix Pipe Count KPIs
- Compute KPIs BEFORE `source_limit` truncation
- Add `"total_aam_connections"` KPI from `pipes_data.get("total_connections")`
- Add `"limited"` boolean flag when `source_limit < total_available`
- Rename `"pipes"` to `"piped_sources"` to be semantically accurate (or actually count pipes)

#### 1.3 Consolidate Reconciliation Endpoints
- Merge `GET /api/dcl/reconciliation` and `POST /api/reconcile` into one endpoint
- Use a single reconciliation function with consistent normalization
- Return a unified response format

#### 1.4 Request-Scope the Run Context
- Replace `_last_aam_run` global dict with a run store keyed by `run_id`
- Pass `run_id` to reconciliation endpoints instead of reading global state
- Add staleness detection (reject reconciliation if DCL run is >N minutes old)

### Phase 2: Eliminate Regression Sources

#### 2.1 Replace Global Singletons with Dependency Injection
- Use FastAPI's `Depends()` for `DCLEngine`, `AAMClient`, `SchemaLoader`
- Make `SourceNormalizer` request-scoped (clear `_discovered_sources` per request)
- Replace class-level caches with proper cache service (or at minimum, thread-safe TTL cache)

#### 2.2 Split `main.py`
- Extract reconciliation endpoints to `backend/api/reconciliation_routes.py`
- Extract AAM-specific endpoints to `backend/api/aam_routes.py`
- Extract MCP endpoints to their own router
- Keep `main.py` as app factory + router registration only

#### 2.3 Split `_build_graph()` in dcl_engine.py
- Extract `_build_fabric_graph()` for AAM mode
- Extract `_build_standard_graph()` for Demo/Farm mode
- Shared helper for node/link creation
- Each path testable independently

#### 2.4 Make Graph IDs Deterministic
- Replace `uuid.uuid4().hex[:8]` with content-hash-based IDs
- `link_id = f"link_{source_system}_{concept}_{hashlib.sha256(...)[:8]}"`
- Same inputs always produce same graph — enables diff, caching, and debugging

### Phase 3: Remove Hardcoded Cheats

#### 3.1 Externalize Configuration
- Move trust/quality score rules to `config/scoring_rules.yaml`
- Move fabric type labels to `config/fabric_types.yaml`
- Move confidence thresholds to `config/mapping_thresholds.yaml`
- Move LLM model name to env var `DCL_LLM_MODEL` with fallback
- Move Pinecone config to env vars

#### 3.2 Fix Ontology-Code Mismatches
- Either add `gl_account` and `currency` concepts to `ontology_concepts.yaml`
- Or remove references to them from `heuristic_mapper.py` and `mapping_validator.py`
- Add `CHRO` to `persona_profiles.yaml` or remove from Persona enum
- Add startup validation that all code-referenced concepts exist in config

#### 3.3 Stop Swallowing Exceptions
- `persist_mappings.py:51`: Do NOT set `_pool_initialized=True` on failure
- `dcl_engine.py:66-71`: Distinguish "table doesn't exist" from "connection failed"
- Add `degraded` flag to run metrics when any fallback path is taken
- Surface fallback status in narration messages AND in API response

### Phase 4: Frontend Alignment

#### 4.1 Backend-Driven Configuration
- Add `GET /api/dcl/config` endpoint returning personas, modes, fabric types
- Frontend fetches config on startup instead of using hardcoded values
- Eliminates persona/mode duplication between frontend and backend

#### 4.2 Fix Persona View Generation
- `App.tsx:114`: Pass actual selected personas, not empty array
- Or defer persona view generation to `handleRun` only

#### 4.3 Type Safety
- Generate TypeScript types from Pydantic models (or use a shared schema)
- Remove all `as unknown as` casts
- Remove defensive `camelCase ?? snake_case` fallbacks by fixing backend serialization

---

## Priority Order

| Priority | Item | Symptom Addressed |
|----------|------|-------------------|
| P0 | 1.1 Unify source identity | Reconciliation failures |
| P0 | 1.2 Fix pipe count KPIs | Wrong pipe counts |
| P0 | 1.4 Request-scope run context | Reconciliation failures + reversions |
| P1 | 1.3 Consolidate reconciliation endpoints | Reconciliation confusion |
| P1 | 2.4 Deterministic graph IDs | Debugging + reversions |
| P1 | 3.2 Fix ontology-code mismatches | Mapping accuracy |
| P1 | 3.3 Stop swallowing exceptions | Silent degradation |
| P2 | 2.1 Dependency injection | Regression prevention |
| P2 | 2.2 Split main.py | Maintainability |
| P2 | 2.3 Split _build_graph() | Regression prevention |
| P3 | 3.1 Externalize configuration | Flexibility |
| P3 | 4.1-4.3 Frontend alignment | UX consistency |

---

## Appendix: File-Level Issue Map

| File | Lines | Issues |
|------|-------|--------|
| `backend/api/main.py` | 1,075 | Monolith; global `_last_aam_run`; two reconciliation endpoints; inline imports; labels stored instead of canonical IDs |
| `backend/engine/dcl_engine.py` | 529 | Monolithic `_build_graph()`; silent exception swallowing; random UUIDs in IDs; hardcoded fabric types; status always "ok" |
| `backend/engine/schema_loader.py` | 667 | Pipe count after truncation; naive normalization; all fields typed "string"; hardcoded trust scores; class-level caches |
| `backend/engine/source_normalizer.py` | 419 | Mutable singleton; never cleared across runs; accumulated `_discovered_sources` |
| `backend/engine/reconciliation.py` | 129 | Different normalization than other paths; compares labels not canonical IDs |
| `backend/engine/persona_view.py` | 173 | Hardcoded persona concepts; class-level cache; 0.8 hardcoded relevance |
| `backend/semantic_mapper/heuristic_mapper.py` | 188 | References nonexistent concepts; 7 magic numbers; overly permissive substring matching |
| `backend/semantic_mapper/persist_mappings.py` | 242 | Pool init failure sets initialized=True; cache race condition |
| `backend/llm/mapping_validator.py` | 261 | References nonexistent concepts in prompt; hardcoded model name |
| `backend/engine/rag_service.py` | 214 | Hardcoded Pinecone config |
| `backend/engine/provenance_service.py` | 224 | Hardcoded provenance data with stale dates |
| `backend/domain/models.py` | ~86 | CHRO in enum but not in config; no cross-reference validation |
| `config/ontology_concepts.yaml` | 190 | Missing gl_account, currency; unused vendor concept |
| `config/persona_profiles.yaml` | 85 | Missing CHRO |
| `src/App.tsx` | 388 | Empty array in persona view gen; hardcoded persona defs; unvalidated KPI display |
| `src/components/ReconciliationPanel.tsx` | 753 | Monolithic component |
| `src/components/MonitorPanel.tsx` | 687 | Monolithic; unsafe type casts; hardcoded polling interval |
| `src/components/EnterpriseDashboard.tsx` | 448 | Defensive camelCase/snake_case fallbacks |
