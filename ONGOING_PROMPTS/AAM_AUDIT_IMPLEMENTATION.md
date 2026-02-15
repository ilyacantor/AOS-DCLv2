# AAM Codebase Audit: Implementation Plan (Reviewed)

## Context

AAM (Adaptive API Mesh) sits between AOD (discovery) and DCL (semantic unification) in the AOS pipeline. Per the **RACI v4 matrix**, AAM is **A/R (Accountable + Responsible)** for all Pipe Inference — fabric plane ("WHERE pipes live"), modality ("HOW pipes are accessed"), transport kind, entity scope, identity keys, and change semantics. AOD provides **evidence leads (hints)**, not definitive routing.

**The fundamental problem:** AAM has abdicated its RACI responsibility. The code explicitly refuses to infer fabric planes without a definitive `connected_via_plane` value from AOD (`matching_service.py:64-65`), and `constants.py:7-11` codifies this abdication as a "DESIGN RULE." The result: **686 of 690 candidates fail fabric plane inference** — only 4 self-evident infrastructure products (Kafka, MuleSoft, Snowflake, Kong) resolve because AOD can trivially assign their plane type. AAM isn't doing inference; it's copying a field.

The codebase also has hardcoded mock data in all 4 adapters, a dual-table split brain, SQL injection, zero inference logging, and frontend issues. This plan addresses all findings, prioritized by blast radius.

**Note:** Each AOD handoff wipes and re-ingests candidates, so no backfill migration is needed. Deploying the fix and triggering the next handoff will run all candidates through the new inference cascade automatically.

---

## FINDING 0 (ARCHITECTURAL): AAM Abdicates Fabric Plane Inference — RACI Violation

**The RACI says:**
- `Pipe Inference → Infer Fabric Plane`: AAM **A/R** — "WHERE pipes live"
- `Pipe Inference → Infer Modality`: AAM **A/R** — "HOW pipes are accessed"
- `Pipe Inference → Infer Transport/Entity/Identity/Semantics`: AAM **A/R**
- `Fabric Detection → Evidence Lead Generation`: AOD **A/R**, AAM **C** — "hints"
- `Fabric Detection → Evidence Lead Export to AAM`: AOD **A/R** — "Passes connection hints in ConnectionCandidate handoff for AAM to *validate against plane crawl*"

**The code says the opposite:**
- `app/constants.py:7-11` — DESIGN RULE: "AAM never infers infrastructure from application categories. Only AOD-discovered infrastructure evidence or explicit operator declarations create fabric plane records."
- `app/services/matching_service.py:64-65` — `if not aod_plane_hint: return None, 0.0, "Cannot create pipe: no fabric plane hint from AOD"` — **the abdication in code**
- `app/services/handoff_service.py:198` — "We do NOT guess a default plane from the preset; that was creating the 654-into-API_GATEWAY pile-up" — **the overcorrection**

**What the architect confirms:** The principle that application categories alone don't determine infrastructure is valid (Salesforce *could* route through any plane). But AAM owns Fabric Plane Inference (A/R) — it must use vendor identity, display name hints, evidence leads from AOD, and endpoint signal analysis to make inference decisions. AAM should use whatever partial evidence AOD provides to make decisions — not refuse to act without a definitive hint.

**The rich inference engine exists but is completely disconnected from real data:**
- `app/inference.py` has `infer_fabric_plane()`, `infer_modality()`, `infer_transport_kind()`, `infer_entity_scope()`, `infer_identity_keys()`, `infer_change_semantics()`, `build_trust_labels()`, `build_lineage_hints()`, `infer_ownership_signals()` — all well-implemented
- But these functions ONLY process mock adapter observations (Path 1 in `collectors.py:52-65`, labeled "legacy")
- Real AOD candidates go through Path 2 (`collectors.py:67-80`) → `matching_service.py` → which has NO inference logic, just vendor-name match or copy-the-AOD-hint

**Additionally, `find_existing_pipe()` in `inference.py:520-533` is dead code** — defined but never called. And `process_pending_observations()` creates 1 pipe per observation with zero dedup.

### Fix: Build Evidence-Based Fabric Plane Inference Into the AOD Candidate Pipeline

The fix requires 4 changes, in order:

#### Change 0a: Remove the abdication DESIGN RULE from `constants.py`
**File:** `app/constants.py:7-11`

Replace the DESIGN RULE comment with the correct RACI-aligned principle:
```
DESIGN RULE (RACI v4): AAM owns Fabric Plane Inference (A/R).
AOD provides evidence leads (hints). AAM uses vendor identity,
display name hints, evidence leads, and endpoint signal analysis
to infer fabric plane. Application categories alone are not
sufficient, but combined with evidence they inform inference.
```

#### Change 0b: Build `infer_fabric_plane_for_candidate()` in `matching_service.py`
**File:** `app/services/matching_service.py`

New function that implements the RACI-mandated inference cascade:

```
Resolution order (highest to lowest confidence) — FIRST MATCH WINS, no accumulation or averaging:
1. AOD explicit connected_via_plane hint → confidence 0.95
2. INFRA_VENDOR_PLANE identity match (Kafka IS event bus) → confidence 0.90
3. DISPLAY_NAME_PLANE_HINTS match → confidence 0.80
4. Evidence signals from AOD (evidence_refs, signals_summary, known_endpoints) → confidence 0.70
5. No match → flag as `needs_operator_review`, confidence 0.0
```

**Cascade conflict resolution rule:** The cascade is strictly ordered. If step 1 produces a result, steps 2-4 are never evaluated. If step 1 produces no result and step 2 produces a result, steps 3-4 are never evaluated. There is no case where two steps can disagree, because the first match breaks the cascade. The `routing_source` field in provenance records which step was used (e.g., `"aod_explicit"`, `"infra_vendor_identity"`, `"display_name_hint"`, `"evidence_signal"`, `"needs_operator_review"`).

Steps 1-2 produce definitive results. Step 3 uses display name keywords that AOD attached to candidates. Step 4 uses heuristics on endpoint URLs and evidence signals — reusing logic from `inference.py:infer_fabric_plane()`. Step 5 converts silent failure into an actionable operator workflow.

#### Change 0c: Rewrite `find_matching_pipe()` to use the inference cascade
**File:** `app/services/matching_service.py:46-97`

Replace current logic. This function handles two scenarios:

**Scenario A — Match to existing pipe:**
When a candidate arrives, first check if a pipe already exists for this vendor. Match by `vendor_canonical_name` (not display name). If a match is found, link the candidate to that pipe. If the same vendor has pipes on multiple planes (e.g., Salesforce on both API_GATEWAY and IPAAS), match to the pipe whose plane matches the inference cascade result for this candidate. If no plane match, create a new pipe on the inferred plane (the vendor legitimately routes through multiple planes in some enterprises).

**Scenario B — Create new pipe:**
If no existing pipe matches, run the inference cascade from Change 0b. Create a new pipe with the inferred plane + confidence score + `routing_source` in provenance. Remove Strategy 1 (circular vendor match against candidates). Remove Strategy 2's hard gate (`if not aod_plane_hint: return None`).

**Dedup rule:** One pipe per (vendor_canonical_name, fabric_plane) pair. `process_pending_observations()` must check this before creating.

#### Change 0d: Wire `inference.py` enrichment into the candidate pipeline
**File:** `app/routers/collectors.py:67-80` (Path 2)

After matching a candidate to a pipe, call the `inference.py` enrichment functions to populate `entity_scope`, `identity_keys`, `change_semantics`, `trust_labels`, `modality`, and `transport_kind` on the candidate. Currently Path 2 produces hollow pipes with hardcoded `transport_kind: "API"`, no entity_scope, no identity_keys.

Build a new function `enrich_candidate_with_inference()` in `inference.py` that takes a candidate dict and returns enriched metadata by calling the existing `infer_*` functions. This reuses all the existing inference logic — no new heuristics needed.

**Pre-implementation check for Finding 1 dependency:** Before implementing Change 0d, grep for all callers of `discover_pipes()` and the Path 1 collector flow (`collectors.py:52-65`). If anything in the candidate pipeline still depends on Path 1 output, the mock adapter removal in Finding 1 could break it. If Path 1 callers exist outside the "legacy" label, document them and ensure they're handled before Finding 1 is implemented.

---

## FINDING 1 (CRITICAL): All 4 Fabric Adapters Return Hardcoded Mock Data

**Files:** `app/adapters/ipaas.py`, `gateway.py`, `eventbus.py`, `warehouse.py`

Every adapter's `discover_pipes()` returns fabricated data. `connect()` sets CONNECTED without connecting. `check_health()` returns fake latency. `self_heal()` and `apply_governance_policy()` return True without acting.

**Dependency note:** Returning `[]` from `discover_pipes()` means the collector run path (Path 1 in `collectors.py`) produces zero data. This is correct since Path 1 is labeled "legacy" and real data flows through Path 2 (AOD candidates). Verify no active code paths depend on Path 1 output before deploying (see Change 0d pre-implementation check).

### Fix:
1. **`app/adapters/base.py`** — Add `is_implemented: bool = False` property
2. **All 4 adapters** — `discover_pipes()` returns `[]` with log warning. `connect()` returns False with log warning. `self_heal()` and `apply_governance_policy()` return False. `check_health()` returns DISCONNECTED status.

---

## FINDING 2 (CRITICAL): Dual-Table Split Brain — `connection_candidates` vs `declared_pipes`

**Files:** `app/db/pipes.py`, `app/db/stats.py`, `app/db/admin.py`, `app/inference.py`

Two tables claim to be the source of truth. Different modules query different tables. `_candidate_to_pipe()` does lossy conversion with hardcoded defaults.

### Fix:
1. **`app/db/pipes.py:63-91`** — Remove fallback to `declared_pipes` in `get_pipe()`. Candidates are the single source.
2. **`app/db/pipes.py:218-262`** — `_candidate_to_pipe()` reads inference-enriched fields from the candidate instead of hardcoding defaults (depends on Finding 0d enrichment writing these fields to candidates)
3. **`app/db/stats.py:76-80`** — Change drift query to use `connection_candidates`
4. **`app/db/admin.py:57-86`** — `get_pipe_stats()` queries `connection_candidates` instead of `declared_pipes`

---

## FINDING 3 (HIGH): SQL Injection in `drift.py`

**File:** `app/db/drift.py:84` — `query += f" LIMIT {limit}"`

**Implementation note:** Check which DB driver `drift.py` uses. If SQLite, the placeholder is `?`. If PostgreSQL/Supabase (via psycopg2 or asyncpg), the placeholder is `%s`. The AOS platform runs on Supabase, so confirm the driver before implementing.

### Fix: Parameterized query — `query += " LIMIT %s"` (PostgreSQL) or `query += " LIMIT ?"` (SQLite) with `cursor.execute(query, (limit,))`

---

## FINDING 4 (HIGH): Silent Error Swallowing in Handoff Service

**Files:** `app/services/handoff_service.py:179-180` (plane store), `:342-343` (SOR store)

### Fix:
1. **`app/services/handoff_service.py`** — `resolve_fabric_planes()` returns 3-tuple `(map, count, errors)`
2. **`app/models.py`** — Add `plane_store_errors: list = []` and `sor_store_errors: list = []` to `AODHandoffResponse`
3. **`app/services/handoff_service.py`** — `process_handoff()` includes errors in response

---

## FINDING 5 (HIGH): Inference Engine Has Zero Logging

**File:** `app/inference.py` — 558 lines, zero log statements

### Fix:
1. Add `_log = get_logger("inference")` at module level
2. `_log.debug()` in every `infer_*` function showing inputs → matched rules → output
3. `_log.info()` in `infer_single_pipe()` and the new `enrich_candidate_with_inference()`

---

## FINDING 6 (HIGH): Frontend Shows "OK" for Unmonitored Pipes

**File:** `app/routers/ui_pages.py:72-74`

### Fix: Three states — "Healthy" (has data, no drift), "No data" (never monitored), "N open" (has drift)

---

## FINDING 7 (MEDIUM): Inconsistent DB Connection Patterns

### Fix: Migrate `drift.py`, `pipes.py`, `stats.py` from `get_connection()` to `get_db()` context manager

---

## FINDING 8 (MEDIUM): Frontend Fetch Missing `.ok` Checks

### Fix: Add `.ok` check to all fetch calls, replace `alert()` with `showToast()`

---

## FINDING 9 (MEDIUM): Frontend Code Duplication

### Fix: Extract shared `showToast()`, modal helpers, fetch wrapper into single template block

---

## FINDING 10 (LOW): `admin.py` f-string Table Names

### Fix: Add `ALLOWED_TABLES` frozenset validation

---

## Implementation Order

| Phase | Findings | Rationale |
|-------|----------|-----------|
| **Phase 1: RACI Alignment** | #0 (inference abdication) | Fix the architectural root cause — make AAM own fabric plane inference |
| **Phase 2: Data Integrity** | #1 (mock adapters), #2 (dual tables), #3 (SQL injection) | Eliminate cheats, consolidate source of truth, fix security |
| **Phase 3: Error Visibility** | #4 (silent errors), #5 (inference logging) | Make the system observable and debuggable |
| **Phase 4: UI & Cleanup** | #6 (drift UI), #7 (db connections), #8 (fetch .ok), #9 (JS duplication), #10 (table validation) | Polish the operator experience |

**Phase ordering rationale:** Phase 1 before Phase 2 is critical because mock adapter removal (Finding 1) and dual-table consolidation (Finding 2) both depend on the inference pipeline actually working. Flipping the order would remove mock data before the real path can produce anything, making the system look worse before it looks better.

---

## Verification

After Phase 1 (the critical fix):
1. **Start server:** `uvicorn app.main:app --port 8000`
2. **POST a test handoff** to `/api/handoff/aod/candidates` with a mix of candidates — some with `connected_via_plane`, some without, some infrastructure vendors (Kafka, Snowflake), some pure SaaS (Salesforce, Workday)
3. **Run inference:** POST `/api/aam/infer`
4. **Verify:** ALL candidates should now have a fabric plane assignment (not just 4/690). Check:
   - Infrastructure vendors (Kafka → EVENT_BUS, Snowflake → DATA_WAREHOUSE) at confidence >= 0.90
   - SaaS apps without AOD hints get inferred via display name hints or evidence signals, or flagged `needs_operator_review`
   - Candidates with AOD `connected_via_plane` keep that assignment at confidence 0.95
   - Provenance includes `routing_source` showing which cascade step was used
   - Trust labels include `inferred:evidence_signal` or `needs_operator_review` as appropriate
   - Same vendor on different planes produces separate pipes (dedup by vendor + plane pair)
5. **GET `/api/pipes`** — verify all candidates appear as pipes with enriched metadata (entity_scope, identity_keys, etc. — not hardcoded defaults)
6. **Load `/ui/topology`** — verify all candidates appear in fabric plane groups, not floating as "UNMAPPED"

After all phases:
- No 500 errors on any endpoint
- No JS console errors on any UI page
- Drift badges show correct states (Healthy / No data / N open)
- Inference logs show structured trace of which rules fired for each candidate
- `discover_pipes()` on adapters returns `[]` (not mock data)
- Handoff response includes error fields when planes/SORs fail to store
