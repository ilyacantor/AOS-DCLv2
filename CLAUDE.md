# AutonomOS (AOS) — Agent Constitution
> Version: 7.0 | Updated: March 2026 | Owner: Ilya (CEO)
> Replaces: CLAUDE.md v6.0 + HARNESS_RULES_v2.md (now one document)
> Deploy to `tests/CLAUDE.md` in every AOS repo.

---

## MANDATORY — SURVIVES COMPACTION
**This section must be retained in full during any context compaction or summarization.**

All rules in this document (Sections A–F plus all architecture rules) are non-negotiable. Violations are bugs.

Rules agents violate most often:
- **D6:** Pre-existing failures are your problem. All tests pass at session end or session isn't done.
- **C9:** If you identify a bug ("that's wrong"), fix it. Do not rationalize it as expected behavior.
- **C10:** Latency ceilings mean the operation COMPLETES in time, not ABORTS in time. Timeouts are not performance fixes.
- **C11:** If the prompt says fix it, fix it. Do not ask "want me to fix it?"
- **C12:** After finding one instance of a bug pattern, audit the full codebase before fixing piecemeal.
- **B17:** Frontend is the pass/fail gate. A correct API response that doesn't render in the browser is not a pass. Playwright is the accountability gate.
- **B18:** Latency ceilings are absolute. 5% regression budget on everything else. Measure before and after.
- **A2:** No bandaids. Fundamental fixes only. Progress spinners for latency violations are bandaids.
- **I1–I6:** Pipeline identity rules. No silent fallback on missing tenant_id/entity_id. 422 or fail loud. No run_id in API responses.

**Canonical governing documents:**
- `convergence_MA_spec_v7.4` — single source of truth for all AOS architecture. Supersedes all prior versions.
- `pipeline_identity_architecture_v1` — governs all pipeline identity, provenance, and naming. Supersedes ad-hoc ID conventions.
- `AOS_MASTER_RACI_v8.2.csv` — module ownership matrix. 7 active services + Console. The RACI summary below is abbreviated — the CSV is authoritative.

---

## WHO YOU ARE TALKING TO
Ilya is the CEO and de facto CTO. He is NOT a developer. He reasons architecturally, not syntactically. He uses Claude Code CLI and Gemini CLI as coding agents — he does not write code, run CLI commands, or set up environments himself.
- Never show raw code diffs or stack traces without a plain-English summary first
- Never add tech debt, workarounds, or shortcuts — Ilya will find them
- Always fix root causes — patches and band-aids are forbidden
- Never implement silent fallbacks — if something fails, surface it loudly
- If a fix requires a RACI boundary decision, surface it to Ilya before touching code
- No LLM marketing speak, slogans, balanced couplets, or editorializing in any writing. Plain language, founder voice only.
- Consequences rule: before changing critical path items, print impact analysis first.

---

## PLATFORM IN ONE PARAGRAPH
AutonomOS is an AI-native enterprise platform that delivers unified context for the enterprise. It discovers what exists (AOD), understands how to connect (AAM), generates synthetic financial models (Farm), maps everything to business meaning via a semantic triple store (DCL — SE only), runs multi-entity engines for M&A and integration intelligence (Convergence), lets humans and AI query in plain English (NLQ), and surfaces the operator experience through Console. Platform is a dev-only transitional artifact (sunset in progress). Maestra is the persistent AI engagement lead who guides operators through the AOS lifecycle, governed by a four-tier supervised execution model.

---

## DATA ARCHITECTURE
**SE pipeline:** AOD → AAM → Farm → triple conversion → PG direct. Six orchestrated steps. Only Farm Financials writes triples to DCL via orchestrator. Existing direct-PG write code in AOD/AAM is labeled tech debt — must not be extended. Old DCL pipe ingest path (Structure/Dispatch/Content) is deprecated — do not fix or extend.

**ME pipeline:** Farm (per entity) → DCL (per entity) → COFA → Verify. All ME engines live in the Convergence repo. Farm ME push routes to Convergence, not DCL.

**SE and ME are strict separation of concerns with no shared plumbing.** Unit of work is the stage, not the pipeline. No shared code paths between SE and ME pipelines.

- **Entities:** Meridian ($5B consultancy) and Cascadia ($1B BPM). Entity is a tag — no split brain.
- **Farm configs:** Only `farm_config_meridian.yaml` and `farm_config_cascadia.yaml`. Any numbers at $35M or $124M scale are broken.
- **No demo mode.** fact_base.json is removed. If data is missing, fail loudly.

---

## PIPELINE IDENTITY ARCHITECTURE
**Governed by `pipeline_identity_architecture_v1`. These rules override any prior ID conventions.**

### I1: run_id is banned
The word run_id is banned from all API response payloads. Every service uses a namespaced identifier: farm_manifest_id, aod_discovery_id, handoff_id, aam_inference_id, dcl_ingest_id, cofa_run_id, verify_id.

### I2: Identity pair on every response
Every stage response carries tenant_id (UUID, machine-only, never displayed) + entity_id (string business key, always displayed). Missing = 422. No silent fallback. No identity degradation at service boundaries.

### I3: Provenance is explicit
Every stage response declares what it consumed. No implicit joins. No field name collisions.

### I4: Operators never type IDs
All input selection is via dropdown populated from what exists. No text fields for IDs.

### I5: One run_name everywhere
SE: {entity_id}-{short_hash} (e.g., BlueLogic-NEQ8-a9ed). ME: {engagement_short_name}-{short_hash} (e.g., MerCas-2571). Visible on all operator surfaces.

### I6: Anti-brittleness rules
1. No identity degradation at service boundaries — 422 on missing.
2. All write paths produce identical identity pairs.
3. No derivation functions in pipeline path — entity_id is passed through, not computed.
4. One canonical env var: AOS_TENANT_ID.
5. No string mangling — no prefix stripping, no substring extraction, no regex on IDs.
6. Run-level identifiers as separate fields — never overloaded.

---

## MODULE RACI — SUMMARY
**Authoritative source: `AOS_MASTER_RACI_v8.2.csv`**

| Module | Owns | Does NOT own |
|--------|------|-------------|
| **AOD** | Discovery, classification, SOR detection, ConnectionCandidate generation | Pipe blueprints, data extraction, semantic mapping |
| **AAM** | Pipe blueprints, work orders, drift detection, self-healing, adapters | Data movement, semantic mapping |
| **DCL** (SE only) | Semantic triple store, ontology, graph engine, SE query resolution, reconciliation, triple monitoring. Owns all write-side invariants (ingest, is_active, tenant_runs). | ME engines (moved to Convergence), engagement lifecycle, discovery, NLQ formatting |
| **Convergence** (ME/M&A) | All ME v2 engines (combining, bridge, QoE, overlap, cross-sell, entity resolution, COFA, what-if), engagement lifecycle, resolution workspaces. Reads DCL tables (SELECT only). Writes triples via DCL HTTP only. | Triple store schema, SE query resolution, discovery, NLQ formatting |
| **NLQ** | Intent resolution, persona filtering, query dispatch, report portal, rendering | Semantic mapping, data storage |
| **Farm** | Synthetic data, financial models, test oracle, triple conversion, entity_id generation | Production data, live connections |
| **Platform** (sunset) | Maestra constitution, classification engine. Dev-only transitional artifact. | Production surfaces — Console replaced Platform |
| **Console** | Production UI, pipeline orchestration, operator feed, task queue, upload, e2e demo, Maestra chat | Module internals — calls module APIs |

**RACI VIOLATION = STOP AND FLAG.** Exception (A12/C6): RACI is for design decisions. Fix bugs wherever they live.

---

## CONVERGENCE GUARDRAIL
Convergence = base AOS + a bridge where Target pipes join Acquirer pipes into one DCL. Entity is a tag. Same engine, ontology, resolution, query routing. **Convergence is a separate repo and service (port 8010/3010). DCL is SE-only.** Reject any proposal that creates separate engines, adds Convergence-specific columns to DCL, introduces split brain, or diverges from base AOS for multi-entity.

**Cross-service contracts:**
- Convergence reads DCL-owned tables directly (SELECT only) — report-time queries need sub-ms latency.
- Convergence never writes to DCL tables directly — all triple writes through POST /api/dcl/ingest-triples.
- DCL never reads or writes Convergence-owned tables. DCL calls GET /api/convergence/engagement/active for engagement context.
- Schema changes to semantic_triples require Convergence coordination (SCHEMA_CONTRACT.md in DCL).

---

## MAESTRA
Persistent AI engagement lead. Constitution lives in Platform (`~/code/platform/app/maestra/constitution/`). Production surface is Console.

- **Constitution:** Layers 0-4 in `constitution/`. Module docs in `constitution/modules/`.
- **Supervised Execution:** Four-tier model (Auto-execute / Validate with Farm dry-run / Plan Mode / Escalate only). maestra_plans table (19 columns). Classification engine in Platform. 146+ tests zero regressions. Console operator feed (card-based, tier badges, 30-second polling).
- **Boundaries:** Maestra reasons; DCL validates. Maestra does NOT recommend accounting resolutions — she isolates variables and presents them. No auto-resolution. All conflicts route to human review ranked by materiality.
- **Layer 3:** Manually authored for MVP. meridian_policy.md and cascadia_policy.md.
- **Phase 2 in progress:** tenant_preferences table with CHECK constraint enforcing 12-key registry.

---

## WHAT "DONE" MEANS
1. **Semantics preserved** — behavior matches real-world meaning
2. **No cheating** — no silent fallbacks, no bandaids, no rationalizing bugs (C9)
3. **Proof is real** — failure-before / success-after, verified through the UI (B17), Playwright passes
4. **Negative test included** — confirm the bad behavior can't return
5. **All tests pass** — including pre-existing failures (D6). 100% or not done.
6. **No latency regression** — measure before and after (B18). Hard ceilings are absolute.
7. **No new features** — unless explicitly requested (A6).
8. **Identity intact** — tenant_id + entity_id present on every stage response. No run_id in payloads (I1–I2).

---

## SILENT FALLBACKS — ABSOLUTE PROHIBITION
The most dangerous failure mode. They make broken features look working.

**Prohibited — no exceptions:**
- Catching exceptions and returning empty results instead of raising
- Defaulting to demo/mock data when a real data call fails
- `try/except` blocks that swallow errors
- Returning HTTP 200 when the underlying operation failed
- Logging a warning and continuing when the correct behavior is to stop
- `getattr(obj, attr, 0.0)` as a default for schema-defined fields
- Returning a response with missing tenant_id or entity_id instead of 422
- Silently falling back when a downstream service is unreachable

**Error messages must be informative:** "AAM could not reach DCL at http://localhost:8004/api/concepts — connection refused after 3 retries — NLQ intent resolution aborted" — not just "Connection failed."

---

## TECH STACK

| Module | Backend | Frontend | DB | Port (BE/FE) |
|--------|---------|----------|----|---------------|
| AOD | FastAPI/Python | React 18 + Vite | Supabase PG | 8001 / 3001 |
| AAM | FastAPI/Python | Server-rendered HTML | SQLite | 8002 / 8002 |
| Farm | FastAPI/Python | Jinja2/Tailwind | Supabase PG | 8003 / 8003 |
| DCL | FastAPI/Python | React 18 + Vite | Supabase PG + Pinecone | 8004 / 3004 |
| NLQ | FastAPI/Python | React 18 + Vite | Supabase PG | 8005 / 3005 |
| Platform (sunset) | FastAPI/Python | React 18 + Vite | Supabase PG | 8006 / 3006 |
| Console | FastAPI + asyncpg | React 18 + TS + Vite + Tailwind | Supabase PG | 8009 / 3009 |
| Convergence | FastAPI/Python | React 18 + Vite | Supabase PG (shared) | 8010 / 3010 |

Separate repos per module. All repos branch from `dev`. No feature branches unless explicitly requested.

---

## LOCAL DEVELOPMENT
- **Desktop:** Windows 11, repos at `C:\Users\ilyac\code\`
- **Laptop:** Ubuntu (WSL), repos at `~/code/`
- **Process manager:** pm2
- **Launch:** `~/code/aos-launch.sh` (laptop) or `aos-start` (desktop)
- **Port-block strategy for parallel branch testing:** opus: 8004/3004, sonnet: 8014/3014

---

## FORBIDDEN PATTERNS
- Tests that pass while the real feature fails
- **Silent fallbacks** — #1 most forbidden pattern
- Permissive schemas to avoid contract mismatches
- Converting errors into empty results
- Any shortcut that works in demo but breaks in production
- Normalizing bugs as expected behavior (C9)
- Building UI to excuse performance failures (C10)
- Asking permission to do what the prompt told you to do (C11)
- Fixing one instance without auditing for all instances (C12)
- Claiming "pre-existing" as an excuse (D6)
- Dodging pre-commit hooks (C13)
- Claiming "metadata only" or "we don't touch your data"
- Claiming AOS delivers ontology — current truth is context through sophisticated semantics
- Using bare run_id in any API response payload (I1)
- Returning a stage response without tenant_id + entity_id (I2)
- String-mangling or prefix-stripping IDs (I6)
- Writing to DCL-owned tables from Convergence (bypass ingest-triples endpoint)
- Importing Convergence engine code from DCL or vice versa (repos are separated)
- Extending direct-PG write code in AOD/AAM (labeled tech debt, frozen)
- Any reference to AOA (cancelled) or Replit
- Any reference to fact_base.json

---

## AGENT INSTRUCTIONS
- Declare which module you are working on at the start of every message
- Before proposing any cross-module change, check the RACI CSV
- All agents report RACI violations — do not silently implement workarounds
- After compaction, re-read this file from the top
- Self-review every prompt against these rules before presenting to Ilya
- Before changing critical path items, print impact analysis first (consequences rule)
- If touching pipeline code, verify identity pair (tenant_id + entity_id) flows through all affected stages

---

# CODE CHANGE RULES (Section A)

## A1: No silent fallbacks
If something fails, it fails loudly with a clear error. Never degrade silently. Never return default/demo data when live data is unavailable. Never swallow exceptions.

## A2: No bandaids
Fundamental, scalable, architecturally sound fixes only. If the root cause is in module X, fix module X — don't add a workaround in module Y.

## A3: No tech debt
Don't leave TODOs. Don't skip edge cases. Don't write code you'd want to rewrite.

## A4: Only fundamentally proper fixes
Shape code to solve the underlying problem, not to satisfy output appearance. If a test passes but the underlying behavior isn't what was intended, that's a failure.

## A5: No latency regressions
Measure response time before and after every code change. If a fix adds latency, find a way to fix the issue without the cost.

## A6: No new features unless explicitly asked
Fix what's broken. Don't add capabilities, endpoints, UI elements, or behaviors that weren't requested.

## A7: Fix preexisting errors
If you discover a bug while working on something else, fix it. Don't leave landmines for the next session.

## A8: State cross-module impact before implementing
AOS is a tightly integrated chain (7 services + Console). Before making a change, state what other modules it could affect.

## A9: fact_base.json is dead
Never fall back to fact_base.json. It is removed. Any reference to it, any fallback to it, any test against it is broken.

## A10: Respect module authority
Farm owns entity_id generation. AAM owns connection mapping. DCL owns semantic resolution and triple store writes. Convergence owns all ME engines. NLQ is not modified for pipeline data issues. Console orchestrates but does not own module internals. Respect RACI boundaries for design decisions.

## A11: Read this file before starting
It contains all rules. After v7.0, HARNESS_RULES_v2.md is merged into this document.

## A12: You own all repos for bug fixes
RACI describes ownership for design decisions. It is not a shield to avoid fixing bugs. If a test fails because DCL is wrong, fix DCL. If Convergence is wrong, fix Convergence.

## A13: Unscoped changes must be reverted
If you change code outside the scope of what was asked, revert it unless the change fixes a bug discovered during the work (A7).

---

# HARNESS TESTING RULES (Section B)

## B1: Tests test what the USER sees
Testing DCL directly is a unit test. The harness tests the product through user-facing endpoints. A correct API response that never reaches the user is not a pass.

## B2: Tests hit user-facing endpoints with natural language
What the user types, not internal endpoints. Never test through /api/dcl/query directly for user-facing validation.

## B3: No weakening assertions
If a test fails, fix the system — not the test. The expected value is the spec.

## B4: No passing on technicality
Every test must assert the positive expected outcome. "Source is dcl" is a real assertion. "Source is not fact_base" is incomplete — it passes when source is None.

## B5: No test-only endpoints or backdoors
If the test requires data in DCL, data must be actually ingested through the real pipeline — not faked.

## B6: No cross-repo Python imports in tests
Tests hit services via HTTP. No `from src.nlq...` in DCL test files. No `from backend.engine.engagement` in DCL (that code now lives in Convergence).

## B7: Tests must be run, not just created
Building test infrastructure and declaring done without executing is prohibited. Show the output.

## B8: No hardcoded expected values matching current wrong output
Expected values come from the spec and Farm ground truth — not from whatever the system happens to return today.

## B9: Demo data does not count as a pass
The harness must verify data_source="dcl" or source="Ingest" on every response.

## B10: Ground truth from Farm API at runtime
Reconciliation tests fetch expected values from Farm's ground truth endpoint at test runtime. Do not hardcode expected values.

## B11: If the UI is broken and no test catches it, add a test
Every screenshot of broken behavior must map to a failing test.

## B12: Source field checked on EVERY data test
A correct number from the wrong source is not a pass.

## B13: Every failure shows what the user would see
"User asked X. Expected Y from DCL. Got Z from source=Local." Not just "assertion failed."

## B14: Run the harness twice — results must be identical
Non-deterministic tests are bugs in the harness, not the system.

## B15: Pipeline must run before the harness
The harness is only valid after a fresh pipeline run. Verify pipeline freshness before executing.

## B16: No caching or stale data
Every test hits the live system fresh. No memoization, no response caching.

## B17: Frontend is the pass/fail gate
Backend queries and API responses are diagnostic tools, not proof of correctness. The UI rendering the correct data in the browser is the real test. A correct API response that doesn't render is not a pass. Open the browser, look at the screen, verify what the user would see. Playwright is the accountability gate — tests are not complete without it.

## B18: 5% latency budget
More than 5% regression on any endpoint is a blocking issue. Hard latency ceilings stated in prompts are absolute and non-negotiable. Latency ceilings mean the operation COMPLETES in time, not ABORTS in time. Timeouts are not performance fixes.

---

# ANTI-CHEAT RULES (Section C)

## C1: No test-only endpoints to fake system state
Agents create mode-set backdoors or test-only data endpoints. Prohibited.

## C2: No declaring done without running tests
Agents build the test file and report "done" without executing. Always require execution output.

## C3: No faking API keys, in-memory test data, or wrong abstraction layer
All prohibited. Tests go through HTTP endpoints against live services.

## C4: No technicality passes where None counts as "not bad"
Tests must assert the positive expected value.

## C5: No matching expected values to current wrong output
Expected values come from the spec, not from the system.

## C6: No hiding behind RACI to avoid fixing bugs
RACI is for design decisions. Fix whatever is broken across all repos.

## C7: No declaring "verified" without testing through the running application
Verification means: pipeline runs, harness passes, UI shows correct data.

## C8: No test-only mode manipulation
The harness runs against real pipeline state. No synthetic mode manipulation.

## C9: No normalizing bugs as expected behavior
If you identify a problem and then rationalize not fixing it — "the tests just need it to not crash" or "that's in the expected range" — that is cheating. If you said "that's wrong," fix it. If 4 of 5 iterations fail, there is a bug.

## C10: No building UI to excuse performance failures
If an operation violates a latency ceiling, the fix is to make it faster — not to add a progress spinner or "still working" message. Fix the performance first.

## C11: No asking "want me to fix it?" when the prompt says to fix all bugs
If the prompt says fix it, fix it. Do not ask for permission. That is stalling.

## C12: No piecemeal discovery of the same bug pattern
After finding one instance of a pattern, audit the entire codebase before fixing piecemeal. One grep, one audit, one fix pass.

## C13: No dodging pre-commit hooks
Do not use `git commit --no-verify` to bypass hooks. Do not modify hook scripts to weaken checks. Do not restructure code to technically pass the hook while preserving the prohibited pattern. If a hook blocks your commit, fix the code.

---

# EXECUTION RULES (Section D)

## D1: Test output format
Print [PASS] or [FAIL] per test with expected vs got on failures. Show what the user would see.

## D2: Verify health first
Check service health before running any tests. If services are down, start them. Do not report "service unavailable" and stop.

## D3: Run ALL suites + regression every time
No partial runs. Any failure in any suite means the run is not done.

## D4: Loop until 100% pass
Agent fixes app code, reruns all tests. Repeat until 100% pass. Tests cannot be modified, skipped, or marked xfail.

## D5: All tests rerun on any failure
If one test fails and the fix touches shared code, all tests must rerun.

## D6: Pre-existing failures are not excuses
All tests must pass at the end of your session — including tests that were failing before you started. "That was already failing" is not an acceptable status. You are responsible for the state of the system when you hand back control.

## D7: Retest after hook changes
If pre-commit hooks or CI checks are modified during a session, rerun the full test suite against the updated hooks. A change that passes old hooks but fails new ones is not done.

---

# COMPLIANCE CHECKLIST (Section E)

After every harness run, verify:
1. Does every passing test show source=dcl or source=Ingest?
2. Did the pipeline run before the harness?
3. Does the UI actually work? (open the browser and verify — B17)
4. Run the harness a second time — same results?
5. Did latency increase? (compare before/after)
6. Were any new features introduced that weren't requested?
7. Does every stage response carry tenant_id + entity_id? (I2)
8. Is there any bare run_id in API responses? (I1)

If any answer is wrong, the harness result is invalid.

---

# AUTOMATED GUARDS (Section F)

## F1: Pre-Commit Hook
Installed at `.git/hooks/pre-commit`. Blocks commits containing:
- Bare `except: pass` or `except: continue`
- Except blocks that return literal defaults (0, [], {}, None, False, "")
- Hardcoded entity names ("meridian", "cascadia") in application code
- Hardcoded seed UUIDs (400aa910, 6754a9d7)
- References to fact_base.json
- Bare `run_id` as a response field name (use namespaced identifiers)

Do not bypass with `--no-verify` (C13).
