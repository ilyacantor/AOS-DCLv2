# AutonomOS (AOS) — Agent Constitution
> Version: 7.1 | Updated: May 2026 | Owner: Ilya (CEO)
> Replaces: CLAUDE.md v6.0 + HARNESS_RULES_v2.md (now one document)
> v7.1: architecture (pipeline topology, RACI table, ports, module wiring) moved to the canonical governing docs; this file holds durable behavioral rules and guardrails only.
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
- `convergence_blueprint_master` — Convergence product architecture, workflow pattern, engines, pipeline identity. Supersedes all convergence_MA_spec versions (v5–v8).
- `mai_blueprint_master` — Mai concierge runtime, constitution, memory, surfaces, build phases. Supersedes all Maestra/Mai specs and Omnipresence Blueprint.
- `convergence_transition_master` — fixture eradication, identity resolver, AOS catalog generalization. Supersedes ME v2 Blueprint.
- `pipeline_identity_architecture_v1` — governs all pipeline identity, provenance, and naming.
- `AOS_MASTER_RACI_v8.6.csv` — module ownership matrix. 8 active services + Console. The RACI summary below is abbreviated — the CSV is authoritative.
- `se_triples_conversion_build_plan_v2.2.md` — AOS/Convergence triple write paths, deferred AOD/AAM conversion specs, EAV data model.

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
Platform overview and module roles are architecture — see `aos_production_architecture_blueprint_v1.1.md` and the RACI (`AOS_MASTER_RACI_v8.6.csv`).

---

## DATA ARCHITECTURE
Pipeline topology, stage order, and triple write paths are architecture — see `se_triples_conversion_build_plan_v2.2.md` and `pipeline_identity_architecture_v1` (AOS), `convergence_blueprint_master` (Convergence), `AAM_Blueprint_v4` (AAM connectivity).

Durable guardrails:
- Existing direct-PG write code in AOD/AAM is tech debt — must not be extended.
- The old DCL pipe ingest path (Structure/Dispatch/Content) is deprecated — do not fix or extend.
- AOS and Convergence are strict separation of concerns — no shared code paths between the two pipelines. Unit of work is the stage, not the pipeline.
- Entity is a tag — no split brain. No hardcoded entity names; fixture configs are transitional and being eradicated (convergence_transition_master). Any numbers at $35M or $124M scale are broken.
- No demo mode. fact_base.json is removed. If data is missing, fail loudly.

### Terminology
- AOS = single-entity product (codebase: SE)
- Convergence = multi-entity product (codebase: ME)
- Convergence M&A = deal-driven engagement workflow (codebase: MA)
- Mai = concierge agent (codebase: maestra pre-rename, mai post-rename)
- Product names (AOS, Convergence) in specs and operator surfaces. Code identifiers (SE, ME) retained until code cleanup phase.

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

## Playwright Acceptance (extends B17)

No feature is done until the agent states in plain English what a working version looks like on screen — specific counts, specific entity names, specific rendered text — and the test asserts exactly that. "Banner appears" means the agent does not understand the feature.

UI-driven only:
- Tests drive the operator path through real UI events — `locator.click()`, `locator.fill()`, `selectOption()`, file pickers, keyboard. The test sits in the operator's seat: open the page, click what they click, type what they type, wait for what they see.
- Calling backend endpoints from the test (`page.request.post()`/`put()`/`patch()`/`delete()`, `fetch()`, `axios`, `curl`, the SDK, route stubs that bypass the user's input) to trigger the feature under test is an API test, not acceptance. API-only paths do not count toward the B17 gate even when green.
- Actual clicks must trigger the sequence. No test-only triggers, no harness-side POSTs that simulate the click, no orchestration calls that bypass the button. If the user's first action is a click, the test's first action is a click.
- Allowed exception: `page.request.get()` to read-only endpoints (Farm ground-truth, health) for fetching expected values. Any mutative call from the test runner is a violation.

Acceptance specificity:
1. Assertions compare against ground truth pulled from the source system at test time. Expected values are never hardcoded and never agent-authored. Farm exposes the ground-truth endpoint; the test fetches and compares.
2. Mutative features require before/after state capture: capture rendered state, perform action, capture again, assert delta matches the claim.
3. Weak assertions fail acceptance even when green. Always-wrong (pre-commit enforced on added lines): `toBeTruthy()`, `.not.toBeNull()`, `length > 0`, `length >= 1`, `toContain('success')`, `toContain('ok')`, `status === 200` as sole assertion. Context-dependent (agent self-review, not hook-enforced): bare `toBeVisible()`, `toHaveCount(n)` not tied to ground truth, `toHaveLength(n)` not tied to ground truth.

Reporting:
4. Every UI test captures a screenshot. Thumbnails embedded in the agent's completion report.
5. Agent runs the test headed once before declaring done. Not required in CI; required in the completion handoff.

Taxonomy:
6. Live-services run against real Farm / DCL / NLQ / pm2 = acceptance. Mocked Playwright = regression only, labeled as such. Both required. A PR with only one is incomplete.
7. Every feature with a visible failure surface ships with a paired negative test asserting the readable error, not the status code.

Prompt-time requirement:
Every CC feature prompt opens with a one-sentence operator-visible outcome statement with specific values before writing code. If the agent cannot write that sentence, it does not understand the feature and does not write the test.

Hook scope (scripts/precommit.sh, sourced from `.playwright-banned-patterns`): inspects ADDED/MODIFIED lines only in staged `*.spec.ts`/`*.spec.js`; enforces the `// Operator-visible outcome: <specific values>` first-line header on NEW spec files only. Existing specs are not retroactively blocked. Rewriting existing specs to satisfy Rules 1 and 2 is tracked as catalog-pass entries in `dcl_deferred_work.md`.

---

## MODULE RACI — SUMMARY
**Authoritative source: `AOS_MASTER_RACI_v8.6.csv`**

Module ownership (Owns / Does NOT own per module, 8 active services + Console) is architecture — read the CSV. The table previously inlined here was abbreviated and drifts.

**RACI VIOLATION = STOP AND FLAG.** Exception (A12/C6): RACI is for design decisions. Fix bugs wherever they live.

---

## CONVERGENCE GUARDRAIL
Convergence topology, ports, and cross-service wiring are architecture — see `convergence_blueprint_master`. Convergence is a separate repo and service; DCL is AOS-only.

Durable guardrails:
- Reject any proposal that creates separate engines, adds Convergence-specific columns to DCL, introduces split brain, or diverges from base AOS for multi-entity. Entity is a tag — same engine, ontology, resolution, query routing.
- Convergence reads DCL-owned tables SELECT-only and writes to its own table — it never writes to DCL-owned tables.
- DCL never reads or writes Convergence-owned tables.
- Schema changes to semantic_triples require Convergence coordination (SCHEMA_CONTRACT.md in DCL).

**Workflow vs Agent category rule:**
- **Agent** (Mai): LLM decides control flow. Concierge scope only. Read-broad, write-admin-only.
- **Workflow** (Convergence): code decides control flow. LLM is a bounded reasoning step inside one node. Convergence calls the Anthropic SDK directly — no dependency on Platform Mai for LLM access.
- Operational tasks (COFA merge, combining, entity resolution, QofE review) route through Convergence workflow endpoints, not through Mai's chat endpoint.
- Operational tools (write_cofa_mapping, write_combining_output, write_resolution_decision) are NOT registered in Mai's tool registry. They exist inside Convergence workflow handlers.
- Do not ship operational execution through Mai. Do not register operational tools in Mai's catalog. This is a permanent exclusion.

---

## MAI
Mai's runtime, scope, constitution layers, supervised-execution mechanics, and observability surfaces are architecture — see `mai_blueprint_master`. Constitution lives in Platform.

Durable guardrails — Mai is the concierge: reads broadly, writes narrowly to admin surfaces only. She does NOT:
- Execute operational workflows, or route COFA merge / combining / entity resolution / QofE through her chat endpoint.
- Register operational tools in her tool catalog.
- Recommend accounting resolutions — she explains results by reading structured data, not by re-reasoning over accounting rules.

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
9. **Workflow routing intact** — no operational execution through Mai's chat endpoint or tool registry.

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
Per-module backend/frontend/DB/ports are architecture — see `aos_production_architecture_blueprint_v1.1.md`; each repo's own ports live in its repo-specific guardrails.

Separate repos per module. All repos branch from `dev`. No feature branches unless explicitly requested.

---

## LOCAL DEVELOPMENT
- **Desktop:** Windows 11, repos at `C:\Users\ilyac\code\`
- **Laptop:** Ubuntu (WSL), repos at `~/code/`
- **Process manager:** pm2
- **Launch:** `~/code/aos-launch.sh` (laptop) or `aos-start` (desktop)
- **Port-block strategy for parallel branch testing:** opus: 8004/3004, sonnet: 8014/3014

---

## Dev/Prod Database Separation

- `.env` = production Supabase credentials. Never use for local development.
- `.env.development` = aos-dev Supabase credentials. Always load this for local runs and testing.
- Before any DB-touching work, confirm which env file is active.
- Never write seed scripts, migrations, or test data against `.env` directly.
- aos-dev project: `glmeqbnuahlkkbolkent` (Supabase). Schema prefixes separate prod projects: `shared_gdbmdr`, `shared_yuxrdo`, `shared_jhvxtl`, plus `console`, `maestra`, `mai_memory`, `aod`.
- Connection strings in `.env.development` set `search_path` via the `options` query param so apps resolve tables transparently.

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
- Routing operational workflow execution through Mai's chat endpoint or tool registry (workflow-through-chat)
- Registering operational tools (write_cofa_mapping, write_combining_output, write_resolution_decision) in Mai's tool catalog
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
- If touching Mai code, verify no operational workflow execution routes through Mai's chat endpoint or tool registry

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
Backend queries and API responses are diagnostic tools, not proof of correctness. The UI rendering the correct data in the browser is the real test. A correct API response that doesn't render is not a pass. Open the browser, look at the screen, verify what the user would see. Playwright is the accountability gate — tests are not complete without it. Tests must drive the operator path through real UI events — `locator.click()`, `locator.fill()`, `selectOption()`, file pickers, keyboard. Backend calls from inside the test (`page.request.post()`, `fetch()`, `curl`, the SDK, route stubs that bypass user input) are API tests, not acceptance. Actual clicks must trigger the sequence — never POSTs from the test runner.

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

---

# Branch hygiene (B17)

- Feature branches are merged to dev and deleted in the same session they are created.
- Unmerged branches at session end are a B17 failure and must be reported.
- `--no-verify` is banned. If a hook blocks a legitimate change, fix the hook scope, then commit.
- Session start: run `git fetch --all --prune && git branch -a` and report stale branches before new work.

---

# Destructive-action contract (B19)

Destructive actions touch state the user or another tenant cannot get back
without recovery effort. In DCL that list is narrow but high-cost:

- `TRUNCATE` / `DROP TABLE` / `DROP COLUMN` against any live table.
- `DELETE FROM semantic_triples | tenant_runs` outside the sanctioned retention
  tools (`purge-inactive`, `purge-old-runs`, `delete_by_run`, test-tenant
  cleanup). The bi-temporal lifecycle (mig017) supersedes rows — it never
  deletes; a hand-written DELETE against history is destruction, not lifecycle.
  (`current_triples` / `semantic_triples_archive` / `swap_and_delete` belonged
  to the April 2026 store rebuild — deployed Apr 13–19, backed out via full
  prod store reset; see SCHEMA_CONTRACT.md "Store lineage". They exist in no
  environment.)
- Running a migration that rewrites or rebuilds the store (any successor of
  mig017).
- Killing or restarting dcl-backend while an in-flight ingest or migration
  holds an open transaction.
- `git reset --hard`, `git push --force`, branch deletion on unmerged work.

Before any destructive action:

1. State the blast radius in one sentence — which table, how many rows, which
   tenants, and whether the change is reversible.
2. Check for in-flight transactions. `apply_mig015` (April 2026 store rebuild,
   since backed out) once held an hours-open backfill txn through a compaction;
   killing dcl-backend would have rolled back hours of scan work. The lesson
   stands for every successor: prove the write path is idle first, and any
   prod-store migration must be batched and lock-budgeted explicitly against
   that incident.
3. Confirm with the user before executing. One-time approval stands for the
   scope stated in that approval, nothing more. "Apply mig016" does not
   authorize "also drop ingest_log."
4. Inside one transaction wherever possible. Partial rollback is safer than
   partial commit.
5. Never use `--no-verify`, `DROP … CASCADE`, `TRUNCATE … CASCADE`, or
   `DELETE … WHERE true` as a shortcut. If a hook or constraint blocks you,
   it is pointing at a real problem. Fix the underlying code instead.

Non-destructive equivalents exist for most debugging: query the state, run
`EXPLAIN`, take a schema-only dump, or spin up a fresh database. Use them.

After any destructive action:

- Verify the post-state by counting rows and re-running store invariants
  (`tests/test_store_invariants.py`).
- Record what ran, against which environment, and the pre/post row counts in
  the session report.

## Deferred work

Any item you defer, park, flag as separate, classify as out-of-scope, or otherwise do not fix in the current session must be appended to `dcl_deferred_work.md` at repo root before declaring done. This repo's file: `dcl_deferred_work.md`.

Required fields per entry, single pipe-delimited line:

  N. YYYY-MM-DD | chat-ref | file:line | reason | severity: blocker/degraded/cosmetic | blocking: <what>

Rules:
- Chat-only parking is not parking. If it is not in the file, it does not exist.
- "Pre-existing," "out of scope," "separate workstream," "environmental," "not caused by this refactor" all require an entry. These phrases are the trigger, not the excuse.
- Append only. Do not rename, reorder, or renumber existing entries.
- Do not delete resolved entries. Mark `RESOLVED YYYY-MM-DD` inline and leave in place.
- chat-ref values must not contain the canonical filename (breaks self-reference greps).
- Cross-repo items: reference the canonical entry, do not duplicate (e.g., `see dcl_deferred_work.md#12`).
- Before starting any session, read the open entries. If the task touches one of them, stop and ask.
- "Logged" said in chat is a claim, not a confirmation. The write must produce a file diff and a commit. Otherwise it didn't happen.

Filename is fixed: `dcl_deferred_work.md`. Do not create `DEFERRED.md`, `TODO.md`, `FOLLOWUPS.md`, `project_deferred_work.md`, or any other variant.

## Deferred work — pre-session check

Before starting any task in this repo, read the open entries in `dcl_deferred_work.md`. For each open entry, note whether the file:line or subject area falls within the scope of the task you are about to execute.

If yes:
- State upfront in your response which deferred items your work plausibly touches, by number.
- At end of session, for each one: verify whether your changes resolved it. If yes, append `RESOLVED YYYY-MM-DD (commit <sha>)` inline on that item. If no, state why not.
- Do not delete resolved items. Do not renumber.

If no items overlap, say so in one line ("Pre-session check: no open deferred items in scope") and proceed.

Skipping the pre-session check is the same class of failure as chat-only parking — it lets the file drift from reality.

## Test result reporting

Before claiming a suite is green/passing/done:
1. Quote the final pytest summary line verbatim ("X passed, Y failed in Zs")
2. If that line is absent from tool output, state: "Suite did not complete. Partial signal: <what was actually observed>"
3. Never map per-test or per-file pass counts to suite-level claims. "test_smoke 6/6" = "6 of 6 smoke tests passed; full suite status unknown" — never "green"
4. No "honest deviation" / "spot-checked" / "looks good" framing as a substitute for the summary line
5. Same rule for any long-running command: no completion claim without the final stdout/exit-code evidence in-context
