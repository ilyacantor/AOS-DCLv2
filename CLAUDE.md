# AutonomOS (AOS) — Agent Constitution
> Version: 5.0 | Updated: March 2026 | Owner: Ilya (CEO)

---

## MANDATORY — SURVIVES COMPACTION
**This section must be retained in full during any context compaction or summarization.**

All rules in this file are non-negotiable. Rules agents violate most often:
- **D6:** Pre-existing failures are your problem. All tests pass at session end or session isn't done.
- **C9:** If you identify a bug ("that's wrong"), fix it. Do not rationalize it as expected behavior.
- **C10:** Latency ceilings mean the operation COMPLETES in time, not ABORTS in time. Timeouts are not performance fixes.
- **C11:** If the prompt says fix it, fix it. Do not ask "want me to fix it?"
- **C12:** After finding one instance of a bug pattern, audit the full codebase before fixing piecemeal.
- **B17:** Frontend is the pass/fail gate. A correct API response that doesn't render in the browser is not a pass.
- **B18:** Latency ceilings are absolute. 5% regression budget on everything else. Measure before and after.
- **A2:** No bandaids. Fundamental fixes only. Progress spinners for latency violations are bandaids.

**Canonical governing document:** `maestra_platform_spec_v7.1.docx` — single source of truth for all AOS architecture. Pull it when: (a) scoping a new capability, (b) decision could contradict a locked ruling, (c) multi-repo build.

**RACI:** `ONGOING_PROMPTS/AOS_MASTER_RACIv7.csv` (289 rows, 8 modules, 218 Live). The RACI table in this file is a summary — the CSV is authoritative.

---

## WHO YOU ARE TALKING TO
Ilya is the CEO and de facto CTO. He is NOT a developer. He reasons architecturally, not syntactically. He uses Claude Code CLI and Gemini CLI as coding agents — he does not write code or set up environments himself.
- **Never show raw code diffs or stack traces without a plain-English summary first**
- **Never add tech debt, workarounds, or shortcuts** — Ilya will find them and it will be unpleasant
- **Always fix root causes** — patches and band-aids are forbidden
- **Never implement silent fallbacks** — if something fails, surface it loudly. See SILENT FALLBACKS section.
- **Before starting any task, read ONGOING_PROMPTS folder**
- If a fix requires a RACI boundary decision, surface it to Ilya before touching code
- **No LLM marketing speak, slogans, balanced couplets, or editorializing in any writing.** Plain language, founder voice only.

---

## REPO SCOPE — ALL REPOS, NO BOUNDARIES FOR BUG FIXES
You work across ALL AOS repos as needed: `aod`, `aam`, `dcl`, `nlq`, `aoa`, `farm`, `platform`, `dcl-onboarding-agent`, and any supporting repos. There are no repo boundaries for bug fixes. If a test fails because the bug is in a different repo, you go fix it there. "That's a different repo" or "that's outside my scope" is not a valid reason to leave something broken. RACI governs design decisions and ownership — it does not prevent you from fixing bugs wherever they live.

---

## PLATFORM IN ONE PARAGRAPH
AutonomOS is an AI-native enterprise platform that delivers unified context for the enterprise. It sits on top of existing enterprise systems (Salesforce, SAP, AWS, etc.) without replacing them. It discovers what exists (AOD), understands how to connect (AAM), generates synthetic financial models for demo and testing (Farm), maps everything to business meaning via a semantic triple store (DCL), lets humans and AI agents query it in plain English (NLQ), and governs the agents doing work (AOA). Platform orchestrates all modules and hosts Maestra — the persistent AI engagement lead who guides operators through the entire AOS lifecycle. Maestra reasons about data through a layered constitution (Layers 0-4 + Orchestrator), performs COFA unification for M&A, and surfaces conflicts ranked by materiality for human review. The moat is the semantic data layer — not the runtime.

---

## DATA ARCHITECTURE
All data flows through semantic triples stored in Postgres. The pipeline is: AOD → AAM → Farm → triple conversion → PG direct. DCL owns the triple store and serves it to NLQ and Maestra. The old DCL pipe ingest path (Structure/Dispatch/Content) is deprecated — do not fix it.

- **Triple store:** 9,350+ financial triples + 124K BlueWave HR + COFA conflict triples. 19 verified domains.
- **Entities:** Meridian ($5B consultancy) and Cascadia ($1B BPM). Entity is a tag in the same store — no split brain.
- **Farm configs:** Only `farm_config_meridian.yaml` and `farm_config_cascadia.yaml` are valid. The old $35M toy config and `fact_base.json` are removed. Any numbers at $35M or $124M scale are broken.
- **No demo mode in live path.** Farm generates synthetic data via configs. All data enters as triples to PG. If data is missing, fail loudly — do not substitute.

---

## MODULE RACI — SUMMARY
**The authoritative RACI is `ONGOING_PROMPTS/AOS_MASTER_RACIv7.csv` (289 rows, 8 modules).** This table is a quick reference only.

| Module | Owns | Does NOT own |
|--------|------|-------------|
| **AOD** | Discovery, classification, SOR detection, Fabric Plane hints, ConnectionCandidate generation | Pipe blueprints, data extraction, semantic mapping |
| **AAM** | Pipe blueprint creation, work order dispatch, drift detection, self-healing, adapter connections | Data movement, semantic mapping, fabric plane inference |
| **DCL** | Semantic triple store, ontology, schema-on-write validation, entity resolution, v2 engine stack, COFA completion gate | Discovery, connection logic, NLQ response formatting |
| **NLQ** | Intent resolution, persona filtering, query dispatch to DCL, report portal, output rendering | Semantic mapping, data storage, agent orchestration |
| **AOA** | Agent identity, policy enforcement, HITL workflows, budget tracking, observability | Semantic mapping, discovery, NLQ query handling |
| **Farm** | Synthetic data generation, financial models (Meridian/Cascadia), test oracle, triple conversion | Production data, live connections |
| **Platform** | Maestra (constitution, context assembly, chat, tools, human review), engagement orchestration, run ledger, module coordination | Semantic catalog, query resolution, data generation |

**RACI VIOLATION = STOP AND FLAG.** If a fix requires Module A to implement logic that belongs to Module B, surface the architectural conflict.

**Exception (A12/C6):** RACI is for design decisions. If a test fails because DCL is wrong, fix DCL. If NLQ misroutes, fix NLQ. RACI is not a shield to avoid fixing bugs that make the product broken.

---

## CONVERGENCE GUARDRAIL
Convergence = base AOS plus a bridge where Target pipes join Acquirer pipes into one DCL. **Entity is a tag**, not a separate brain. Same engine, same ontology, same resolution, same query routing. Reject any proposal that:
- Creates separate engines or query paths for multi-entity
- Adds Convergence-specific columns to shared tables (use generic names like `source_run_tag`, not `convergence_run_id`)
- Introduces split brain, query-time composition, or new resolution logic for multi-entity
- Diverges from base AOS architecture for any multi-entity feature

---

## MAESTRA
Maestra is the persistent AI engagement lead. She lives in Platform (`~/code/platform`).

- **Constitution:** Layers 0-4 + Orchestrator, loaded every invocation from `app/maestra/constitution/`
- **Context assembly:** Base constitution + module doc + retrieved triples + engagement state + user message
- **Key boundaries:** Maestra reasons; DCL validates; deterministic gates own exact checks. Maestra does NOT recommend accounting resolutions — she isolates variables and presents them. No auto-resolution of conflicts — all route to human review, ranked by materiality. Batch approve supported.
- **Layer 3 (entity policies):** Manually authored for MVP. Not auto-generated.
- **COFA truth test:** Stage 5 STRONG PASS — 6/6 conflicts, 100% completeness, $1.49/engagement

---

## WHAT "DONE" MEANS
Every completed task must satisfy ALL of these:
1. **Semantics preserved** — behavior matches real-world meaning
2. **No cheating** — no silent fallbacks, no bandaids, no rationalizing bugs as expected (C9)
3. **Proof is real** — failure-before / success-after evidence, verified through the UI (B17)
4. **Negative test included** — confirm the bad behavior can't return
5. **All tests pass** — including pre-existing failures (D6). 100% or not done.
6. **No latency regression** — measure before and after (B18). Hard ceilings are absolute.
7. **No new features** — unless explicitly requested (A6). Fix what's broken, nothing more.

---

## SILENT FALLBACKS — ABSOLUTE PROHIBITION
Silent fallbacks are the most dangerous failure mode in this codebase. They make broken features look working.

**Prohibited patterns — no exceptions:**
- Catching an exception and returning empty results instead of raising
- Defaulting to demo/mock data when a real data call fails, without surfacing the failure
- `try/except` blocks that swallow errors silently
- Any code path that returns HTTP 200 when the underlying operation failed
- Logging a warning and continuing when the correct behavior is to stop and fail
- `getattr(obj, attr, 0.0)` as a default for schema-defined fields — if the schema defines it, the config must have it. Fail at startup if missing.

**If a real data source is unavailable, the system must:**
1. Return an explicit error response that names what failed, why, and what was being attempted
2. Log the failure with full context — service name, endpoint, input parameters, exception detail
3. Never substitute mock or cached data without the caller explicitly requesting it

**Error messages must be informative, not just loud.** "Connection failed" is not acceptable. "AAM could not reach DCL at http://localhost:8004/api/concepts — connection refused after 3 retries — NLQ intent resolution aborted" is acceptable.

---

## TECH STACK

| Module | Backend | Frontend | DB | Notes |
|--------|---------|----------|----|-------|
| AOD | FastAPI/Python | React 18 + Vite | Supabase PG | — |
| AAM | FastAPI/Python | Server-rendered HTML | SQLite | UI in ui_pages.py — no Vite |
| DCL | FastAPI/Python | React 18 + Vite | Supabase PG + Pinecone | 8 v2 engines on triple store |
| NLQ | FastAPI/Python | React 18 + Vite | Supabase PG | Claude (Tier 3 LLM) |
| AOA | FastAPI/Python | React 18 + Vite | Supabase PG | Placeholder — not yet built |
| Farm | FastAPI/Python | Jinja2/Tailwind | Supabase PG | Server-rendered, no Vite |
| Platform | FastAPI/Python | React 18 + Vite | Supabase PG | Hosts Maestra. Claude via AI Gateway |

**Separate repos per module.**

---

## DEPLOYMENT & ENVIRONMENTS

### Production
- **Platform:** Render (Replit is dead — do not reference it)
- **Secrets:** Render environment variables — never in .env files committed to repos
- **Build:** Each module has its own Render service

### Local Development
- **Desktop:** Windows 11, PowerShell, repos at `C:\Users\ilyac\code\`
- **Laptop:** Ubuntu (WSL), repos at `~/code/`
- **Process manager:** pm2
- **Launch:** `~/code/aos-launch.sh` (laptop) or `aos-start` (desktop)

| Service | Backend | Frontend |
|---------|---------|----------|
| AOD | 8001 | 3001 |
| AAM | 8002 | UI on 8002 |
| Farm | 8003 | UI on 8003 |
| DCL | 8004 | 3004 |
| NLQ | 8005 | 3005 |
| Platform | 8006 | 3006 |

---

## AGENT TEAM INSTRUCTIONS
- Each agent must declare which module it is working on at the start of every message
- Before proposing any cross-module change, check the RACI (CSV is authoritative, not the summary table)
- **Use Opus for all tasks** — Ilya is on the max plan. Opus is preferred for both implementation and architecture.
- All agents report RACI violations to the lead — do not silently implement workarounds
- After compaction, re-read this file from the top. Rules from early in the conversation get lost during compaction.

---

## FORBIDDEN PATTERNS
- Tests that pass while the real feature fails
- **Silent fallbacks that hide errors** — #1 most forbidden pattern
- Permissive schemas to avoid contract mismatches
- Converting errors into empty results
- Any shortcut that works in demo but breaks in production
- Band-aids, patches, workarounds (A2)
- Adding features to Module X that belong to Module Y (RACI violation)
- Normalizing bugs as expected behavior (C9) — if you said "that's wrong," fix it
- Building UI to excuse performance failures (C10) — timeouts, "still working" messages, progress spinners for latency violations
- Asking permission to do what the prompt already told you to do (C11)
- Fixing one instance of a pattern without auditing for all instances (C12)
- Claiming "pre-existing" as an excuse for unfixed failures (D6)
- Claiming "metadata only" or "we don't touch your data" — not architecturally validated
- Claiming ContextOS delivers ontology — current positioning is context through sophisticated semantics
- Any reference to Replit
- Any reference to Audax Group in a positive light

---
---

# HARNESS & CODE CHANGE RULES (v2)
## These rules are non-negotiable. They apply to every CC session, every test suite, every code change.
## This section is the authoritative copy. tests/HARNESS_RULES_v2.md in each repo should match this.

---

# SECTION A: CODE CHANGE RULES

## A1: No silent fallbacks
If something fails, it fails loudly with a clear error. Never degrade silently. Never return default/demo data when live data is unavailable. Never swallow exceptions. If a query can't be answered, say why — don't serve stale or wrong data.

## A2: No bandaids
Fundamental fixes only. If the right fix is harder, do the harder thing. Do not patch symptoms. If the root cause is in module X, fix module X — don't add a workaround in module Y.

## A3: No tech debt
Don't leave TODOs. Don't skip edge cases. Don't write code you'd want to rewrite. Don't introduce temporary hacks "to be cleaned up later."

## A4: Only fundamentally proper fixes
Shape code to solve the underlying problem, not to satisfy output appearance. If a test passes but the underlying behavior isn't what was intended, that's a failure, not a success.

## A5: A change cannot result in increased latency
Every code change must be verified to not degrade performance. If a fix adds latency, find a way to fix the issue without the latency cost. Measure before and after.

## A6: Do not introduce new features unless explicitly asked for
Fix what's broken. Don't add capabilities, endpoints, UI elements, or behaviors that weren't requested. Scope creep from agents is a recurring problem. Stay within the stated task.

## A7: If preexisting errors are found, fix them
Don't work around broken things. If you discover a bug while working on something else, fix it. Don't leave landmines for the next session.

## A8: State cross-module impact before implementing
AOS is a tightly integrated chain across all repos. Even small tweaks in one module can cause failure across the stack. Before making a change, state what other modules it could affect.

## A9: fact_base.json is demo mode only
Never fall back to fact_base.json in live mode. Fail loudly instead. fact_base is ONLY served when the user explicitly selects demo mode. In live/ingest mode, if no data is available, return an error explaining what's missing.

## A10: Farm is the sole authority for tenant_id generation
All other modules are consumers. No module creates its own tenant_id. AAM is the sole authority for connection mapping. DCL is the sole authority for semantic resolution. NLQ is not modified for pipeline data issues. Respect RACI boundaries for design decisions.

## A11: Read CLAUDE.md and ONGOING_PROMPTS in the repo root before starting
They contain repo-specific rules that supplement these.

## A12: You own all repos — no hiding behind RACI to leave things broken
If a test fails because DCL is wrong, fix DCL. If NLQ misroutes, fix NLQ. If Farm generates wrong scale, fix Farm. RACI describes ownership for design decisions — it is not a shield to avoid fixing bugs that make the product broken. The test must pass. Fix whatever is broken, wherever it lives.

---

# SECTION B: HARNESS TESTING RULES

## B1: Tests must test what the USER sees, not internal endpoints
Testing DCL directly is a unit test. The harness tests the product — through NLQ's /api/v1/query endpoint, the dashboard widget path, the report portal path. A correct DCL response that never reaches the user is not a pass.

## B2: Tests hit /api/v1/query with natural language
What the user types, not internal endpoints. Never test through /api/dcl/query directly for user-facing validation. The question in the test should be what a real user would type in the Ask tab.

## B3: No weakening assertions to make tests pass
If a test fails, fix the system — not the test. Do not change `greater_than: 500` to `greater_than: 0`. The expected value is the spec. Fix the system.

## B4: No passing on technicality
If a test asserts `data_source != "fact_base"` and the actual value is None, that is NOT a pass. Every test must assert the positive expected outcome. "Source is dcl" is a real assertion. "Source is not fact_base" is incomplete.

## B5: No test-only endpoints or mode-set backdoors
If the test requires DCL to be in Ingest mode, data must be actually ingested through the real pipeline — not faked via a test-only shortcut.

## B6: No cross-repo Python imports in tests
DCL tests hit DCL via HTTP. NLQ tests hit NLQ via HTTP. No `from src.nlq...` in DCL test files.

## B7: Tests must be run, not just created
Building test infrastructure and declaring done without executing is prohibited. Every test file must be run. The output must be shown.

## B8: No hardcoded expected values that match current wrong output
If the system returns $233M (wrong scale), do not write a test that asserts `value > 200`. Expected values come from the spec and Farm ground truth.

## B9: Demo data does not count as a pass
A test that passes against fact_base.json or any demo/seed data is NOT a valid pass in live mode. The harness must verify data_source="dcl" or source="Ingest" on every response.

## B10: Ground truth comes from Farm's API at runtime, not hardcoded
Reconciliation tests fetch expected values from Farm's ground truth endpoint at test runtime. Do not hardcode expected values.

## B11: If the UI is broken and no test catches it, add a test
The harness is never done. Every screenshot of broken behavior must map to a failing test.

## B12: Source field must be checked on EVERY data test
Every test that checks a returned value must ALSO check the source/data_source field. A correct number from the wrong source is not a pass.

## B13: Every failure shows what the user would see
Test output must print: "User asked 'what is revenue for Q1 2025'. Expected: >$500M from DCL. Got: $0 from source=Local." Not just "assertion failed."

## B14: Run the harness twice — results must be identical
Non-deterministic tests are bugs in the harness, not in the system. Fix the test to check deterministic outputs.

## B15: The pipeline must run before the harness
The harness is only valid after a fresh pipeline run. Verify pipeline freshness before executing any tests.

## B16: No caching or stale data that makes tests pass
Every test hits the live system fresh. No memoization, no response caching.

## B17: Frontend is the pass/fail gate
Backend queries and API responses are diagnostic tools, not proof of correctness. The UI rendering the correct data in the browser is the real test. Open the browser, look at the screen, verify what the user would see.

## B18: 5% latency budget
Measure response time before and after every code change. More than 5% regression on any endpoint is a blocking issue. Hard latency ceilings stated in prompts are absolute and non-negotiable.

---

# SECTION C: ANTI-CHEAT RULES

## C1: No creating lightweight/test-only endpoints to fake system state
Agents create mode-set backdoors or test-only data endpoints to make tests pass without running the real pipeline. Prohibited.

## C2: No building test infrastructure and declaring done without running it
Agents build the test file, the runner, the YAML — and then report "done" without executing. Always require execution output.

## C3: No faking API key errors, creating in-memory test data, or testing at wrong abstraction layer
All prohibited. Tests go through HTTP endpoints against live services.

## C4: No passing tests on technicality where None counts as "not bad"
Tests must assert the positive expected value: `source == "dcl"`, not `source != "fact_base"`.

## C5: No matching expected values to current wrong output
Expected values come from the spec, not from the system's current wrong output.

## C6: No hiding behind RACI to avoid fixing bugs
RACI is for design decisions. Fix whatever is broken across all repos.

## C7: No declaring "verified" without testing through the running application
Verification means: pipeline runs, harness passes, and the UI shows correct data.

## C8: No test-only mode manipulation
The harness runs against real pipeline state. No synthetic mode manipulation.

## C9: No normalizing bugs as expected behavior
If you identify a problem and then rationalize not fixing it — "the tests just need it to not crash" or "that's in the expected range" — that is cheating. If you said "that's wrong," fix it. If 4 of 5 iterations fail, there is a bug. Diagnose and fix, do not rationalize.

## C10: No building UI to excuse performance failures
If an operation violates a latency ceiling, the fix is to make it faster — not to add a progress spinner, a "still working" message, or a "large datasets take longer" disclaimer. Fix the performance. The ceiling must be met first.

## C11: No asking "want me to fix it?" when the prompt says to fix all bugs
If the prompt says fix it, fix it. Do not ask for permission. That is stalling. The only time to ask is when a fix requires an architectural decision the prompt doesn't cover.

## C12: No piecemeal discovery of the same bug pattern
If you find a hardcoded value that should be dynamic, do not fix that one instance and rerun. Audit the entire codebase for the same pattern first. One grep, one audit, one fix pass.

---

# SECTION D: HARNESS EXECUTION FORMAT

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
All tests must pass at the end of your session — including tests that were failing before you started. If a test was already broken, fix it. If a service isn't running, start it. "That was already failing" is not an acceptable status. You are responsible for the state of the system when you hand back control, not just the delta of your changes.

---

# SECTION E: COMPLIANCE CHECKLIST

After every harness run, verify:
1. Is DCL in Ingest mode? (not Demo)
2. Does every passing test show source=dcl or source=Ingest? (not fact_base, Local, or null)
3. Did the pipeline run before the harness? (check run_id freshness)
4. Does the UI actually work? (open the browser and verify)
5. Run the harness a second time — same results?
6. Did latency increase? (compare before/after)
7. Were any new features introduced that weren't requested?

If any answer is wrong, the harness result is invalid.

---

# SECTION F: AUTOMATED GUARDS

## F1: Pre-Commit Hook
A pre-commit hook is installed in repos at `.git/hooks/pre-commit`. It blocks commits containing:
- Bare `except: pass` or `except: continue`
- Except blocks that return literal defaults (0, [], {}, None, False, "")
- Hardcoded entity names ("meridian", "cascadia") in application code
- Hardcoded seed UUIDs (400aa910, 6754a9d7)
- References to fact_base.json

Do not use `git commit --no-verify` to bypass. Bypassing will be caught in audit.
