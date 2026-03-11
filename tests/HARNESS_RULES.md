# HARNESS & CODE CHANGE RULES
# ============================
# This file is the single source of truth for all testing, harness, and code change rules.
# Every CC prompt, every test suite, every code change must include or reference these rules.
# They are non-negotiable. Copy this file into tests/HARNESS_RULES.md in every repo.

---

# SECTION A: CODE CHANGE RULES (apply to ALL code changes, not just testing)

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

# SECTION B: HARNESS TESTING RULES (apply to all test suites)

## B1: Tests must test what the USER sees, not internal endpoints
Testing DCL directly is a unit test. The harness tests the product — through NLQ's /api/v1/query endpoint, the dashboard widget path, the report portal path. A correct DCL response that never reaches the user is not a pass. The user doesn't interact with DCL directly — they interact with NLQ. Test NLQ.

## B2: Tests hit /api/v1/query with natural language
What the user types, not internal endpoints. Never test through /api/dcl/query directly for user-facing validation (DCL direct queries are only for cross-checks). The question in the test should be what a real user would type in the Ask tab.

## B3: No weakening assertions to make tests pass
If a test fails, fix the system — not the test. Do not change `greater_than: 500` to `greater_than: 0`. Do not change `not_contains: "stumped"` to `not_contains: "fatal_error"`. The expected value is the spec. Fix the system.

## B4: No passing on technicality
If a test asserts `data_source != "fact_base"` and the actual value is None, that is NOT a pass. Every test must assert the positive expected outcome, not just the absence of a bad outcome. "Source is dcl" is a real assertion. "Source is not fact_base" is incomplete — it passes when source is None, null, empty string, or anything else that's also wrong.

## B5: No test-only endpoints or mode-set backdoors
If the test requires DCL to be in Ingest mode, data must be actually ingested through the real pipeline — not faked via a `POST /api/dcl/mode` endpoint or any other test-only shortcut. Tests must validate real system state, not manufactured state. If an endpoint exists only because a test needs it, delete it.

## B6: No cross-repo Python imports in tests
DCL tests hit DCL via HTTP. NLQ tests hit NLQ via HTTP. No `from src.nlq...` in DCL test files. No `from backend...` in NLQ test files. If a test needs data from another service, it queries the HTTP API.

## B7: Tests must be run, not just created
Building test infrastructure and declaring done without executing is prohibited. Every test file must be run. The output must be shown. "74/74 pass" means nothing if the product is broken — show the output alongside a manual verification that the product works.

## B8: No hardcoded expected values that match current wrong output
If the system returns $233M (wrong scale), do not write a test that asserts `value > 200`. The test should assert `value > 500` (correct Meridian quarterly scale). Expected values come from the spec and Farm ground truth — not from whatever the system happens to return today.

## B9: Demo data does not count as a pass
A test that passes against fact_base.json or any demo/seed data is NOT a valid pass in live mode. The harness must verify data_source="dcl" or source="Ingest" on every response. If DCL is in Demo mode, the entire harness result is INVALID. Pipeline must run first, DCL must be in Ingest mode with real Farm-generated data, and every query must return data from the ingest store — not fact_base.

## B10: Ground truth comes from Farm's API at runtime, not hardcoded
Reconciliation tests fetch expected values from Farm's ground truth endpoint at test runtime. Do not hardcode `expected: 1323.43`. If Farm regenerates with a different seed, expected values should automatically update because they're fetched live.

## B11: If the UI is broken and no test catches it, add a test
The harness is never done. Every screenshot of broken behavior must map to a failing test. If the test passes while the UI shows errors, the test is testing the wrong thing.

## B12: Source field must be checked on EVERY data test
Every test that checks a returned value must ALSO check the source/data_source field. A correct number from the wrong source (fact_base returning $1,323M because it happens to match) is not a pass. The source must be "dcl", "Ingest", or "ingest" — never "fact_base", "Local", "demo", or null.

## B13: Every failure shows what the user would see
Test output must print: "User asked 'what is revenue for Q1 2025'. Expected: >$500M from DCL. Got: $0 from source=Local. The user sees an all-zeros P&L." Not just "assertion failed."

## B14: Run the harness twice — results must be identical
Non-deterministic tests (flaky passes/fails between runs) are bugs in the harness, not in the system. If a test passes on run 1 and fails on run 2 with no system changes, the test is testing LLM non-determinism. Fix the test to check deterministic outputs (tool calls, values) not LLM prose.

## B15: The pipeline must run before the harness
The harness is only valid after a fresh pipeline run (`--entities=meridian,cascadia`). Running the harness against stale data, demo data, or a cold system is not a valid test. The harness runner should verify pipeline freshness (check run_id timestamp, verify DCL is in Ingest mode) before executing any tests.

## B16: No caching or stale data that makes tests pass
If a test passes because NLQ cached a previous correct response, but a fresh query would fail, the test is meaningless. Every test hits the live system fresh. No memoization, no response caching, no "it worked last time so skip."

---

# SECTION C: ANTI-CHEAT RULES (specific agent cheat patterns we've caught)

## C1: No creating lightweight/test-only endpoints to fake system state
Agents create mode-set backdoors, test-only data endpoints, or lightweight configuration endpoints to make tests pass without running the real pipeline. If the test requires DCL to be in Ingest mode, data must be actually ingested — not faked via a test endpoint.

## C2: No building test infrastructure and declaring done without running it
Agents frequently build the test file, the runner, the YAML — and then report "done" without executing. Always require execution output and actual results.

## C3: No faking API key errors, creating in-memory test data, or testing at wrong abstraction layer
Agents fake API key errors to skip real API calls, create in-memory test data that bypasses the pipeline, and test internal functions instead of HTTP endpoints. All prohibited.

## C4: No passing tests on technicality where None counts as "not bad"
Agents write assertions like `source != "fact_base"` which pass when source is None. Tests must assert the positive expected value: `source == "dcl"`.

## C5: No matching expected values to current wrong output
Agents observe the system returns $233M and write tests asserting `value > 200`. The expected values come from the spec ($5B annual = ~$1.25B/quarter), not from the system's current (wrong) output.

## C6: No hiding behind RACI to avoid fixing bugs
Agents report "this is an NLQ-scope issue" and stop, leaving the product broken. RACI is for design decisions. If the user sees a broken product, fix whatever is broken across all repos.

## C7: No declaring "verified" without testing through the running application
Agents frequently claim fixes are "verified" based on code review or unit-level checks without actually running the full pipeline and testing through the UI. Verification means: pipeline runs, harness passes, and the UI shows correct data.

## C8: No test-only mode manipulation (specific to fact_base gating)
The fact_base gate tests must run after a real pipeline ingest — the same pipeline that the E2E script triggers. The harness runs in order: pipeline generates and ingests, then gate tests verify against real ingest state. No synthetic mode manipulation.

---

# SECTION D: HARNESS EXECUTION FORMAT

## D1: Test output format
Print [PASS] or [FAIL] per test with expected vs got on failures. Show what the user would see when a test fails.

## D2: Verify health first
Check DCL + NLQ health before running any tests. If services are down, fail immediately with a clear message.

## D3: Run ALL suites + regression every time
No partial runs. Every harness execution runs all test suites plus all regression tests. Any failure in any suite means the run is not done.

## D4: Loop until 100% pass
Agent fixes app code, reruns all tests. Repeat until 100% pass. No partial pass. Tests cannot be modified, skipped, or marked xfail. Agent fixes app code only.

## D5: All tests rerun on any failure
If one test fails and the fix touches shared code, all tests must rerun. No "just rerun the failed test."

---

# SECTION E: HOW TO INCLUDE THESE RULES

Add this block to every CC prompt that involves testing or code changes:

```
Read tests/HARNESS_RULES.md before starting any work.
All rules in Sections A through D are non-negotiable. Violations are bugs.
```

## Compliance checklist (verify after every harness run):
1. Is DCL in Ingest mode? (not Demo)
2. Does every passing test show source=dcl or source=Ingest? (not fact_base, Local, or null)
3. Did the pipeline run before the harness? (check run_id freshness)
4. Does the UI actually work? (open the browser and verify)
5. Run the harness a second time — same results?
6. Did latency increase? (compare before/after)
7. Were any new features introduced that weren't requested?

If any answer is wrong, the harness result is invalid.
