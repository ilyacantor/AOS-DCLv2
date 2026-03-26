# HARNESS & CODE CHANGE RULES (v2)
> Deploy to `tests/HARNESS_RULES_v2.md` in every AOS repo.
> These rules are non-negotiable. They apply to every CC session, every test suite, every code change.

---

# SECTION A: CODE CHANGE RULES

## A1: No silent fallbacks
If something fails, it fails loudly with a clear error. Never degrade silently. Never return default/demo data when live data is unavailable. Never swallow exceptions.

## A2: No bandaids
Fundamental fixes only. If the root cause is in module X, fix module X — don't add a workaround in module Y.

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
AOS is a tightly integrated chain. Before making a change, state what other modules it could affect.

## A9: fact_base.json is dead
Never fall back to fact_base.json. It is removed. Any reference to it, any fallback to it, any test against it is broken.

## A10: Respect module authority
Farm owns tenant_id generation. AAM owns connection mapping. DCL owns semantic resolution. NLQ is not modified for pipeline data issues. Respect RACI boundaries for design decisions.

## A11: Read CLAUDE.md before starting
It contains repo-specific rules that supplement these.

## A12: You own all repos for bug fixes
RACI describes ownership for design decisions. It is not a shield to avoid fixing bugs. If a test fails because DCL is wrong, fix DCL.

---

# SECTION B: HARNESS TESTING RULES

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
Tests hit services via HTTP. No `from src.nlq...` in DCL test files.

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

# SECTION C: ANTI-CHEAT RULES

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

# SECTION D: EXECUTION RULES

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

# SECTION E: COMPLIANCE CHECKLIST

After every harness run, verify:
1. Does every passing test show source=dcl or source=Ingest?
2. Did the pipeline run before the harness?
3. Does the UI actually work? (open the browser and verify — B17)
4. Run the harness a second time — same results?
5. Did latency increase? (compare before/after)
6. Were any new features introduced that weren't requested?

If any answer is wrong, the harness result is invalid.

---

# SECTION F: AUTOMATED GUARDS

## F1: Pre-Commit Hook
Installed at `.git/hooks/pre-commit`. Blocks commits containing:
- Bare `except: pass` or `except: continue`
- Except blocks that return literal defaults (0, [], {}, None, False, "")
- Hardcoded entity names ("meridian", "cascadia") in application code
- Hardcoded seed UUIDs (400aa910, 6754a9d7)
- References to fact_base.json

Do not bypass with `--no-verify` (C13).
