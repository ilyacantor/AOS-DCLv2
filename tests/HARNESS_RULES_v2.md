# HARNESS_RULES.md — ADDITIONS
# Paste these into the appropriate sections of the existing file.

# ─────────────────────────────────────────────────
# Add to SECTION B (after B16):
# ─────────────────────────────────────────────────

## B17: Frontend is the pass/fail gate
Backend queries and API responses are diagnostic tools, not proof of correctness. The UI rendering the correct data in the browser is the real test. A correct API response that doesn't render is not a pass. A correct DCL response that NLQ never surfaces to the user is not a pass. Open the browser, look at the screen, verify what the user would see.

## B18: 5% latency budget
Measure response time before and after every code change. More than 5% regression on any endpoint is a blocking issue — fix it before moving on. Hard latency ceilings stated in prompts (e.g., "≤ 130 seconds") are absolute and non-negotiable. If a change causes a latency regression, find a way to fix the issue without the latency cost.

# ─────────────────────────────────────────────────
# Add to SECTION C (after C8):
# ─────────────────────────────────────────────────

## C9: No normalizing bugs as expected behavior
If you identify a problem — "That's wrong, Consulting should get a big chunk" — and then rationalize not fixing it — "the tests just need it to not crash" or "the fallback path handles it properly" or "that's in the expected range" — that is cheating. If you said "that's wrong," fix it. If 4 of 5 iterations fail, there is a bug. "Expected range" does not include majority failure. Diagnose and fix, do not rationalize.

## C10: No building UI to excuse performance failures
If an operation violates a latency ceiling, the fix is to make it faster — not to add a progress spinner, a "still working" message, or a "large datasets take longer" disclaimer. Comfort messages for latency violations are bandaids (A2 violation). Fix the performance. Then add the progress indicator if you want, but the ceiling must be met first.

## C11: No asking "want me to fix it?" when the prompt says to fix all bugs
If the prompt says "fix all bugs" or "loop until 100% pass," do not ask for permission to fix individual bugs. That is stalling. Fix them. The only time to ask is when a fix requires an architectural decision that the prompt doesn't cover.

## C12: No piecemeal discovery of the same bug pattern
If you find a hardcoded value that should be dynamic, do not fix that one instance and rerun. First, audit the entire codebase for the same pattern. One grep, one audit, one fix pass. The fix-run-discover-fix loop wastes cycles and produces partial fixes. After discovering the first instance of a pattern, assume it exists everywhere and find all instances before writing any code.

# ─────────────────────────────────────────────────
# Add to SECTION D (after D5):
# ─────────────────────────────────────────────────

## D6: Pre-existing failures are not excuses
All tests must pass at the end of your session — including tests that were failing before you started. If a test was already broken, fix it. If a service isn't running, start it. "That was already failing" is not an acceptable status. Your session ends with 100% pass across all repos touched, or it doesn't end. You are responsible for the state of the system when you hand back control, not just the delta of your changes.
