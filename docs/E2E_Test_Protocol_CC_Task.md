# End-to-End Test Protocol — CC Execution

## Your Job

You are running the full integration test protocol. You start the services, hit the endpoints, validate responses, and report results. If something fails, diagnose and fix it. Don't hand anything back to the human until all 3 levels pass or you've identified a blocker you can't resolve.

## Prerequisites

Before starting tests, get the services running:

1. **DCL** — Start the DCL backend. Find the start command (likely `python -m backend.api.main` or `uvicorn backend.api.main:app`). Confirm it's listening by hitting its health/root endpoint. Note the URL and port.

2. **NLQ** — Start the NLQ backend in a separate process. Find the start command. Confirm it's listening. Note the URL and port.

3. **AAM** — If available, start it. If not, note it and continue — DCL should degrade gracefully.

4. **Onboarding Agent** — If available, start it. If not, Level 3 uses sample contour data.

If any service fails to start, diagnose and fix the startup error before proceeding. Common issues: missing env vars, port conflicts, missing dependencies. Fix them.

---

## Level 1: Component Tests

Run each module's test suite. Stop and fix if anything fails.

### DCL
```bash
cd /path/to/DCLv2
python -m pytest tests/ -v --tb=short
```
All tests must pass. If any fail, fix them and re-run before proceeding.

### NLQ
```bash
cd /path/to/AOS-NLQ
python -m pytest tests/ -v --tb=short
```
All tests must pass.

### AAM (if running)
```bash
cd /path/to/AAM
python -m pytest tests/ -v --tb=short
```

Report: pass count / total for each module.

---

## Level 2: Cross-Module Integration

### Test 2A: Graph Health

Hit `GET {DCL_URL}/api/dcl/graph/stats`.

Validate the response contains:
- concept nodes > 0
- dimension nodes > 0
- system nodes > 0
- field nodes > 0
- CLASSIFIED_AS edges > 0
- LIVES_IN edges > 0
- SLICEABLE_BY edges > 0
- HIERARCHY_PARENT edges > 0
- AUTHORITATIVE_FOR edges > 0

If MAPS_TO edges = 0, note that AAM edges are missing (acceptable if AAM isn't running). If any of the required edge types = 0, the graph is broken — diagnose and fix.

If the graph is empty because the normalizer hasn't run, trigger a classification run first (`POST {DCL_URL}/api/dcl/run` with appropriate payload), wait for it to complete, then check stats again.

### Test 2B: DCL Resolve — 10 Queries

Send each of these to `POST {DCL_URL}/api/dcl/resolve`. Validate the response.

**Query 1: Single concept — total revenue**
```json
{"concepts": ["revenue"], "dimensions": [], "filters": [], "persona": "CFO"}
```
Expect: `can_answer: true`, confidence >= 0.7

**Query 2: Concept + dimension — revenue by region**
```json
{"concepts": ["revenue"], "dimensions": ["region"], "filters": [], "persona": "CFO"}
```
Expect: `can_answer: true`, confidence >= 0.7

**Query 3: Cross-system — revenue by cost center for Cloud**
```json
{"concepts": ["revenue"], "dimensions": ["cost_center", "division"], "filters": [{"dimension": "division", "operator": "equals", "value": "Cloud"}], "persona": "CFO"}
```
Expect: `can_answer: true`, confidence >= 0.5, filter resolved to include "Cloud East" and "Cloud West"

**Query 4: Invalid combination — sprint by profit center**
```json
{"concepts": ["sprint"], "dimensions": ["profit_center"], "filters": [], "persona": "CTO"}
```
Expect: `can_answer: false`, reason mentions "cannot" or "invalid" or "not valid"

**Query 5: Hierarchy drill-down — headcount for Engineering**
```json
{"concepts": ["headcount"], "dimensions": ["cost_center"], "filters": [{"dimension": "cost_center", "operator": "equals", "value": "Engineering"}], "persona": "CHRO"}
```
Expect: `can_answer: true`, confidence >= 0.7, filter resolved to include "Cloud Engineering" and "Platform Engineering"

**Query 6: Management overlay — revenue by board segment**
```json
{"concepts": ["revenue"], "dimensions": ["division"], "filters": [], "persona": "CFO", "metadata": {"use_management_overlay": true}}
```
Expect: `can_answer: true`, confidence >= 0.7

**Query 7: Multi-concept — revenue and headcount by department**
```json
{"concepts": ["revenue", "headcount"], "dimensions": ["department"], "filters": [], "persona": "CFO"}
```
Expect: `can_answer: true`, confidence >= 0.5

**Query 8: SOR authority — employees by department**
```json
{"concepts": ["employee"], "dimensions": ["department"], "filters": [], "persona": "CHRO"}
```
Expect: `can_answer: true`, confidence >= 0.8, provenance mentions "authoritative" or "Workday"

**Query 9: Revenue by cost center (basic cross-system)**
```json
{"concepts": ["revenue"], "dimensions": ["cost_center"], "filters": [], "persona": "CFO"}
```
Expect: `can_answer: true`, confidence >= 0.5

**Query 10: Unknown concept — florbatz by region**
```json
{"concepts": ["florbatz"], "dimensions": ["region"], "filters": [], "persona": "CFO"}
```
Expect: `can_answer: false`, reason mentions "not recognized" or "unknown"

**If a query fails:**
- Check the error response for clues
- Check DCL logs for the traceback
- Common issues: concept alias mismatch (ontology uses different name than test), dimension name mismatch, sample contour missing hierarchy nodes, endpoint payload shape mismatch
- Fix the issue, re-run the failing query
- Commit the fix

### Test 2C: NLQ Natural Language → Graph — 8 Queries

Send natural language questions through NLQ's actual query endpoint. Find NLQ's query endpoint (likely `POST /api/v1/query` or similar — check the router).

**E2E-1:** "What is total revenue?"
Expect: graph path used, confidence >= 0.7

**E2E-2:** "Show me revenue by region"
Expect: graph path used, confidence >= 0.7

**E2E-3:** "What is revenue for North America?"
Expect: graph path used, response includes US/Canada or hierarchy expansion

**E2E-4:** "Revenue by department"
Expect: graph path used, cross-system join noted, confidence >= 0.5

**E2E-5:** "Revenue by board segment"
Expect: graph path used, management overlay resolved

**E2E-6:** "Revenue and headcount by department"
Expect: graph path used, multiple concepts resolved, confidence >= 0.5

**E2E-7:** "Sprint velocity by profit center"
Expect: graph path used, can_answer = false, clean rejection message

**E2E-8:** "What is revenue by cost center for the Cloud division?"
Expect: graph path used, cross-system join, filter resolution (Cloud → Cloud East + Cloud West), confidence >= 0.5

**If a query uses flat fallback when it should use graph:**
- NLQ's intent parser may not be producing the right concept/dimension names
- Check what NLQ sends to DCL resolve vs what DCL expects
- The concept names in NLQ's parser must match DCL's 107-concept ontology aliases
- Fix the mapping, re-run

**If NLQ can't reach DCL:**
- Check the DCL URL NLQ is configured with
- Check for CORS issues
- Check NLQ's DCL client configuration

---

## Level 3: Full Scenario Simulation

### If Onboarding Agent Is Running

1. Create a session:
```
POST {AGENT_URL}/api/sessions
{"customer_name": "Test Corp", "stakeholder_name": "Jane Smith", "stakeholder_role": "VP Finance"}
```

2. Send these scripted stakeholder messages one at a time, waiting for the agent's response after each:

Message 1: "We have three divisions: Cloud, Professional Services, and Platform. Cloud is the biggest, about 60% of revenue."

Message 2: "NetSuite is our ERP, Workday for HR, Salesforce for CRM. NetSuite is source of truth for financials, Workday for org structure."

Message 3: "Yes, those 12 cost centers look right. Engineering has Cloud Engineering and Platform Engineering under it. Sales has Enterprise and Mid-Market."

Message 4: "The regions are correct. North America is US and Canada. EMEA is UK and Germany."

Message 5: "The board sees three segments: Cloud, Services, and Platform. Cloud on the board deck combines Cloud East and Cloud West from Workday."

Message 6: "The biggest pain is revenue by cost center. It takes two weeks to reconcile because NetSuite and Workday don't agree on cost center codes."

3. Get the contour map: `GET {AGENT_URL}/api/sessions/{id}/contour`

4. Submit to DCL (if DCL has a contour map ingestion endpoint) or note the contour map completeness.

5. Rebuild DCL graph and re-run the boss query.

### If Agent Is NOT Running

Skip to priority queries using sample contour data (already loaded in DCL).

### Priority Queries — The Demo Proof

These are the queries a real stakeholder would ask. Run them against DCL resolve:

**Pain Point 1:** Revenue by cost center
```json
{"concepts": ["revenue"], "dimensions": ["cost_center"], "filters": [], "persona": "CFO"}
```
Must return: can_answer true, provenance showing cross-system path

**Pain Point 2:** Headcount by department
```json
{"concepts": ["headcount"], "dimensions": ["department"], "filters": [], "persona": "CHRO"}
```
Must return: can_answer true, Workday as authoritative source

**The Boss Query:** Revenue by cost center for Cloud division
```json
{"concepts": ["revenue"], "dimensions": ["cost_center", "division"], "filters": [{"dimension": "division", "operator": "equals", "value": "Cloud"}], "persona": "CFO"}
```
Must return: can_answer true, cross-system join path, Cloud resolved to Cloud East + Cloud West, confidence > 0.5, full provenance chain

---

## Reporting

After all 3 levels, produce a summary table:

```
LEVEL 1: COMPONENT TESTS
  DCL:  [X/Y passed]
  NLQ:  [X/Y passed]
  AAM:  [X/Y passed] or [not running]

LEVEL 2: CROSS-MODULE INTEGRATION
  Graph health:     [PASS/FAIL] — [node counts, edge counts]
  DCL resolve:      [X/10 passed]
  NLQ E2E:          [X/8 passed]

LEVEL 3: FULL SCENARIO
  Agent simulation: [completed/skipped]
  Pain Point 1:     [PASS/FAIL] — confidence X.XX
  Pain Point 2:     [PASS/FAIL] — confidence X.XX
  Boss Query:       [PASS/FAIL] — confidence X.XX

OVERALL: [PASS/FAIL]
```

For any failures, include: the query, expected result, actual result, root cause, and what you did to fix it (or why it's a blocker you can't resolve).

---

## Rules

- Fix issues as you find them. Don't just report failures — diagnose and fix.
- Commit each fix with a descriptive message.
- If a fix in one module breaks another, catch it and fix the cascade.
- If you can't fix something (e.g., requires an API key you don't have, or a service on a different machine), report it as a blocker with the specific error.
- Don't modify the test expectations to make them pass. Fix the code.
- The boss query must pass. That's the non-negotiable.
