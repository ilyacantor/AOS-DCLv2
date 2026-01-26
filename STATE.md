# Engineering Loop State

**Last Updated:** 2026-01-26T20:55:00Z
**Last Updated By:** Claude Code (ACT - INV-003 spec refined)

---

## Current Stage

```
STAGE: ACT
```

Valid stages: `OBSERVE` | `DIAGNOSE` | `PLAN` | `ACT` | `REFLECT` | `CHECKPOINT` | `IDLE`

---

## Active Violation

```yaml
violation_id: V-2026-01-26-002
invariant: INV-003
observed: "row_count=0 but aggregations non-empty for queries 1,3"
expected: "if row_count=0 then aggregations must be empty"
canary_query: "What was revenue last year?, What is our burn rate?"
status: UNDER_INVESTIGATION
```

### OBSERVE Findings

```yaml
observe_id: obs_2026-01-26T20:45:00Z
violation_reproduced: true
code_path_traced: true

data_flow:
  1_bll_executor: "backend/bll/executor.py - _compute_summary() generates aggregations regardless of row count"
  2_nlq_endpoint: "backend/api/main.py:704-756 - SCALAR intent handling"
  3_scalar_intent: "When output_shape=SCALAR (line 704), scalar_data=[] (line 749) but summary/aggregations preserved"
  4_row_count: "Line 786: row_count = len(response_data) = len([]) = 0"
  5_aggregations: "Line 793: summary = result.summary.model_dump() - includes non-empty aggregations"

key_finding: |
  This is INTENTIONAL DESIGN, not a bug. SCALAR intent queries (e.g., "What was revenue last year?",
  "What is our burn rate?") are designed to return aggregate totals WITHOUT row data.

  The code explicitly comments (line 747-748):
    "For SCALAR intent: suppress rows, return only the aggregate
     This is the BINDING enforcement"

  The invariant INV-003 ("if row_count=0 then aggregations must be empty") is TOO STRICT
  for SCALAR intent queries where the aggregation IS the answer.

code_references:
  - "backend/api/main.py:704 - is_scalar_intent detection"
  - "backend/api/main.py:749 - scalar_data = [] (intentional)"
  - "backend/api/main.py:786 - row_count = len(response_data)"
  - "backend/api/main.py:793 - summary preserved with aggregations"
  - "backend/nlq/normalized_intent.py:171 - OutputShape.SCALAR enum"
  - "backend/nlq/normalized_intent.py:1140-1163 - validate_output_against_intent for SCALAR"

classification: SPEC_ISSUE (invariant too strict, not code bug)

options:
  A_refine_invariant: |
    Change INV-003 to: "if row_count=0 AND output_shape != SCALAR, then aggregations must be empty"
    This preserves SCALAR query behavior while still catching genuine "no data" errors.
  B_add_scalar_marker: |
    For SCALAR queries, set row_count=1 (representing the aggregate result) instead of 0,
    or add is_scalar_result=true flag to response metadata.
  C_accept_as_designed: |
    Document that INV-003 does not apply to SCALAR intent queries; this is expected behavior.
```

### Resolved Violations (this cycle)

```yaml
- violation_id: V-2026-01-26-001
  invariant: INV-001
  status: RESOLVED
  fix_commit: 8034bc5
```

---

## Diagnosis (if in DIAGNOSE or later)

```yaml
diagnosis_id: diag_2026-01-26T20:50:00Z
component: STAGE_PROMPTS.md (invariant specification)
root_cause_hypothesis: |
  INV-003 is specified as: "if row_count=0 then aggregations must be empty"

  This invariant doesn't account for SCALAR intent queries where:
  - row_count=0 is intentional (no row data for scalar results)
  - aggregations contain the actual answer (population_total, etc.)

  The invariant needs refinement to exclude SCALAR output_shape from this check.

evidence:
  - "SCALAR intent queries intentionally return row_count=0 with aggregations (main.py:747-749)"
  - "output_shape is available in response metadata (main.py:791)"
  - "INV-003 definition in STAGE_PROMPTS.md doesn't account for output_shape"
confidence_in_diagnosis: 1.0
estimated_complexity: low
human_decision: "Option A - Refine invariant to exclude SCALAR queries"
```

---

## Plan (if in PLAN or later)

```yaml
plan_id: plan_2026-01-26T20:50:00Z
diagnosis_id: diag_2026-01-26T20:50:00Z
strategy: |
  Update INV-003 specification in STAGE_PROMPTS.md to exclude SCALAR intent queries.

  Change from:
    "INV-003: if row_count = 0, aggregations must be empty"

  To:
    "INV-003: if row_count = 0 AND output_shape != SCALAR, aggregations must be empty"

  This preserves the invariant's intent (catching genuine "no data" errors) while
  correctly exempting SCALAR queries where row_count=0 with aggregations is expected.

files_to_modify:
  - path: STAGE_PROMPTS.md
    change: "Update INV-003 definition at lines 36, 289 to include output_shape exception"
success_criteria:
  - "INV-003 definition updated in all occurrences"
  - "SCALAR intent queries no longer flagged as violations"
  - "Non-SCALAR queries with row_count=0 and non-empty aggregations still flagged"
risk_level: low
risk_factors:
  files: 1
  lines_estimate: 2
  component_type: documentation/specification
rollback_steps:
  - "git checkout STAGE_PROMPTS.md"
requires_human_approval: false
```

---

## Act (if in ACT or later)

```yaml
act_id: act_2026-01-26T20:55:00Z
plan_id: plan_2026-01-26T20:50:00Z
changes_made:
  - file: STAGE_PROMPTS.md
    line: 36
    change: "Added 'AND output_shape != SCALAR' to INV-003 definition"
  - file: STAGE_PROMPTS.md
    line: 289
    change: "Added '(unless output_shape=SCALAR)' to INV-003 shorthand"
deviations_from_plan: []
```

---

## Reflect (if in REFLECT or later)

```yaml
# Cleared for new cycle - INV-003 investigation
```

---

## Session History

| Timestamp | Stage | Action | Result |
|-----------|-------|--------|--------|
| 2026-01-26T10:00:00Z | SETUP | Initial state file created | Ready for OBSERVE |
| 2026-01-26T18:40:00Z | OBSERVE | Ran 4 canary queries | 2 invariant violations found (INV-001, INV-003) |
| 2026-01-26T19:15:00Z | DIAGNOSE | Traced INV-001 to intent_matcher.py:510 | Root cause: unbounded score assigned to confidence without clamping |
| 2026-01-26T19:30:00Z | PLAN | Designed fix: clamp confidence at line 510 | risk=low, 1 file, 1 line change, no approval needed |
| 2026-01-26T19:45:00Z | ACT | Implemented 1-line fix at intent_matcher.py:510 | confidence clamped to [0.0, 1.0], 0 deviations |
| 2026-01-26T20:15:00Z | REFLECT | Verified fix with canary queries and test suite | INV-001 RESOLVED, 185 tests pass, 12 pre-existing failures, outcome=SUCCESS |
| 2026-01-26T20:30:00Z | CHECKPOINT | Compiled summary, assessed health | INV-001 closed, INV-003 remains, recommend NEW_CYCLE |
| 2026-01-26T20:35:00Z | OBSERVE | NEW_CYCLE started for INV-003 | Human approved, investigating row_count/aggregations issue |
| 2026-01-26T20:45:00Z | OBSERVE | Code path traced | Found: INTENTIONAL DESIGN for SCALAR intent; INV-003 is SPEC_ISSUE not code bug |
| 2026-01-26T20:50:00Z | DIAGNOSE/PLAN | Human selected Option A | Plan: refine INV-003 to exclude SCALAR output_shape |
| 2026-01-26T20:55:00Z | ACT | Updated STAGE_PROMPTS.md | INV-003 refined to exclude SCALAR output_shape |

---

## Next Action

**Stage: ACT** - Implementing Option A

Update STAGE_PROMPTS.md to refine INV-003 specification:
- Add "AND output_shape != SCALAR" exception to invariant definition

---

## Notes

_Space for human or Claude Code to leave notes for the next session._
