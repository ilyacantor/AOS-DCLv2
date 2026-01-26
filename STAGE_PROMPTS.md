# Stage Prompts

Each prompt below corresponds to one stage of the engineering loop. 
Paste the SYSTEM PROMPT first, then the appropriate stage prompt.

Check STATE.md to know which stage you're in.

---

## OBSERVE Prompt

```
## TASK: OBSERVE

Read STATE.md to confirm you're in the OBSERVE stage.

### What You Must Do

1. Run these canary queries against the NLQ system (via curl, httpx, or direct function call):

   - "What was revenue last year?"
   - "Top 5 customers by revenue"  
   - "What is our burn rate?"
   - "Show me zombie resources"

2. For each query, capture:
   - confidence score
   - row_count
   - definition_id matched
   - aggregations returned
   - any errors

3. Check these invariants:
   - INV-001: confidence must be in [0.0, 1.0]
   - INV-002: execution must complete in < 5000ms
   - INV-003: if row_count = 0, aggregations must be empty
   - INV-005: no 500 errors

4. Report findings in this format:

### Required Output

```yaml
observation_id: obs_[timestamp]
timestamp: [ISO timestamp]
canary_results:
  - query: "..."
    confidence: [number]
    row_count: [number]
    definition_id: "..."
    aggregations: {...}
    violations: [list of INV-XXX]
overall_status: HEALTHY | INVARIANT_VIOLATED
violations_summary:
  - invariant: INV-XXX
    observed: [value]
    expected: [constraint]
    canary_query: "..."
```

5. Update STATE.md:
   - If HEALTHY: set stage to IDLE, clear active violation
   - If INVARIANT_VIOLATED: set stage to DIAGNOSE, populate active violation with the first critical violation

6. State: "Next stage: [DIAGNOSE/IDLE]. Awaiting next session."

7. STOP.
```

---

## DIAGNOSE Prompt

```
## TASK: DIAGNOSE

Read STATE.md to confirm you're in the DIAGNOSE stage and see the active violation.

### What You Must Do

1. Read the active violation from STATE.md

2. Trace the source of the violation:
   - For confidence issues: trace from API response → executor → scorer → hypothesis
   - For row/aggregation issues: trace from executor → data loader → summary computer
   - For timeout issues: profile the execution path
   - For 500 errors: find the exception source

3. Identify the SPECIFIC location:
   - File path
   - Function name
   - Line range

4. Form a hypothesis about WHY it's broken

5. Gather evidence (code snippets, debug output, logic analysis)

### Required Output

```yaml
diagnosis_id: diag_[timestamp]
violation: [from STATE.md]
component: backend/nlq/[file].py
function: [function_name]
line_range: [start]-[end]
root_cause_hypothesis: |
  [Specific explanation of what's wrong and why]
evidence:
  - "[Evidence 1]"
  - "[Evidence 2]"
  - "[Evidence 3]"
confidence_in_diagnosis: [0.0-1.0]
estimated_complexity: low | medium | high
```

6. Update STATE.md:
   - Fill in the Diagnosis section
   - Set stage to PLAN
   - Add entry to Session History

7. State: "Next stage: PLAN. Awaiting next session."

8. STOP.

### Constraints

- Do NOT fix the code yet
- Do NOT modify any files except STATE.md
- If confidence_in_diagnosis < 0.6, note that more investigation is needed
```

---

## PLAN Prompt

```
## TASK: PLAN

Read STATE.md to confirm you're in the PLAN stage and see the diagnosis.

### What You Must Do

1. Read the diagnosis from STATE.md

2. Design a fix strategy:
   - What specific changes need to be made?
   - Which files will be modified?
   - What is the minimal change that fixes the issue?

3. Define success criteria:
   - What must be true after the fix?
   - Which canary queries must pass?
   - Which invariants must be satisfied?

4. Assess risk:
   - low: 1 file, <20 lines, non-core component
   - medium: 2-3 files, 20-100 lines, service component  
   - high: 4+ files, >100 lines, or core engine

5. Create rollback plan

### Required Output

```yaml
plan_id: plan_[timestamp]
diagnosis_id: [from STATE.md]
strategy: |
  [Clear description of what will be changed and why]
files_to_modify:
  - path: backend/nlq/[file].py
    change: "[description of change]"
success_criteria:
  - "Canary query 'X' returns confidence in [0.0, 1.0]"
  - "INV-001 passes for all canary queries"
  - "Existing tests pass"
risk_level: low | medium | high
risk_factors:
  files: [count]
  lines_estimate: [count]
  component_type: utility | service | core
rollback_steps:
  - "git checkout [file]"
  - "Re-run canary queries to confirm rollback"
requires_human_approval: true | false
```

6. Update STATE.md:
   - Fill in the Plan section
   - If risk_level is HIGH or requires_human_approval is true:
     - Set stage to CHECKPOINT
     - Note: "Awaiting human approval before ACT"
   - Otherwise:
     - Set stage to ACT
   - Add entry to Session History

7. State: "Next stage: [CHECKPOINT/ACT]. Awaiting next session."

8. STOP.

### Constraints

- Do NOT implement the fix yet
- Do NOT modify any files except STATE.md
- Keep the change MINIMAL — fix only what's broken
```

---

## ACT Prompt

```
## TASK: ACT

Read STATE.md to confirm you're in the ACT stage and see the plan.

### What You Must Do

1. Read the plan from STATE.md

2. Create a snapshot point:
   - Note the current git commit or create a marker

3. Implement ONLY the changes specified in the plan:
   - Modify the files listed
   - Make the changes described
   - Add a comment: # FIX: [diagnosis_id] - [brief description]

4. Verify you stayed within scope:
   - Only files in the plan were modified
   - No "while I'm here" improvements
   - No refactoring beyond the fix
   - No test modifications

### Required Output

```yaml
act_id: act_[timestamp]
plan_id: [from STATE.md]
snapshot_commit: [git commit hash or marker]
changes_made:
  - file: backend/nlq/[file].py
    lines_changed: [count]
    description: "[what was changed]"
deviations_from_plan: []  # Must be empty or justified
```

Then show the actual diff or code changes made.

5. Update STATE.md:
   - Fill in the Act section
   - Set stage to REFLECT
   - Add entry to Session History

6. State: "Next stage: REFLECT. Awaiting next session."

7. STOP.

### Constraints

- ONLY modify files in the plan
- Do NOT add features
- Do NOT refactor
- Do NOT modify tests to make them pass
- Do NOT skip to verification — that's REFLECT's job
```

---

## REFLECT Prompt

```
## TASK: REFLECT

Read STATE.md to confirm you're in the REFLECT stage and see what was changed.

### What You Must Do

1. Read the original violation and the changes made from STATE.md

2. Run the SAME canary queries from OBSERVE:
   - "What was revenue last year?"
   - "Top 5 customers by revenue"
   - "What is our burn rate?"
   - "Show me zombie resources"

3. Check ALL invariants (not just the one that was violated):
   - INV-001: confidence in [0.0, 1.0]
   - INV-002: execution < 5000ms
   - INV-003: row_count=0 implies empty aggregations
   - INV-005: no 500 errors

4. Run existing tests:
   - pytest tests/nlq/ (or relevant test suite)
   - Note pass/fail counts

5. Determine outcome:
   - SUCCESS: original violation fixed, no new violations, tests pass
   - PARTIAL: original violation fixed, but other issues remain
   - REGRESSION: fix broke something else
   - FAILURE: original violation not fixed
   - WORSE: more violations than before

### Required Output

```yaml
reflect_id: refl_[timestamp]
act_id: [from STATE.md]
original_violation: [INV-XXX]
canary_results:
  - query: "..."
    confidence: [before] → [after]
    violations: [list]
original_violation_status: RESOLVED | UNRESOLVED
new_violations: []
test_results:
  passed: [count]
  failed: [count]
  skipped: [count]
outcome: SUCCESS | PARTIAL | REGRESSION | FAILURE | WORSE
reasoning: |
  [Explanation of the outcome]
```

6. Update STATE.md:
   - Fill in the Reflect section
   - Based on outcome:
     - SUCCESS → stage = CHECKPOINT
     - PARTIAL → stage = CHECKPOINT (note remaining issues)
     - REGRESSION → rollback, stage = DIAGNOSE
     - FAILURE → stage = DIAGNOSE (with new evidence)
     - WORSE → rollback, stage = CHECKPOINT (escalate to human)
   - Add entry to Session History

7. State: "Next stage: [X]. Awaiting next session."

8. STOP.
```

---

## CHECKPOINT Prompt

```
## TASK: CHECKPOINT

Read STATE.md to see the full history and current state.

### What You Must Do

1. Compile a summary of work done:
   - What violation was addressed?
   - What was the diagnosis?
   - What fix was applied?
   - What was the outcome?

2. Assess current system health:
   - Are there remaining violations?
   - Are there new issues to address?

3. Recommend next action:
   - IDLE: System healthy, no work needed
   - NEW_CYCLE: Start OBSERVE for remaining/new issues
   - ESCALATE: Problem requires human decision

### Required Output

```yaml
checkpoint_id: chkpt_[timestamp]
summary:
  violation_addressed: INV-XXX
  diagnosis: "[brief]"
  fix_applied: "[brief]"
  outcome: [SUCCESS/PARTIAL/etc]
system_health:
  violations_remaining: [count]
  issues: []
recommendation: IDLE | NEW_CYCLE | ESCALATE
reasoning: |
  [Why this recommendation]
next_action_for_human: |
  [Clear instruction for what the human should do next]
```

4. Update STATE.md:
   - Clear working sections (Diagnosis, Plan, Act, Reflect) if starting fresh
   - Set stage to IDLE or OBSERVE based on recommendation
   - Add entry to Session History
   - Update "Next Action" section for human

5. STOP.

This is a natural stopping point. The human will review and decide whether to continue.
```

---

## Quick Reference: Which Prompt to Use

| STATE.md shows | Use this prompt |
|----------------|-----------------|
| `STAGE: OBSERVE` | OBSERVE Prompt |
| `STAGE: DIAGNOSE` | DIAGNOSE Prompt |
| `STAGE: PLAN` | PLAN Prompt |
| `STAGE: ACT` | ACT Prompt |
| `STAGE: REFLECT` | REFLECT Prompt |
| `STAGE: CHECKPOINT` | CHECKPOINT Prompt |
| `STAGE: IDLE` | Either do nothing, or OBSERVE Prompt to start a new cycle |
