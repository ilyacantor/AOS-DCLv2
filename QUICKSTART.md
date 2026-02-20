# Human-Supervised Engineering Loop: Quick Start

## What This Is

A system where YOU are the supervisor and Claude Code is the worker. Claude Code executes one stage at a time, you check in every 60-180 minutes to advance to the next stage.

---

## Setup (One Time)

1. Add these files to your AOS-DCLv2 repo root:
   - `STATE.md` (tracks loop progress)
   - `SYSTEM_PROMPT.md` (reference for the system prompt)
   - `STAGE_PROMPTS.md` (reference for stage prompts)

2. Commit them so they persist.

---

## Your Check-In Workflow

Every 60-180 minutes:

### Step 1: Check STATE.md

Open `STATE.md` in your repo. Look at:
```
## Current Stage

STAGE: [something]
```

### Step 2: Open Claude Code

Start a new session.

### Step 3: Paste System Prompt

Copy the system prompt from `SYSTEM_PROMPT.md` and paste it.

### Step 4: Paste Stage Prompt

Based on what STATE.md says, copy the matching prompt from `STAGE_PROMPTS.md`:

| STATE.md says | Paste this |
|---------------|------------|
| `STAGE: OBSERVE` | OBSERVE Prompt |
| `STAGE: DIAGNOSE` | DIAGNOSE Prompt |
| `STAGE: PLAN` | PLAN Prompt |
| `STAGE: ACT` | ACT Prompt |
| `STAGE: REFLECT` | REFLECT Prompt |
| `STAGE: CHECKPOINT` | CHECKPOINT Prompt |
| `STAGE: IDLE` | Nothing to do, or OBSERVE to start new cycle |

### Step 5: Let Claude Code Work

It should:
- Execute the stage
- Produce output in the specified format
- Update STATE.md
- State the next stage
- Stop

### Step 6: Verify

- Check that STATE.md was updated
- Review the output for sanity
- If something looks wrong, don't advance — investigate

### Step 7: Close Session

You're done until the next check-in.

---

## Starting Fresh

To begin the loop for the first time:

1. Set STATE.md to:
   ```
   STAGE: OBSERVE
   ```

2. Open Claude Code

3. Paste System Prompt + OBSERVE Prompt

4. Let it run, it will detect the INV-001 (confidence=2.15) violation

5. It should set STATE.md to DIAGNOSE

6. Next check-in: paste DIAGNOSE prompt

---

## When Things Go Wrong

**Claude Code asks "what would you like me to do?"**
→ Re-paste the system prompt. Emphasize: "Execute the task. Do not ask questions."

**Claude Code tries to implement the supervisor**
→ Say: "You are the worker, not the supervisor. Execute the [STAGE] task."

**Claude Code skips stages**
→ Say: "Stop. You're in [STAGE]. Complete only this stage. Do not advance."

**Claude Code doesn't update STATE.md**
→ Say: "Update STATE.md now with your results."

**STATE.md gets corrupted**
→ Reset it manually based on where you think you are.

---

## The Stages Explained

```
OBSERVE    →  Run canary queries, check invariants, find violations
    ↓
DIAGNOSE   →  Trace the violation to root cause
    ↓
PLAN       →  Design a minimal fix with success criteria
    ↓
ACT        →  Implement the fix (code changes)
    ↓
REFLECT    →  Verify the fix worked, check for regressions
    ↓
CHECKPOINT →  Summarize, decide next action
    ↓
IDLE or back to OBSERVE
```

---

## Time Expectations

| Stage | Typical Duration |
|-------|------------------|
| OBSERVE | 5-10 min |
| DIAGNOSE | 15-30 min |
| PLAN | 10-15 min |
| ACT | 15-45 min |
| REFLECT | 10-20 min |
| CHECKPOINT | 5 min |

One full cycle: ~60-120 minutes of Claude Code work, spread across your check-ins.

---

## Files Summary

| File | Purpose | Who Updates |
|------|---------|-------------|
| `STATE.md` | Tracks current stage and data | Claude Code |
| `SYSTEM_PROMPT.md` | Reference: role definition | You (read-only) |
| `STAGE_PROMPTS.md` | Reference: task prompts | You (read-only) |

---

## First Session Checklist

- [ ] STATE.md added to repo with `STAGE: OBSERVE`
- [ ] Opened Claude Code
- [ ] Pasted System Prompt
- [ ] Pasted OBSERVE Prompt
- [ ] Claude Code ran canary queries
- [ ] Claude Code found INV-001 violation (confidence > 1.0)
- [ ] Claude Code updated STATE.md to `STAGE: DIAGNOSE`
- [ ] Session closed
- [ ] Ready for next check-in
