# Autonomous Engineering Worker - Replit Setup

## Quick Start

### 1. Add the worker to your repo

Copy `autonomous_worker.py` to your AOS-DCLv2 repo root.

### 2. Install dependencies

In the Replit shell:
```bash
pip install anthropic httpx pyyaml slack-sdk
```

### 3. Set environment variables

In Replit Secrets (🔒 icon in sidebar), add:

| Key | Value |
|-----|-------|
| `ANTHROPIC_API_KEY` | Your Claude API key from console.anthropic.com |
| `SLACK_WEBHOOK_URL` | (Optional) Slack incoming webhook URL |
| `NLQ_ENDPOINT` | `http://localhost:5000/api/nlq/registry/execute` |
| `REPO_PATH` | `/home/runner/AOS-DCLv2` (or your repo path) |
| `CHECK_INTERVAL` | `60` (minutes between cycles) |

### 4. Test it

```bash
# Run one stage
python autonomous_worker.py once

# Just run observation
python autonomous_worker.py observe
```

### 5. Run continuously

```bash
python autonomous_worker.py run
```

---

## Deployment Options

### Option A: Run alongside your app

Add to your existing `replit.nix` or start script:

```bash
# Start NLQ backend
python run_backend.py &

# Start autonomous worker (after backend is up)
sleep 10 && python autonomous_worker.py run &
```

### Option B: Separate Replit deployment

Create a new Replit that:
1. Clones your repo
2. Runs only the worker
3. Calls your deployed NLQ endpoint via HTTPS

Update `NLQ_ENDPOINT` to your public URL:
```
https://aos-dclv2.ilyacantor.repl.co/api/nlq/registry/execute
```

### Option C: Replit Scheduled Tasks (if available)

If your Replit plan supports scheduled tasks:
```bash
# Run every hour
python autonomous_worker.py once
```

---

## How It Works

```
┌─────────────────────────────────────────────────────────────┐
│                   autonomous_worker.py                       │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  1. Read STATE.md         → Know current stage               │
│  2. Run canary queries    → Check system health              │
│  3. Call Claude API       → Get analysis/recommendations     │
│  4. Execute edits         → Apply approved changes           │
│  5. Run tests             → Verify fix worked                │
│  6. Update STATE.md       → Record progress                  │
│  7. Notify Slack          → Alert on checkpoints             │
│  8. Sleep                  → Wait for next cycle              │
│                                                              │
│  Loop cost: ~$0.30 per full cycle (OBSERVE→CHECKPOINT)       │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

---

## Key Differences from Claude Code for Web

| Aspect | Claude Code for Web | Autonomous Worker |
|--------|---------------------|-------------------|
| Who decides actions | Claude | Your script |
| Who executes | Claude | Your script |
| Validation | Trust Claude | Script validates before executing |
| Loop control | Manual prompts | Automatic |
| Cost | Free/cheap | ~$0.30/cycle API cost |
| Gaming tests | Possible | Harder (script checks invariants) |

---

## Configuration

### Invariants

Edit the `INVARIANTS` dict in `autonomous_worker.py` to add/modify checks:

```python
INVARIANTS = {
    "INV-001": {
        "name": "confidence_range",
        "check": lambda r: 0.0 <= r.get("confidence_score", 0) <= 1.0,
        "expected": "[0.0, 1.0]",
        "severity": "CRITICAL",
    },
    # Add more...
}
```

### Canary Queries

Edit the `CANARY_QUERIES` list:

```python
CANARY_QUERIES = [
    "What was revenue last year?",
    "Top 5 customers by revenue",
    # Add more...
]
```

### HITL Thresholds

In the `Config` class:
```python
max_cycles_before_hitl: int = 5    # Force human review after N cycles
check_interval_minutes: int = 60   # Time between autonomous runs
```

---

## Monitoring

### Slack Notifications

You'll receive notifications for:
- 🔔 High-risk plans requiring approval
- ✅ Successful fixes
- ⚠️ Partial fixes (some violations remain)
- 🚨 Failures or regressions

### STATE.md

Check `STATE.md` in your repo to see:
- Current stage
- Active violations
- Diagnosis and plan details
- Session history

### Logs

The worker prints progress to stdout. In Replit, check the Console tab.

---

## Troubleshooting

### "ANTHROPIC_API_KEY not set"

Add your API key to Replit Secrets.

### "Connection refused" on canary queries

Your NLQ backend isn't running. Start it first:
```bash
python run_backend.py
```

### "File not found" during edits

Claude suggested editing a file that doesn't exist. Check the diagnosis - it may be hallucinating file paths.

### Worker keeps failing on same issue

The diagnosis or plan may be wrong. Check STATE.md and manually verify the suggested fix makes sense.

---

## Cost Estimate

| Usage | API Calls | Monthly Cost |
|-------|-----------|--------------|
| Light (2 cycles/day) | ~60 | ~$18 |
| Medium (5 cycles/day) | ~150 | ~$45 |
| Heavy (10 cycles/day) | ~300 | ~$90 |

Plus Replit hosting (~$7/month for Reserved VM).

**Total: $25-100/month** for autonomous development.
