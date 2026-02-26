# AutonomOS (AOS) — Agent Constitution
> Version: 4.0 | Updated: February 2026 | Owner: Ilya (CEO)

---

## WHO YOU ARE TALKING TO
Ilya is the CEO and technical lead. He is NOT a developer. He reasons architecturally, not syntactically.
- **Never show raw code diffs or stack traces without a plain-English summary first**
- **Never add tech debt, workarounds, or shortcuts** — Ilya will find them and it will be unpleasant
- **Always fix root causes** — patches and band-aids are forbidden
- **Never implement silent fallbacks** — if something fails, surface it loudly. Substituting mock data, empty results, or swallowing exceptions when real calls fail is forbidden. See SILENT FALLBACKS section.
- **Before starting any task, read everything in the ONGOING_PROMPTS folder** — focus on the latest version of the RACI CSV (highest version number). If ONGOING_PROMPTS is not in your context pack, ask for it before proceeding.
- If a fix requires a RACI boundary decision, surface it to Ilya before touching code

---

## PLATFORM IN ONE PARAGRAPH
AutonomOS is an AI-native enterprise operating system that sits on top of existing enterprise systems (Salesforce, SAP, AWS, etc.) without replacing them. It discovers what exists (AOD), understands how to connect (AAM), maps everything to business meaning (DCL), lets humans and AI agents query it in plain English (NLQ), and governs the agents doing work (AOA). The moat is the semantic data layer — not the runtime. MEI/Convergence is a product extension of AOS applied to multi-entity M&A scenarios; ~90% shared technology.

---

## MODULE RACI — THE LAW

| Module | Owns | NEVER touches |
|--------|------|---------------|
| **AOD** | Discovery, classification (Governed/Shadow/Zombie), SOR detection, Fabric Plane hints, ConnectionCandidate generation | Pipe blueprints, data extraction, semantic mapping |
| **AAM** | Pipe blueprint creation, work order dispatch, drift detection, self-healing | Data movement, fabric plane inference (that's AOD's job), semantic mapping |
| **DCL** | Semantic catalog, ontology, schema-on-write validation, MCP exposure, entity resolution | Discovery logic, connection logic, NLQ response formatting |
| **NLQ** | Intent resolution (3-tier), persona filtering, query dispatch to DCL, output rendering | Semantic mapping, data storage, agent orchestration |
| **AOA** | Agent identity, policy enforcement, HITL workflows, budget tracking, observability | Semantic mapping, discovery, NLQ query handling |
| **Farm** | Synthetic data generation, financial models, test oracle | Production data, live connections |

**RACI VIOLATION = STOP AND FLAG.** If a fix requires Module A to implement logic that belongs to Module B, do not implement it. Surface the architectural conflict with a clear description and proposed resolution.

---

## CURRENT PRIORITIES (Feb 2026)

### P1 — NLQ Demo Stability & UX
The AOD→AAM→Farm→DCL flow is stable. NLQ is now the critical path. It must work reliably in demo mode without human hand-holding.

Known issues (fix these before anything else in NLQ):
- KPI boxes are too large — reduce size, tighten layout
- Clicking EBITDA KPI should trigger a trend/revenue chart — this is broken or unreliable
- Dashboard auto-generation from "build me a CFO dashboard" must be consistent
- Tier 1 free cache hit rate target: 60-70% — if below this, something is wrong

### P2 — DCL→NLQ E2E Throughput
Goal: Complete the real data pipeline handoff — currently clunky and manual.
- DCL and NLQ are 90% connected; the last 10% is the blocking issue
- Pushing/pulling metadata dumps manually is not acceptable — must be automated pipeline
- When this works, it's the demo-worthy moment: real data, real semantics, real query

### P3 — DCL Semantic Catalog
Goal: Expand the ontology to cover real enterprise data elements with proper depth.
- MCP integration in progress — DCL must expose semantic catalog via MCP tools: `concept_lookup`, `semantic_export`, `query`, `provenance`
- Current 3-tier normalization funnel must not degrade under load
- Target: any MCP-compatible agent can query DCL without custom integration

### P4 — Consolidated Demo UX
Goal: Single AOS demo file housing all modules via iframes + AOA panel.
- Currently modules demo independently — investor/customer demo requires switching between tabs
- MEI/Convergence demo is credible once AOS demo works — don't build separately yet
- AOA governance panel must be integrated into the consolidated view

---

## WHAT "DONE" MEANS
Every completed task must satisfy ALL four:
1. **Semantics preserved** — behavior matches real-world meaning
2. **No cheating** — no silent fallbacks, no optional-everything, no demo-only paths
3. **Proof is real** — failure-before / success-after evidence
4. **Negative test included** — confirm the bad behavior can't return

---

## SILENT FALLBACKS — ABSOLUTE PROHIBITION
Silent fallbacks are the most dangerous failure mode in this codebase. They make broken features look working.

**Prohibited patterns — no exceptions:**
- Catching an exception and returning empty results instead of raising
- Defaulting to demo/mock data when a real data call fails, without surfacing the failure
- `try/except` blocks that swallow errors silently
- Any code path that returns a successful HTTP 200 when the underlying operation failed
- Logging a warning and continuing when the correct behavior is to stop and fail

**If a real data source is unavailable, the system must:**
1. Return an explicit error response that names what failed, why, and what was being attempted
2. Log the failure with full context — service name, endpoint, input parameters, exception detail
3. Never substitute mock or cached data without the caller explicitly requesting it

**Error messages must be informative, not just loud.** "Connection failed" is not acceptable. "AAM could not reach DCL at http://localhost:8004/api/concepts — connection refused after 3 retries — NLQ intent resolution aborted" is acceptable. The error must tell an engineer exactly where to look.

---

## TECH STACK QUICK REFERENCE

| Module | Backend | Frontend | DB | Notes |
|--------|---------|----------|----|-------|
| NLQ | FastAPI/Python | React 18 + Vite | Supabase PG | Claude (Tier 3 LLM) |
| DCL | FastAPI/Python | React 18 + Vite | Supabase PG + Pinecone | Gemini 2.5 Flash + OpenAI |
| AOD | FastAPI/Python | React 18 + Vite | Supabase PG | — |
| AAM | FastAPI/Python | Server-rendered HTML | SQLite | UI lives in ui_pages.py — no Vite |
| AOA | FastAPI/Python | React 18 + Vite | Supabase PG | **Not yet built — placeholder only** |
| Farm | FastAPI/Python | Jinja2/Tailwind | Supabase PG | Server-rendered, no Vite |
| FinOps | Express/Node | React 18 | Neon PG + Pinecone | **Anomaly: Node backend** |

**Separate repos per module.**

---

## DEPLOYMENT & ENVIRONMENTS

### Production
- **Platform:** Render (migrated from Replit — Replit is dead, do not reference it)
- **Secrets:** Render environment variables — never in .env files committed to repos
- **Build:** Each module has its own Render service

### Local Development — Desktop (Windows)
- **OS:** Windows 11, PowerShell terminal
- **Repos:** `C:\Users\ilyac\code\`
- **Process manager:** pm2 via PowerShell
- **Aliases:** `aos-start` (all backends), `aos-stop`, `aos-frontends` (Vite dev servers)
- **Local ports:**

| Service | Backend | Frontend |
|---------|---------|----------|
| AOD | 8001 | 3001 |
| AAM | 8002 | UI on 8002 (server-rendered) |
| DCL | 8004 | 3004 |
| Farm | 8003 | UI on 8003 (server-rendered) |
| NLQ | 8005 | 3005 |
| Platform | 8006 | 3006 |

### Local Development — Laptop (Ubuntu / WSL)
- **OS:** Ubuntu (WSL on Windows laptop)
- **Repos:** `~/code/` (also accessible at `\\wsl$\Ubuntu\home\ilyac\code\` from Windows Explorer)
- **Process manager:** pm2 via bash
- **Launch script:** `~/code/aos-launch.sh` — interactive service selector with git pull + pm2 start + browser open
- **Alias:** `aos` runs the launch script, `aos-stop` kills everything
- **Ports:** same as desktop above

---

## AGENT TEAM INSTRUCTIONS
- Each agent must declare which module it is working on at the start of every message
- Before proposing any cross-module change, agents must check the RACI table above
- **Use Opus for all tasks** — Ilya is on the 200x plan. Opus is preferred for both implementation and architecture.
- All agents report RACI violations to the lead — do not silently implement workarounds
- Do not reference or suggest Replit in any context

---

## FORBIDDEN PATTERNS
- Tests that pass while the real feature fails
- **Silent fallbacks that hide errors** ← this is the #1 most forbidden pattern
- Permissive schemas to avoid contract mismatches
- Converting errors into empty results
- Any shortcut that works in demo but breaks in production
- Band-aids, patches, workarounds
- Adding features to Module X that belong to Module Y (RACI violation)
- FinOps pattern propagating to other modules (Node.js anomaly — don't spread it)
- Any reference to Replit, Replit Secrets, or Replit-specific configuration

---

## MEI / CONVERGENCE NOTE
MEI (Multi-Entity Intelligence) = Convergence product. Lives under AOS umbrella.
- ~90% AOS technology reused
- Primary use case: M&A post-close semantic reconciliation across heterogeneous systems
- The insight: reconciling two companies' systems is architecturally identical to reconciling multiple ERP instances — which AOS already solves
- Do NOT build a separate MEI demo until the core AOS demo (P4) is working
- MEI demo will be a product extension narrative on top of AOS, not a separate stack
