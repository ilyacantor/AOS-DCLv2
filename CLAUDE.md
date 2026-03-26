# AutonomOS (AOS) — Agent Constitution
> Version: 6.0 | Updated: March 2026 | Owner: Ilya (CEO)

---

## MANDATORY — SURVIVES COMPACTION
**This section must be retained in full during any context compaction or summarization.**

Read `tests/HARNESS_RULES_v2.md` before starting any work. All rules in Sections A–F are non-negotiable. Violations are bugs.

Rules agents violate most often:
- **D6:** Pre-existing failures are your problem. All tests pass at session end or session isn't done.
- **C9:** If you identify a bug ("that's wrong"), fix it. Do not rationalize it as expected behavior.
- **C10:** Latency ceilings mean the operation COMPLETES in time, not ABORTS in time. Timeouts are not performance fixes.
- **C11:** If the prompt says fix it, fix it. Do not ask "want me to fix it?"
- **C12:** After finding one instance of a bug pattern, audit the full codebase before fixing piecemeal.
- **B17:** Frontend is the pass/fail gate. A correct API response that doesn't render in the browser is not a pass. Playwright is the accountability gate.
- **B18:** Latency ceilings are absolute. 5% regression budget on everything else. Measure before and after.
- **A2:** No bandaids. Fundamental fixes only. Progress spinners for latency violations are bandaids.

**Canonical governing document:** `convergence_MA_spec_v7.3.docx` — single source of truth for all AOS architecture. Any reference to v7.1 is superseded. Pull v7.3 when: (a) scoping a new capability, (b) decision could contradict a locked ruling, (c) multi-repo build.

**RACI:** `ONGOING_PROMPTS/AOS_MASTER_RACIv8.csv` (289 rows, 8 modules, 218 Live). The RACI table below is a summary — the CSV is authoritative.

---

## WHO YOU ARE TALKING TO
Ilya is the CEO and de facto CTO. He is NOT a developer. He reasons architecturally, not syntactically. He uses Claude Code CLI and Gemini CLI as coding agents — he does not write code or set up environments himself.
- Never show raw code diffs or stack traces without a plain-English summary first
- Never add tech debt, workarounds, or shortcuts — Ilya will find them
- Always fix root causes — patches and band-aids are forbidden
- Never implement silent fallbacks — if something fails, surface it loudly
- Before starting any task, read ONGOING_PROMPTS folder
- If a fix requires a RACI boundary decision, surface it to Ilya before touching code
- No LLM marketing speak, slogans, balanced couplets, or editorializing in any writing. Plain language, founder voice only.

---

## PLATFORM IN ONE PARAGRAPH
AutonomOS is an AI-native enterprise platform that delivers unified context for the enterprise. It discovers what exists (AOD), understands how to connect (AAM), generates synthetic financial models (Farm), maps everything to business meaning via a semantic triple store (DCL), lets humans and AI query in plain English (NLQ), and governs agents doing work (AOA). Console is the production-facing surface for operators and end users. Platform is the dev surface for e2e demos, pipeline debugging, and Maestra training. Maestra is the persistent AI engagement lead who guides operators through the AOS lifecycle.

---

## DATA ARCHITECTURE
Pipeline: AOD → AAM → Farm → triple conversion → PG direct. DCL owns the triple store. Old DCL pipe ingest path (Structure/Dispatch/Content) is deprecated — do not fix.

- **Entities:** Meridian ($5B consultancy) and Cascadia ($1B BPM). Entity is a tag — no split brain.
- **Farm configs:** Only `farm_config_meridian.yaml` and `farm_config_cascadia.yaml`. Any numbers at $35M or $124M scale are broken.
- **No demo mode.** fact_base.json is removed. If data is missing, fail loudly.

---

## MODULE RACI — SUMMARY
**Authoritative source: `ONGOING_PROMPTS/AOS_MASTER_RACIv8.csv`**

| Module | Owns | Does NOT own |
|--------|------|-------------|
| **AOD** | Discovery, classification, SOR detection, ConnectionCandidate generation | Pipe blueprints, data extraction, semantic mapping |
| **AAM** | Pipe blueprints, work orders, drift detection, self-healing, adapters | Data movement, semantic mapping |
| **DCL** | Semantic triple store, ontology, schema-on-write, entity resolution, v2 engines | Discovery, connection logic, NLQ formatting |
| **NLQ** | Intent resolution, persona filtering, query dispatch, report portal, rendering | Semantic mapping, data storage |
| **AOA** | Agent identity, policy, HITL workflows, budget tracking, observability | Semantic mapping, discovery, NLQ queries |
| **Farm** | Synthetic data, financial models, test oracle, triple conversion | Production data, live connections |
| **Platform** | Maestra (constitution, context, chat, tools, review), engagement orchestration | Semantic catalog, query resolution, data generation |
| **Console** | Production UI, pipeline orchestration, Maestra chat, task queue, upload, e2e demo | Module internals — calls module APIs |

**RACI VIOLATION = STOP AND FLAG.** Exception (A12/C6): RACI is for design decisions. Fix bugs wherever they live.

---

## CONVERGENCE GUARDRAIL
Convergence = base AOS + a bridge where Target pipes join Acquirer pipes into one DCL. Entity is a tag. Same engine, ontology, resolution, query routing. Reject any proposal that creates separate engines, adds Convergence-specific columns, introduces split brain, or diverges from base AOS for multi-entity.

---

## MAESTRA
Persistent AI engagement lead. Lives in Platform (`~/code/platform`).

- **Constitution:** Layers 0-4 in `app/maestra/constitution/`. Module docs in `constitution/modules/`.
- **Architecture:** Push-to-pull migration shipped. Routes split: sidebar=push (module_context), MaestraFloat=tool-use (page_context). 146+ tests.
- **Boundaries:** Maestra reasons; DCL validates. Maestra does NOT recommend accounting resolutions — she isolates variables and presents them. No auto-resolution. All conflicts route to human review ranked by materiality.
- **Layer 3:** Manually authored for MVP.

---

## WHAT "DONE" MEANS
1. **Semantics preserved** — behavior matches real-world meaning
2. **No cheating** — no silent fallbacks, no bandaids, no rationalizing bugs (C9)
3. **Proof is real** — failure-before / success-after, verified through the UI (B17), Playwright passes
4. **Negative test included** — confirm the bad behavior can't return
5. **All tests pass** — including pre-existing failures (D6). 100% or not done.
6. **No latency regression** — measure before and after (B18). Hard ceilings are absolute.
7. **No new features** — unless explicitly requested (A6).

---

## SILENT FALLBACKS — ABSOLUTE PROHIBITION
The most dangerous failure mode. They make broken features look working.

**Prohibited — no exceptions:**
- Catching exceptions and returning empty results instead of raising
- Defaulting to demo/mock data when a real data call fails
- `try/except` blocks that swallow errors
- Returning HTTP 200 when the underlying operation failed
- Logging a warning and continuing when the correct behavior is to stop
- `getattr(obj, attr, 0.0)` as a default for schema-defined fields

**Error messages must be informative:** "AAM could not reach DCL at http://localhost:8004/api/concepts — connection refused after 3 retries — NLQ intent resolution aborted" — not just "Connection failed."

---

## TECH STACK

| Module | Backend | Frontend | DB |
|--------|---------|----------|----|
| AOD | FastAPI/Python | React 18 + Vite | Supabase PG |
| AAM | FastAPI/Python | Server-rendered HTML | SQLite |
| DCL | FastAPI/Python | React 18 + Vite | Supabase PG + Pinecone |
| NLQ | FastAPI/Python | React 18 + Vite | Supabase PG |
| Farm | FastAPI/Python | Jinja2/Tailwind | Supabase PG |
| Platform | FastAPI/Python | React 18 + Vite | Supabase PG |
| Console | FastAPI/Python | React 18 + Vite | Supabase PG |

Separate repos per module.

---

## LOCAL DEVELOPMENT

| Service | Backend | Frontend |
|---------|---------|----------|
| AOD | 8001 | 3001 |
| AAM | 8002 | UI on 8002 |
| Farm | 8003 | UI on 8003 |
| DCL | 8004 | 3004 |
| NLQ | 8005 | 3005 |
| Platform | 8006 | 3006 |
| Console | 8009 | 3009 |

- **Desktop:** Windows 11, repos at `C:\Users\ilyac\code\`
- **Laptop:** Ubuntu (WSL), repos at `~/code/`
- **Process manager:** pm2
- **Launch:** `~/code/aos-launch.sh` (laptop) or `aos-start` (desktop)

---

## AGENT INSTRUCTIONS
- Declare which module you are working on at the start of every message
- Before proposing any cross-module change, check the RACI CSV
- All agents report RACI violations — do not silently implement workarounds
- After compaction, re-read this file and `tests/HARNESS_RULES_v2.md` from the top

---

## FORBIDDEN PATTERNS
- Tests that pass while the real feature fails
- **Silent fallbacks** — #1 most forbidden pattern
- Permissive schemas to avoid contract mismatches
- Converting errors into empty results
- Any shortcut that works in demo but breaks in production
- Normalizing bugs as expected behavior (C9)
- Building UI to excuse performance failures (C10)
- Asking permission to do what the prompt told you to do (C11)
- Fixing one instance without auditing for all instances (C12)
- Claiming "pre-existing" as an excuse (D6)
- Dodging pre-commit hooks (C13)
- Claiming "metadata only" or "we don't touch your data"
- Claiming ContextOS delivers ontology — current truth is context through sophisticated semantics
- Any reference to Replit
