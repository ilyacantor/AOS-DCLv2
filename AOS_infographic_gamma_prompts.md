# Onta/AOS Internal Infographics — Gamma Prompts (v2, 2026-06-16)

Reconciled to `contextOS_blueprint_v1.5`, `context_lab_blueprint_v0.5`, `AOS_MASTER_RACI` (post-review).
Reframe: **"what you get" (outcome + value), not "how it works" (mechanism walkthrough).**
Cut (per request): **(1) full Onta/AOS platform, (2) AAM, (3) DCL.** Nice-to-have.

These are **internal** — component names (AOD, AAM, DCL, NLQ, Farm, DCL-MCP) are permitted.
Customer-facing copy would use only *Semantics* and *contextOS* (contextOS_blueprint §0); these are not customer copy.

Hard facts to hold (v1.5): Onta = company; AOS = platform; **Semantics** = base tier (resolved/governed/provenanced
data), **contextOS** = premium tier (relationship graph, graph-grounded retrieval, arbitration, as-of). The consumer
surface is **DCL-MCP** — there is **no "AIS"**. AAM connects + transports only; DCL does all semantic work. Demos =
**engineering attrition** (hero, traversal) + **cloud spend** (connect vehicle + FinOps agent via DCL-MCP). AR/AP "Acme"
retired. No Convergence on any AOS surface. Honest empty tile (never fabricate). contextOS is demonstrable and under
active development — never stated to a buyer as generally available.

---

## 1. Full Onta/AOS Platform

**What you get (one line):** AOS resolves your scattered enterprise data into a governed context layer and answers
questions no single system can — Semantics (trustworthy, resolved, provenanced data) plus contextOS (answers from
relationships, arbitration, and time) — deployed in days, over your systems, without owning your data.

**Gamma prompt (paste this):**

> Create one internal platform infographic titled **"AOS — Governed Enterprise Context, in Two Tiers."** Brand: Onta
> (company), AOS (platform). Tone: plain, founder-voice, no slogans. Muted palette, one cyan accent, Onta wordmark.
>
> Hero: *AOS turns data scattered across your systems into one governed context layer your people and your agents can
> trust — and answers questions no single system holds.*
>
> Two stacked tiers:
> - **Semantics (base) — trustworthy data.** Resolves raw fields to meaning and one canonical identity; stores triples
>   with provenance; detects conflicts; keeps history. Grounds any consumer. Ships today; goes to market first.
> - **contextOS (premium) — the answer changes.** Stitches resolved entities into a traversable graph; answers by
>   traversing it; arbitrates disagreeing sources to one decisive value; answers as-of a point in time. The correctness
>   lift *is* the product. Demonstrable and under active development.
>
> Footprint band: *Palantir-grade depth at an overlay footprint. Resolve in place — sources stay authoritative (Plaid
> pattern); AOS hosts a resolved working-set, not your estate. Deploy in days. Go after gravity (the few connections a
> decision turns on), never boil the ocean.*
>
> Trust band: *Honest empty tiles — a connected source missing a fact returns a real "no"; no source for a domain
> returns "no coverage." Absence is information; AOS never fabricates.*
>
> Two proof answers (cards): (1) *"Where is attrition highest and what's driving it?"* — traverses to the comp × market
> edge no single list holds. (2) *"Which team drives cloud-cost growth, and is the spend justified?"* — cost → service →
> team across sources; a FinOps agent answers via DCL-MCP and can act.
>
> Moat footer: *The compounding asset is the cross-customer mapping + gravity-prior library — generic system structure
> that transfers; customer data never does (the data wall).*
>
> Thin component footer (internal): *AOD discovers · AAM connects + transports raw records · DCL resolves & builds
> context · consumers read via DCL-MCP (agents, BI/curl) and NLQ (humans).*

---

## 2. AAM (Adaptive API Mesh)

**What you get (one line):** Governed data flowing in days — connect to the 6–10 integration planes you already run
(not 200–500 apps), your data stays yours (resolve in place, sources authoritative), and DCL does all the meaning.

**Gamma prompt (paste this):**

> Create one internal infographic titled **"AAM — Connect Once, at the Fabric. Days, Not Months."** Onta brand, cyan
> accent. Lead with outcome, not mechanism.
>
> Hero contrast: *A mid-market enterprise runs 200–500 apps but only 6–10 integration planes. AAM connects to the
> planes — so governed data arrives in days, not months.*
>
> Four fabric planes (one row, equal weight): **iPaaS · API Gateway · Event Bus · Data Warehouse.** "Same connect path,
> structurally different planes."
>
> Three "what you get" blocks:
> - **Speed.** Connect 6–10 planes; the integrations already in flight reveal themselves; first governed data in days.
> - **Your data stays yours.** Resolve in place — no bulk ingestion. Sources remain system-of-record; AOS hosts only a
>   resolved working-set (move-and-store derivative). Overlay, not migration.
> - **Raw in, meaning downstream.** AAM transports raw records and hands off via `/api/dcl/ingest-records`; it produces
>   **no business triples** — all mapping, identity, and semantics are DCL's. You pre-map nothing.
>
> Proof vehicle (**cloud spend**): *Connected through a warehouse pull; surfaced with full provenance — total,
> by-service, by-team, savings. A FinOps agent reads the same context via DCL-MCP and acts on it (the payoff).*
>
> Trust strip: *Every record carries provenance — source, field, plane, confidence. Nothing is invented; thin coverage
> is shown, not faked.*
>
> Footer (internal): *AAM = connect + transport + handoff. Boundary is firm — no semantic mapping or identity
> resolution of business records inside AAM (RACI).*

---

## 3. DCL (the context engine — Semantics + contextOS)

**What you get (one line):** Answers flat retrieval can't produce — DCL resolves identity, meaning, provenance,
conflict and history (Semantics), then stitches a graph and answers by traversing it, arbitrates conflicting sources,
and answers as-of (contextOS). The correctness lift is the product.

**Gamma prompt (paste this):**

> Create one internal infographic titled **"DCL — Where the Answer Changes."** Onta brand, cyan accent. Before/after spine.
>
> Before/after hero: *Semantics at full strength (all the resolved data) still gives one flat answer. contextOS gives
> the governed, traversed, arbitrated answer.* The baseline loses on **capability**, never on access — if the only way
> to make it lose is to withhold data, that's a rig.
>
> Two tier blocks:
> - **Semantics (base).** Meaning (field→concept); canonical identity ("customer means the same thing everywhere");
>   provenance/lineage per fact (a backstop, surfaced on demand — never the headline); conflict detection; temporal
>   store (history kept, nothing deleted).
> - **contextOS (premium).** Stitched relationship graph (edges that hold information no single record does);
>   graph-grounded retrieval (a node-anchored walk, not a flat lookup); authority arbitration (one decisive value on
>   conflict, with disclosure); as-of (what was true when).
>
> The four answer-better mechanisms (callout strip): **resolution · traversal · arbitration · as-of** — each makes the
> answer diverge from raw *toward correctness*, attributable to a named mechanism. (Coverage honesty is a trust
> backstop — a non-answer, never a headline.)
>
> How the graph is built (small band): **propose → verify → learn.** An LLM proposes connections (gravity-directed,
> never exhaustive); each is verified deterministically against the resolved data (unverified dies, no human); what
> survives is cached. HITL is risk-weighted and decays with precedent — minimized, never eliminated. (Developed and
> proven in the Context Lab against synthetic ground truth.)
>
> Served to consumers via **DCL-MCP** (read-only: query, list domains, concept lookup, semantic export, provenance,
> lineage) — agents, BI/curl, and NLQ over one read path.
>
> Two demos (cards): (1) **attrition (traversal)** — highest in Engineering → Platform; comp + growth-path driven;
> senior bands ~13% below market, via the comp × market edge Semantics can't join. (2) **cloud spend (arbitration /
> decomposition)** — concentration and drivers via cost → service → team; honest unexplained remainder stated, not hidden.
>
> Moat footer: *cross-customer mappings + gravity priors compound across tenants; the data wall keeps customer data
> per-tenant, always.*

---

*Prompts only — paste each into Gamma to generate; do not reuse the old diagrams. NLQ-detail is folded into the
platform + DCL stories; if you want NLQ as its own fourth, say so.*
