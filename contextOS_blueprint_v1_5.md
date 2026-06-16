# contextOS Blueprint v1.5

**Status.** Canonical specification for AOS core context. Extracts and supersedes the AOS-context material previously scattered across `convergence_blueprint_master.md §4 (DCL)` and `AAM_Blueprint §1.0 / DCL-side MCP`. Subordinate to `AOS_MASTER_RACI`; where this conflicts with the RACI, the RACI wins. Does not govern Convergence.

**Why this version exists.** The prior context spec treated provenance as the end goal. Provenance is not the deliverable; it is a backstop. The headline value of AOS context is **relationships** — answers that are correct because the layer resolved identity and traversed connections that no single record holds. This blueprint reframes around that and defines the base/premium product split that expresses it.

**v1.1.** Corrected against the verified pipeline map: graph-traversal and as-of endpoints already exist and are unconsumed; `current_triples` absence cited to `mig017:20`; the dormant Prod-mode corrector noted.

**v1.2.** Demo re-aimed off the over-budget framing to two canonical questions: engineering attrition (the hero) and cloud spend.

**v1.3.** §7 rewritten to the proving-context standard — the four answer-better mechanisms (resolution, traversal, arbitration, as-of), coverage honesty demoted to a trust backstop, raw shown wrong, synthetic data present-but-fragmented (never a withheld field).

**v1.4.** Beds down the architecture worked out in strategy review. Adds the relationship-derivation approach — **propose → verify → learn** (§3.1) — as the path off hand-authored edges; the **gravity** principle (§3); the **cross-customer learning moat and the data wall** (§3.2); data residency stated. Build state corrected to the **honest delivered state** (§5). **Positioning stance** added (§0, §4): Semantics leads to market; contextOS is the premium roadmap, positioned with conviction and demonstrable, never stated to a buyer as generally available.

**v1.5.** Sets the **Context Lab** in its place — the empirical R&D engine that develops and proves propose-verify-learn (§3.1) and accrues and validates the cross-customer library (§3.2). Folds in **generality** (the method is one loop; use cases are test material) and the **two proofs** (synthetic proves the method; a customer pilot proves coverage). Adds **§3.3**: contextOS and the lab are product and engine on different clocks — the build is a real, shippable deliverable judged on delivered value, **not a temporary bridge** to what R&D learns. New invariant (§9) locks the stance.

**Companion.** `context_lab_blueprint` — the empirical R&D engine behind §3.1–§3.2; subordinate to this blueprint.

---

## 0. Naming

"ContextOS" previously named the single-entity product line; that usage is retired. "contextOS" now names a **premium tier**, not a product line.

| Term | Meaning |
| :---- | :---- |
| Onta | Company |
| AOS | Platform / core product (single-tenant enterprise) |
| **Semantics** | **Base tier** — resolved, governed, provenanced data served to consumers |
| **contextOS** | **Premium tier** — relationship graph, graph-grounded answers, arbitration, as-of |
| Convergence | Separate product (M&A, multi-entity). Out of scope here. |

Customer-facing copy uses *Semantics* and *contextOS*. Internal component names (AOD, AAM, DCL, NLQ, Farm) never appear in customer copy.

**Positioning stance.** Semantics leads to market — it is shippable now and earns the first dollar. contextOS is the premium tier and on the roadmap; it is positioned with full conviction as a real, demonstrable capability, and is never represented to a buyer as generally available while it is still being built. The distinction is load-bearing: this is a trust product, and a buyer who catches "done" stated as fact about an unfinished capability discounts the resolved base too.

---

## 1. The Reframe

"Context is built on top of semantics" is now both an architecture statement and a product statement.

- **Semantics (base)** resolves raw fields to meaning and identity, stores them as triples with provenance, detects conflicts, and keeps history. It grounds a consumer. On its own it does not change the answer versus good flat retrieval — and that is acceptable, because it is the base.
- **contextOS (premium)** is where the answer changes: it stitches resolved entities into a traversable graph, answers by traversing it, arbitrates conflicting sources to a decisive value, and answers as-of a point in time.

Provenance becomes a backstop: produced in the base layer, carried on every triple, surfaced on demand behind an answer ("see sources"), never front-loaded as the headline.

---

## 2. Composition

| Component | Supplies | Tier |
| :---- | :---- | :---- |
| Meaning (field→concept) | Term / metric disambiguation | Semantics |
| Identity (canonical resolution) | "Customer means the same thing everywhere" | Semantics |
| Provenance / lineage | Source, field, confidence, freshness per fact | Semantics (backstop in both) |
| Conflict detection | Sources disagree → flagged | Semantics |
| Temporal store (supersession) | History kept, nothing deleted | Semantics |
| **Relationships (stitched graph)** | Typed links across entities that no single record holds | **contextOS** |
| **Graph-grounded retrieval** | Answers by traversing objects + links, not flat lookup | **contextOS** |
| **Authority arbitration** | One decisive value when sources disagree | **contextOS** |
| **As-of / point-in-time** | What was true when | **contextOS** |

Calculation semantics — how revenue is computed, the grain, the filters — are consumed from the enterprise's existing semantic layer (dbt / Cube / OSI) or elicited at onboarding. AOS does not rebuild them. That line keeps the layer light.

A Prod-mode LLM/RAG corrector exists in the persist path (it can rewrite low-confidence field→concept before write). It is parked today; it is the seed of the proposer in §3.1.

---

## 3. Architectural Position — depth without data gravity

The wedge: Palantir-grade conceptual depth (objects + links + answer-correctness) at an Atlan-grade footprint (overlay, no data ownership). High depth, low footprint — the corner neither incumbent occupies.

How depth is reached without owning data:

- **Resolve in place.** Sources stay authoritative. AOS hosts a normalized working-set of triples with provenance (the Plaid pattern), not the estate. Federation is the exception, not the default.
- **Stitch at the semantic layer.** The graph is derived over *resolved* entities, not by ingesting and re-modeling the data. An edge earns its place only when establishing it required resolution across sources — never when it merely mirrors a foreign key a single record already carries.
- **Deploy in days.** The model is bootstrapped from connected schema and samples and refined through a short interview, not hand-modeled in a services engagement.
- **Go after gravity, not coverage.** The connections worth having are a small, dense subset of the combinatorial space, and the count is unknowable per enterprise — so we target the principle, not a number. A connection earns its place by *decision-dependence × cross-source difficulty*: it is hard (no single system holds it) and consequential (a real decision turns on it). Relevance lives in the decisions the data feeds, not in the data — found by watching what gets asked and acted on, not by mining the estate. The graph grows toward instrumented demand, seeded by cross-customer priors about which connections bear decisions in which domains. Gravity concentrates around a finite set of recurring decisions — pricing, retention, spend, risk, pipeline, people — which the persona/domain map already charts. Dense where it matters, absent where it doesn't. Never boil the ocean. This is also the operational form of the light footprint: the incumbent models everything bespoke; we model what decisions pull for and densify by use.

**World stance.** Closed-world inside resolved coverage — a connected source that lacks a fact returns a real "no." Open-world at the coverage boundary — no source covering the domain returns "no coverage," the honest empty tile. A tile is never dropped; absence is information.

**The base↔premium seam is an architectural requirement, not just a price line.** Semantics must stand and sell without contextOS. The graph, traversal, and arbitration must activate as a separable, metered layer on top — never woven so deep into resolution that the base cannot ship or the premium cannot be sold apart.

---

## 3.1 How the graph is built — propose, verify, learn

Today edges are derived by general, name-free rules — hand-authored, applied across any entity, verified to carry cross-record information. That is the trustworthy floor, not the embarrassing placeholder; it is what keeps early deployments safe while the learning ramps. The path off it — the way the system decides *what to connect* at scale while minimizing human review — is **propose → verify → learn**.

- **Propose.** An LLM reads the resolved fields across sources and proposes connections, including relationship types no one pre-wrote. Proposing is gravity-directed (toward decisions and demand), never exhaustive. Proposing the whole combinatorial space is boiling the ocean: it buries the few connections that signify under thousands that verify but don't.
- **Verify.** Each proposal is checked deterministically against the actual structured data — do the keys resolve, does the relationship hold across rows, is it stable. Proposals that don't verify die automatically, no human. This lever exists only because the data is structured and resolved: the model's guess is grounded in reality. (This is why open-domain LLM knowledge-graph construction, which extracts from prose, is unreliable and ours need not be — our proposals are checkable.) Verification confirms a connection is *structurally valid*, not *meaningful*; whether a verified connection is consequential is a separate judgment, shrunk by learning but not erased.
- **Learn.** What survives is cached. Within a tenant, every confirm/reject fills in that customer's specifics and review drops through the deployment. Across tenants, the *mappings and gravity priors* transfer (§3.2).

**HITL is risk-weighted and decays with precedent** — near-zero for low-stakes, well-precedented connections; retained for high-materiality or novel ones. Minimize, not eliminate: eliminating it is how a confident-wrong connection reaches an agent that acts on it. It is a trajectory — early deployments carry more review because learning needs those decisions as its signal; the dividend arrives with scale, and the asymptote is not 100% (the common systems automate, the homegrown long tail always needs hands).

**Premise: inference keeps getting cheaper.** The consequence is not only affordability — cheap inference makes *redundant, continuous* verification viable: propose with one model, check with another, re-derive on every data change, ensemble against hallucination. That is how an unreliable component becomes a reliable system — brute-force the checking. Cheap inference helps everyone, including a customer running their own model, so the moat is not the LLM; it is §3.2.

**Where the method is developed and proven — the Context Lab.** Propose-verify-learn is not tuned in production; it is developed and hardened in the Context Lab (companion doc): an empirical engine that runs candidate proposer/verifier methods against synthetic enterprises with known ground truth and scores them by whether the connections they find and trust produce right answers. The loop is general — the same method runs whatever the decision or domain; specific cases (attrition, fee leakage, key-person risk) are test material, not the thing built. "General" is earned there, not asserted: a method counts only if it lifts answers on connection-types and questions it was never shown. Winners graduate into the engine above.

**Status.** The proposer is parked (the dormant corrector, §2); live derivation is hand-authored rules. Turning the proposer on, behind verification, is roadmap — not done.

---

## 3.2 The moat — cross-customer learning and the data wall

Where context lives is settled: **move-and-store**. AOS hosts the resolved working-set — triples, graph, mappings — per tenant; source systems stay system-of-record. We host a *resolved derivative*; the customer owns their data. That distinction is the contract and trust line the light footprint depends on.

The compounding asset is the **mapping and gravity-prior layer, not the data**. How the common systems (Workday, Salesforce, NetSuite, …) structure their fields, and which relationship types bear decisions in which domains, is knowledge about systems and patterns — generic and transferable. It compounds across tenants: every deployment makes the next faster and more automated, so per-deployment cost falls with scale. That is the structural edge over the incumbent, whose ontology compounds only *within* a customer and whose every deployment is bespoke and roughly as costly as the last. The thinness anyone can copy; the accumulated library they can't.

**The data wall is an invariant, not a feature.** A hard separation between what transfers and what does not:

- *Transfers (shared, compounding):* generic mappings and gravity priors — common system structure, general relationship types.
- *Never transfers (isolated, per-tenant):* customer data — values, entities, resolved records.

Even mappings can leak: one that only makes sense given a customer's proprietary schema is that customer's IP, not transfer fodder. Transfer the generic, isolate the specific. Get the wall wrong and the moat becomes a breach. Whether customers contractually *permit* the transferable-layer learning is a precondition to the moat accruing at all — it must be addressable in the deployment agreement.

**Proven, not assumed — and where coverage is validated.** That the library transfers is established empirically in the Context Lab, by held-out generalization across tenants and connection-types: a prior earns "transferable" only when it lifts answers on cases it was not built from. Two proofs sit behind the premium claim — the lab proves the *method* (the system finds and trusts well, and generalizes) on synthetic ground truth, needing no customer; a customer deployment proves *coverage* (those connections matter for that buyer's real decisions) on their real questions. The method is provable without a buyer; coverage is where a real buyer is load-bearing.

---

## 3.3 contextOS and the Context Lab — product and engine, different clocks

contextOS is a real, shippable build under active development — **not a temporary bridge** to whatever the lab learns. It ships and sells on the value it delivers now: answers flat retrieval cannot produce, via resolution, traversal, arbitration, and as-of. That value is real whether a connection was found autonomously or derived by an authored, verified rule — the customer gets a correct answer either way.

The Context Lab is the R&D engine that develops and proves the propose-verify-learn method (§3.1) and accrues and validates the cross-customer library (§3.2). What it changes over time is the *cost and autonomy* of producing connections — the share found and trusted without a human, and the per-deployment review that decays with scale. It does not change whether contextOS is real, and it does not gate the product.

Different clocks. contextOS ships, is supported, and improves on its own roadmap now; the lab compounds the moat over a longer horizon, its winners graduating into the product. Learnings are folded *into* the build — they never make the current build provisional or justify under-investing in it. Today's hand-authored edges are the trustworthy floor (§3.1), a real deliverable, not a placeholder to discard when the proposer turns on. Treating contextOS as a stopgap for the lab is a category error: the lab makes the product cheaper and more autonomous to deliver — it is not a substitute for delivering it.

---

## 4. Tiering

### Semantics (base)

- Connect-in-place + transport; field→concept classification; canonical identity resolution; triple store with provenance, conflict detection, and supersession history; served via MCP as resolved triples + provenance.
- **For:** any consumer (agent, BI, human) needing governed, resolved, provenanced data over a connected estate.
- **Proves:** trustworthy resolution and lineage at a days-to-deploy footprint.
- **Goes to market first.** It is shippable and earns the first dollar.

### contextOS (premium)

- Stitched relationship graph; graph-grounded retrieval; authority arbitration; as-of answers; provenance surfaced on demand behind answers.
- **For:** consumers whose questions are answerable only across relationships, time, or conflicting sources.
- **Proves:** answers flat retrieval cannot produce — the correctness lift *is* the product.
- **Premium, on the roadmap.** Positioned with conviction and demonstrable; a real, shippable build under active development; not stated to a buyer as generally available while building.

### Onboarding (spans both)

- Mai-fronted interview → Contour-Map, applied on approve. The deploy-in-days vehicle.

---

## 5. Feature Set & Build State

| # | Feature | Tier | Status |
| :---- | :---- | :---- | :---- |
| 1 | Connect-in-place + transport | Semantics | Have |
| 2 | Meaning + identity resolution | Semantics | Have |
| 5 | Provenance + conflict detection + history (record-level) | Semantics | Finish — complete the initiative, put behind a reveal |
| 7 | Deploy-in-days onboarding (Mai → apply-on-approve) | Both | Finish |
| 3 | Stitched relationship graph | contextOS | Slice delivered — one cross-source join; depth (a second, gravity-gated join) is the increment |
| 4 | Graph-grounded retrieval (the consumer) | contextOS | Slice delivered — but retrieval grabs the whole subgraph; a node-anchored walk is the real traversal increment |
| 6 | Authority arbitration + as-of | contextOS | Arbitration delivered (genuine); as-of present, correctly unused where no time dimension exists |

**Delivered state, honestly (strategy-review audit).** Rows 3/4/6 were carried to a working slice and verified, with three gaps that define the real remaining work:

- *Depth is one cross-source join.* The comp × market edge (internal comp ⋈ external market, the ~13% gap) is the only cross-source resolution in the graph; the org→team hop is structural navigation re-standing on that one join. Depth — chaining a second independent cross-source join where a decision pulls for it (e.g. comp × performance, or comp × backfill-cost) — is the next increment, gravity-gated, not pursued uniformly.
- *Retrieval grabs, it doesn't walk.* The consumer fetched the whole subgraph and let the LLM select edges. Functional at toy scale; it must become a node-anchored walk (a bounded neighborhood from the query anchor) to be traversal and to survive a real graph.
- *Derivation is hand-authored; the proposer is parked.* §3.1 is the path forward.

Arbitration is genuine. As-of was correctly *refused* where the data had no real time dimension, rather than faked — that judgment is the standard, not a miss.

The company is the increments above, plus the propose-verify-learn engine (§3.1) and the cross-customer library (§3.2). Everything else exists or is finish-work.

---

## 6. Build Sequence

Each item is built **and demonstrated** — a demo being an automated sequencing of real, manually-runnable platform operations, with no demo-only logic.

**Foundation (prerequisite to trustworthy context):**

1. **Normalization** — currency / unit-scale / date, on the cross-source path, before conflict detection.
2. **current_triples store** — the canonical current-state store that surfacing, as-of, and normalization read and write. Does not exist today (`mig017:20` is a dead comment; writes go to `semantic_triples`). Establish what removed it before rebuilding.

**contextOS core:**

3. **Stitched graph (#3)** — edges derived across resolved entities; each edge carries information no single record holds.
4. **Graph-grounded retrieval (#4)** — wire `traverse_graph` and graph endpoints into the consumer and a surface; verify the consumer actually traverses.
5. **Authority arbitration + as-of (#6)** — surface as-of off the run-diff API; build arbitration to a decisive value on conflict with disclosure.

**Next increments (from the audit):**

6. **Node-anchored traversal** — retrieval walks a bounded neighborhood from the query anchor; it must not grab the whole subgraph.
7. **A second cross-source join** — gravity-gated depth where a real decision pulls for it; derived by general rules, perturbation-clean; never a manufactured deep path.
8. **Propose-verify-learn engine (§3.1)** — turn the proposer on behind deterministic verification; instrument consumption so the graph grows toward demand; begin accruing the cross-customer mapping/gravity-prior library behind the data wall. Developed and proven in the Context Lab (companion).

**Backstop & onboarding:** complete provenance (#5) behind a reveal; complete Mai onboarding (#7).

---

## 7. Proving context — the demo standard

**The definition.** Context is proven only when the governed answer *diverges* from the raw / direct-feed answer, the divergence runs toward correctness, and it is attributable to a named mechanism. Raw == grounded proves nothing — the scoreboard trap. Raw + citations is provenance, not context. The baseline is not a crippled "raw" — it is Semantics at full strength (the same resolved data), which loses on the *capability* (no graph, no traversal, no arbitration, no as-of), never on access.

**Two families.**

- *Additive* — context answers what raw can't: **resolution** (raw undercounts under name variants), **traversal** (raw can't assemble across relationships).
- *Corrective* — context fixes what raw gets wrong: **arbitration** (raw picks or blends disagreeing sources), **as-of** (raw returns current state for a past period), **coverage honesty** (raw fabricates for what isn't in the store).

**What you demo.** The four answer-better mechanisms — resolution, traversal, arbitration, as-of — each produces a better *answer*. Coverage honesty produces a non-answer ("no data"); it is a trust backstop, never a headline demo. **A headline demo never ends in a refusal.**

**Rules for every demo question.**

- Show the baseline answer wrong, incomplete, or overreaching — *because* the mechanism is absent.
- Show the grounded answer correct, *via* the named mechanism. Declare the mechanism.
- If the baseline isn't wrong, the question proves nothing — change it.
- The baseline (Semantics) is shown at full strength — same resolved data, not handicapped; it loses on capability, never on withheld access. If the only way to make it lose is to take data away or dumb it down, that is a rig — stop.
- Provenance behind a reveal. No source recitation or "audited · rate-limited" chrome as the headline.
- The data is synthetic — engineer the entity to hold the relationships richly; the "baseline loses" condition must be a real enterprise reality (siloed feeds, name variants, a stale source), not a contrivance.

**The two demonstrations.**

- **Engineering attrition — the hero (additive / traversal).** *Where is attrition highest and what's driving it?* Grounded = highest in Engineering → Platform team; compensation + growth-path driven; senior bands ~13% below market — reached by traversing to the comp × market edge. Today this rests on the single comp × market join (§5); deepening it (reasons, performance as additional cross-source joins) is the gravity-gated increment. Semantics has the same resolved facts as separate lists and cannot make the join. The demo must match the site hero — answer richly, never refuse.
- **Cloud spend — the additive counterpart (connect vehicle + agent consumer).** Concentration/drivers via cost → service → team across sources; the FinOps agent answers via DCL-MCP what raw can't assemble. *(Exact question provisional.)*

**Retired.** The AR/AP aging demo, and the coverage-honesty "the data isn't broken out, I can't say" framing as a headline.

---

## 8. Boundaries

What stays out, to protect the footprint:

- **No bulk ingestion** — resolve in place.
- **No access-governance platform** — arbitrate the answer, do not police the estate.
- **No write-back or actions** — that is Convergence.

Crossing any of these turns AOS heavy and collapses the wedge.

---

## 9. Invariants

- Tenant model is 1:1 — each customer enterprise is one tenant. One-to-many is Convergence only.
- AAM connects and transports only. All semantic work — meaning, identity, graph, arbitration — is DCL's. Boundary violations are RACI breaches.
- contextOS / Semantics is AOS-only. No Convergence entities in AOS triples; if they appear, it is a routing bug.
- An edge earns its place only by gravity — decision-dependence × cross-source difficulty. The graph grows toward demand, never by exhaustive coverage.
- Every proposed connection is grounded by deterministic verification against the data before it is trusted. HITL is risk-weighted and decays with precedent — never eliminated.
- **The data wall.** Generic mappings and gravity priors transfer across tenants; customer data never does. Getting this wrong is a breach, not a bug.
- contextOS is positioned with conviction and is demonstrable, but is never represented to a buyer as generally available while on the roadmap.
- **contextOS is a real, shipped product judged on delivered value — not a temporary bridge to R&D.** The Context Lab deepens its autonomy and lowers its cost over time; it never gates the product or makes the current build provisional.
- Subordinate to AOS_MASTER_RACI.
