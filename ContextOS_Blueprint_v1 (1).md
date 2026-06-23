# ContextOS Blueprint v1

**Status:** Governing document. Ranks alongside CLAUDE.md, AAM_Blueprint, AOS_MASTER_RACI for the ContextOS build.
**Date:** 2026-06-10
**Supersedes:** ContextOS_OaaS_Debate_v2 as the build reference. The debate's locked definitions are carried forward; its timeline and hyperscaler analysis are retired.

---

## 1. Charter

ContextOS is the product composition of existing AOS modules plus targeted new builds. It produces a governed, living **context graph** of the enterprise: what data means, how entities relate, where systems disagree, who decides, and what precedent says.

Positioning: Palantir's ambition with Plaid's ergonomics. Hyperscalers (Fabric IQ, Google) built single-stack context layers; ContextOS is the cross-stack context graph deployed in days. Catalogs (Collibra, Alation) stop at Level 2–3; ContextOS sits on top of them, not against them. No competitor does cross-system conflict **reconciliation** as a centerpiece, and every reconciliation compounds into precedent.

Honest-claims rule: we never claim Level 5 formal ontology. We claim Level 3 (shipped), Level 4 (building), and the decision-trace layer (the 2026 differentiator).

## 2. Locked definitions

Carried from the debate (still correct) plus the 2026 vocabulary:

| Term | Definition |
|---|---|
| Semantics | Assigning and preserving contextualized meaning — field, entity, relationship level — with accuracy, confidence, provenance. No formal logic required. |
| Ontology (as we build it) | A structured, queryable knowledge graph of concepts, entities, relationships, constraints. Level 4. Never claimed as Level 5. |
| The spectrum | L1 glossary → L2 taxonomy → L3 semantic layer → L4 knowledge graph → L5 formal ontology. |
| Context graph | Industry term (Foundation Capital 12/2025, Gartner 02/2026): a graph capturing decision traces, workflow logic, and tribal knowledge alongside data relationships. |
| Decision trace | The durable record of how a decision was made: query, context consulted, exception logic, precedent cited, human approval. Captured in the execution path at commit time; cannot be reconstructed after the fact. |
| Bi-temporal | Every fact carries valid-time (true in the world) and ingest-time (when the system learned it). Supersession closes validity windows; nothing is deleted. |
| Conflict Register | First-class queryable catalog of every detected disagreement — system↔system, stakeholder↔stakeholder, stakeholder↔system — with dimension, sources, evidence, resolution status, recommended action. |
| Authority map | Which system is authoritative for which attribute/dimension. Configurable per tenant. Input to reconciliation. |

## 3. The three-layer context model

1. **Semantic layer (L3)** — meaning, confidence, provenance. **Shipped** in DCL: 161 concepts / 12 domains, computed confidence per triple, provenance contract enforced at ingest; store scale: prod 1.4M rows, dev ~100K (verified 06/11 — the prod data plane was reset and re-seeded 2026-04-19 during the current_triples rebuild rollback; no prod data predates that timestamp).
2. **Knowledge graph (L4)** — typed entity↔entity edges, concept hierarchy, entity resolution, constraints. **Building.** Entity resolution is already live (golden records, 5-tier matching, HITL); edges and hierarchy are absent.
3. **Decision-trace layer** — agent queries, conflict resolutions, HITL approvals as queryable precedent. **Seeded** in MCP/HITL audit tables; not yet graph-native or searchable as precedent.

## 4. Architecture: module composition

ContextOS is **not a new repo.** New capabilities land inside DCL per layer-boundary law (AAM = connectivity/transport only; DCL = all semantic interpretation). One net-new module.

| Module | Role in ContextOS | Structural work for v1 |
|---|---|---|
| AOD | Discovery | None |
| AAM | Transport, raw records + provenance | None |
| DCL | The context graph: semantics, KG, temporal substrate, conflicts, traces, MCP | All gates land here |
| NLQ | Human query surface | Consumes new capabilities; no structural work |
| Console | SE operator surface | Conflict drill UI in Gate 1 |
| Farm | Synthetic data / CI; generates demo months | Scenario datasets only |
| **Align (new)** | Stakeholder elicitation agent | Net-new repo, Gate 3 |

**Align placement (decided):** sibling repo `align`. Producer/consumer against DCL, like Farm. Conducts elicitation, writes **proposals** into DCL's HITL queues. DCL retains all semantic ownership. Not inside DCL (different lifecycle/surface); not inside Console (Console faces SE operators, Align faces client stakeholders).

## 5. Capability map and build state

| # | Capability | State (verified by 06/10 code review) |
|---|---|---|
| 1 | Acquisition (AOD/AAM) | Live; zero-access mode absent |
| 2 | Semantic layer | Live; persona-aware execution shipped 06/12 at domain-scoping grain (MCP + resolve + browse, traced); per-persona metric redefinition remains unbuilt — claim language must say "domain-scoped answers," nothing more |
| 3 | Knowledge graph (typed edges, hierarchy, constraints) | Absent — the L3→L4 transition |
| 4 | Entity resolution | Live — the sleeper asset; verify merge-undo |
| 5 | Temporal substrate (bi-temporal) | Seed only (`is_current`, unexposed `previous_run`) |
| 6 | Conflict & reconciliation | Structural half live (hardcoded 6-entry authority table); value half absent |
| 7 | Decision-trace layer | Seed (audit tables; not graph-native, no precedent search) |
| 8 | Elicitation (Align) | 0% — net-new |
| 9 | Living ontology (drift, change proposals) | DB seeds only |
| 10 | Consumption (MCP, NLQ, explorer, exports) | MCP read (6 tools) + NLQ live; export JSON-only; no OWL/RDF/OSI |
| 11 | Governance (RBAC at MCP boundary, approval chains, versioning) | Audit live; rest absent |
| 12 | Deployment (tenants, VPC, packs) | Tenant isolation live; rest absent |

## 6. Bi-temporal substrate (Gate 0 spec)

- Two timelines per fact: `valid_from`/`valid_to` and `ingested_at`/`superseded_at`.
- Supersession closes validity windows. Nothing deleted. Corrections and late-arriving data are first-class.
- `is_current` becomes a derived view preserving exact current semantics — zero consumer changes in Gate 0; consumers migrate to temporal queries in later gates.
- Provenance contract unchanged, enforced at ingest. Canonical snapshot name shape (`{entity_id}-{short_hash}`) enforced at every persistence boundary.
- Run-over-run diff API exposed on top of the temporal model (the `previous_run` pointer exists, unexposed).
- As-of (time-travel) reads at the query layer.
- Gate #54 resolved (06/11 diagnostics): the current_triples rebuild ran in prod Apr 13–19, 2026, and was backed out via a full prod store reset. Its two field-proven failure modes — hours-long backfill transactions and cross-repo `is_active` coupling — are design constraints here: dev migration runs in seconds; prod migration is a separate batched, B19-gated, explicitly approved step; `is_active` survives as a generated column (identical name/semantics; writers fail loudly; Convergence SELECT-only reads unaffected).
- One substrate yields five capabilities: versioning/rollback, time-travel, run diff, drift detection, audit-grade history.

## 7. Knowledge graph (Gate 1 spec, track B)

- Entity↔entity edge storage with types: HAS, GENERATES, BELONGS_TO, REPORTS_TO + tenant-defined custom types.
- Edges are bi-temporal (same columns as facts).
- Concept hierarchy (library is flat today).
- Constraint/cardinality rules per edge type; violations flagged, not silently dropped.
- Traversal queries via MCP and NLQ.
- Acceptance bar: the platform-site hero illustration (typed edges: includes/rate/had/bound_by, inspector panel) renders from real DCL data, not fixtures.

## 8. Conflict & reconciliation (Gate 1 spec, track A)

- **Value-level conflict detection:** same entity, same concept, same period, different values across sources, above configurable materiality threshold.
- **Conflict Register** as a first-class queryable object (the never-built `/api/dcl/conflicts/detect` becomes real).
- **Authority map:** configurable per tenant; the hardcoded 6-entry table dies here.
- **Reconciliation dispositions:** accept-A / accept-B / escalate / manual, through HITL. Every disposition recorded as a decision trace.
- **Decomposition:** reconciling items shown where line-derivable (credits, untagged resources, posting timing, contractor classification); honest "unexplained variance: $X" where not. Empty-tile principle applies — unexplained is information. No demo that only works on a pre-arranged dataset.
- **Precedent:** lookup by conflict class ("this credit-timing class resolved accept-billing twice before"); precedent auto-proposes disposition; promotion of recurring precedent to a standing rule requires approval.
- Conflict drill UI in Console (today: a count with no drill).

## 9. Decision-trace layer (Gate 2 spec)

- Capture at the DCL-MCP boundary: agent identity, query, context served, resolution taken.
- HITL decisions (ER confirmations, conflict dispositions, change approvals) stored graph-native and queryable.
- Precedent search API; trace→rule promotion with approval.
- Strategic basis: decision traces must be captured in the execution path at commit time. DCL-MCP **is** the execution path. The existing MCP audit table is the seed — it is a product asset, not a compliance artifact.

## 10. Consumption & standards

- **MCP** is the primary interface (now Linux Foundation-governed; bet vindicated). Read tools live; add `conflict_query`, `reconciliation_recommend` (Gate 1), trace/precedent tools (Gate 2). Write-with-approval later.
- **Governance at the MCP boundary:** scope enforced per agent identity at query time, not only at the storage layer.
- **Exports (Gate 2):** OWL/RDF (Turtle + JSON-LD) for the graph — client owns it, portable to any compatible platform; OSI/MetricFlow YAML for metric definitions. JSON stays.
- NLQ and the graph explorer consume the same APIs as MCP — one read path, no parallel semantics.

## 11. Living ontology (Gate 3)

- Structural drift detection (schema, hierarchy, new systems) on schedule; value drift near-real-time (Gate 1 engine re-used).
- Change-proposal agent: detected drift → targeted stakeholder message → conversational response → proposed graph change → HITL approval.
- All built on the bi-temporal substrate; no separate versioning machinery.

## 12. Use-case strategy

Market reality (06/2026): proven KG budgets are customer 360, AI grounding/GraphRAG, fraud/AML, compliance, supply chain. Successful deployments start with one narrow lighthouse, not enterprise-wide modeling. ER accuracy below ~85% poisons everything downstream — our live ER+HITL is the precondition, not a side feature.

**Sober acknowledgment:** conflict reconciliation is not a proven standalone purchasing category today; it is bought inside MDM, financial close, and FinOps tools. Our bet: the agent era makes cross-system disagreement an agent-correctness problem. That is a thesis. The Conflict Register is therefore the **differentiator inside proven use cases**, not the headline category.

| Use case | Role |
|---|---|
| Agent grounding via MCP | **Lead.** Budget flows here now; we are already in the execution path. |
| FinOps / cloud-spend reconciliation | **Lighthouse.** Narrow, recurring, decomposable; the standing `cloud_spend` vehicle. |
| Customer 360-lite (ER) | **Supporting proof.** Demonstrates golden records; do not pitch against MDM incumbents. |
| Metric consistency / persona | **Defensive.** Fix persona execution; don't lead — warehouse vendors give it away. |
| Fraud/AML, supply chain, drug discovery | **Don't chase.** |

## 13. The demo (Gate 1 definition-of-done)

**Form:** ground the agent, before/after, same model, same data access, presented in the platform-site hero experience (live graph traversal + answer panel + entity inspector), fed by real DCL data.

**Before panel (must be genuinely un-hobbled):** competent agent, full raw access to both feeds, decent prompt, plain text answer. Expected failure mode is not ignorance — it is **unauditable arbitration**: one number, confidently, having silently picked a source or blended. If the model notices the disagreement, fine — it still cannot decompose, name authority, cite precedent, or show an approval trail. Panel B wins on governance, not on noticing.

**After panel (via DCL-MCP):** governed answer; conflict disclosed and decomposed; authority map cited; HITL disposition; precedent on repeat occurrences; drill to source record IDs, confidence, ingest time.

**Question 1 (headline):** "Where is attrition highest and what's driving it?" — multi-hop (departures → exit themes → comp policy → market comp) and carries the conflict beat naturally: HR system vs finance cost-center rollup disagree on headcount → denominator differs → 22% vs 19% attrition → disclosure, authority, precedent. Workforce domain (within SE-parity rich-domain set).

**Question 2:** cloud_spend — "March compute spend for FluxEdge": $412K (billing) vs $387K (GL); decomposition: credits not yet posted, untagged resources, unexplained remainder stated honestly. FinOps agent as consumer.

**Eval harness:** 8–10 questions over Farm-generated months with varied conflict classes; scored on correctness, provenance presence, conflict disclosure. Prospect picks the month. Same artifact = demo + regression suite + grounding proof.

**Honesty constraints:** new-way entity, no SE fallback, all tiles honest. No live SAP/Salesforce connection implied — feeds are "shaped to vendor export formats" and stated as such. SAP-vs-Salesforce survives as a positioning sentence describing the problem class, never as the staged demo. AR/AP framing banned.

## 14. Deployment tiers

1. **Schema-only (zero trust):** client-exported dictionaries/dumps processed locally; no standing access; one-time engagement.
2. **Perimeter:** value-level monitoring reads values inside client VPC or via hashes/aggregates. Resolves the contradiction between "no business data stored" and value-conflict detection — stated explicitly, not papered over.
3. **Live:** standing scoped connections, continuous monitoring, change-proposal agent, instant revocation, real-time access dashboard.

Days-to-deploy holds because Discover-grade capability is mostly the existing canonical v4 path; the genuinely new builds are scoped in §15.

## 15. Build sequence

| Gate | Content | Definition of done |
|---|---|---|
| **0** | Bi-temporal substrate (§6); subsumes #54 root-cause + rebuild | Suite green (verbatim pytest summary); FluxEdge-TMZ8 parity (24,165 active triples / 259 concepts) through compatibility view; one supersession case end-to-end |
| **1A** | Value conflicts + Register + authority map + reconciliation + precedent (§8) | The demo (§13) runs on prospect-picked month |
| **1B** | Typed edges + hierarchy + constraints (§7) — parallel with 1A | Hero illustration renders from real DCL data |
| **2** | Decision traces (§9); persona-aware query execution (removes unbacked demo claim); OWL/RDF + OSI exports; `conflict_query` / `reconciliation_recommend` MCP tools | Precedent search live; persona demo real or claim retired; exports validate against W3C spec |
| **3** | Align module; Live monitoring; RBAC at MCP boundary + approval chains | Contour Map produced through HITL; drift → proposal → approval loop closed |

AOD/AAM/Farm/NLQ require no structural work. DCL-centered build + one new repo.

## 16. Out of scope / standing bans

- AR/AP and the Acme invoice identity-resolution case: never as demo, proof, or example.
- Level 5 formal ontology claims.
- Convergence: zero presence in AOS code; ME/MerCas/Meridian/Cascadia naming banned outside the Convergence repo.
- Cosmetic fixes: no demo state that works only via old-SE fallback or pre-arranged data.
- New repos duplicating DCL semantics.

## 17. Open risks

| Risk | Disposition |
|---|---|
| Hyperscaler window compressing (Fabric IQ shipping, Google entered) | Cross-stack + days-to-deploy is the surviving flank; speed of Gates 0–1 is the mitigation |
| Conflict Register is a thesis, not a proven budget line | Mitigated by leading with agent grounding (§12); register is the demo beat, not the category claim |
| Website hero shows typed edges code can't serve | Gate 1B makes it true; until then a technical buyer asking for live traversal gets a Sankey — known exposure |
| Persona-aware demo claim unbacked by code | RESOLVED 06/12 by narrowing: domain-scoped persona execution is built and traced; the metric-redefinition example (2,400-vs-8,100) stays unclaimable pending a live persona-definitions implementation (dead data today, deferred) |
| Frontier model in demo Panel A notices the conflict | Script anticipates it; Panel B wins on decomposition/authority/precedent/audit, not on noticing |
| Farm business-data push previously landed nothing (farm #16, 410-GONE endpoint) | Demo datasets depend on Farm → verify the fixed path feeds the canonical route before Gate 1 demo work |
