# Context Lab — Blueprint v0.5

**Status.** Draft, to iterate. The empirical engine that develops and proves a general capability: a system that, on its own, finds the cross-source connections enterprise decisions depend on and trusts them. Feeds the propose-verify-learn engine (contextOS Blueprint §3.1) and the cross-customer library (§3.2). Subordinate to the contextOS Blueprint and the RACI.

**Companion:** `context_lab_FS1_instantiation.md` — one fully worked instance (a synthetic financial-services firm). The blueprint is the method; the companion is test material.

**v0.4 → v0.5.** Makes the **methodology legible as general** (§1.1–§1.2): the lab builds one reusable loop — propose, verify, trust — that is indifferent to which connection or decision it handles; any use case (attrition, fee leakage, key-person risk) is *test material*, never the product. Sharpens how generality is *proven* — held out across connection-types (§4, §9). Corrects the OEC base rate on a pure gravity set (§3.2). Splits what synthetic ground truth proves from what a customer pilot validates (§7, §12).

---

## 1. Mission — the only thing the lab is for

> **Get the system to find the connections worth making on its own, and trust them.**

Two halves, and nothing outside them:

- **Find** — the system autonomously proposes the connections that matter (decision-bearing, cross-source), without a human telling it what to connect.
- **Trust** — the system autonomously knows which proposals are right and worth surfacing, without a human checking each one.

Everything in this lab serves find, trust, or both. Anything that serves neither is out of scope, by construction. Focus is the constraint that makes progress — a narrow mission, measured hard.

### 1.1 The method is general; a use case is test material

The product is not an answer to any one question. It is a **method** — a loop that, for *any* decision-question over a resolved base, finds the cross-source connections the answer needs and trusts the ones that hold. That loop does not know or care whether the case is attrition, fee leakage, or key-person risk; it runs the same way on all of them.

Concrete cases appear throughout this lab because a general method can only be **scored** against concrete questions with known answers — abstract methodology has no number. So a case like attrition is a worked example used to test and harden the loop, never the thing being built. **Where a section reads as "solve attrition," read it as "here is one question we ran through the general loop to measure it."**

### 1.2 The loop, in general terms

For any decision-question over the resolved base, the method:

1. **Proposes** the cross-source connections the answer would require.
2. **Verifies** each against the structured data.
3. **Trusts** the connections that hold; rejects the ones that don't (including planted decoys).
4. **Escalates** only the few it can't verify.

The same four steps run whatever the question. What the lab develops and hardens is this loop — better proposing (find) and better verifying and calibration (trust). What it does *not* do is accumulate hand-authored answers question by question; that is the services-shop trap, and it is out of scope (§6.5). Generality is a property of the loop, proven by running it across many connection-types and on connections it was never shown (§4).

---

## 2. The core asset — the ground-truth backtest. Build this first.

Find-and-trust is "unsolved" largely because nobody can cheaply measure it — real data has no answer key. Farm gives us one: we generate the synthetic case, so we know which connections are worth making, which are traps, and the right answers to the questions that matter. That makes any method automatically scoreable. The harness is the product; nothing else is built until it scores a baseline end-to-end.

---

## 3. Objectives

Every experiment carries these before it runs; results are read against them, never rationalized after.

### 3.1 Hypothesis — if / then / because

A falsifiable claim that names the mechanism. *If [change to how the system finds or trusts], then [answer-lift moves], because [why].* The "because" forces a theory, so a null result still teaches.

### 3.2 OEC — answer-lift

The one metric. It *is* the mission made measurable: did the connections the system found and trusted on its own produce right answers. **Held constant across every experiment**, so a change in score is attributable to the connections and nothing else:

- **The answerer** — one fixed model/agent that reads the context available to it and produces an answer.
- **The grader** — one fixed automatic judge that scores an answer against the question's answer key; correct only if it contains every required element and no disqualifying error. Versioned, never tuned per experiment.

For each question *q* in the gravity set Q:

- `base(q)` — the fixed answerer's graded answer reading **only the Semantics base** (resolved entities, no discovered connections).
- `method(q)` — the **same** answerer's graded answer reading the graph **the method under test produced**.

```
Answer-lift = correct_rate(method) − correct_rate(base)   over Q
correct_rate = (questions graded correct) / |Q|
```

One number, in percentage points; reported per block and overall with the variance from replication (§4). Because every gravity question is base-fails by admission (§3.5), the base scores ≈0 on Q — so **answer-lift on Q is essentially the method's own correct-rate on Q**. *E.g., base 0/12, method 10/12 → answer-lift = +83 pts; the decoy guardrail then decides whether it stands.* Base-answerable questions (a single-source total, a relationship owner) are tracked **separately, as a no-regression guard** — the method must not break the easy ones — never folded into answer-lift.

Q contains only gravity questions (§3.5), so the OEC is structurally about the right thing. **It is the first focus guardrail — off-mission work doesn't move it.** Confident-wrong is not netted in; it is a separate guardrail (§3.4) that can veto a positive result.

### 3.3 Secondary (diagnostic)

Edge precision/recall; resolution accuracy; HITL rate; traversal depth used; noise rate. Each experiment's OEC movement is **attributed to find or trust** — "it helped somehow" is never a result.

### 3.4 Guardrails — boundaries no experiment may cross

Confident-wrong rate must not rise (the cardinal one — an agent acting on a confident-wrong connection is the failure that ends a trust product). Generalization gap (Farm-to-real divergence) stays bounded. Data-wall integrity (no customer data crosses tenants). Provenance integrity. Verification grounding (no proposal trusted without passing the deterministic check). **HITL rate is the autonomy guardrail** — a win bought by leaning *more* on human review has not advanced the mission.

### 3.5 Gravity — what earns a place in the question set

The OEC's integrity rests on Q. A question is admitted only if **all four** hold:

1. **Decision-bearing** — a real enterprise decision turns on the answer (retention, spend, pricing, risk, pipeline, people). Not trivia, not a metric lookup.
2. **Cross-source** — the correct answer requires connecting facts from two or more sources; no single system holds it. This is what makes it test find-and-trust, not a within-source query.
3. **Gradable answer** — a definitive correct answer exists, with required elements, checkable automatically.
4. **Base-fails** — the Semantics base answers it wrongly or not at all. If the base already answers it, it doesn't test the lift — excluded.

**Anchored outside the automated loop (§13).** The questions and answer keys are set and validated by a person now, drawn from real decisions, and migrated to real customer questions as the anchor matures; held out from the proposal methods during a run; chosen because they matter, never because they are the ones a method can answer. The set is never edited to flatter a method. The instances are §14.

---

## 4. Experimental design — the four levers, offline form

The lab tests methods over datasets and seeds, not users over traffic. **Control:** every method measured against the current best and the Semantics baseline; answer-lift is always a delta. **Randomization:** datasets, entities, questions randomly held out and sampled — the test set is drawn, not chosen. **Replication:** LLM proposers are stochastic — multiple seeds and entities; the verdict is the effect *and its variance*. **Blocking:** compare like with like (vertical, schema messiness, relationship type) so a method that only shines on easy data is caught.

**Generality is proven, not asserted.** Questions and connection-types are held out, and a method earns the word "general" only by lifting answer-lift on connection-types and questions it was **never shown**. A method that only works on the cases it saw is caught here — this is the difference between a general loop (§1.2) and a memorized answer. Same design graduates to live experiments once there's product traffic, same OEC and guardrails.

---

## 5. Technical design — infrastructure to measure objectively and to enforce focus

The objectives and the focus guardrails are wired into the harness, not left to judgment. It **computes** the OEC, secondaries, and guardrails on every run; **enforces** the design (randomized held-out sets, required seeds, tagged blocks) so an uncontrolled run produces no number; **requires** the hypothesis pre-registered; **auto-rejects** any run that breaches a guardrail or fails the focus gate (§6), regardless of OEC movement; and **logs** every experiment to a ledger — hypothesis, design, OEC vs control with variance, find/trust attribution, guardrail status, verdict. The experimenter cannot move the number by choosing the test set, the seed, or the interpretation. Build this layer before any method is explored.

---

## 6. Focus guardrails — keeping agents on the mission

The mission is narrow on purpose. Two drift modes to prevent: agents building data that doesn't test find-or-trust, and agents running experiments that improve something else. The OEC is the first line of defense; the rules below are enforced by the harness and the governor, because agents can still burn effort on — or game — the wrong things.

### 6.1 The focus gate — every dataset and every experiment, before it runs

One question, answered honestly, or it does not proceed:

> *Does this make the system better at finding connections worth making on its own, or at trusting them on its own?*

No → not built, not run. The governor checks it before commissioning a dataset or pre-registering an experiment. The mission sentence (§1) sits at the top of every governor and subagent prompt.

### 6.2 Synthetic data generation — guardrails

**A dataset's only job is to pose a find-and-trust problem with a known answer — not to look like a realistic enterprise.** The harness rejects any dataset that fails these:

- **Labeled target connections.** The cross-source connections worth making are specified ground truth. No labeled targets → can't test find → rejected.
- **Targets are cross-source.** Each requires resolution across sources; no single record holds it. Single-source foreign-key mirrors don't test the mission → rejected.
- **Adversarial decoys, mandatory.** Connections that verify but carry no gravity (to test rejection), and connections that look right but are wrong — stale, conflicting, coincidental (to test that trust isn't fooled). Trust can only be tested against things that should *not* be trusted. No decoys → tests find but not trust → incomplete.
- **A gravity question set.** Decision-bearing questions whose correct answers *require* the target connections. If the questions can be answered without the targets, the dataset doesn't test the mission.
- **Minimal-adversarial, not rich.** Every element of complexity must map to a find-or-trust challenge it creates — name variants, conflict, missing data are in *because* they make finding or trusting harder. Detail that doesn't change find/trust difficulty (transaction volume, UI states, narrative flavor) is cut. The right dataset is the smallest one that poses the hardest find/trust problem.
- **Vast and diverse means breadth of challenge, not bloat per case.** The library spans many kinds of connection-to-find and trust-trap across domains and verticals; each dataset stays minimal-adversarial. Diversity of the problem space; parsimony within each case.

### 6.3 Discovery experimentation — guardrails

**Every experiment improves the system's autonomous finding or trusting — or it doesn't run.**

- **Find or trust hypothesis.** The if/then/because must predict a gain in autonomous finding or trusting, measured by answer-lift attributed to that half. Anything else fails the focus gate.
- **Gains from autonomy, not crutches.** The mission is the system doing it *on its own*. A method that lifts answer-lift by adding human review or hand-authoring has not advanced the mission; the HITL guardrail catches it.
- **Find and trust attributed separately.** Each experiment targets find, trust, or explicitly both; the harness attributes the movement.
- **Out of scope, auto-rejected.** Real work, but not this lab's R&D — the binding list is §6.5. If it isn't making the system find or trust connections better on its own, it isn't a lab experiment.

### 6.4 Drift detection

The governor audits the ledger on a cadence. If recent experiments aren't moving answer-lift through find or trust — effort is going to peripheral tuning, or datasets are sliding toward flavor — it flags drift and re-anchors the agents to the mission sentence. Activity is not progress; movement on find and trust is.

### 6.5 Out of scope — the binding list

The lab runs no experiment on these. Enforced by the focus gate (§6.1) and the harness; anchored outside the loop and extendable as new drift modes appear (§13).

**Not the lab's problem — other parts of the system own these.**

- **Identity / entity resolution** — the lab takes resolved entities as input; Semantics provides them. The lab is about connections *over* resolved entities, not the resolving.
- **Connection, transport, discovery** — AAM / AOD.
- **Provenance, audit, governance plumbing** — product features, enforced here as guardrails, never experiment targets.

**Not find-or-trust — they don't move the mission.**

- **Presentation** — visualization, UI, graph rendering, dashboards, answer formatting.
- **Performance / ops** — resolution speed, query latency, throughput, infra scaling. (Cost is a soft guardrail, not a target.)
- **The answerer** — NLQ parsing, prompt wording, answer tone; held constant (§3.2). The lab measures the connections, not the talker.
- **Coverage breadth** — more sources or integrations for their own sake. Breadth is not find-quality.
- **Demo / sales artifacts.**
- **Data realism for its own sake** — richer synthetic enterprises than the find/trust problem requires (also §6.2).

**Forbidden — gaming the mission. Auto-rejected and flagged.**

- **A human crutch** — lifting answer-lift by adding human review or hand-authoring connections. Autonomy is the mission; the HITL guardrail (§3.4) catches it.
- **Tuning the test** — editing the grader, the answerer, or the question set to raise a score. The test is anchored outside the loop; changing it to win is the cardinal violation.
- **Overfitting to planted answers** — optimizing to Farm's known answers in ways that don't transfer to real data (§7); the generalization guardrail catches it.

---

## 7. What synthetic ground truth proves, and what a customer pilot validates

The synthetic backtest proves the **method**, on ground truth we own: that it finds and trusts well, is nearly autonomous (the HITL rate, §3.4), and **generalizes to held-out connections** (§4). That is the defensible capability, and it needs no real data to establish.

What synthetic data **cannot** self-certify is **coverage of a specific customer's real questions** — whether the connection-types we planted match the distribution of decisions that customer actually makes. That is validated in a **pilot**: point the proven method at the customer's resolved base and their real questions, and measure the same answer-lift. The generalization-gap guardrail bounds synthetic-to-real drift once real data is in hand.

Two proofs: the lab earns *"general and trustworthy"*; the pilot earns *"covers the majority of your questions."* Circularity is avoided because the method is proven to generalize to connections it was never shown (§4), not merely to recover what we planted.

---

## 8. Datasets

Generated in Farm under the §6.2 guardrails. Vast and diverse across the find/trust problem space and across domains/verticals — but apply the lab's own focus to itself: **start with the one vertical where there is demand and a path to real-data calibration**, and widen only as the harness proves out. The first worked instance is FS-1 (companion doc).

---

## 9. What gets explored — variants of the one loop, across the problem space

Only ways to make the loop (§1.2) find or trust better. Not new products, and not use-case verticals as such — the use cases are test material (§1.1). The §6.5 out-of-scope list is binding.

- **Finding** — relationship-type identification; proposal methods (LLM proposers, prompt and schema-aware strategies, ensembles); gravity-direction (proposing toward decisions, never exhaustively).
- **Trusting** — verification strategies (grounding a proposal against structured data); telling correct-and-consequential from merely-computes; calibration (knowing when a proposal should *not* be trusted); what transfers as a generic, trustworthy pattern across tenants.

Every method is measured across connection-types and on held-out questions (§4); a method that only works on the cases it was shown does not graduate.

---

## 10. Execution — governors and subagents

One or several governors own experiment arcs; CC subagents run individual methods. The focus gate (§6.1) and the harness (§5) keep it from becoming research-theater: mission sentence atop every prompt, every experiment pre-registered and on-mission, every result a verdict against control, every guardrail enforced automatically. Inference keeps cheapening, so the lab can afford many methods and redundant verification — brute-force the search, on-mission.

---

## 11. What compounds

Winners graduate into the propose-verify-learn engine (§3.1). The durable output — find-and-trust methods, gravity priors, per-vertical mapping libraries — is the cross-customer transferable asset (§3.2), behind the data wall: generic patterns transfer, customer data never does. The lab builds the moat.

---

## 12. Lab-design risks

- **Circularity** — no method trusted on Farm recovery alone; generality is proven on held-out connections (§4), and real-world coverage is validated in a pilot (§7).
- **Sequencing** — the lab is built and run on synthetic ground truth; the customer pilot validates coverage (§7). Pursue the pilot in parallel — coverage of a real question distribution is the half synthetic can't certify, not a reason to delay the build.
- **Measurement before exploration** — harness, OEC, guardrails, and focus gate exist and score a baseline before any swarm runs.
- **Scope** — one vertical first; widen on proof, not ambition.

---

## 13. Open decisions

- **The gravity question set's anchor** — set outside the automated loop, never by it: a person curates and validates representative, held-out, ungameable questions and answers now; the anchor migrates to real customer questions as it matures. The optimizer uses Q to score; it never defines Q. Most consequential.
- **The out-of-scope list's anchor** — the harness *enforces* it automatically; a person *sets* the boundary and audits that the swarm isn't gaming it, with authority to extend it as new tangents appear. The thing being graded does not get to redefine what's allowed.
- Which answerer is fixed as the controlled factor (and when a model upgrade is allowed to reset the baseline).
- The bound on the generalization-gap guardrail.
- Which vertical first; where the real-data anchor comes from.
- Compute envelope and number of parallel governors.

---

## 14. Instances across connection-types (test material)

Use cases used to test and harden the loop — illustrations of the general method (§1.1), not categories of product. Each plants a cross-source connection a decision depends on, with a known answer and decoys. The set spans connection-types on purpose, so a method is proven across them and on held-out ones, not on one. The first fully worked instance — a synthetic financial-services firm with planted truths, decoys, objectives, and answer keys — is the **FS-1 companion doc**.

| # | Question | Connection it requires (cross-source) | Answer key — required elements | Why the base fails |
|---|---|---|---|---|
| 1 | Where is attrition highest, and what's driving it? | exits/org (HRIS) × compensation (HRIS) × external market (benchmark) | names the concentrated team; names compensation as driver; cites the senior-band gap vs market | has exits, comp, market as separate facts; can't join comp to market to substantiate the driver |
| 2 | Which team is driving cloud-cost growth, and is the spend justified by their output? | cloud billing (cost) × resource ownership (tagging/CMDB → team) × output (delivery/usage) | names the team; isolates the service driving growth; states whether spend tracks output | cost, ownership, output in different systems; can't attribute cost → team → output |
| 3 | Which at-risk renewals are worth saving? | product usage (telemetry) × contract value (CRM/billing) × support burden (ticketing) | lists accounts with declining usage AND high value AND rising support load | usage, value, support are separate; can't combine into a risk-weighted list |
| 4 | Is our revenue dangerously concentrated, and where? | revenue (GL/billing) × customer identity (CRM), reconciled across systems that name customers differently | top-N customers as % of revenue; flags concentration over threshold; correct only after reconciling variant customer identities | revenue and customer identity disagree across billing/GL/CRM; can't reconcile to a true concentration |
| 5 | Which deals in the forecast are not actually progressing? | CRM stage × real activity (email/calendar/product touch) × historical conversion | flags deals marked committed with no real activity or below the historical-conversion pattern | stage, activity, history are separate; base takes the CRM stage at face value |
| 6 | What software are we paying for that nobody uses? | spend (finance/AP) × entitlements (IdP/license) × actual usage (logs/SSO) | lists tools with spend and licenses but ~zero usage | spend, licenses, usage in different systems; can't cross them |
