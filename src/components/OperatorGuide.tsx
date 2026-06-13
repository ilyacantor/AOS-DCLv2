// Operator Guide — the end-to-end DCL + Demo walkthrough, stage by stage.
// Plain English, written for an operator. Replaces the deprecated UserGuide.

export function OperatorGuide() {
  return (
    <div className="h-full overflow-auto bg-background p-8">
      <div className="max-w-4xl mx-auto space-y-8">

        <div>
          <h1 className="text-3xl font-bold text-foreground mb-2">DCL Operator Guide</h1>
          <p className="text-muted-foreground">
            How your data moves through DCL, end to end — and how the Demo proves it.
          </p>
        </div>

        {/* ── What DCL does ───────────────────────────────────────────── */}
        <section className="space-y-3">
          <h2 className="text-xl font-semibold text-foreground border-b border-border pb-2">
            What DCL does, in one line
          </h2>
          <p className="text-foreground/90 leading-relaxed">
            DCL takes raw data from many systems, works out <strong>what each value
            actually means</strong> to the business, notices where systems
            <strong> disagree</strong>, and keeps a living, governed picture of the
            enterprise that an agent or a person can ask questions of — with every
            answer carrying its source, its confidence, and its history.
          </p>
          <p className="text-foreground/90 leading-relaxed">
            The rest of this guide follows one batch of data through the six stages
            below. Each stage maps to a tab along the top of the screen.
          </p>
        </section>

        {/* ── The journey, stage by stage ─────────────────────────────── */}
        <section className="space-y-6">
          <h2 className="text-xl font-semibold text-foreground border-b border-border pb-2">
            The journey of your data — six stages
          </h2>

          <Stage
            n="1" name="Ingest" tab="Ingest tab"
            what="Data arrives. Either an upstream system (Farm, or a real source through AAM) pushes it in, or records are loaded directly. Every batch is tagged with who it is about (the entity) and where it came from (the source system)."
            see="The ingest log and the list of snapshots — each snapshot is one entity's data as of one run."
            doNote="Nothing to do by hand on the happy path — ingest is automatic. If a batch is missing its entity or source tag, DCL rejects it loudly rather than guessing."
          />

          <Stage
            n="2" name="Normalize" tab="happens automatically"
            what="DCL turns cryptic source fields into canonical business concepts. A column called KUNNR or cust_rev_ytd becomes Account or Revenue. Every value keeps a link back to the exact source field it came from, plus a confidence score for how sure DCL is."
            see="The mappings and triples views — each row is one fact: an entity, a concept, a value, where it came from."
            doNote="Review low-confidence mappings if a number looks wrong. The source link tells you exactly which field fed it."
          />

          <Stage
            n="3" name="Semantics" tab="Graph tab"
            what="Concepts gain meaning and relationships — the knowledge graph. DCL knows Revenue rolls up to the CFO, which entities relate to which, and which systems are authoritative for which facts. This is the layer that lets a question get a grounded answer instead of a guess."
            see="The graph: business concepts as nodes, relationships as links, with the source systems behind them."
            doNote="Use it to trace how a concept connects — what feeds it, who owns it, what it relates to."
          />

          <Stage
            n="4" name="Context" tab="Context tab"
            what="The full picture of one entity. Pick a snapshot and DCL shows how complete it is — how many business areas (domains) are populated, what is known and what is missing. Missing is information too: an empty area is shown honestly, not hidden."
            see="Domain coverage (e.g. 'Domain Coverage 12 / 38'), the contextualization summary, and the entity's concepts."
            doNote="Pick the snapshot you want to inspect from the selector at the top. Following 'latest' tracks the newest run; pin one to hold it."
          />

          <Stage
            n="5" name="Reconcile" tab="Context / Recon tab"
            what="Where two systems claim different values for the same fact — SAP says $100, Salesforce says $110 for the same revenue, same period — DCL flags a conflict. You drill in, see both claims side by side with their sources, and decide (the Conflict Register). The authority map says which system wins by default; a resolved conflict becomes precedent for next time."
            see="The Conflict Register: each row is one disagreement, with the claims, the materiality, and a recommended resolution."
            doNote="Type a short reason, then accept the authoritative source. The losing value is superseded (kept in history, not deleted), and the decision is recorded as a trace."
          />

          <Stage
            n="6" name="Monitor" tab="Monitor tab"
            what="DCL watches for change over time. Structural drift = a field appears or disappears between runs. Value drift = sources that used to agree start disagreeing. Each finding becomes a change proposal. The checks run on a schedule you can see and pause."
            see="The two drift jobs with their last-run time and on/off state, and the drift findings in the Change Proposals list."
            doNote="Approve a proposal to fold the change into the graph (with full provenance), or reject it. Pause a job from this tab whenever you want the checks to stop."
          />
        </section>

        {/* ── Around the edges ────────────────────────────────────────── */}
        <section className="space-y-3">
          <h2 className="text-xl font-semibold text-foreground border-b border-border pb-2">
            Two things that wrap the whole flow
          </h2>
          <p className="text-foreground/90 leading-relaxed">
            <strong>Onboarding (Mai).</strong> Before data flows, Mai interviews
            stakeholders in chat to learn the org structure, which system is the source
            of truth for what, the local vocabulary, and known disagreements. What she
            learns lands as proposals in the same queue you approve from — nothing she
            captures becomes official until a person approves it.
          </p>
          <p className="text-foreground/90 leading-relaxed">
            <strong>Governance.</strong> Agents and tools connect under a named
            identity that is scoped to only the tools, business areas, and personas it
            is allowed to touch — anything outside its scope is refused, loudly, and
            recorded. Sensitive approvals can be configured to require two different
            people (the proposer cannot approve their own change).
          </p>
        </section>

        {/* ── The Demo ────────────────────────────────────────────────── */}
        <section className="space-y-4">
          <h2 className="text-xl font-semibold text-foreground border-b border-border pb-2">
            The Demo — proving it, before and after
          </h2>
          <p className="text-foreground/90 leading-relaxed">
            The <strong>Demo tab</strong> runs a before/after on the same question, the
            same data, and the same model. You pick the entity (and the month). Every
            number is pulled live from DCL — nothing is staged.
          </p>
          <div className="grid md:grid-cols-2 gap-4">
            <div className="bg-card border border-border rounded-lg p-4 space-y-2">
              <h3 className="font-medium text-foreground">Panel A — raw agent</h3>
              <p className="text-sm text-foreground/80 leading-relaxed">
                A capable AI with full, direct access to both feeds. It answers — but it
                cannot show its work. It quietly picks one source or blends them, and
                cannot decompose the difference, name which system is authoritative,
                cite precedent, or show an approval trail.
              </p>
            </div>
            <div className="bg-card border border-border rounded-lg p-4 space-y-2">
              <h3 className="font-medium text-foreground">Panel B — through DCL</h3>
              <p className="text-sm text-foreground/80 leading-relaxed">
                The same question, governed. It discloses the conflict, decomposes the
                variance (credits, timing, untagged items — and an honest "unexplained"
                where it cannot), names the authoritative system, cites precedent on
                repeats, shows the approval trail, and drills to the source records.
              </p>
            </div>
          </div>
          <div className="bg-muted/30 border border-border rounded-lg p-4">
            <h3 className="font-medium text-foreground mb-1">What a good run looks like</h3>
            <p className="text-sm text-foreground/80 leading-relaxed">
              Both panels answer. Panel B shows a provenance badge on every answer, its
              numbers match the DCL ground truth, it discloses each conflict, and where
              there is no data it says so plainly rather than inventing a figure.
            </p>
          </div>
        </section>

        {/* ── Quick reference ─────────────────────────────────────────── */}
        <section className="space-y-3">
          <h2 className="text-xl font-semibold text-foreground border-b border-border pb-2">
            Quick reference — the tabs
          </h2>
          <div className="bg-card border border-border rounded-lg divide-y divide-border text-sm">
            <Row tab="Ingest" use="Watch data arrive; see ingest log and snapshots." />
            <Row tab="Graph" use="Explore the knowledge graph — concepts, relationships, sources." />
            <Row tab="Context" use="Inspect one entity: domain coverage and what's known." />
            <Row tab="Recon" use="Reconcile conflicts where sources disagree." />
            <Row tab="Monitor" use="Drift detection over time; approve/reject change proposals; pause the schedule." />
            <Row tab="Dashboard" use="Persona dashboards (CFO/CRO/COO/CTO) over the current data." />
            <Row tab="Demo" use="Run the before/after grounded demo." />
            <Row tab="?" use="This guide." />
          </div>
        </section>

        <p className="text-xs text-muted-foreground pt-4 border-t border-border">
          DCL — the context layer of the AutonomOS platform. Every answer carries its
          source, confidence, and history; nothing is deleted, only superseded.
        </p>
      </div>
    </div>
  );
}

function Stage({ n, name, tab, what, see, doNote }: {
  n: string; name: string; tab: string; what: string; see: string; doNote: string;
}) {
  return (
    <div className="space-y-2">
      <h3 className="text-lg font-medium text-foreground">
        <span className="text-primary font-semibold">Stage {n} · {name}</span>
        <span className="text-muted-foreground text-sm font-normal"> — {tab}</span>
      </h3>
      <p className="text-foreground/90 leading-relaxed">{what}</p>
      <p className="text-sm text-foreground/70"><strong className="text-foreground/90">You'll see:</strong> {see}</p>
      <p className="text-sm text-foreground/70"><strong className="text-foreground/90">You'll do:</strong> {doNote}</p>
    </div>
  );
}

function Row({ tab, use }: { tab: string; use: string }) {
  return (
    <div className="flex gap-4 px-4 py-2">
      <span className="font-mono text-primary w-24 shrink-0">{tab}</span>
      <span className="text-foreground/80">{use}</span>
    </div>
  );
}
