// Operator Guide — the end-to-end DCL + Demo walkthrough, stage by stage.
// Plain English, written for an operator. Every claim here is checked against
// what the tab actually does — no aspirational descriptions.

export function OperatorGuide() {
  return (
    <div className="h-full overflow-auto bg-background p-8">
      <div className="max-w-4xl mx-auto space-y-8">

        <div>
          <h1 className="text-3xl font-bold text-foreground mb-2">DCL Operator Guide</h1>
          <p className="text-muted-foreground">
            What each tab actually does, in the order data moves through it — and how the Demo proves it.
          </p>
        </div>

        {/* ── What DCL does ───────────────────────────────────────────── */}
        <section className="space-y-3">
          <h2 className="text-xl font-semibold text-foreground border-b border-border pb-2">
            What DCL does, in one line
          </h2>
          <p className="text-foreground/90 leading-relaxed">
            DCL takes raw data from many systems and turns each value into a
            <strong> business fact</strong> — a concept, a value, the source it came
            from, and a confidence score. It keeps every fact's history, flags where
            two systems <strong>disagree</strong>, and exposes the result for NLQ and
            agents to query. The DCL tabs below are how you <em>inspect and govern</em>
            what DCL knows; the actual question-answering happens downstream in NLQ and
            the agents that read from DCL.
          </p>
          <p className="text-foreground/90 leading-relaxed">
            The tabs are listed below in the order data flows. Each section says what
            the tab shows and what you can do there — nothing it doesn't.
          </p>
        </section>

        {/* ── The journey, stage by stage ─────────────────────────────── */}
        <section className="space-y-6">
          <h2 className="text-xl font-semibold text-foreground border-b border-border pb-2">
            The journey of your data, tab by tab
          </h2>

          <Stage
            n="1" name="Ingest" tab="Ingest tab"
            what="Data arrives. An upstream system pushes a batch in, tagged with which entity it is about and which source systems it came from. DCL classifies each incoming field/record into business concepts as it lands — that classification is what you browse in the next two tabs."
            see="The ingest log: one row per batch — timestamp, entity, the sources in it, how many triples were received / written / rejected, the duration, and the run id."
            doNote="Watch that batches are landing and that 'rejected' stays at zero. A non-zero reject count means rows failed validation (e.g. missing identity) — DCL rejects loudly rather than guessing."
          />

          <Stage
            n="2" name="The facts" tab="Dashboard tab"
            what="The raw facts DCL produced, as a table you can filter. Each row is one triple: an entity, a business concept, a property, a value, the period, the source system, and a confidence score. This is the ground truth everything else is built from."
            see="A filterable table — Concept, Property, Value, Period, Source, Confidence, Entity. (Note: this is a data browser, not a KPI dashboard — persona KPI dashboards live in NLQ, not here.)"
            doNote="Filter to a concept, source, or entity to confirm a number and see exactly which source field produced it and how confident DCL is."
          />

          <Stage
            n="3" name="The shape" tab="Graph tab"
            what="A flow diagram (a Sankey) of how one entity's data maps together: source systems on the left flow through their fields into business concepts, and on to the dimensions those concepts can be sliced by. It shows where each concept comes from — what feeds it — for the selected entity."
            see="A left-to-right flow: sources → fields → concepts → dimensions. One entity at a time (pick it at the top). A persona filter (CFO/CRO/…) narrows the flow to the concepts that persona cares about."
            doNote="Pick an entity, then trace a concept back to the source field(s) feeding it. Use the persona filter to focus on one role's concepts. (It does not show entity-to-entity relationships — that's a separate capability, not this tab.)"
          />

          <Stage
            n="4" name="Context & conflicts" tab="Context tab"
            what="The picture of one entity, plus the place you resolve disagreements. Pick a snapshot and DCL shows how complete it is — which business areas (domains) are populated, with how many concepts, from how many sources, at what confidence. Below that is the Conflict Register: where two sources claim different values for the same entity, concept, and period."
            see="A domain-coverage table (domain, concepts, sources, average confidence) and a confidence distribution; then the Conflict Register listing each open disagreement with its competing claims."
            doNote="Drill a conflict row to see both claims with their sources, type a short reason, and Accept the authoritative source. The losing value is superseded (kept in history, not deleted) and the decision is recorded. This is where reconciliation actually happens."
          />

          <Stage
            n="5" name="Monitoring" tab="Monitor tab"
            what="DCL watches for change over time on a schedule. Structural drift = a field appears or disappears between an entity's runs. Value drift = sources that agreed start disagreeing. Each finding becomes a change proposal."
            see="The two drift jobs (structural, value) with their on/off state, interval, and last-run time; and the drift findings in the Change Proposals list."
            doNote="Approve a proposal to fold the change in (recorded with its source), or reject it. Pause or resume a job, or change its interval, right from this tab."
          />

          <Stage
            n="6" name="The Demo" tab="Demo tab"
            what="The before/after that proves the point on the same question, same data, same model. It shows a captured run comparing an ungoverned agent against the same question answered through DCL."
            see="Two panels. Panel A — a capable agent with raw access; it answers, but silently picks or blends sources, so it can be confidently wrong. Panel B — the same question through DCL: it detects the conflict, decomposes why the sources differ, and names which one decides. If no run has been captured, it says so."
            doNote="Pick the entity; read the two panels side by side. A good run: both answer, but only Panel B discloses the conflict and names the authoritative source — that resolution is the product. The source trail behind each figure is how you verify it, not the headline."
          />
        </section>

        {/* ── Around the edges ────────────────────────────────────────── */}
        <section className="space-y-3">
          <h2 className="text-xl font-semibold text-foreground border-b border-border pb-2">
            Two things that wrap the whole flow
          </h2>
          <p className="text-foreground/90 leading-relaxed">
            <strong>Onboarding (Mai).</strong> Before data flows, you can interview
            stakeholders through Mai in chat — org structure, which system is the source
            of truth for what, local vocabulary, known disagreements. What Mai captures
            lands as proposals in the same approval queue; nothing becomes official until
            a person approves it.
          </p>
          <p className="text-foreground/90 leading-relaxed">
            <strong>Governance.</strong> Agents and tools connect under a named identity
            scoped to only the tools, business areas, and personas they are allowed to
            touch — anything outside that scope is refused, loudly, and recorded.
            Sensitive approvals can be configured so the person who proposes a change
            cannot also approve it.
          </p>
        </section>

        {/* ── Quick reference ─────────────────────────────────────────── */}
        <section className="space-y-3">
          <h2 className="text-xl font-semibold text-foreground border-b border-border pb-2">
            Quick reference — the tabs
          </h2>
          <div className="bg-card border border-border rounded-lg divide-y divide-border text-sm">
            <Row tab="Ingest" use="Watch data arrive — the ingest log (per-batch counts, sources, run id)." />
            <Row tab="Dashboard" use="Browse the facts — a filterable table of triples (concept, value, source, confidence)." />
            <Row tab="Graph" use="See the shape — a Sankey of sources → fields → concepts → dimensions, per entity, persona-filterable." />
            <Row tab="Context" use="One entity's domain coverage + the Conflict Register (drill and resolve disagreements)." />
            <Row tab="Monitor" use="Scheduled drift detection; approve/reject change proposals; pause the schedule." />
            <Row tab="Demo" use="The before/after grounded demo (Panel A raw vs Panel B governed)." />
            <Row tab="?" use="This guide." />
          </div>
        </section>

        <p className="text-xs text-muted-foreground pt-4 border-t border-border">
          DCL — the context layer of the AutonomOS platform. Every fact carries its
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
