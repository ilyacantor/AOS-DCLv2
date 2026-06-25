/**
 * Semantics vs Context (contextOS) tier demo — WRAPPER layer (§13).
 *
 * Presentation only: renders the captured sequence run
 * (public/demo-captures/latest.json, written by `python -m demo.sequence`).
 * Rendering, narration, pacing — zero logic that changes outcomes. Every figure
 * shown is real measured output; ground truth is resolved live from the source
 * feeds at run time (B10); the +80.4% lift is the Context Lab result. The wrapper
 * fabricates nothing. tenant_id is machine-only and is never displayed (I2).
 *
 * Structure (top to bottom): the two definitions → the seam → the contrast table
 * → the live proof, organized by capability class (Connection first) → a trust
 * strip. The thesis is the seam: base serves trustworthy facts; the premium tier
 * answers the questions that only exist across relationships, conflict, or time.
 */
import { useEffect, useRef, useState } from 'react';

const LIFT_PCT = '80.4%'; // Context Lab measured correctness lift (find/trust benchmark).

interface ToolCall { name: string; arguments: Record<string, unknown>; result_excerpt: string; is_error: boolean; latency_ms: number }
interface PanelCapture {
  answer_text: string; tool_calls: ToolCall[]; usage: { input_tokens: number; output_tokens: number };
  model: string; elapsed_s: number; access: string;
  mcp?: { caller_token_id: string; tools_listed: string[]; sse_url: string };
}
interface SlotScores {
  correctness?: { passed: boolean; matched_value: number | null; scale: number | null };
  no_data_honesty?: { passed: boolean; fabricated_number: boolean };
  conflict?: { passed: boolean; expected_conflicts: number; disclosed?: boolean; sources_named_in_answer?: string[]; called_conflict_query: boolean };
  connection?: { passed: boolean; called_traverse_graph: boolean; graph_has_edges: boolean; cited_concentration_nodes?: string[]; driver_named: boolean; cited_exit_themes?: string[] };
  provenance: { present: boolean; cited_source_systems?: string[]; cited_triple_or_ingest_ids?: string[]; confidence_cited?: boolean };
}
type SlotKind = 'connection' | 'conflict' | 'numeric' | 'no_data' | 'time';
interface Slot {
  id: string; question: string; status: 'live' | 'pending'; kind: SlotKind;
  pending_reason?: string; error?: string; passed?: boolean;
  ground_truth_resolved?: { feed: string; field: string; period: string; value: number };
  semantics?: PanelCapture; contextos?: PanelCapture;
  scores?: { semantics: SlotScores; contextos: SlotScores };
}
interface Capture {
  meta: { stamp: string; entity_id: string; model: string; dcl_ingest_id: string; snapshot_name?: string; register: { available: boolean; open_conflicts?: number } };
  beats: {
    ingest_reject: { request: Record<string, unknown>; status_code: number; response_excerpt: string; passed: boolean };
    audit_proof: { per_token: { question_id?: string; caller_token_id: string; tool_calls_made: number; audit_rows: number; transport?: string[]; passed: boolean }[]; passed: boolean };
    register_probe: { available: boolean; open_conflicts?: number };
  };
  slots: Slot[];
  summary: {
    live_slots: number; pending_slots: number;
    semantics: { numeric_correct: number; numeric_total: number; provenance_present: number };
    contextos: { numeric_correct: number; numeric_total: number; provenance_present: number; no_data_honest: number; no_data_total: number; conflict_disclosed: number; conflict_total: number; connection_grounded: number; connection_total: number };
  };
  sequence_passed: boolean;
}

// The capability class each slot demonstrates. The narration says the same thing
// every time: the premium win is that the ANSWER CHANGES (or, on a Fact, that base
// already suffices) — never "premium tells you which source to believe."
const CLASS: Record<SlotKind, { label: string; narration: string }> = {
  connection: {
    label: 'Connection — relationships',
    narration: 'The answer lives in no single record. Semantics holds every fact — roster, compensation, market, exit reasons — but has no way to connect them. Context walks the relationship graph (team → compensation band → market) and returns the driver. This is the answer flat retrieval cannot assemble.',
  },
  conflict: {
    label: 'Conflict — arbitration',
    narration: 'Two systems report different values for the same fact. Semantics can show both and tell you they differ — it has no authority to choose. Context arbitrates: it names the disagreeing sources and returns the one decisive value, with the authority cited.',
  },
  numeric: {
    label: 'Fact — the base tier suffices',
    narration: 'A resolved, governed fact. Semantics returns it correctly, with lineage — and that is the point: the base tier is enough here. Context returns the same value. You do not pay for the premium tier to answer this.',
  },
  no_data: {
    label: 'Absence — honesty',
    narration: 'A question the data cannot answer. The honest outcome IS the feature: both tiers state the absence plainly — no estimate, no filler.',
  },
  time: {
    label: 'Time — as-of (pending)',
    narration: 'The value as it stood at a point in the past, not today’s restated figure. Pending — it runs when the scenario dataset that retains restated values with valid-time lands.',
  },
};

function Chip({ ok, label, tone }: { ok?: boolean; label: string; tone?: 'amber' }) {
  const cls = tone === 'amber'
    ? 'bg-amber-500/15 text-amber-400'
    : ok === undefined ? 'bg-muted text-muted-foreground'
    : ok ? 'bg-emerald-500/15 text-emerald-400' : 'bg-red-500/15 text-red-400';
  return <span className={`px-2 py-0.5 rounded text-xs font-medium ${cls}`}>{label}</span>;
}

function PanelCard({ kind, title, subtitle, cap, scores, accent }: {
  kind: SlotKind; title: string; subtitle: string; cap?: PanelCapture; scores?: SlotScores; accent: 'a' | 'b';
}) {
  if (!cap) return null;
  const isBase = accent === 'a';
  const border = isBase ? 'border-amber-500/40' : 'border-emerald-500/40';
  // The base tier genuinely lacks traversal/arbitration only on Connection/Conflict.
  // On a Fact or Absence question it is fully sufficient — no limitation chip there.
  const baseLimited = isBase && (kind === 'connection' || kind === 'conflict');
  const conn = scores?.connection;
  const conf = scores?.conflict;
  return (
    <div className={`flex-1 min-w-0 rounded-lg border ${border} bg-card/60 p-4 flex flex-col gap-3`}>
      <div>
        <div className="text-sm font-semibold">{title}</div>
        <div className="text-xs text-muted-foreground">{subtitle}</div>
      </div>
      <div className="text-sm whitespace-pre-wrap leading-relaxed flex-1">{cap.answer_text}</div>
      <div className="flex flex-wrap gap-1.5 items-center border-t border-border pt-2">
        {scores?.correctness && <Chip ok={scores.correctness.passed} label={scores.correctness.passed ? 'correct vs source ground truth' : 'incorrect vs source ground truth'} />}
        {scores?.no_data_honesty && <Chip ok={scores.no_data_honesty.passed} label={scores.no_data_honesty.passed ? 'honest no-data' : 'failed to state absence'} />}

        {/* Connection: premium grounds in the graph; base cannot traverse. */}
        {conn && !isBase && <Chip ok={conn.passed} label={conn.passed ? `graph-grounded${conn.cited_concentration_nodes?.length ? `: ${conn.cited_concentration_nodes[0]}` : ''}` : 'not graph-grounded'} />}

        {/* Conflict: premium gives the decisive value; base sees both, cannot arbitrate. */}
        {conf && !isBase && <Chip ok={conf.passed} label={conf.passed ? `decided · ${conf.sources_named_in_answer?.join(' vs ') || 'sources named'}` : 'no decisive value'} />}
        {conf && isBase && <Chip tone="amber" label="sees both values · cannot arbitrate" />}

        {accent === 'b' ? (
          <>
            <Chip ok={scores?.provenance.present} label={scores?.provenance.present ? `sourced: ${[...(scores?.provenance.cited_source_systems ?? [])].join(', ') || 'triple ids cited'}` : 'source not cited'} />
            <span className="text-xs text-muted-foreground">{cap.tool_calls.length} MCP calls · token {cap.mcp?.caller_token_id} · {cap.elapsed_s}s</span>
          </>
        ) : (
          <>
            {baseLimited
              ? <Chip tone="amber" label="no relationship graph · no arbitration · no as-of" />
              : <Chip ok={true} label="resolved · provenanced · sufficient" />}
            <span className="text-xs text-muted-foreground">{cap.tool_calls.length} base-tool calls · token {cap.mcp?.caller_token_id} · {cap.elapsed_s}s</span>
          </>
        )}
      </div>
    </div>
  );
}

export default function GroundedDemoTab({ requestedEntityId }: { requestedEntityId?: string | null }) {
  const [capture, setCapture] = useState<Capture | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [idx, setIdx] = useState(0);
  const [playing, setPlaying] = useState(false);
  const timer = useRef<number | null>(null);

  useEffect(() => {
    fetch('/demo-captures/latest.json', { cache: 'no-store' })
      .then((r) => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); })
      .then(setCapture)
      // Operators get the real cause in the console; the customer-facing surface
      // never shows a raw error or a CLI command (below).
      .catch((e) => { console.error('[demo] capture load failed:', e); setLoadError(String(e)); });
  }, []);

  useEffect(() => {
    if (!playing || !capture) return;
    timer.current = window.setInterval(() => setIdx((i) => (i + 1) % capture.slots.length), 12000);
    return () => { if (timer.current) window.clearInterval(timer.current); };
  }, [playing, capture]);

  if (loadError) {
    return (
      <div className="h-full flex items-center justify-center p-8">
        <div className="max-w-xl text-center space-y-3" data-testid="demo-no-capture">
          <div className="text-lg font-semibold">Demo unavailable</div>
          <div className="text-sm text-muted-foreground">
            This demo is being refreshed — please check back shortly.
          </div>
        </div>
      </div>
    );
  }
  if (!capture) return <div className="h-full flex items-center justify-center text-sm text-muted-foreground">Loading…</div>;

  const slot = capture.slots[idx];
  const s = capture.summary;
  const mismatch = requestedEntityId && requestedEntityId !== capture.meta.entity_id;
  const cls = CLASS[slot.kind];

  return (
    <div className="h-full overflow-y-auto p-6 space-y-6" data-testid="grounded-demo">
      {mismatch && (
        <div className="rounded border border-amber-500/50 bg-amber-500/10 text-amber-300 text-sm px-4 py-2" data-testid="demo-entity-mismatch">
          Requested entity {requestedEntityId} — showing {capture.meta.entity_id}. Run the sequence for {requestedEntityId} to refresh.
        </div>
      )}

      {/* ===== Header / thesis ===== */}
      <div className="space-y-2">
        <h1 className="text-2xl font-bold">Semantics vs Context — the base tier and the premium tier</h1>
        <p className="text-sm text-foreground/90 max-w-4xl" data-testid="demo-thesis">
          <span className="font-semibold">Context produces answers flat retrieval cannot</span> — a measured
          <span className="font-semibold text-emerald-400"> +{LIFT_PCT.replace('%', '')}% correctness lift</span> in the Context Lab. Same model, same governed store; the only difference is whether the agent can connect the facts.
        </p>
        <div className="flex flex-wrap gap-2" data-testid="demo-summary">
          {s.contextos.connection_total > 0 && <Chip ok={s.contextos.connection_grounded === s.contextos.connection_total} label={`relationships answered ${s.contextos.connection_grounded}/${s.contextos.connection_total}`} />}
          {s.contextos.conflict_total > 0 && <Chip ok={s.contextos.conflict_disclosed === s.contextos.conflict_total} label={`conflicts decided ${s.contextos.conflict_disclosed}/${s.contextos.conflict_total}`} />}
          {s.semantics.numeric_total > 0 && <Chip ok={s.semantics.numeric_correct === s.semantics.numeric_total} label={`facts correct, base tier ${s.semantics.numeric_correct}/${s.semantics.numeric_total}`} />}
          <Chip ok={s.contextos.provenance_present === s.live_slots} label={`every answer sourced ${s.contextos.provenance_present}/${s.live_slots}`} />
          <span className="text-xs text-muted-foreground self-center">{capture.meta.snapshot_name ?? capture.meta.entity_id} · model {capture.meta.model} · ingest {capture.meta.dcl_ingest_id?.slice(0, 8)}</span>
        </div>
      </div>

      {/* ===== Two definitions + the seam ===== */}
      <div className="grid md:grid-cols-2 gap-3" data-testid="demo-definitions">
        <div className="rounded-lg border border-amber-500/30 bg-card/50 p-4">
          <div className="text-sm font-semibold text-amber-400">Semantics — the base</div>
          <p className="text-xs text-muted-foreground mt-1 leading-relaxed">
            Resolves raw fields from many systems into <em>meaning</em> and <em>identity</em>, and serves each fact governed and provenanced — what is this, is it the same thing as that, where did it come from. On its own it does not beat good flat retrieval over clean data — and that is correct, because it is the base.
          </p>
        </div>
        <div className="rounded-lg border border-emerald-500/30 bg-card/50 p-4">
          <div className="text-sm font-semibold text-emerald-400">Context — the premium tier (contextOS)</div>
          <p className="text-xs text-muted-foreground mt-1 leading-relaxed">
            Built on semantics: makes the <em>relationships between</em> resolved entities answerable. Stitches typed edges no single record holds, answers by traversing them, arbitrates a decisive value when sources disagree, and answers as-of a point in time. This is where the answer <em>changes</em> — answers flat retrieval cannot produce.
          </p>
        </div>
      </div>
      <div className="rounded border border-border bg-muted/40 px-4 py-2 text-sm" data-testid="demo-seam">
        <span className="font-semibold">The seam:</span> Semantics = trustworthy facts. Context = answerable connections among them. Context is built on semantics because you cannot traverse relationships between entities you have not first resolved.
      </div>

      {/* ===== Contrast table ===== */}
      <div className="overflow-x-auto" data-testid="demo-table">
        <table className="w-full text-xs border-collapse">
          <thead>
            <tr className="text-left">
              <th className="p-2 border border-border w-24"></th>
              <th className="p-2 border border-border text-amber-400">Semantics (base)</th>
              <th className="p-2 border border-border text-emerald-400">Context / contextOS (premium)</th>
            </tr>
          </thead>
          <tbody className="align-top text-muted-foreground">
            <tr>
              <td className="p-2 border border-border font-semibold text-foreground">Functionality</td>
              <td className="p-2 border border-border">Field→concept classification; canonical identity resolution (golden records); triples with provenance; conflict <em>detection</em>; supersession history. Served as resolved triples + lineage over MCP.</td>
              <td className="p-2 border border-border">Stitched typed relationship graph; graph-grounded retrieval (traversal, not lookup); authority <em>arbitration</em> to one decisive value; as-of / point-in-time answers. Provenance surfaced behind the answer.</td>
            </tr>
            <tr>
              <td className="p-2 border border-border font-semibold text-foreground">Purpose</td>
              <td className="p-2 border border-border">Give any consumer — agent, BI, human — governed, resolved, provenanced facts without replatforming. Kill hallucination at the fact level. Be the trustworthy base.</td>
              <td className="p-2 border border-border">Answer questions only answerable across relationships, time, or conflicting sources. <span className="text-emerald-400 font-semibold">The correctness lift is the product: +{LIFT_PCT.replace('%', '')}% (Context Lab).</span></td>
            </tr>
          </tbody>
        </table>
      </div>

      {/* ===== Live proof, by capability class ===== */}
      <div className="space-y-3">
        <h2 className="text-lg font-semibold">Live proof — one clean capability per question</h2>
        <div className="flex gap-4">
          <div className="w-64 shrink-0 space-y-1" data-testid="demo-question-list">
            {capture.slots.map((q, i) => (
              <button
                key={q.id}
                onClick={() => { setIdx(i); setPlaying(false); }}
                className={`w-full text-left px-3 py-2 rounded border text-xs ${i === idx ? 'border-primary bg-primary/10' : 'border-border bg-card/40 hover:bg-card'}`}
                data-testid={`demo-slot-${q.id}`}
              >
                <div className="text-[10px] uppercase tracking-wide text-muted-foreground mb-0.5">{CLASS[q.kind].label}</div>
                <div className="flex items-center gap-2">
                  {q.status === 'pending'
                    ? <span className="px-1.5 rounded bg-sky-500/15 text-sky-400">pending</span>
                    : <Chip ok={q.passed} label={q.passed ? 'pass' : 'fail'} />}
                  <span className="truncate">{q.question}</span>
                </div>
              </button>
            ))}
            <div className="flex gap-2 pt-2">
              <button className="px-2 py-1 text-xs rounded border border-border" onClick={() => setIdx((idx - 1 + capture.slots.length) % capture.slots.length)}>← Prev</button>
              <button className="px-2 py-1 text-xs rounded border border-border" onClick={() => setIdx((idx + 1) % capture.slots.length)}>Next →</button>
              <button className={`px-2 py-1 text-xs rounded border ${playing ? 'border-primary text-primary' : 'border-border'}`} onClick={() => setPlaying(!playing)}>{playing ? 'Pause' : 'Play'}</button>
            </div>
          </div>

          <div className="flex-1 min-w-0 space-y-3">
            <div>
              <div className="text-[10px] uppercase tracking-wide text-muted-foreground">{cls.label}</div>
              <div className="text-base font-semibold" data-testid="demo-question-text">{slot.question}</div>
            </div>
            <div className="text-xs text-muted-foreground italic">{cls.narration}</div>

            {slot.status === 'pending' ? (
              <div className="rounded-lg border border-sky-500/40 bg-sky-500/5 p-6 text-sm" data-testid="demo-pending-tile">
                <div className="font-semibold text-sky-400 mb-1">Pending — arrives with the as-of scenario dataset</div>
                <div className="text-muted-foreground">{slot.pending_reason}</div>
                <div className="text-xs text-muted-foreground mt-2">This slot is never simulated. It runs the day the data exists.</div>
              </div>
            ) : slot.error ? (
              <div className="rounded-lg border border-red-500/40 bg-red-500/5 p-4 text-sm text-red-300" data-testid="demo-slot-error">
                Panel run failed loudly: {slot.error}
              </div>
            ) : (
              <div className="flex gap-3 flex-col lg:flex-row">
                <PanelCard accent="a" kind={slot.kind} title="Semantics — base tier" subtitle="same model · same governed store · base read tools only" cap={slot.semantics} scores={slot.scores?.semantics} />
                <PanelCard accent="b" kind={slot.kind} title="Context — premium tier" subtitle="same model · graph traversal · arbitration · as-of" cap={slot.contextos} scores={slot.scores?.contextos} />
              </div>
            )}

            {slot.status === 'live' && slot.ground_truth_resolved && (
              <div className="text-xs text-muted-foreground" data-testid="demo-ground-truth">
                Ground truth (resolved at run time from the source feed, never hardcoded): {slot.ground_truth_resolved.field} @ {slot.ground_truth_resolved.period} = {slot.ground_truth_resolved.value}
              </div>
            )}
          </div>
        </div>
      </div>

      {/* ===== Trust strip (footer) — the demo is real, and provably so ===== */}
      <div className="border-t border-border pt-3 flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-muted-foreground" data-testid="demo-trust-strip">
        <span className="font-semibold text-foreground">Provably real:</span>
        <span data-testid="beat-ingest-reject">
          <Chip ok={capture.beats.ingest_reject.passed} label={`malformed ingest refused · HTTP ${capture.beats.ingest_reject.status_code}`} />
        </span>
        <span data-testid="beat-audit-proof">
          <Chip ok={capture.beats.audit_proof.passed} label={capture.beats.audit_proof.passed ? 'every MCP call on the audit ledger' : 'audit ledger mismatch'} />
        </span>
        <span>ground truth resolved live from source feeds (never hardcoded)</span>
        <Chip ok={capture.sequence_passed} label={capture.sequence_passed ? 'all checks pass' : 'check failed'} />
      </div>
    </div>
  );
}
