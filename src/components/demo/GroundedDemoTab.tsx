/**
 * Grounded-agent before/after demo — WRAPPER layer (§13).
 *
 * Presentation only: renders the latest captured sequence run
 * (public/demo-captures/latest.json, written by `python -m demo.sequence`).
 * Rendering, narration, pacing — zero logic that changes outcomes.
 * Pending (Gate 1A) slots render as pending; missing capture renders an
 * honest empty state. tenant_id is machine-only and is never displayed (I2).
 */
import { useEffect, useRef, useState } from 'react';

interface ToolCall { name: string; arguments: Record<string, unknown>; result_excerpt: string; is_error: boolean; latency_ms: number }
interface PanelCapture {
  answer_text: string; tool_calls: ToolCall[]; usage: { input_tokens: number; output_tokens: number };
  model: string; elapsed_s: number; access: string;
  mcp?: { caller_token_id: string; tools_listed: string[]; sse_url: string };
}
interface SlotScores {
  correctness?: { passed: boolean; matched_value: number | null; scale: number | null };
  no_data_honesty?: { passed: boolean; fabricated_number: boolean };
  conflict?: { passed: boolean; expected_conflicts: number; sources_named_in_answer?: string[]; called_conflict_query: boolean };
  provenance: { present: boolean; cited_source_systems?: string[]; cited_triple_or_ingest_ids?: string[]; confidence_cited?: boolean };
}
interface Slot {
  id: string; question: string; status: 'live' | 'pending'; kind: string;
  pending_reason?: string; error?: string; passed?: boolean;
  ground_truth_resolved?: { feed: string; field: string; period: string; value: number };
  panel_a?: PanelCapture; panel_b?: PanelCapture;
  scores?: { a: SlotScores; b: SlotScores };
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
    panel_a: { numeric_correct: number; numeric_total: number; provenance_present: number };
    panel_b: { numeric_correct: number; numeric_total: number; provenance_present: number; no_data_honest: number; no_data_total: number; conflict_disclosed: number; conflict_total: number };
  };
  sequence_passed: boolean;
}

const NARRATION: Record<string, string> = {
  numeric: 'Same model, same question. Panel A reads the raw warehouse exports directly. Panel B answers through the governed context layer — every figure grounded in queried triples, with provenance.',
  no_data: 'A question the data cannot answer. The honest outcome IS the feature: the grounded panel states the absence plainly — no estimate, no filler.',
  conflict: 'Where sources disagree, raw access has no register to consult. The grounded panel discloses the detected conflict from the Conflict Register — sources named, nothing arbitrated silently.',
};

function Chip({ ok, label }: { ok: boolean | undefined; label: string }) {
  const tone = ok === undefined ? 'bg-muted text-muted-foreground' : ok ? 'bg-emerald-500/15 text-emerald-400' : 'bg-red-500/15 text-red-400';
  return <span className={`px-2 py-0.5 rounded text-xs font-medium ${tone}`}>{label}</span>;
}

function PanelCard({ title, subtitle, cap, scores, accent }: {
  title: string; subtitle: string; cap?: PanelCapture; scores?: SlotScores; accent: 'a' | 'b';
}) {
  if (!cap) return null;
  const border = accent === 'a' ? 'border-amber-500/40' : 'border-emerald-500/40';
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
        {scores?.conflict && <Chip ok={scores.conflict.passed} label={scores.conflict.passed ? `conflict disclosed (${scores.conflict.sources_named_in_answer?.join(' vs ') || 'register'})` : 'conflict not disclosed'} />}
        {accent === 'b' ? (
          <>
            <Chip ok={scores?.provenance.present} label={scores?.provenance.present ? `provenance: ${[...(scores?.provenance.cited_source_systems ?? [])].join(', ') || 'triple ids cited'}` : 'no provenance cited'} />
            <span className="text-xs text-muted-foreground">{cap.tool_calls.length} MCP calls · token {cap.mcp?.caller_token_id} · {cap.elapsed_s}s</span>
          </>
        ) : (
          <>
            <span className="px-2 py-0.5 rounded text-xs font-medium bg-amber-500/15 text-amber-400">no audit trail · no provenance · unauditable arbitration</span>
            <span className="text-xs text-muted-foreground">{cap.tool_calls.length} raw reads · {cap.elapsed_s}s</span>
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
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then(setCapture)
      .catch((e) => setLoadError(String(e)));
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
          <div className="text-lg font-semibold">No demo capture found</div>
          <div className="text-sm text-muted-foreground">
            The wrapper renders real captured runs only. Run the headless sequence first:
            <code className="block mt-2 p-2 bg-muted rounded text-xs">python -m demo.sequence --entity &lt;entity&gt;</code>
            ({loadError})
          </div>
        </div>
      </div>
    );
  }
  if (!capture) return <div className="h-full flex items-center justify-center text-sm text-muted-foreground">Loading capture…</div>;

  const slot = capture.slots[idx];
  const s = capture.summary;
  const mismatch = requestedEntityId && requestedEntityId !== capture.meta.entity_id;

  return (
    <div className="h-full overflow-y-auto p-6 space-y-5" data-testid="grounded-demo">
      {mismatch && (
        <div className="rounded border border-amber-500/50 bg-amber-500/10 text-amber-300 text-sm px-4 py-2" data-testid="demo-entity-mismatch">
          Requested entity {requestedEntityId} — but the latest capture is for {capture.meta.entity_id}. Run the sequence for {requestedEntityId} to refresh.
        </div>
      )}

      <div className="flex flex-wrap items-center gap-3">
        <div>
          <h1 className="text-xl font-bold">Grounded agent — before / after</h1>
          <div className="text-xs text-muted-foreground" data-testid="demo-meta">
            {capture.meta.snapshot_name ?? capture.meta.entity_id} · model {capture.meta.model} · captured {capture.meta.stamp} · ingest {capture.meta.dcl_ingest_id}
          </div>
        </div>
        <div className="flex gap-2 ml-auto flex-wrap" data-testid="demo-summary">
          <Chip ok={capture.sequence_passed} label={capture.sequence_passed ? 'sequence PASS' : 'sequence FAIL'} />
          <Chip ok={s.panel_b.numeric_correct === s.panel_b.numeric_total} label={`grounded correct ${s.panel_b.numeric_correct}/${s.panel_b.numeric_total}`} />
          <Chip ok={undefined} label={`raw correct ${s.panel_a.numeric_correct}/${s.panel_a.numeric_total}`} />
          <Chip ok={s.panel_b.provenance_present === s.live_slots} label={`provenance ${s.panel_b.provenance_present}/${s.live_slots}`} />
          {s.panel_b.conflict_total > 0 && <Chip ok={s.panel_b.conflict_disclosed === s.panel_b.conflict_total} label={`conflicts disclosed ${s.panel_b.conflict_disclosed}/${s.panel_b.conflict_total}`} />}
          {s.pending_slots > 0 && <span className="px-2 py-0.5 rounded text-xs font-medium bg-sky-500/15 text-sky-400">{s.pending_slots} pending (Gate 1A)</span>}
        </div>
      </div>

      {/* Real-condition beats */}
      <div className="grid md:grid-cols-2 gap-3">
        <div className="rounded-lg border border-border bg-card/60 p-3" data-testid="beat-ingest-reject">
          <div className="flex items-center gap-2 text-sm font-semibold">
            Malformed ingest is rejected loudly
            <Chip ok={capture.beats.ingest_reject.passed} label={`HTTP ${capture.beats.ingest_reject.status_code}`} />
          </div>
          <div className="text-xs text-muted-foreground mt-1">POST /api/dcl/ingest-records with an empty pipes envelope — the boundary refuses, nothing is written.</div>
          <code className="block mt-2 p-2 bg-muted rounded text-xs whitespace-pre-wrap break-all">{capture.beats.ingest_reject.response_excerpt}</code>
        </div>
        <div className="rounded-lg border border-border bg-card/60 p-3" data-testid="beat-audit-proof">
          <div className="flex items-center gap-2 text-sm font-semibold">
            Every grounded call is on the audit ledger
            <Chip ok={capture.beats.audit_proof.passed} label={capture.beats.audit_proof.passed ? 'rows == calls' : 'ledger mismatch'} />
          </div>
          <div className="text-xs text-muted-foreground mt-1">mai_mcp_audit, read via GET /api/dcl/mcp/audit — per token: calls made vs rows present.</div>
          <div className="mt-2 space-y-1">
            {capture.beats.audit_proof.per_token.map((t) => (
              <div key={t.caller_token_id} className="text-xs flex gap-2 items-center">
                <Chip ok={t.passed} label={`${t.audit_rows}/${t.tool_calls_made}`} />
                <span className="text-muted-foreground">{t.question_id ?? ''} · token {t.caller_token_id} · {(t.transport ?? []).join(',')}</span>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Question stepper */}
      <div className="flex gap-4">
        <div className="w-64 shrink-0 space-y-1" data-testid="demo-question-list">
          {capture.slots.map((q, i) => (
            <button
              key={q.id}
              onClick={() => { setIdx(i); setPlaying(false); }}
              className={`w-full text-left px-3 py-2 rounded border text-xs ${i === idx ? 'border-primary bg-primary/10' : 'border-border bg-card/40 hover:bg-card'}`}
              data-testid={`demo-slot-${q.id}`}
            >
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
          <div className="text-base font-semibold" data-testid="demo-question-text">{slot.question}</div>
          <div className="text-xs text-muted-foreground italic">{NARRATION[slot.kind] ?? ''}</div>

          {slot.status === 'pending' ? (
            <div className="rounded-lg border border-sky-500/40 bg-sky-500/5 p-6 text-sm" data-testid="demo-pending-tile">
              <div className="font-semibold text-sky-400 mb-1">Pending — arrives with Gate 1A scenario data</div>
              <div className="text-muted-foreground">{slot.pending_reason}</div>
              <div className="text-xs text-muted-foreground mt-2">This slot is never simulated. It runs the day the data exists.</div>
            </div>
          ) : slot.error ? (
            <div className="rounded-lg border border-red-500/40 bg-red-500/5 p-4 text-sm text-red-300" data-testid="demo-slot-error">
              Panel run failed loudly: {slot.error}
            </div>
          ) : (
            <div className="flex gap-3 flex-col lg:flex-row">
              <PanelCard accent="a" title="Before — ungoverned" subtitle="same model · direct raw-feed access" cap={slot.panel_a} scores={slot.scores?.a} />
              <PanelCard accent="b" title="After — grounded via DCL-MCP" subtitle="same model · bearer token · audited · rate-limited" cap={slot.panel_b} scores={slot.scores?.b} />
            </div>
          )}

          {slot.status === 'live' && slot.ground_truth_resolved && (
            <div className="text-xs text-muted-foreground" data-testid="demo-ground-truth">
              Ground truth (resolved at run time from the raw source feed, never hardcoded): {slot.ground_truth_resolved.field} @ {slot.ground_truth_resolved.period} = {slot.ground_truth_resolved.value}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
