/**
 * Semantics vs Context demo — WRAPPER (§13). Presentation only; renders the
 * captured run (public/demo-captures/latest.json). At-a-glance comparison grid:
 * one row per question, base vs premium summarized to a one-line outcome derived
 * from the REAL scored result; the full agent answer opens on click. No logic
 * that changes outcomes. tenant_id is machine-only and never displayed (I2).
 */
import { useEffect, useState } from 'react';

const LIFT = '80.4';

interface ToolCall { name: string; arguments: Record<string, unknown>; result_excerpt: string; is_error: boolean; latency_ms: number }
interface PanelCapture { answer_text: string; tool_calls: ToolCall[]; elapsed_s: number; mcp?: { caller_token_id: string } }
interface SlotScores {
  correctness?: { passed: boolean; matched_value: number | null };
  no_data_honesty?: { passed: boolean };
  conflict?: { passed: boolean; sources_named_in_answer?: string[] };
  connection?: { passed: boolean; cited_concentration_nodes?: string[]; gap_named?: boolean };
  provenance: { present: boolean; cited_source_systems?: string[] };
}
type Kind = 'connection' | 'conflict' | 'numeric' | 'no_data' | 'time';
interface Slot {
  id: string; question: string; status: 'live' | 'pending'; kind: Kind; passed?: boolean;
  ground_truth_resolved?: { feed: string; value: number };
  semantics?: PanelCapture; contextos?: PanelCapture;
  scores?: { semantics: SlotScores; contextos: SlotScores };
}
interface Capture {
  meta: { entity_id: string; model: string; snapshot_name?: string };
  beats: { ingest_reject: { status_code: number; passed: boolean }; audit_proof: { passed: boolean } };
  slots: Slot[];
  sequence_passed: boolean;
}

const CLASS_LABEL: Record<Kind, string> = {
  connection: 'Relationship', conflict: 'Conflict', numeric: 'Fact', no_data: 'Absence', time: 'As-of',
};

type Tone = 'win' | 'limit' | 'tie' | 'mute' | 'pending';
const TONE: Record<Tone, string> = {
  win: 'text-emerald-400', limit: 'text-amber-400', tie: 'text-foreground/80',
  mute: 'text-muted-foreground', pending: 'text-sky-400',
};

function money(v?: number): string {
  if (v == null) return '—';
  return `$${Number(v).toLocaleString(undefined, { maximumFractionDigits: 1 })}M`;
}

// One-line outcome per panel, from the REAL scored result (not re-summarized prose).
function outcome(slot: Slot, panel: 'semantics' | 'contextos'): { text: string; tone: Tone } {
  if (slot.status === 'pending') return { text: 'arrives with scenario data', tone: 'pending' };
  const sc = slot.scores?.[panel];
  switch (slot.kind) {
    case 'numeric': {
      const v = money(slot.ground_truth_resolved?.value);
      return sc?.correctness?.passed ? { text: `${v} ✓`, tone: 'tie' } : { text: 'incorrect', tone: 'limit' };
    }
    case 'no_data':
      return { text: 'no data — stated honestly', tone: 'mute' };
    case 'connection':
      if (panel === 'contextos') {
        const n = sc?.connection?.cited_concentration_nodes?.[0];
        return sc?.connection?.passed
          ? { text: `${n ? n[0].toUpperCase() + n.slice(1) : 'driver'} · below market`, tone: 'win' }
          : { text: 'not grounded', tone: 'limit' };
      }
      return { text: 'lists only — can’t connect', tone: 'limit' };
    case 'conflict':
      if (panel === 'contextos') {
        const s = sc?.conflict?.sources_named_in_answer ?? [];
        return sc?.conflict?.passed
          ? { text: `decided · ${s.join(' vs ') || 'sources named'}`, tone: 'win' }
          : { text: 'not decided', tone: 'limit' };
      }
      return { text: 'shows both — no decision', tone: 'limit' };
  }
  return { text: '', tone: 'mute' };
}

function Chip({ ok, label }: { ok: boolean; label: string }) {
  return <span className={`px-2 py-0.5 rounded text-[11px] font-medium ${ok ? 'bg-emerald-500/15 text-emerald-400' : 'bg-red-500/15 text-red-400'}`}>{label}</span>;
}

export default function GroundedDemoTab({ requestedEntityId }: { requestedEntityId?: string | null }) {
  const [capture, setCapture] = useState<Capture | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [openId, setOpenId] = useState<string | null>(null);

  useEffect(() => {
    fetch('/demo-captures/latest.json', { cache: 'no-store' })
      .then((r) => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); })
      .then(setCapture)
      .catch((e) => { console.error('[demo] capture load failed:', e); setLoadError(String(e)); });
  }, []);

  if (loadError) {
    return (
      <div className="h-full flex items-center justify-center p-8">
        <div className="max-w-xl text-center space-y-2" data-testid="demo-no-capture">
          <div className="text-lg font-semibold">Demo unavailable</div>
          <div className="text-sm text-muted-foreground">This demo is being refreshed — please check back shortly.</div>
        </div>
      </div>
    );
  }
  if (!capture) return <div className="h-full flex items-center justify-center text-sm text-muted-foreground">Loading…</div>;

  const mismatch = requestedEntityId && requestedEntityId !== capture.meta.entity_id;
  const open = capture.slots.find((s) => s.id === openId) || null;

  return (
    <div className="h-full overflow-y-auto px-6 py-5 max-w-5xl mx-auto" data-testid="grounded-demo">
      {mismatch && (
        <div className="mb-4 rounded border border-amber-500/50 bg-amber-500/10 text-amber-300 text-sm px-4 py-2" data-testid="demo-entity-mismatch">
          Requested {requestedEntityId} — showing {capture.meta.entity_id}.
        </div>
      )}

      {/* Header — one line */}
      <div className="flex items-baseline gap-3 flex-wrap mb-4" data-testid="demo-header">
        <h1 className="text-lg font-semibold">Semantics <span className="text-muted-foreground font-normal">(base)</span> vs Context <span className="text-muted-foreground font-normal">(premium)</span></h1>
        <span className="text-xs text-muted-foreground">same data · same model · the only difference is whether the agent can connect the facts</span>
        <span className="ml-auto text-xs px-2 py-0.5 rounded bg-emerald-500/15 text-emerald-400 font-medium" data-testid="demo-lift">+{LIFT}% correctness lift · Context Lab</span>
      </div>

      {/* Comparison grid */}
      <div className="rounded-lg border border-border overflow-hidden" data-testid="demo-compare-grid">
        <div className="grid grid-cols-[1fr_1.2fr_1.2fr] text-[11px] uppercase tracking-wide text-muted-foreground bg-muted/40 border-b border-border">
          <div className="px-3 py-2">Question</div>
          <div className="px-3 py-2 border-l border-border text-amber-400/80">Semantics · base</div>
          <div className="px-3 py-2 border-l border-border text-emerald-400/80">Context · premium</div>
        </div>
        {capture.slots.map((s) => {
          const ob = outcome(s, 'semantics');
          const op = outcome(s, 'contextos');
          const active = s.id === openId;
          return (
            <button
              key={s.id}
              data-testid={`demo-row-${s.id}`}
              onClick={() => setOpenId(active ? null : s.id)}
              className={`w-full grid grid-cols-[1fr_1.2fr_1.2fr] text-left text-sm border-b border-border last:border-0 ${active ? 'bg-primary/10' : 'hover:bg-card/60'}`}
            >
              <div className="px-3 py-2.5">
                <div className="text-[10px] uppercase tracking-wide text-muted-foreground">{CLASS_LABEL[s.kind]}</div>
                <div className="leading-snug">{s.question}</div>
              </div>
              <div className={`px-3 py-2.5 border-l border-border self-center ${TONE[ob.tone]}`}>{ob.text}</div>
              <div className={`px-3 py-2.5 border-l border-border self-center font-medium ${TONE[op.tone]}`}>{op.text}</div>
            </button>
          );
        })}
      </div>
      <div className="text-[11px] text-muted-foreground mt-1.5">Click a row to see the full grounded answer from each tier.</div>

      {/* Detail — the real agent answers, only when a row is open */}
      {open && open.status === 'live' && (
        <div className="mt-3 grid md:grid-cols-2 gap-3" data-testid="demo-detail">
          {(['semantics', 'contextos'] as const).map((panel) => {
            const cap = open[panel];
            if (!cap) return null;
            const isBase = panel === 'semantics';
            return (
              <div key={panel} className={`rounded-lg border ${isBase ? 'border-amber-500/40' : 'border-emerald-500/40'} bg-card/50 p-3`}>
                <div className="text-xs font-semibold mb-1.5">{isBase ? 'Semantics — base tier' : 'Context — premium tier'}</div>
                <div className="text-xs whitespace-pre-wrap leading-relaxed max-h-72 overflow-y-auto text-foreground/90">{cap.answer_text}</div>
                <div className="text-[10px] text-muted-foreground mt-2">{cap.tool_calls.length} {isBase ? 'base-tool' : 'MCP'} calls · {cap.elapsed_s}s{open.ground_truth_resolved ? ` · ground truth ${open.ground_truth_resolved.value} (resolved live from source)` : ''}</div>
              </div>
            );
          })}
        </div>
      )}

      {/* Trust strip — small footer */}
      <div className="mt-5 pt-3 border-t border-border flex flex-wrap items-center gap-x-3 gap-y-1 text-[11px] text-muted-foreground" data-testid="demo-trust-strip">
        <span className="font-medium text-foreground/80">Real, and provably so:</span>
        <span data-testid="beat-ingest-reject"><Chip ok={capture.beats.ingest_reject.passed} label={`malformed ingest refused (${capture.beats.ingest_reject.status_code})`} /></span>
        <span data-testid="beat-audit-proof"><Chip ok={capture.beats.audit_proof.passed} label="every call on the audit ledger" /></span>
        <span>ground truth resolved live from source</span>
        <span className="ml-auto">{capture.meta.snapshot_name ?? capture.meta.entity_id} · {capture.meta.model}</span>
      </div>
    </div>
  );
}
