import '@xyflow/react/dist/style.css';
import { useEffect, useState, type ReactNode } from 'react';
import { ReactFlowProvider, MarkerType, type Node, type Edge } from '@xyflow/react';
import { Activity, ArrowRight, Boxes, CheckCircle2, KeyRound, ShieldCheck, XCircle } from 'lucide-react';
import { Canvas } from '../glassbox/Canvas';
import type { EngineNodeData } from '../glassbox/trace/types';

// Agent Arc — the render layer for the agent-context arc (finops cloud_spend).
// The HEADLESS arc runs the REAL ops and writes the capture; this tab RENDERS
// that capture (replay-only, zero logic that changes outcomes). The Beat-3
// traversal reuses the Glass Box React-Flow EngineNode/Canvas so the dark
// node-graph look is identical. No silent fallback (A1): a load failure surfaces
// a readable error and resolves nothing.

interface BeatCheck { label: string; pass: boolean; expected: unknown; got: unknown }
interface Beat {
  beat: number;
  name: string;
  status: string;
  checks: BeatCheck[];
  values: Record<string, unknown>;
  error: string | null;
}
interface Target {
  dcl: string;
  finops: string;
  db_ref: string;
  tenant_id: string;
  entity_id: string;
  identity: string;
}
interface BoundaryRecord {
  who_asked: string;
  reads_under_identity: number;
  what_resolved: { worst_efficiency_team: string; usd_per_deploy: number };
  what_action_correlation_ids: string[];
  joined_by: string[];
}
interface Revoke {
  before_domains: string[];
  before_rows: number;
  after_narrow: string;
  after_rows: number;
  restored_domains: string[];
  confirm_rows: number;
}
interface Headline {
  worst_efficiency_team: string;
  usd_per_deploy: number;
  action_correlation_ids: { hitl: string; autonomous: string };
  revoke: Revoke;
  boundary_record: BoundaryRecord;
}
interface Capture {
  arc: string;
  stamp: string;
  overall: string;
  target: Target;
  run_started_at: string;
  caller_token_id: string;
  headline: Headline;
  beats: Beat[];
  cleanup: string[];
  _capture_file?: string;
}

// Beat value shapes the render reads (cast at the read site — the capture is the
// source of truth, never invented).
interface Beat2Values { total_cost: number; top_service: { name: string; cost: number } }
interface Beat3Values {
  worst_team: string;
  worst_usd_per_deploy: number;
  worst_cost: number;
  worst_output: number;
  provenance: { cost_source: string; output_source: string };
}
interface ActionRow { target: string; mode: string; approver: string; outcome: string; basis_sources: string[] }
interface Beat4Values {
  correlation_ids: { hitl: string; autonomous: string };
  hitl_row: ActionRow;
  autonomous_row: ActionRow;
}
interface Beat6Values { revoke: Revoke }

const usd = (n: number): string => `$${n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
// Per-deploy figure: toFixed never injects thousands separators, so it renders
// exactly as the capture's number (e.g. 1145.18) — the answer the arc resolves.
const perDeploy = (n: number): string => `$${n.toFixed(2)}`;

const ACTIVE_STROKE = '#38bdf8'; // sky-400 — the trusted cross-source path
const SEVERED_STROKE = '#ef4444'; // red-500 — the shallow billing-only dead end

function engineNode(id: string, x: number, y: number, data: EngineNodeData): Node {
  return { id, type: 'engine', position: { x, y }, data };
}

function activeEdge(id: string, source: string, target: string): Edge {
  return {
    id,
    source,
    target,
    type: 'smoothstep',
    animated: true,
    style: { stroke: ACTIVE_STROKE, strokeWidth: 2 },
    markerEnd: { type: MarkerType.ArrowClosed, color: ACTIVE_STROKE },
  };
}

function severedEdge(id: string, source: string, target: string): Edge {
  return {
    id,
    source,
    target,
    type: 'smoothstep',
    animated: false,
    style: { stroke: SEVERED_STROKE, strokeWidth: 2, strokeDasharray: '6 5' },
    markerEnd: { type: MarkerType.ArrowClosed, color: SEVERED_STROKE },
  };
}

/**
 * Build the Beat-3 traversal graph from the capture's beat values — never
 * hardcoded. The billing-only path is shallow (excised); the two source reads
 * join into the verified cross-source answer (the worst-efficiency team).
 */
function buildBeat3Graph(entityId: string, b2: Beat2Values, b3: Beat3Values): { nodes: Node[]; edges: Edge[] } {
  const nodes: Node[] = [
    engineNode('intake', 300, 0, {
      label: 'Agent query',
      state: 'verified',
      icon: 'parser',
      detail: `cost → team → output · ${entityId}`,
    }),
    engineNode('billing', -60, 175, {
      label: 'billing (single system)',
      state: 'excised',
      icon: 'database',
      detail: `${usd(b2.total_cost)} · no team / no output`,
      badge: 'shallow',
    }),
    engineNode('src_cost', 300, 175, {
      label: `Source: ${b3.provenance.cost_source}`,
      state: 'verified',
      icon: 'database',
      detail: `cost ${usd(b3.worst_cost)}`,
      badge: 'cost',
    }),
    engineNode('src_output', 660, 175, {
      label: `Source: ${b3.provenance.output_source}`,
      state: 'verified',
      icon: 'database',
      detail: `output ${b3.worst_output} deploys`,
      badge: 'output',
    }),
    engineNode('verify', 480, 350, {
      label: 'cross-source efficiency',
      state: 'verified',
      icon: 'shield',
      detail: 'join: $/deploy = cost ÷ output',
    }),
    engineNode('answer', 480, 525, {
      label: b3.worst_team,
      state: 'verified',
      icon: 'reducer',
      detail: `${perDeploy(b3.worst_usd_per_deploy)} / deploy · worst efficiency`,
    }),
  ];
  const edges: Edge[] = [
    severedEdge('e-intake-billing', 'intake', 'billing'),
    activeEdge('e-intake-cost', 'intake', 'src_cost'),
    activeEdge('e-intake-output', 'intake', 'src_output'),
    activeEdge('e-cost-verify', 'src_cost', 'verify'),
    activeEdge('e-output-verify', 'src_output', 'verify'),
    activeEdge('e-verify-answer', 'verify', 'answer'),
  ];
  return { nodes, edges };
}

function beatByNum(capture: Capture, n: number): Beat | undefined {
  return capture.beats.find((b) => b.beat === n);
}

function StatusPill({ status }: { status: string }) {
  const pass = status === 'PASS';
  return (
    <span
      className={`inline-flex items-center gap-1 rounded px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wider ${
        pass
          ? 'bg-emerald-500/15 text-emerald-300 ring-1 ring-emerald-500/30'
          : 'bg-red-500/15 text-red-300 ring-1 ring-red-500/30'
      }`}
    >
      {pass ? <CheckCircle2 size={11} /> : <XCircle size={11} />}
      {status}
    </span>
  );
}

function Chip({ children, tone = 'slate' }: { children: ReactNode; tone?: 'slate' | 'sky' | 'emerald' | 'amber' }) {
  const tones = {
    slate: 'bg-slate-800/70 text-slate-300 ring-slate-600/40',
    sky: 'bg-sky-500/10 text-sky-300 ring-sky-500/30',
    emerald: 'bg-emerald-500/10 text-emerald-300 ring-emerald-500/30',
    amber: 'bg-amber-500/10 text-amber-300 ring-amber-500/30',
  };
  return <span className={`inline-block rounded px-1.5 py-0.5 font-mono text-[11px] ring-1 ${tones[tone]}`}>{children}</span>;
}

function ReplayTag() {
  return (
    <span className="inline-flex items-center gap-1.5 rounded-md border border-slate-800 bg-slate-900/70 px-2.5 py-1 text-[11px] font-medium text-slate-400 backdrop-blur-sm">
      <Activity size={12} className="text-sky-400" />
      captured arc
      <span className="text-slate-600">·</span>
      <span className="text-amber-400/80">replay</span>
    </span>
  );
}

function ErrorBox({ msg }: { msg: string }) {
  return (
    <div className="flex h-full w-full items-center justify-center bg-slate-950 p-8 text-slate-100">
      <div
        data-testid="arc-error"
        className="max-w-xl rounded-lg border border-red-500/30 bg-red-950/30 px-5 py-4 text-sm leading-relaxed text-red-200"
      >
        <p className="mb-1 font-semibold text-red-300">Agent Arc could not load the capture</p>
        {msg}
      </div>
    </div>
  );
}

function SectionHeading({ beat, title }: { beat: Beat | undefined; title: string }) {
  if (!beat) return null;
  return (
    <div className="mb-3 flex items-center gap-2">
      <span className="font-mono text-[11px] text-slate-500">Beat {beat.beat}</span>
      <h3 className="text-sm font-semibold tracking-tight text-slate-100">{title}</h3>
      <StatusPill status={beat.status} />
    </div>
  );
}

export default function AgentArcTab() {
  const [capture, setCapture] = useState<Capture | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    // No silent fallback (A1): surface the backend's actionable message rather
    // than rendering an empty or invented arc.
    fetch('/api/demo/finops-arc')
      .then(async (r) => {
        if (!r.ok) {
          const body = await r.json().catch(() => null);
          throw new Error(body?.detail || `finops-arc endpoint returned ${r.status}`);
        }
        return r.json();
      })
      .then((data: Capture) => {
        if (!cancelled) setCapture(data);
      })
      .catch((e) => {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      });
    return () => {
      cancelled = true;
    };
  }, []);

  if (error) return <ErrorBox msg={error} />;
  if (!capture) {
    return (
      <div className="flex h-full w-full items-center justify-center bg-slate-950 text-sm text-slate-500">
        Loading the captured arc…
      </div>
    );
  }

  const beat2 = beatByNum(capture, 2);
  const beat3 = beatByNum(capture, 3);
  const beat4 = beatByNum(capture, 4);
  const beat5 = beatByNum(capture, 5);
  const beat6 = beatByNum(capture, 6);
  if (!beat2 || !beat3 || !beat4 || !beat5 || !beat6) {
    return <ErrorBox msg="The capture is missing one of the required beats (2, 3, 4, 5, 6); refusing to render a partial arc." />;
  }

  const b2 = beat2.values as unknown as Beat2Values;
  const b3 = beat3.values as unknown as Beat3Values;
  const b4 = beat4.values as unknown as Beat4Values;
  const b6 = beat6.values as unknown as Beat6Values;
  const { headline, target } = capture;
  const br = headline.boundary_record;
  const { nodes, edges } = buildBeat3Graph(target.entity_id, b2, b3);
  const actionRows: ActionRow[] = [b4.hitl_row, b4.autonomous_row];

  return (
    <div className="h-full w-full overflow-y-auto bg-slate-950 text-slate-100">
      <div className="mx-auto max-w-6xl px-6 py-6">
        {/* Header */}
        <header className="mb-6 rounded-xl border border-slate-800 bg-slate-900/40 p-5">
          <div className="flex flex-wrap items-center gap-2">
            <Boxes size={18} className="text-emerald-400" />
            <h1 className="text-sm font-semibold tracking-tight text-slate-100">
              contextOS <span className="text-slate-500">·</span> Agent Arc
            </h1>
            <span
              data-testid="arc-overall"
              className={`ml-1 inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-[11px] font-semibold uppercase tracking-wider ring-1 ${
                capture.overall === 'PASS'
                  ? 'bg-emerald-500/10 text-emerald-300 ring-emerald-500/30'
                  : 'bg-red-500/10 text-red-300 ring-red-500/30'
              }`}
            >
              {capture.overall === 'PASS' ? <CheckCircle2 size={12} /> : <XCircle size={12} />}
              {capture.overall}
            </span>
            <div className="ml-auto flex items-center gap-2">
              <ReplayTag />
            </div>
          </div>

          <div data-testid="arc-headline" className="mt-4">
            <p className="text-2xl font-semibold tracking-tight text-slate-50">
              {headline.worst_efficiency_team}
              <span className="mx-2 text-slate-600">·</span>
              <span className="text-emerald-300">{perDeploy(headline.usd_per_deploy)} / deploy</span>
            </p>
            <p className="mt-1 text-sm text-slate-400">
              worst cost-efficiency team — found by cross-source traversal over{' '}
              <span className="font-mono text-slate-300">{b3.provenance.cost_source}</span> ×{' '}
              <span className="font-mono text-slate-300">{b3.provenance.output_source}</span>. A single billing system
              cannot reach this answer.
            </p>
          </div>

          {/* Target + caller identity. entity_id is the displayed business key (I2);
              the tenant UUID is machine-only, shown masked, never in full. */}
          <div className="mt-4 flex flex-wrap items-center gap-2 border-t border-slate-800 pt-4">
            <span className="flex items-center gap-1.5 text-[11px] text-slate-500">
              <KeyRound size={12} className="text-sky-400" /> caller identity
            </span>
            <Chip tone="sky">{target.identity}</Chip>
            <span className="ml-2 text-[11px] text-slate-500">entity</span>
            <Chip tone="emerald">{target.entity_id}</Chip>
            <span className="ml-2 text-[11px] text-slate-500">tenant</span>
            <Chip>{target.tenant_id.slice(0, 8)}…</Chip>
            <span className="ml-2 text-[11px] text-slate-500">scope</span>
            <Chip tone="amber">{b6.revoke.before_domains.join(', ')}</Chip>
          </div>
        </header>

        {/* 6-beat timeline */}
        <div className="mb-6 grid grid-cols-2 gap-2 sm:grid-cols-3 lg:grid-cols-6">
          {capture.beats.map((b) => (
            <div
              key={b.beat}
              data-testid={`beat-${b.beat}`}
              className="rounded-lg border border-slate-800 bg-slate-900/40 p-3"
            >
              <div className="mb-2 flex items-center justify-between">
                <span className="font-mono text-[11px] text-slate-500">Beat {b.beat}</span>
                <StatusPill status={b.status} />
              </div>
              <p className="text-[12px] leading-snug text-slate-300">{b.name}</p>
            </div>
          ))}
        </div>

        {/* Beat 3 — cross-source traversal (React-Flow Glass Box) */}
        <section className="mb-6 rounded-xl border border-slate-800 bg-slate-900/40 p-5">
          <SectionHeading beat={beat3} title="Cross-source traversal" />
          <p className="mb-3 text-xs text-slate-400">
            cost → team → output. The billing-only path is shallow and excised; the trusted answer joins{' '}
            <span className="font-mono text-slate-300">{b3.provenance.cost_source}</span> (cost) with{' '}
            <span className="font-mono text-slate-300">{b3.provenance.output_source}</span> (output).
          </p>
          <div className="h-[560px] w-full overflow-hidden rounded-lg border border-slate-800 bg-slate-950">
            <ReactFlowProvider>
              <Canvas nodes={nodes} edges={edges} />
            </ReactFlowProvider>
          </div>
        </section>

        {/* Beat 4 — ACT (HITL + autonomous) */}
        <section className="mb-6 rounded-xl border border-slate-800 bg-slate-900/40 p-5">
          <SectionHeading beat={beat4} title="Act — both modes" />
          <div className="space-y-2" data-testid="action-rows">
            {actionRows.map((row) => {
              const cid = row.mode === 'hitl' ? b4.correlation_ids.hitl : b4.correlation_ids.autonomous;
              return (
                <div
                  key={row.mode}
                  data-testid={`action-${row.mode}`}
                  className="flex flex-wrap items-center gap-x-3 gap-y-1.5 rounded-lg border border-slate-800 bg-slate-950/60 px-3 py-2.5"
                >
                  <Chip tone={row.mode === 'autonomous' ? 'amber' : 'sky'}>{row.mode}</Chip>
                  <span className="text-[11px] text-slate-500">target</span>
                  <Chip tone="emerald">{row.target}</Chip>
                  <span className="text-[11px] text-slate-500">identity</span>
                  <Chip tone="sky">{target.identity}</Chip>
                  <span className="text-[11px] text-slate-500">correlation_id</span>
                  <Chip>{cid}</Chip>
                  <span className="ml-auto flex items-center gap-1.5">
                    <span className="text-[11px] text-slate-500">{row.approver}</span>
                    <span
                      className={`inline-flex items-center gap-1 text-[11px] font-semibold ${
                        row.outcome === 'success' ? 'text-emerald-300' : 'text-red-300'
                      }`}
                    >
                      <CheckCircle2 size={12} /> {row.outcome}
                    </span>
                  </span>
                  <div className="flex w-full items-center gap-1.5 pt-1">
                    <span className="text-[11px] text-slate-500">basis</span>
                    {row.basis_sources.map((s) => (
                      <Chip key={s} tone="slate">{s}</Chip>
                    ))}
                  </div>
                </div>
              );
            })}
          </div>
        </section>

        {/* Beat 5 — Governance boundary record (read ↔ action join) */}
        <section className="mb-6 rounded-xl border border-slate-800 bg-slate-900/40 p-5">
          <SectionHeading beat={beat5} title="Governance boundary record" />
          <div
            data-testid="governance-record"
            className="grid grid-cols-1 gap-px overflow-hidden rounded-lg border border-slate-800 bg-slate-800/60 md:grid-cols-4"
          >
            <div className="bg-slate-950/70 p-3">
              <div className="mb-1.5 flex items-center gap-1.5 text-[10px] font-medium uppercase tracking-wider text-slate-500">
                <KeyRound size={11} className="text-sky-400" /> who asked
              </div>
              <Chip tone="sky">{br.who_asked}</Chip>
            </div>
            <div className="bg-slate-950/70 p-3">
              <div className="mb-1.5 text-[10px] font-medium uppercase tracking-wider text-slate-500">
                served under scope
              </div>
              <p className="text-sm text-slate-200">
                {br.reads_under_identity} <span className="text-[11px] text-slate-500">MCP reads under identity</span>
              </p>
            </div>
            <div className="bg-slate-950/70 p-3">
              <div className="mb-1.5 text-[10px] font-medium uppercase tracking-wider text-slate-500">what resolved</div>
              <Chip tone="emerald">{br.what_resolved.worst_efficiency_team}</Chip>
              <span className="ml-1.5 font-mono text-[12px] text-emerald-300">
                {perDeploy(br.what_resolved.usd_per_deploy)}/deploy
              </span>
            </div>
            <div className="bg-slate-950/70 p-3">
              <div className="mb-1.5 flex items-center gap-1.5 text-[10px] font-medium uppercase tracking-wider text-slate-500">
                <ShieldCheck size={11} className="text-emerald-400" /> what action
              </div>
              <div className="space-y-1">
                {br.what_action_correlation_ids.map((cid) => (
                  <Chip key={cid}>{cid}</Chip>
                ))}
              </div>
            </div>
          </div>
          <p className="mt-3 flex flex-wrap items-center gap-1.5 text-xs text-slate-400">
            <ShieldCheck size={13} className="text-emerald-400" />
            read ↔ action joined by
            {br.joined_by.map((j) => (
              <Chip key={j} tone="emerald">{j}</Chip>
            ))}
            — the same scoped identity that read the context is the identity that acted, tied by correlation_id.
          </p>
        </section>

        {/* Beat 6 — live revocation strip (before → denied → after) */}
        <section className="mb-2 rounded-xl border border-slate-800 bg-slate-900/40 p-5">
          <SectionHeading beat={beat6} title="Live revocation — query-time enforcement" />
          <div data-testid="revocation-strip" className="flex flex-wrap items-stretch gap-2">
            <div className="flex-1 rounded-lg border border-emerald-500/30 bg-emerald-950/20 p-3">
              <div className="text-[10px] font-medium uppercase tracking-wider text-emerald-400/80">before</div>
              <p className="mt-1 text-sm text-emerald-200">
                allowed · <span className="font-mono">{b6.revoke.before_rows}</span> rows
              </p>
              <p className="mt-0.5 font-mono text-[11px] text-slate-500">{b6.revoke.before_domains.join(', ')}</p>
            </div>
            <div className="flex items-center text-slate-600">
              <ArrowRight size={18} />
            </div>
            <div className="flex-1 rounded-lg border border-red-500/40 bg-red-950/25 p-3">
              <div className="text-[10px] font-medium uppercase tracking-wider text-red-400/80">scope narrowed</div>
              <p className="mt-1 text-sm font-semibold text-red-200">
                {b6.revoke.after_narrow} · <span className="font-mono">{b6.revoke.after_rows}</span> rows
              </p>
              <p className="mt-0.5 text-[11px] text-slate-500">same token, denied at query time</p>
            </div>
            <div className="flex items-center text-slate-600">
              <ArrowRight size={18} />
            </div>
            <div className="flex-1 rounded-lg border border-emerald-500/30 bg-emerald-950/20 p-3">
              <div className="text-[10px] font-medium uppercase tracking-wider text-emerald-400/80">restored</div>
              <p className="mt-1 text-sm text-emerald-200">
                allowed · <span className="font-mono">{b6.revoke.confirm_rows}</span> rows
              </p>
              <p className="mt-0.5 font-mono text-[11px] text-slate-500">{b6.revoke.restored_domains.join(', ')}</p>
            </div>
          </div>
        </section>

        <p className="px-1 pb-4 pt-3 text-[11px] text-slate-600">
          {capture.arc} · {capture._capture_file ?? capture.stamp}
        </p>
      </div>
    </div>
  );
}
