/**
 * Entity-edge graph (ContextOS Gate 1B hero, Stage 4 renderer).
 *
 * Renders the persisted, typed entity↔entity subgraph for one entity —
 * GET /api/dcl/graph/subgraph?entity_id=<selected> (tenant_id resolves
 * server-side from the entity, never typed or displayed: I2/I4). This is
 * DISTINCT from the fabric Sankey (DataDrivenSankey) — the fabric view shows
 * fabric_plane → source → ontology flow; this shows department → job_family /
 * exit_theme relationships with the synthesized edge properties (the
 * cross-source comp-gap the agent traverses).
 *
 * Two layers: departments on the left, job_families + exit_themes on the
 * right (every edge runs department → {job_family | exit_theme}). Simple SVG
 * node-link (kept consistent with the SVG approach in DataDrivenSankey).
 *
 * Fail loud on fetch error (A1) — never a silent empty graph.
 */

import { useEffect, useMemo, useRef, useState } from 'react';
import { useResizeObserver } from '../../hooks/useResizeObserver';

const BG_COLOR = '#060a14';

/** Edge as returned by /api/dcl/graph/subgraph. */
interface SubgraphEdge {
  src_type: string;
  src_key: string;
  edge_type: string;
  dst_type: string;
  dst_key: string;
  properties: Record<string, unknown> | null;
  source_system?: string | null;
  confidence_tier?: string | null;
  derivation?: string | null;
}

interface SubgraphNode {
  node_type: string;
  node_key: string;
  label?: string;
}

interface SubgraphResponse {
  tenant_id: string;
  entity_id: string;
  counts: { nodes: number; edges: number; by_type: Record<string, number> };
  nodes: SubgraphNode[];
  edges: SubgraphEdge[];
}

/** Node fill by node_type — distinct colors, no hardcoded entity names. */
const NODE_COLOR: Record<string, string> = {
  department: '#1e3a8a',     // deep blue — the left layer
  job_family: '#0e7490',     // teal — resolved role family
  exit_theme: '#9333ea',     // violet — attrition driver
};
const NODE_TEXT: Record<string, string> = {
  department: '#c7d2fe',
  job_family: '#cffafe',
  exit_theme: '#f3e8ff',
};
const NODE_FALLBACK = '#475569';

/** Edge stroke + style by edge_type. */
const EDGE_STYLE: Record<string, { color: string; dash?: string; label: string }> = {
  BELOW_MARKET: { color: '#f43f5e', label: 'below market' },           // red — the comp-gap hero
  DRIVEN_BY: { color: '#a855f7', dash: '6 4', label: 'driven by' },    // violet dashed
  RESOLVES_TO: { color: '#14b8a6', dash: '2 4', label: 'resolves to' },// teal dotted
};
const EDGE_FALLBACK = { color: '#64748b', label: 'related' };

const nodeColor = (t: string) => NODE_COLOR[t] ?? NODE_FALLBACK;
const nodeText = (t: string) => NODE_TEXT[t] ?? '#e2e8f0';
const edgeStyle = (t: string) => EDGE_STYLE[t] ?? EDGE_FALLBACK;

const num = (v: unknown): number | null =>
  typeof v === 'number' && Number.isFinite(v) ? v : null;

interface LaidNode extends SubgraphNode {
  id: string;
  x: number;
  y: number;
}
interface LaidEdge extends SubgraphEdge {
  id: string;
  x1: number; y1: number; x2: number; y2: number;
}

const NODE_R = 9;
const MARGIN = { top: 56, bottom: 28, left: 150, right: 170 };

/** Two-column layout: departments left, job_family+exit_theme right. */
function layout(
  data: SubgraphResponse,
  width: number,
  height: number,
): { nodes: LaidNode[]; edges: LaidEdge[] } {
  const key = (t: string, k: string) => `${t}::${k}`;
  const left = data.nodes.filter((n) => n.node_type === 'department');
  const right = data.nodes.filter((n) => n.node_type !== 'department');

  const colX = (col: 0 | 1) =>
    col === 0 ? MARGIN.left : width - MARGIN.right;
  const colY = (idx: number, count: number) => {
    const usable = Math.max(1, height - MARGIN.top - MARGIN.bottom);
    if (count <= 1) return MARGIN.top + usable / 2;
    return MARGIN.top + (usable * idx) / (count - 1);
  };

  const pos = new Map<string, { x: number; y: number }>();
  const nodes: LaidNode[] = [];
  left.forEach((n, i) => {
    const p = { x: colX(0), y: colY(i, left.length) };
    pos.set(key(n.node_type, n.node_key), p);
    nodes.push({ ...n, id: key(n.node_type, n.node_key), ...p });
  });
  right.forEach((n, i) => {
    const p = { x: colX(1), y: colY(i, right.length) };
    pos.set(key(n.node_type, n.node_key), p);
    nodes.push({ ...n, id: key(n.node_type, n.node_key), ...p });
  });

  const edges: LaidEdge[] = [];
  data.edges.forEach((e, i) => {
    const s = pos.get(key(e.src_type, e.src_key));
    const t = pos.get(key(e.dst_type, e.dst_key));
    if (!s || !t) return; // a node the subgraph didn't list — skip, never invent
    edges.push({ ...e, id: `ee-${i}`, x1: s.x, y1: s.y, x2: t.x, y2: t.y });
  });

  return { nodes, edges };
}

/** Human inspector line for an edge. */
function edgeInspectorLine(e: SubgraphEdge): string {
  const p = e.properties ?? {};
  if (e.edge_type === 'BELOW_MARKET') {
    const gap = num(p.gap_pct);
    const internal = num(p.internal_median);
    const market = num(p.market_median);
    const iSrc = String(p.internal_source ?? '');
    const mSrc = String(p.market_source ?? '');
    return (
      `${e.src_key} ${gap !== null ? `${gap}% below market` : 'below market'}: ` +
      `internal ${internal ?? '?'} (${iSrc}) vs market ${market ?? '?'} (${mSrc}) ` +
      `→ ${e.dst_key}`
    );
  }
  if (e.edge_type === 'DRIVEN_BY') {
    const share = num(p.share);
    return (
      `${e.src_key} exits driven by ${e.dst_key}` +
      (share !== null ? ` (share ${(share * 100).toFixed(1)}%)` : '') +
      (p.source ? ` — ${String(p.source)}` : '')
    );
  }
  if (e.edge_type === 'RESOLVES_TO') {
    return `${e.src_key} resolves to job family ${e.dst_key}`;
  }
  return `${e.src_key} ${edgeStyle(e.edge_type).label} ${e.dst_key}`;
}

export function EntityEdgeGraph({ entityId }: { entityId: string }) {
  const containerRef = useRef<HTMLDivElement>(null);
  const size = useResizeObserver(containerRef, { debounceMs: 120, initialDelay: 40 });
  const [data, setData] = useState<SubgraphResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedEdgeId, setSelectedEdgeId] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  useEffect(() => {
    if (!entityId) {
      setData(null);
      setError(null);
      return;
    }
    if (abortRef.current) abortRef.current.abort();
    const controller = new AbortController();
    abortRef.current = controller;
    setLoading(true);
    setError(null);
    setSelectedEdgeId(null);

    const url = `/api/dcl/graph/subgraph?entity_id=${encodeURIComponent(entityId)}`;
    fetch(url, { signal: controller.signal })
      .then(async (res) => {
        if (!res.ok) {
          const body = await res.json().catch(() => null);
          const detail =
            (body && (body.detail?.message || body.detail)) || `HTTP ${res.status}`;
          throw new Error(
            `Relationships graph could not load for ${entityId} — ${detail} (GET ${url})`,
          );
        }
        return res.json() as Promise<SubgraphResponse>;
      })
      .then((body) => {
        if (controller.signal.aborted) return;
        setData(body);
        setLoading(false);
      })
      .catch((err) => {
        if (controller.signal.aborted) return;
        setError(err instanceof Error ? err.message : 'Failed to load relationships graph');
        setLoading(false);
      });

    return () => controller.abort();
  }, [entityId]);

  const laid = useMemo(() => {
    if (!data || size.width === 0 || size.height === 0) return null;
    return layout(data, size.width, size.height);
  }, [data, size.width, size.height]);

  const selectedEdge = useMemo(
    () => (laid && selectedEdgeId ? laid.edges.find((e) => e.id === selectedEdgeId) ?? null : null),
    [laid, selectedEdgeId],
  );

  // Fail loud — show the real error, never a silent empty graph (A1).
  if (error) {
    return (
      <div
        data-testid="entity-edge-graph"
        data-state="error"
        className="w-full h-full flex items-center justify-center"
        style={{ backgroundColor: BG_COLOR }}
      >
        <div className="text-center p-6 rounded-lg border border-destructive/30 bg-destructive/5 max-w-lg">
          <p className="text-sm text-destructive font-medium" data-testid="ee-error">
            {error}
          </p>
        </div>
      </div>
    );
  }

  return (
    <div
      ref={containerRef}
      data-testid="entity-edge-graph"
      data-entity-id={entityId}
      data-edge-count={data?.counts.edges ?? 0}
      className="w-full h-full overflow-hidden relative select-none"
      style={{ backgroundColor: BG_COLOR }}
    >
      {(!laid || loading) && (
        <div className="absolute inset-0 flex items-center justify-center">
          <span className="text-xs text-slate-400">
            {loading ? 'Loading relationships…' : 'No relationships'}
          </span>
        </div>
      )}

      {laid && (
        <svg
          width={size.width}
          height={size.height}
          role="img"
          aria-label="Entity relationship graph"
          className="overflow-visible"
        >
          {/* Column headers */}
          <text x={MARGIN.left} y={28} textAnchor="middle" fill="rgba(148,163,184,0.7)"
                fontSize={11} fontFamily="ui-monospace, monospace" letterSpacing="0.05em">
            departments
          </text>
          <text x={size.width - MARGIN.right} y={28} textAnchor="middle" fill="rgba(148,163,184,0.7)"
                fontSize={11} fontFamily="ui-monospace, monospace" letterSpacing="0.05em">
            job families · exit themes
          </text>

          {/* Edges */}
          <g aria-label="Relationships">
            {laid.edges.map((e) => {
              const st = edgeStyle(e.edge_type);
              const isSel = e.id === selectedEdgeId;
              const gap = e.edge_type === 'BELOW_MARKET' ? num(e.properties?.gap_pct) : null;
              return (
                <line
                  key={e.id}
                  data-testid="ee-edge"
                  data-edge-type={e.edge_type}
                  data-src={e.src_key}
                  data-dst={e.dst_key}
                  {...(gap !== null ? { 'data-gap-pct': String(gap) } : {})}
                  x1={e.x1}
                  y1={e.y1}
                  x2={e.x2}
                  y2={e.y2}
                  stroke={st.color}
                  strokeWidth={isSel ? 3.5 : 1.8}
                  strokeDasharray={st.dash}
                  opacity={isSel ? 1 : 0.65}
                  className="cursor-pointer"
                  style={{ pointerEvents: 'stroke', transition: 'opacity 150ms' }}
                  onMouseEnter={() => setSelectedEdgeId(e.id)}
                  onClick={() => setSelectedEdgeId(e.id)}
                  aria-label={`${e.src_key} ${st.label} ${e.dst_key}`}
                />
              );
            })}
          </g>

          {/* Nodes */}
          <g aria-label="Entities">
            {laid.nodes.map((n) => (
              <g
                key={n.id}
                data-testid="ee-node"
                data-node-type={n.node_type}
                data-node-key={n.node_key}
                transform={`translate(${n.x},${n.y})`}
              >
                <circle r={NODE_R} fill={nodeColor(n.node_type)} stroke="#0b1220" strokeWidth={1.5} />
                <text
                  x={n.node_type === 'department' ? -(NODE_R + 6) : NODE_R + 6}
                  y={4}
                  textAnchor={n.node_type === 'department' ? 'end' : 'start'}
                  fill={nodeText(n.node_type)}
                  fontSize={11}
                  fontFamily="ui-monospace, monospace"
                >
                  {n.node_key}
                </text>
              </g>
            ))}
          </g>
        </svg>
      )}

      {/* Inspector — the hovered/clicked edge's synthesized properties */}
      {selectedEdge && (
        <div
          data-testid="ee-inspector"
          data-edge-type={selectedEdge.edge_type}
          className="absolute bottom-3 left-3 right-3 max-w-2xl rounded-lg border border-border bg-card/95 p-3 shadow-xl pointer-events-none"
        >
          <div className="flex items-center gap-2">
            <span
              className="inline-block w-2.5 h-2.5 rounded-full"
              style={{ backgroundColor: edgeStyle(selectedEdge.edge_type).color }}
            />
            <span className="text-xs font-mono font-semibold text-foreground">
              {selectedEdge.edge_type}
            </span>
            {selectedEdge.derivation && (
              <span className="text-[10px] text-muted-foreground">
                {selectedEdge.derivation}
                {selectedEdge.source_system ? ` · ${selectedEdge.source_system}` : ''}
              </span>
            )}
          </div>
          <p className="text-xs text-foreground/90 mt-1.5" data-testid="ee-inspector-text">
            {edgeInspectorLine(selectedEdge)}
          </p>
        </div>
      )}
    </div>
  );
}
