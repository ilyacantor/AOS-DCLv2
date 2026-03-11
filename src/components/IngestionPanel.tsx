import { useState, useEffect, useCallback, Fragment } from 'react';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface IngestStats {
  total_runs: number;
  total_rows_buffered: number;
  total_drift_events: number;
  pipes_tracked: number;
  unique_sources: number;
  unique_tenants: number;
  tenant_names: string[];
  latest_run_id: string | null;
  latest_run_at: string | null;
  first_run_at: string | null;
  max_runs: number;
  max_rows: number;
  activity_entries?: number;
  total_drops?: number;
}

interface DropEntry {
  pipe_id: string;
  reason: string;
  error_code: string;
  source_system: string;
  timestamp: string;
  run_id: string;
  dispatch_id: string;
  snapshot_name: string;
  tenant_id: string;
}

interface ActivityEntry {
  phase: 'structure' | 'dispatch' | 'content';
  source: string;
  snapshot_name: string;
  run_id: string;
  timestamp: string;
  pipes: number;
  sors: number;
  tooling_pipes: number;
  fabrics: number;
  mapped_pipes: number;
  unmapped_pipes: number;
  sor_pipes: number;
  other_pipes: number;
  rows: number;
  records: number;
  dispatch_id: string;
  aod_run_id: string;
}

interface DispatchPipeDetail {
  pipe_id: string;
  source_system: string;
  row_count: number;
  schema_drift: boolean;
  category: 'mapped' | 'unmapped' | 'tooling' | 'unknown';
}

interface DispatchDetail {
  dispatch_id: string;
  mapped_count: number;
  unmapped_count: number;
  tooling_count: number;
  pipes: DispatchPipeDetail[];
}

const POLL_INTERVAL_MS = 5000;

// Phase display config
const PHASE_CONFIG: Record<string, { label: string; color: string; icon: string }> = {
  structure: { label: 'Structure', color: 'text-blue-400', icon: '1' },
  dispatch:  { label: 'Dispatch',  color: 'text-amber-400', icon: '2' },
  content:   { label: 'Content',   color: 'text-emerald-400', icon: '3' },
};

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export function IngestionPanel() {
  const [stats, setStats] = useState<IngestStats | null>(null);
  const [activity, setActivity] = useState<ActivityEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [drops, setDrops] = useState<DropEntry[]>([]);
  const [dropsOpen, setDropsOpen] = useState(false);
  const [expandedSnap, setExpandedSnap] = useState<string | null>(null);
  const [expandedPhase, setExpandedPhase] = useState<string | null>(null);
  const [resetting, setResetting] = useState(false);
  const [drillDispatchId, setDrillDispatchId] = useState<string | null>(null);
  const [drillData, setDrillData] = useState<DispatchDetail | null>(null);
  const [drillFilter, setDrillFilter] = useState<string | null>(null);
  const [drillLoading, setDrillLoading] = useState(false);

  const fetchStats = async () => {
    try {
      const res = await fetch('/api/dcl/ingest/stats');
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json: IngestStats = await res.json();
      setStats(json);
      setError(null);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : 'Failed to fetch ingestion data';
      setError(msg);
    } finally {
      setLoading(false);
    }
  };

  const fetchDrops = async () => {
    try {
      const res = await fetch('/api/dcl/ingest/drops');
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = await res.json();
      setDrops(json.drops ?? []);
    } catch (err) {
      console.error('[IngestionPanel] Failed to fetch drops:', err);
    }
  };

  const fetchActivity = async () => {
    try {
      const res = await fetch('/api/dcl/ingest/activity');
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = await res.json();
      setActivity(json.activity ?? []);
    } catch (err) {
      console.error('[IngestionPanel] Failed to fetch activity:', err);
    }
  };

  const handleReset = async () => {
    if (!window.confirm('Clear all ingest data and pipe definitions? This cannot be undone.')) return;
    setResetting(true);
    try {
      await fetch('/api/dcl/ingest/flush', { method: 'POST' });
      fetchAll();
    } catch (e) {
      console.error('[IngestionPanel] Reset failed:', e);
    } finally {
      setResetting(false);
    }
  };

  const fetchDrillDown = async (dispatchId: string, filter?: string) => {
    if (drillDispatchId === dispatchId && drillFilter === (filter ?? null)) {
      // Toggle off
      setDrillDispatchId(null);
      setDrillData(null);
      setDrillFilter(null);
      return;
    }
    setDrillDispatchId(dispatchId);
    setDrillFilter(filter ?? null);
    setDrillLoading(true);
    try {
      const res = await fetch(`/api/dcl/ingest/dispatches/${dispatchId}`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = await res.json();
      setDrillData(json);
    } catch (err) {
      console.error('[IngestionPanel] Failed to fetch dispatch detail:', err);
      setDrillData(null);
    } finally {
      setDrillLoading(false);
    }
  };

  const fetchAll = useCallback(() => {
    fetchStats();
    fetchActivity();
    fetchDrops();
  }, []);

  useEffect(() => {
    fetchAll();
    const interval = setInterval(fetchAll, POLL_INTERVAL_MS);
    return () => clearInterval(interval);
  }, [fetchAll]);

  // --- Helpers ---

  const fmtDate = (ts: string) => {
    try {
      return new Date(ts).toLocaleString('en-US', {
        month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', hour12: true,
      });
    } catch { return ts; }
  };

  const fmtRows = (n: number) => n >= 1000 ? `${(n / 1000).toFixed(1)}k` : String(n);

  // Group activity by dispatch_id (one collapsible entry per run cycle)
  const groupedByRun: Record<string, ActivityEntry[]> = {};
  const runOrder: string[] = [];
  let _unkeyed = 0;
  for (const entry of activity) {
    const key = entry.dispatch_id || `_unkeyed_${_unkeyed++}`;
    if (!groupedByRun[key]) {
      groupedByRun[key] = [];
      runOrder.push(key);
    }
    groupedByRun[key].push(entry);
  }

  // --- Render ---

  if (loading) {
    return (
      <div className="h-full flex flex-col min-h-0">
        <div className="shrink-0 flex items-center justify-between px-6 py-3 border-b border-border bg-card/50">
          <h2 className="text-sm font-semibold">Ingest Activity</h2>
        </div>
        <div className="flex-1 flex items-center justify-center text-muted-foreground">
          <div className="flex flex-col items-center gap-3">
            <div className="w-8 h-8 border-2 border-primary border-t-transparent rounded-full animate-spin" />
            <span className="text-sm">Loading ingestion data...</span>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="h-full flex flex-col min-h-0">
      {/* Header */}
      <div className="shrink-0 px-6 py-3 border-b border-border bg-card/50">
        <div className="flex items-center justify-between">
          <h2 className="text-sm font-semibold">Ingest Activity</h2>
          <div className="flex items-center gap-2">
            <span className="text-[10px] text-muted-foreground">
              Auto-refresh {POLL_INTERVAL_MS / 1000}s
            </span>
            <button
              onClick={handleReset}
              disabled={resetting}
              className="px-3 py-1 text-xs rounded bg-red-500/20 text-red-400 hover:bg-red-500/30 border border-red-500/30 disabled:opacity-50"
            >
              {resetting ? 'Resetting...' : 'Reset'}
            </button>
            <button
              onClick={fetchAll}
              className="px-3 py-1 text-xs rounded bg-primary text-primary-foreground hover:bg-primary/90"
            >
              Refresh
            </button>
          </div>
        </div>
      </div>

      <div className="flex-1 overflow-y-auto p-4 space-y-4 min-h-0">
        {error && (
          <div className="rounded-lg border border-red-500/20 bg-red-500/10 p-3 text-center">
            <span className="text-sm text-red-400">{error}</span>
            <button onClick={fetchAll} className="ml-3 px-3 py-1 text-xs rounded bg-primary text-primary-foreground hover:bg-primary/90">
              Retry
            </button>
          </div>
        )}

        {/* Summary bar */}
        {stats && (stats.total_runs > 0 || activity.length > 0) && (
          <div className="rounded-lg border border-border bg-card/30 px-4 py-2.5">
            <div className="flex items-center gap-6 text-xs font-mono">
              <span><span className="text-foreground font-semibold">{runOrder.length}</span> <span className="text-muted-foreground">runs</span></span>
              <span><span className="text-foreground font-semibold">{activity.length}</span> <span className="text-muted-foreground">events</span></span>
              <span><span className="text-foreground font-semibold">{stats.total_runs}</span> <span className="text-muted-foreground">receipts</span></span>
              <span><span className="text-foreground font-semibold">{fmtRows(stats.total_rows_buffered)}</span> <span className="text-muted-foreground">rows</span></span>
              {stats.total_drift_events > 0 && (
                <span><span className="text-amber-400 font-semibold">{stats.total_drift_events}</span> <span className="text-muted-foreground">drift</span></span>
              )}
              {(stats.total_drops ?? 0) > 0 && (
                <span><span className="text-red-400 font-semibold">{stats.total_drops}</span> <span className="text-muted-foreground">drops</span></span>
              )}
            </div>
          </div>
        )}

        {/* Drops section */}
        {drops.length > 0 && (
          <div className="rounded-lg border border-red-500/30 bg-red-500/5 overflow-hidden">
            <button
              onClick={() => setDropsOpen(!dropsOpen)}
              className="w-full flex items-center gap-2 px-4 py-2.5 text-xs hover:bg-red-500/10 transition-colors"
            >
              <svg
                className={`w-2.5 h-2.5 shrink-0 transition-transform duration-150 text-red-400 ${dropsOpen ? 'rotate-90' : ''}`}
                fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}
              >
                <path strokeLinecap="round" strokeLinejoin="round" d="M9 5l7 7-7 7" />
              </svg>
              <span className="font-semibold text-red-400">Drops</span>
              <span className="text-red-400/70 font-mono">{drops.length}</span>
            </button>
            {dropsOpen && (
              <table className="w-full text-xs">
                <thead>
                  <tr className="border-t border-red-500/20 text-[10px] uppercase tracking-wider text-muted-foreground">
                    <th className="text-left px-3 py-2 font-medium">Source</th>
                    <th className="text-left px-3 py-2 font-medium">Tenant</th>
                    <th className="text-left px-3 py-2 font-medium">Error</th>
                    <th className="text-left px-3 py-2 font-medium">Reason</th>
                    <th className="text-left px-3 py-2 font-medium">Time</th>
                  </tr>
                </thead>
                <tbody>
                  {drops.map((drop, idx) => (
                    <tr key={idx} className="border-t border-red-500/10 hover:bg-red-500/5 transition-colors" title={`pipe_id: ${drop.pipe_id}`}>
                      <td className="px-3 py-1.5 font-semibold text-foreground/80">
                        {drop.source_system}
                      </td>
                      <td className="px-3 py-1.5 text-muted-foreground font-mono">
                        {drop.tenant_id || '-'}
                      </td>
                      <td className="px-3 py-1.5">
                        <span className="inline-block px-1.5 py-0.5 rounded text-[10px] font-semibold bg-red-500/20 text-red-400 border border-red-500/30">
                          {drop.error_code}
                        </span>
                      </td>
                      <td className="px-3 py-1.5 text-muted-foreground max-w-[200px] truncate" title={drop.reason}>
                        {drop.reason}
                      </td>
                      <td className="px-3 py-1.5 text-muted-foreground whitespace-nowrap">
                        {fmtDate(drop.timestamp)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        )}

        {/* Activity log — 3-phase view */}
        <div>
          {activity.length === 0 ? (
            <div className="rounded-lg border border-border bg-card/30 p-6 text-center">
              <div className="text-muted-foreground text-sm">No ingestion activity yet</div>
              <div className="text-muted-foreground text-xs mt-1">
                Waiting for AAM to push structure, dispatch manifest, and Farm to push content...
              </div>
            </div>
          ) : (
            <div className="rounded-lg border border-border bg-card/30 overflow-hidden">
              <table className="w-full text-xs">
                <thead>
                  <tr className="border-b border-border text-[10px] uppercase tracking-wider text-muted-foreground">
                    <th className="text-left px-3 py-2 font-medium">Snapshot</th>
                    <th className="text-left px-3 py-2 font-medium">AOD Run</th>
                    <th className="text-left px-3 py-2 font-medium">Phase</th>
                    <th className="text-left px-3 py-2 font-medium">Source</th>
                    <th className="text-left px-3 py-2 font-medium">Date / Time</th>
                    <th className="text-right px-3 py-2 font-medium">Pipes</th>
                    <th className="text-right px-3 py-2 font-medium">SORs</th>
                    <th className="text-right px-3 py-2 font-medium">Tooling</th>
                    <th className="text-right px-3 py-2 font-medium">Fabrics</th>
                  </tr>
                </thead>
                <tbody>
                  {runOrder.map((runKey) => {
                    const entries = groupedByRun[runKey];
                    // Sort: structure first, dispatch second, content third
                    const phaseOrder = { structure: 0, dispatch: 1, content: 2 };
                    const sorted = [...entries].sort(
                      (a, b) => (phaseOrder[a.phase] ?? 9) - (phaseOrder[b.phase] ?? 9)
                    );
                    const isExpanded = expandedSnap === runKey;
                    const snapName = sorted[0]?.snapshot_name || '(unknown)';

                    return (
                      <Fragment key={runKey}>
                        {/* Run group header */}
                        <tr
                          onClick={() => setExpandedSnap(isExpanded ? null : runKey)}
                          className="border-b border-border/50 bg-card/10 hover:bg-card/30 cursor-pointer transition-colors"
                        >
                          <td colSpan={9} className="px-3 py-2">
                            <div className="flex items-center gap-2">
                              <svg
                                className={`w-2.5 h-2.5 shrink-0 transition-transform duration-150 text-muted-foreground ${isExpanded ? 'rotate-90' : ''}`}
                                fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}
                              >
                                <path strokeLinecap="round" strokeLinejoin="round" d="M9 5l7 7-7 7" />
                              </svg>
                              <span className="font-mono font-semibold text-foreground">{snapName}</span>
                              <span className="text-muted-foreground/60 text-[10px] ml-2">
                                {sorted.length} phase{sorted.length !== 1 ? 's' : ''}
                              </span>
                              {/* Phase dots */}
                              <div className="flex items-center gap-1 ml-2">
                                {(['structure', 'dispatch', 'content'] as const).map((p) => {
                                  const has = sorted.some((e) => e.phase === p);
                                  return (
                                    <span
                                      key={p}
                                      className={`w-2 h-2 rounded-full ${has ? phaseDotColor(p) : 'bg-muted-foreground/20'}`}
                                      title={`${PHASE_CONFIG[p].label}: ${has ? 'received' : 'pending'}`}
                                    />
                                  );
                                })}
                              </div>
                              {sorted[0]?.aod_run_id && (
                                <span className="text-muted-foreground/50 text-[10px] font-mono ml-auto">
                                  {sorted[0].aod_run_id}
                                </span>
                              )}
                            </div>
                          </td>
                        </tr>

                        {/* Individual phase rows (shown when expanded) */}
                        {isExpanded && sorted.map((entry, idx) => {
                          const cfg = PHASE_CONFIG[entry.phase] ?? { label: entry.phase, color: 'text-muted-foreground', icon: '?' };
                          const srcColor = entry.source === 'Farm'
                            ? 'text-emerald-400'
                            : entry.source === 'AAM'
                              ? 'text-blue-400'
                              : 'text-amber-400';
                          const phaseKey = `${runKey}-${entry.phase}-${idx}`;
                          const isPhaseExpanded = expandedPhase === phaseKey;

                          return (
                            <Fragment key={phaseKey}>
                              <tr
                                onClick={() => setExpandedPhase(isPhaseExpanded ? null : phaseKey)}
                                className="border-b border-border/30 hover:bg-card/20 cursor-pointer transition-colors"
                              >
                                <td className="px-3 py-1.5 pl-8 font-mono text-[10px] text-foreground/70">
                                  <div className="flex items-center gap-1.5">
                                    <svg
                                      className={`w-2 h-2 shrink-0 transition-transform duration-150 text-muted-foreground/50 ${isPhaseExpanded ? 'rotate-90' : ''}`}
                                      fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}
                                    >
                                      <path strokeLinecap="round" strokeLinejoin="round" d="M9 5l7 7-7 7" />
                                    </svg>
                                    {entry.snapshot_name || '-'}
                                  </div>
                                </td>
                                <td className="px-3 py-1.5 font-mono text-[10px] text-muted-foreground/60">
                                  {entry.aod_run_id || '-'}
                                </td>
                                <td className="px-3 py-1.5">
                                  <span className={`inline-flex items-center gap-1 font-semibold ${cfg.color}`}>
                                    <span className="w-4 h-4 rounded-full bg-current/10 border border-current/30 flex items-center justify-center text-[9px]">
                                      {cfg.icon}
                                    </span>
                                    {cfg.label}
                                  </span>
                                </td>
                                <td className={`px-3 py-1.5 font-semibold ${srcColor}`}>
                                  {entry.source}
                                </td>
                                <td className="px-3 py-1.5 text-muted-foreground">
                                  {fmtDate(entry.timestamp)}
                                </td>
                                <td className="px-3 py-1.5 text-right font-mono text-foreground">
                                  {entry.pipes > 0 ? entry.pipes : (
                                    <span className="text-muted-foreground/40">-</span>
                                  )}
                                </td>
                                <td className="px-3 py-1.5 text-right font-mono text-foreground">
                                  {entry.sors > 0 ? entry.sors : (
                                    <span className="text-muted-foreground/40">-</span>
                                  )}
                                </td>
                                <td className="px-3 py-1.5 text-right font-mono">
                                  {entry.tooling_pipes > 0 ? (
                                    <span className="text-amber-400" title="Tooling pipes (non-SOR)">
                                      {entry.tooling_pipes}
                                    </span>
                                  ) : (
                                    <span className="text-muted-foreground/40">-</span>
                                  )}
                                </td>
                                <td className="px-3 py-1.5 text-right font-mono text-foreground">
                                  {entry.fabrics > 0 ? entry.fabrics : (
                                    <span className="text-muted-foreground/40">-</span>
                                  )}
                                </td>
                              </tr>

                              {/* Drill-down table for mapped/unmapped/tooling */}
                              {entry.phase === 'content' && drillDispatchId === entry.dispatch_id && drillData && (
                                <tr className="border-b border-border/20 bg-card/5">
                                  <td colSpan={9} className="px-3 py-3 pl-12">
                                    <div className="flex items-center justify-between mb-2">
                                      <span className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wider">
                                        Pipe Breakdown
                                        {drillFilter && (
                                          <span className="ml-2 normal-case tracking-normal">
                                            — showing <span className={
                                              drillFilter === 'mapped' ? 'text-emerald-400' :
                                              drillFilter === 'unmapped' ? 'text-red-400' :
                                              'text-amber-400'
                                            }>{drillFilter}</span>
                                          </span>
                                        )}
                                      </span>
                                      <button
                                        onClick={(e) => { e.stopPropagation(); setDrillDispatchId(null); setDrillData(null); setDrillFilter(null); }}
                                        className="text-[10px] text-muted-foreground hover:text-foreground"
                                      >
                                        Collapse
                                      </button>
                                    </div>
                                    {drillLoading ? (
                                      <div className="text-[10px] text-muted-foreground">Loading...</div>
                                    ) : (
                                      <table className="w-full text-[10px]">
                                        <thead>
                                          <tr className="text-muted-foreground/60 uppercase tracking-wider">
                                            <th className="text-left px-2 py-1 font-medium">Pipe ID</th>
                                            <th className="text-left px-2 py-1 font-medium">Source</th>
                                            <th className="text-right px-2 py-1 font-medium">Rows</th>
                                            <th className="text-left px-2 py-1 font-medium">Category</th>
                                            <th className="text-left px-2 py-1 font-medium">Drift</th>
                                          </tr>
                                        </thead>
                                        <tbody>
                                          {(drillData.pipes || [])
                                            .filter((p) => !drillFilter || p.category === drillFilter)
                                            .map((p, i) => (
                                            <tr key={i} className="border-t border-border/10 hover:bg-card/10">
                                              <td className="px-2 py-1 font-mono text-foreground/80">{p.pipe_id}</td>
                                              <td className="px-2 py-1 text-muted-foreground">{p.source_system}</td>
                                              <td className="px-2 py-1 text-right font-mono text-foreground/80">{fmtRows(p.row_count)}</td>
                                              <td className="px-2 py-1">
                                                <span className={`inline-block px-1.5 py-0.5 rounded text-[9px] font-semibold border ${
                                                  p.category === 'mapped' ? 'bg-emerald-500/20 text-emerald-400 border-emerald-500/30' :
                                                  p.category === 'unmapped' ? 'bg-red-500/20 text-red-400 border-red-500/30' :
                                                  p.category === 'tooling' ? 'bg-amber-500/20 text-amber-400 border-amber-500/30' :
                                                  'bg-muted/20 text-muted-foreground border-muted/30'
                                                }`}>
                                                  {p.category}
                                                </span>
                                              </td>
                                              <td className="px-2 py-1 text-muted-foreground">
                                                {p.schema_drift ? <span className="text-amber-400">yes</span> : '-'}
                                              </td>
                                            </tr>
                                          ))}
                                        </tbody>
                                      </table>
                                    )}
                                  </td>
                                </tr>
                              )}

                              {/* Phase detail sub-row */}
                              {isPhaseExpanded && (
                                <tr className="border-b border-border/20 bg-card/5">
                                  <td colSpan={9} className="px-3 py-2 pl-12">
                                    <div className="flex flex-wrap gap-x-6 gap-y-1 text-[10px] font-mono">
                                      <span>
                                        <span className="text-muted-foreground/60">dispatch_id </span>
                                        <span className="text-foreground/80">{entry.dispatch_id || '-'}</span>
                                      </span>
                                      <span>
                                        <span className="text-muted-foreground/60">records </span>
                                        <span className="text-foreground/80">{entry.records > 0 ? fmtRows(entry.records) : '-'}</span>
                                      </span>
                                      {entry.phase === 'content' && (
                                        <>
                                          <span>
                                            <span className="text-muted-foreground/60">mapped </span>
                                            <span
                                              className="text-emerald-400 cursor-pointer underline decoration-dotted hover:decoration-solid"
                                              onClick={(e) => { e.stopPropagation(); fetchDrillDown(entry.dispatch_id, 'mapped'); }}
                                            >{entry.mapped_pipes}</span>
                                            <span className="text-muted-foreground/40"> / </span>
                                            <span className="text-muted-foreground/60">unmapped </span>
                                            <span
                                              className="text-red-400 cursor-pointer underline decoration-dotted hover:decoration-solid"
                                              onClick={(e) => { e.stopPropagation(); fetchDrillDown(entry.dispatch_id, 'unmapped'); }}
                                            >{entry.unmapped_pipes}</span>
                                          </span>
                                          <span>
                                            <span className="text-muted-foreground/60">SOR systems </span>
                                            <span className="text-blue-400">{entry.sors}</span>
                                            <span className="text-muted-foreground/40"> / </span>
                                            <span className="text-muted-foreground/60">tooling </span>
                                            <span
                                              className="text-amber-400 cursor-pointer underline decoration-dotted hover:decoration-solid"
                                              onClick={(e) => { e.stopPropagation(); fetchDrillDown(entry.dispatch_id, 'tooling'); }}
                                            >{entry.tooling_pipes}</span>
                                          </span>
                                        </>
                                      )}
                                    </div>
                                  </td>
                                </tr>
                              )}
                            </Fragment>
                          );
                        })}
                      </Fragment>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

// Phase dot colors for the snapshot header summary
function phaseDotColor(phase: string): string {
  switch (phase) {
    case 'structure': return 'bg-blue-400';
    case 'dispatch':  return 'bg-amber-400';
    case 'content':   return 'bg-emerald-400';
    default:          return 'bg-muted-foreground/40';
  }
}
