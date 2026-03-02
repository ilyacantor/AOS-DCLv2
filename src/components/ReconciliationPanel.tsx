import { useState, useEffect, useMemo } from 'react';
import { ChevronDown, ChevronRight } from 'lucide-react';

interface CrossSystemData {
  snapshot_name: string;
  aod_run_id: string;
  dispatch_id: string;
  recon_at: string;
  systems: {
    aam: {
      total_pipes: number;
      dispatched: number;
      failed_pre_dispatch: number;
      sors: number;
      fabrics: number;
      fabric_names: string[];
      vendor_names: string[];
    };
    farm: {
      total_received: number;
      pushed_to_dcl: number;
      failed_execution: number;
    };
    dcl: {
      total_definitions: number;
      ingested: number;
      sors_category: number;
      sors_governed: number;
      tooling_pipes: number;
      fabrics_active: number;
      fabrics_defined: number;
      mapped_pipes: number;
      unmapped_pipes: number;
      other_pipes: number;
      rows: number;
      drops_total: number;
      drops_unique_pipes: number;
      drop_pipe_ids: string[];
    };
  };
  category_breakdown: Record<string, number>;
  governance: { governed: number; ungoverned: number };
  drops_by_error: Record<string, number>;
  deltas: Array<{
    label: string;
    left: string;
    right: string;
    delta: number;
    explanation: string;
    severity: string;
  }>;
  failed_pipes: Array<{
    pipe_id: string;
    vendor: string;
    category: string;
    fabric_plane: string;
    reason?: string;
  }>;
  activity: {
    structure: Record<string, unknown> | null;
    dispatch: Record<string, unknown> | null;
    content: Record<string, unknown> | null;
  };
}

function downloadJson(data: unknown, filename: string) {
  const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

function getDateString(): string {
  const now = new Date();
  const y = now.getFullYear();
  const m = String(now.getMonth() + 1).padStart(2, '0');
  const d = String(now.getDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
}

/** Compact stat row used inside system columns */
function StatRow({ label, value, color, title }: {
  label: React.ReactNode;
  value: React.ReactNode;
  color?: string;
  title?: string;
}) {
  return (
    <div className="flex justify-between items-center" title={title}>
      <span className="text-muted-foreground">{label}</span>
      <span className={`font-mono font-semibold ${color ?? 'text-foreground'}`}>{value}</span>
    </div>
  );
}

interface ReconciliationPanelProps {
  runId?: string;
}

export function ReconciliationPanel({ runId }: ReconciliationPanelProps) {
  const [xsysData, setXsysData] = useState<CrossSystemData | null>(null);
  const [xsysLoading, setXsysLoading] = useState(true);
  const [xsysError, setXsysError] = useState<string | null>(null);

  // Failed pipes state
  const [failedOpen, setFailedOpen] = useState(false);
  const [failedFilter, setFailedFilter] = useState('');

  const fetchXsysData = async () => {
    setXsysLoading(true);
    setXsysError(null);
    try {
      const res = await fetch('/api/dcl/reconciliation/cross-system');
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = await res.json();
      setXsysData(json);
    } catch (e: any) {
      setXsysError(e.message || 'Failed to fetch cross-system reconciliation data');
    } finally {
      setXsysLoading(false);
    }
  };

  useEffect(() => {
    fetchXsysData();
  }, [runId]);

  const filteredFailedPipes = useMemo(() => {
    const pipes = xsysData?.failed_pipes ?? [];
    if (!failedFilter) return pipes;
    const q = failedFilter.toLowerCase();
    return pipes.filter(fp =>
      fp.pipe_id.toLowerCase().includes(q) ||
      fp.vendor.toLowerCase().includes(q) ||
      fp.category.toLowerCase().includes(q) ||
      fp.fabric_plane.toLowerCase().includes(q) ||
      (fp.reason ?? '').toLowerCase().includes(q)
    );
  }, [xsysData?.failed_pipes, failedFilter]);

  const formatTimestamp = (ts: string) => {
    try {
      return new Date(ts).toLocaleString('en-US', {
        timeZone: 'America/Los_Angeles',
        month: 'short',
        day: 'numeric',
        hour: 'numeric',
        minute: '2-digit',
        hour12: true,
      });
    } catch {
      return ts;
    }
  };

  if (xsysLoading) {
    return (
      <div className="h-full flex flex-col min-h-0">
        <div className="shrink-0 flex items-center justify-between px-6 py-2 border-b border-border bg-card/50">
          <h2 className="text-sm font-semibold">Reconciliation</h2>
        </div>
        <div className="flex-1 flex items-center justify-center">
          <div className="text-sm text-muted-foreground">Loading reconciliation data...</div>
        </div>
      </div>
    );
  }

  if (xsysError) {
    return (
      <div className="h-full flex flex-col min-h-0">
        <div className="shrink-0 flex items-center justify-between px-6 py-2 border-b border-border bg-card/50">
          <h2 className="text-sm font-semibold">Reconciliation</h2>
        </div>
        <div className="flex-1 flex items-center justify-center">
          <div className="text-center space-y-2">
            <div className="text-sm text-red-400">{xsysError}</div>
            <button
              onClick={fetchXsysData}
              className="px-3 py-1 text-xs rounded bg-primary text-primary-foreground hover:bg-primary/90"
            >
              Retry
            </button>
          </div>
        </div>
      </div>
    );
  }

  if (!xsysData) return null;

  const { systems, deltas, category_breakdown, governance, drops_by_error } = xsysData;
  const { aam, farm, dcl } = systems;
  const failedPipeCount = (xsysData.failed_pipes ?? []).length;

  const handleDownload = () => {
    downloadJson(xsysData, `dcl-xsys-recon-${getDateString()}.json`);
  };

  return (
    <div className="h-full flex flex-col min-h-0">
      {/* Header */}
      <div className="shrink-0 flex items-center justify-between px-6 py-2 border-b border-border bg-card/50">
        <h2 className="text-sm font-semibold">Reconciliation</h2>
        <div className="flex items-center gap-2">
          <button
            onClick={handleDownload}
            className="px-3 py-1 text-xs rounded bg-secondary text-secondary-foreground hover:bg-secondary/80"
          >
            Download JSON
          </button>
          <button
            onClick={fetchXsysData}
            className="px-3 py-1 text-xs rounded bg-primary text-primary-foreground hover:bg-primary/90"
          >
            Refresh
          </button>
        </div>
      </div>

      {/* Scrollable content */}
      <div className="flex-1 overflow-y-auto px-4 py-3 space-y-3 min-h-0">
        {/* Snapshot identity bar */}
        {xsysData.snapshot_name && (
          <div className="rounded border border-border bg-card/30 px-3 py-1.5">
            <div className="flex items-center gap-4 text-[11px] font-mono">
              <span>
                <span className="text-muted-foreground">Snapshot</span>{' '}
                <span className="text-foreground font-semibold">{xsysData.snapshot_name}</span>
              </span>
              {xsysData.aod_run_id && (
                <span>
                  <span className="text-muted-foreground">AOD</span>{' '}
                  <span className="text-foreground/70">{xsysData.aod_run_id}</span>
                </span>
              )}
              <span className="text-muted-foreground/50 ml-auto">{formatTimestamp(xsysData.recon_at)}</span>
            </div>
          </div>
        )}

        {/* Pipeline flow — 3-column system comparison (compact) */}
        <div className="grid grid-cols-3 gap-2">
          {/* AAM */}
          <div className="rounded border border-blue-500/30 bg-blue-500/5 px-2.5 py-2 space-y-1 text-[11px]">
            <div className="text-[10px] font-semibold text-blue-400 uppercase tracking-wider">AAM</div>
            <StatRow label="Total pipes" value={aam.total_pipes} />
            <StatRow label="Dispatched" value={aam.dispatched} color="text-emerald-400" />
            {aam.failed_pre_dispatch > 0 && (
              <StatRow label="Failed (pre)" value={aam.failed_pre_dispatch} color="text-red-400" />
            )}
            <div className="border-t border-blue-500/20 pt-1 mt-1" />
            <StatRow label="SORs" value={aam.sors} title="Unique vendor names from AAM pipe definitions" />
            <StatRow label="Fabrics" value={aam.fabrics} />
            {aam.fabric_names.length > 0 && (
              <div className="text-[9px] text-muted-foreground/50 font-mono truncate">
                {aam.fabric_names.join(', ')}
              </div>
            )}
          </div>

          {/* Farm */}
          <div className="rounded border border-emerald-500/30 bg-emerald-500/5 px-2.5 py-2 space-y-1 text-[11px]">
            <div className="text-[10px] font-semibold text-emerald-400 uppercase tracking-wider">Farm</div>
            <StatRow label="Received" value={farm.total_received} />
            <StatRow label="Pushed to DCL" value={farm.pushed_to_dcl} color="text-emerald-400" />
            {farm.failed_execution > 0 && (
              <StatRow label="Failed" value={farm.failed_execution} color="text-red-400" />
            )}
          </div>

          {/* DCL */}
          <div className="rounded border border-amber-500/30 bg-amber-500/5 px-2.5 py-2 space-y-1 text-[11px]">
            <div className="text-[10px] font-semibold text-amber-400 uppercase tracking-wider">DCL</div>
            <StatRow label="Definitions" value={dcl.total_definitions} />
            <StatRow label="Ingested" value={dcl.ingested} color="text-emerald-400" />
            <div className="border-t border-amber-500/20 pt-1 mt-1" />
            <StatRow label="SORs (cat)" value={dcl.sors_category} title="Category-based SOR count" />
            <StatRow label="SORs (gov)" value={dcl.sors_governed} title="Governance-based SOR count" />
            <StatRow label="Tooling" value={dcl.tooling_pipes} color="text-amber-400" />
            <StatRow
              label={
                <>
                  Fabrics
                  {dcl.fabrics_active < dcl.fabrics_defined && (
                    <span className="text-[9px] text-muted-foreground/40 ml-0.5">/{dcl.fabrics_defined}</span>
                  )}
                </>
              }
              value={dcl.fabrics_active}
              title={`${dcl.fabrics_active} active of ${dcl.fabrics_defined} defined`}
            />
            <StatRow
              label="Mapped / Unmapped"
              value={
                <>
                  <span className="text-emerald-400">{dcl.mapped_pipes}</span>
                  <span className="text-muted-foreground/40"> / </span>
                  <span className="text-red-400">{dcl.unmapped_pipes}</span>
                </>
              }
            />
            <StatRow label="Rows" value={dcl.rows.toLocaleString()} />
            {dcl.drops_unique_pipes > 0 && (
              <>
                <div className="border-t border-red-500/20 pt-1 mt-1" />
                <StatRow label="Drops (pipes)" value={dcl.drops_unique_pipes} color="text-red-400" />
                <StatRow label="Drop events" value={dcl.drops_total} color="text-muted-foreground" />
              </>
            )}
          </div>
        </div>

        {/* Deltas — gap explanations (compact) */}
        {deltas.length > 0 && (
          <div className="space-y-1.5">
            <h3 className="text-[11px] font-semibold uppercase text-muted-foreground tracking-wide">
              Gap Analysis ({deltas.length})
            </h3>
            {deltas.map((d, idx) => {
              const sev = d.severity === 'error'
                ? 'border-red-500/30 bg-red-500/5'
                : d.severity === 'warning'
                  ? 'border-amber-500/30 bg-amber-500/5'
                  : 'border-blue-500/30 bg-blue-500/5';
              const sevColor = d.severity === 'error'
                ? 'text-red-400'
                : d.severity === 'warning'
                  ? 'text-amber-400'
                  : 'text-blue-400';
              return (
                <div key={idx} className={`rounded border ${sev} px-2.5 py-2`}>
                  <div className="flex items-center gap-2 text-[11px]">
                    <span className="font-semibold text-foreground">{d.label}</span>
                    <span className={`font-mono font-bold ${sevColor}`}>
                      {d.delta > 0 ? `+${d.delta}` : d.delta}
                    </span>
                    <span className="flex items-center gap-1 ml-auto text-[10px] font-mono text-muted-foreground">
                      <span>{d.left}</span>
                      <span className="text-muted-foreground/40">&rarr;</span>
                      <span>{d.right}</span>
                    </span>
                  </div>
                  <p className="text-[10px] text-muted-foreground/70 mt-1 leading-relaxed">
                    {d.explanation}
                  </p>
                </div>
              );
            })}
          </div>
        )}

        {/* Failed pipes — collapsible, filterable */}
        {failedPipeCount > 0 && (
          <div className="space-y-1.5">
            <button
              onClick={() => setFailedOpen(prev => !prev)}
              className="flex items-center gap-1.5 text-[11px] font-semibold uppercase text-red-400/80 tracking-wide hover:text-red-400 transition-colors w-full text-left"
            >
              {failedOpen
                ? <ChevronDown className="w-3.5 h-3.5 shrink-0" />
                : <ChevronRight className="w-3.5 h-3.5 shrink-0" />}
              Failed Pipes ({failedPipeCount})
            </button>
            {failedOpen && (
              <>
                <input
                  type="text"
                  value={failedFilter}
                  onChange={(e) => setFailedFilter(e.target.value)}
                  placeholder="Filter by pipe ID, vendor, category, fabric, or reason..."
                  className="w-full px-2 py-1 text-[11px] rounded border border-border bg-background text-foreground placeholder:text-muted-foreground/50"
                />
                <div className="rounded border border-red-500/20 bg-red-500/5 overflow-hidden">
                  <table className="w-full text-[11px]">
                    <thead>
                      <tr className="border-b border-red-500/10 text-muted-foreground">
                        <th className="text-left px-2 py-1.5 font-medium">Pipe ID</th>
                        <th className="text-left px-2 py-1.5 font-medium">Vendor</th>
                        <th className="text-left px-2 py-1.5 font-medium">Category</th>
                        <th className="text-left px-2 py-1.5 font-medium">Fabric</th>
                        <th className="text-left px-2 py-1.5 font-medium">Reason</th>
                      </tr>
                    </thead>
                    <tbody>
                      {filteredFailedPipes.length === 0 ? (
                        <tr>
                          <td colSpan={5} className="px-2 py-2 text-center text-muted-foreground/50">
                            No pipes match filter
                          </td>
                        </tr>
                      ) : (
                        filteredFailedPipes.map((fp) => (
                          <tr key={fp.pipe_id} className="border-b border-red-500/5 last:border-0">
                            <td className="px-2 py-1.5 font-mono text-red-400">{fp.pipe_id}</td>
                            <td className="px-2 py-1.5 text-foreground">{fp.vendor}</td>
                            <td className="px-2 py-1.5 text-muted-foreground">{fp.category}</td>
                            <td className="px-2 py-1.5 text-muted-foreground">{fp.fabric_plane}</td>
                            <td className="px-2 py-1.5 text-muted-foreground/70 max-w-xs">
                              {fp.reason ?? 'Unknown'}
                            </td>
                          </tr>
                        ))
                      )}
                    </tbody>
                  </table>
                </div>
              </>
            )}
          </div>
        )}

        {/* Category breakdown + governance (compact inline) */}
        {Object.keys(category_breakdown).length > 0 && (
          <div className="rounded border border-border bg-card/30 px-2.5 py-2">
            <div className="flex items-center justify-between mb-1.5">
              <h3 className="text-[10px] font-semibold uppercase text-muted-foreground tracking-wide">
                Pipe Categories
              </h3>
              <span className="text-[10px] text-muted-foreground">
                Gov <span className="text-emerald-400 font-semibold">{governance.governed}</span>
                {' / '}
                Ungov <span className="text-muted-foreground/50">{governance.ungoverned}</span>
              </span>
            </div>
            <div className="grid grid-cols-4 gap-1">
              {Object.entries(category_breakdown).sort((a, b) => b[1] - a[1]).map(([cat, count]) => (
                <div key={cat} className="flex justify-between text-[11px] px-1.5 py-0.5 rounded bg-card/50">
                  <span className="text-muted-foreground font-mono">{cat}</span>
                  <span className="font-mono font-semibold text-foreground">{count}</span>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Drop breakdown by error code (compact) */}
        {Object.keys(drops_by_error).length > 0 && (
          <div className="rounded border border-red-500/20 bg-red-500/5 px-2.5 py-2">
            <h3 className="text-[10px] font-semibold uppercase text-red-400/80 tracking-wide mb-1">
              Drops by Error Code
            </h3>
            <div className="space-y-0.5">
              {Object.entries(drops_by_error).sort((a, b) => b[1] - a[1]).map(([code, count]) => (
                <div key={code} className="flex justify-between text-[11px] px-1.5 py-0.5 rounded bg-red-500/5">
                  <span className="font-mono text-red-400/80">{code}</span>
                  <span className="font-mono font-semibold text-red-400">{count}</span>
                </div>
              ))}
            </div>
            {dcl.drop_pipe_ids.length > 0 && dcl.drop_pipe_ids.length <= 20 && (
              <div className="mt-1 text-[9px] text-muted-foreground/50 font-mono">
                {dcl.drop_pipe_ids.join(', ')}
              </div>
            )}
          </div>
        )}

        {/* 3-phase activity (compact) */}
        <div className="rounded border border-border bg-card/30 px-2.5 py-2">
          <h3 className="text-[10px] font-semibold uppercase text-muted-foreground tracking-wide mb-1.5">
            3-Phase Activity
          </h3>
          <div className="grid grid-cols-3 gap-2 text-[11px]">
            {(['structure', 'dispatch', 'content'] as const).map((phase) => {
              const entry = xsysData.activity[phase];
              const colors = {
                structure: { border: 'border-blue-500/30', text: 'text-blue-400', bg: 'bg-blue-500/10' },
                dispatch: { border: 'border-amber-500/30', text: 'text-amber-400', bg: 'bg-amber-500/10' },
                content: { border: 'border-emerald-500/30', text: 'text-emerald-400', bg: 'bg-emerald-500/10' },
              };
              const c = colors[phase];
              return (
                <div key={phase} className={`rounded border ${c.border} ${c.bg} px-2 py-1.5`}>
                  <div className={`font-semibold ${c.text} uppercase text-[9px] tracking-wider mb-0.5`}>
                    {phase}
                  </div>
                  {entry ? (
                    <div className="space-y-0 font-mono text-[10px]">
                      <div className="text-muted-foreground/60 truncate">
                        {(entry as any).source} &middot; {formatTimestamp((entry as any).timestamp)}
                      </div>
                      <div>Pipes: <span className="text-foreground font-semibold">{(entry as any).pipes}</span></div>
                      {(entry as any).sors > 0 && <div>SORs: {(entry as any).sors}</div>}
                      {(entry as any).fabrics > 0 && <div>Fabrics: {(entry as any).fabrics}</div>}
                      {(entry as any).tooling_pipes > 0 && <div>Tooling: {(entry as any).tooling_pipes}</div>}
                      {(entry as any).rows > 0 && <div>Rows: {((entry as any).rows as number).toLocaleString()}</div>}
                    </div>
                  ) : (
                    <div className="text-muted-foreground/40 text-[10px]">Not received</div>
                  )}
                </div>
              );
            })}
          </div>
        </div>
      </div>
    </div>
  );
}
