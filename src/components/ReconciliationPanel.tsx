import { useState, useEffect } from 'react';

interface ReconciliationData {
  status: string;
  summary: {
    aamConnections: number;
    dclLoadedSources: number;
    matched: number;
    inAamNotDcl: number;
    inDclNotAam: number;
    unmappedCount: number;
  };
  diffCauses: Array<{
    cause: string;
    description: string;
    severity: string;
    count: number;
  }>;
  fabricBreakdown: Array<{
    planeType: string;
    vendor: string;
    aamConnections: number;
    dclLoaded: number;
    delta: number;
    missingFromDcl: string[];
  }>;
  inAamNotDcl: Array<{
    sourceName: string;
    vendor: string;
    fabricPlane: string;
    pipeId: string;
    fieldCount: number;
    cause: string;
  }>;
  inDclNotAam: Array<{
    sourceName: string;
    cause: string;
  }>;
  pushMeta: {
    pushId: string;
    pushedAt: string;
    pipeCount: number;
    payloadHash: string;
    aodRunId: string | null;
  } | null;
  reconMeta?: {
    dclRunId: string | null;
    dclRunAt: string | null;
    reconAt: string;
    aodRunId: string | null;
    dataMode: string | null;
    dclSourceCount: number;
    aamConnectionCount: number;
    snapshotName?: string;
  };
  trace?: {
    aamPipeNames: string[];
    dclLoadedSourceNames: string[];
    pushPipeCount: number;
    exportPipeCount: number;
    unmappedCount: number;
  };
}

interface SorCoverageSource {
  source: string;
  event: string;
  qualityScore: number;
  loaded: boolean;
}

interface SorCoverageRow {
  entity: string;
  entityName: string;
  sources: SorCoverageSource[];
  isCovered: boolean;
  conflictCount: number;
  resolutionStatus?: 'single_source' | 'resolved' | 'needs_resolution';
}

interface SorResolvedEntity {
  entity: string;
  entityName: string;
  winner: string;
  sourceCount: number;
}

interface SorConflict {
  entity: string;
  entityName: string;
  claimants: Array<{ source: string; event: string; qualityScore: number }>;
  recommendation: string;
}

interface SorEntityGap {
  entity: string;
  entityName: string;
  referencedBy: string[];
  pack: string;
}

interface SorReconciliationData {
  status: string;
  summary: {
    totalBindings: number;
    totalEntities: number;
    totalMetrics: number;
    loadedSources: number;
    bindingSources: number;
    orphanSources: number;
    missingSources: number;
    entityCoverageGaps: number;
    sorConflicts: number;
    resolvedEntities?: number;
  };
  coverageMatrix: SorCoverageRow[];
  sorConflicts: SorConflict[];
  resolvedEntities?: SorResolvedEntity[];
  orphanSources: string[];
  missingSources: string[];
  entityGaps: SorEntityGap[];
  reconMeta?: {
    dclRunId: string | null;
    dclRunAt: string | null;
    reconAt: string;
    dataMode: string | null;
    loadedSourceCount: number;
    snapshotName?: string;
  };
}

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
  activity: {
    structure: Record<string, unknown> | null;
    dispatch: Record<string, unknown> | null;
    content: Record<string, unknown> | null;
  };
}

const statusColors: Record<string, string> = {
  synced: 'bg-emerald-500/20 text-emerald-400 border-emerald-500/30',
  drifted: 'bg-amber-500/20 text-amber-400 border-amber-500/30',
  critical: 'bg-red-500/20 text-red-400 border-red-500/30',
  empty: 'bg-gray-500/20 text-gray-400 border-gray-500/30',
  no_pushes: 'bg-gray-500/20 text-gray-400 border-gray-500/30',
  gaps: 'bg-amber-500/20 text-amber-400 border-amber-500/30',
  conflicts: 'bg-red-500/20 text-red-400 border-red-500/30',
  no_data: 'bg-gray-500/20 text-gray-400 border-gray-500/30',
};

const severityStyles: Record<string, { bg: string; text: string; icon: string }> = {
  warning: { bg: 'bg-amber-500/10 border-amber-500/20', text: 'text-amber-400', icon: '⚠' },
  error: { bg: 'bg-red-500/10 border-red-500/20', text: 'text-red-400', icon: '✕' },
  info: { bg: 'bg-blue-500/10 border-blue-500/20', text: 'text-blue-400', icon: 'ℹ' },
};

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

interface ReconciliationPanelProps {
  runId?: string;
  dataMode?: 'Demo' | 'Farm' | 'AAM';
}

export function ReconciliationPanel({ runId, dataMode }: ReconciliationPanelProps) {
  const isFarm = dataMode === 'Farm';
  const [activeTab, setActiveTab] = useState<'aam' | 'sor' | 'xsys'>('xsys');
  const [data, setData] = useState<ReconciliationData | null>(null);
  const [sorData, setSorData] = useState<SorReconciliationData | null>(null);
  const [xsysData, setXsysData] = useState<CrossSystemData | null>(null);
  const [loading, setLoading] = useState(true);
  const [sorLoading, setSorLoading] = useState(true);
  const [xsysLoading, setXsysLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [sorError, setSorError] = useState<string | null>(null);
  const [xsysError, setXsysError] = useState<string | null>(null);
  const [unmappedExpanded, setUnmappedExpanded] = useState(false);
  const [unmappedLimit, setUnmappedLimit] = useState(50);
  const [traceExpanded, setTraceExpanded] = useState(false);

  const fetchData = async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch('/api/dcl/reconciliation');
      if (res.status === 409) {
        const body = await res.json();
        setError(body?.detail?.message || 'No AAM run found. Run the pipeline in AAM mode first.');
        return;
      }
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = await res.json();
      setData(json);
    } catch (e: any) {
      setError(e.message || 'Failed to fetch reconciliation data');
    } finally {
      setLoading(false);
    }
  };

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

  const fetchSorData = async () => {
    setSorLoading(true);
    setSorError(null);
    try {
      const res = await fetch('/api/dcl/reconciliation/sor');
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = await res.json();
      setSorData(json);
    } catch (e: any) {
      setSorError(e.message || 'Failed to fetch SOR reconciliation data');
    } finally {
      setSorLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
    fetchSorData();
    fetchXsysData();
  }, [runId]);

  const formatTimestamp = (ts: string) => {
    try {
      return new Date(ts).toLocaleString('en-US', {
        timeZone: 'America/Los_Angeles',
        year: 'numeric',
        month: 'short',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: true,
      }) + ' PST';
    } catch {
      return ts;
    }
  };

  const renderLoading = () => (
    <div className="h-full flex items-center justify-center text-muted-foreground">
      <div className="flex flex-col items-center gap-3">
        <div className="w-8 h-8 border-2 border-primary border-t-transparent rounded-full animate-spin" />
        <span className="text-sm">Loading reconciliation data...</span>
      </div>
    </div>
  );

  const renderError = (errorMsg: string, retry: () => void) => (
    <div className="h-full flex items-center justify-center">
      <div className="flex flex-col items-center gap-3 text-center p-8">
        <span className="text-red-400 text-lg">✕</span>
        <span className="text-sm text-muted-foreground">{errorMsg}</span>
        <button
          onClick={retry}
          className="px-3 py-1.5 text-xs rounded bg-primary text-primary-foreground hover:bg-primary/90"
        >
          Retry
        </button>
      </div>
    </div>
  );

  const renderAamTab = () => {
    if (loading) return renderLoading();
    if (error) return renderError(error, fetchData);
    if (!data) return null;

    const sourceLabel = isFarm ? 'Farm Sources' : 'AAM Connections';
    const notDclLabel = isFarm ? 'In Farm Not DCL' : 'In AAM Not DCL';
    const notSourceLabel = isFarm ? 'In DCL Not Farm' : 'In DCL Not AAM';
    const summaryCards = [
      { label: sourceLabel, value: data.summary.aamConnections ?? 0 },
      { label: 'DCL Loaded', value: data.summary.dclLoadedSources ?? 0 },
      { label: 'Matched', value: data.summary.matched ?? 0 },
      { label: notDclLabel, value: data.summary.inAamNotDcl ?? 0 },
      { label: notSourceLabel, value: data.summary.inDclNotAam ?? 0 },
      { label: 'Unmapped', value: data.summary.unmappedCount ?? 0 },
    ];

    return (
      <div className="flex-1 overflow-y-auto p-6 space-y-6 min-h-0">
        <div className="rounded-lg border border-border bg-card/30 p-4 space-y-2">
          <div className="flex items-center gap-3 flex-wrap">
            <span className={`px-2.5 py-1 text-xs font-medium rounded border ${statusColors[data.status] || statusColors.empty}`}>
              {data.status.toUpperCase()}
            </span>
            {data.reconMeta?.dclRunId && (
              <span className="text-xs font-mono text-muted-foreground bg-secondary/30 px-2 py-0.5 rounded">
                Run {data.reconMeta.dclRunId.slice(0, 8)}
              </span>
            )}
            {data.reconMeta?.dataMode && (
              <span className="text-xs font-medium text-muted-foreground bg-secondary/30 px-2 py-0.5 rounded">
                {data.reconMeta.dataMode}
              </span>
            )}
            {data.reconMeta?.snapshotName && (
              <span className="text-xs font-medium text-blue-400 bg-blue-500/10 px-2 py-0.5 rounded border border-blue-500/20">
                {data.reconMeta.snapshotName}
              </span>
            )}
            <span className="text-xs text-muted-foreground">
              {data.reconMeta?.aamConnectionCount ?? data.pushMeta?.pipeCount ?? 0} pipes
            </span>
          </div>
          {data.reconMeta && (
            <div className="flex items-center gap-3 flex-wrap text-[10px] text-muted-foreground border-t border-border pt-2">
              <span>Reconciled: <span className="text-foreground font-mono">{formatTimestamp(data.reconMeta.reconAt)}</span></span>
              {data.reconMeta.dclRunAt && (
                <span>Pipeline ran: <span className="text-foreground font-mono">{formatTimestamp(data.reconMeta.dclRunAt)}</span></span>
              )}
              <span>{isFarm ? 'Farm' : 'AAM'}: <span className="text-foreground font-mono">{data.reconMeta.aamConnectionCount}</span></span>
              <span>DCL: <span className="text-foreground font-mono">{data.reconMeta.dclSourceCount}</span></span>
            </div>
          )}
        </div>

        <div className="grid grid-cols-2 gap-3">
          {summaryCards.map((card) => (
            <div key={card.label} className="rounded-lg border border-border bg-card/30 p-3">
              <div className="text-[10px] uppercase text-muted-foreground tracking-wide">{card.label}</div>
              <div className="text-xl font-mono font-semibold mt-1">{card.value.toLocaleString()}</div>
            </div>
          ))}
        </div>

        {(data.diffCauses?.length ?? 0) > 0 && (
          <div>
            <h3 className="text-xs font-semibold uppercase text-muted-foreground tracking-wide mb-3">Diff Causes</h3>
            <div className="space-y-2">
              {data.diffCauses.map((cause, i) => {
                const style = severityStyles[cause.severity] || severityStyles.info;
                return (
                  <div key={i} className={`rounded-lg border p-3 ${style.bg}`}>
                    <div className="flex items-center gap-2">
                      <span className={`text-sm ${style.text}`}>{style.icon}</span>
                      <span className="text-sm font-medium">{cause.cause}</span>
                      <span className={`ml-auto text-xs font-mono ${style.text}`}>{cause.count}</span>
                    </div>
                    <p className="text-xs text-muted-foreground mt-1">{cause.description}</p>
                  </div>
                );
              })}
            </div>
          </div>
        )}

        {(data.fabricBreakdown?.length ?? 0) > 0 && (
          <div>
            <h3 className="text-xs font-semibold uppercase text-muted-foreground tracking-wide mb-3">Fabric Breakdown</h3>
            <div className="rounded-lg border border-border overflow-hidden">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border bg-card/50">
                    <th className="text-left px-3 py-2 text-xs font-medium text-muted-foreground">Plane</th>
                    <th className="text-right px-3 py-2 text-xs font-medium text-muted-foreground">{isFarm ? 'Farm' : 'AAM'}</th>
                    <th className="text-right px-3 py-2 text-xs font-medium text-muted-foreground">DCL</th>
                    <th className="text-right px-3 py-2 text-xs font-medium text-muted-foreground">Delta</th>
                  </tr>
                </thead>
                <tbody>
                  {data.fabricBreakdown.map((row, i) => (
                    <tr key={i} className="border-b border-border/50 last:border-0">
                      <td className="px-3 py-2">
                        <span className="font-mono text-xs">{row.planeType}</span>
                        {row.vendor && <span className="text-xs text-muted-foreground ml-1">({row.vendor})</span>}
                      </td>
                      <td className="text-right px-3 py-2 font-mono">{row.aamConnections}</td>
                      <td className="text-right px-3 py-2 font-mono">{row.dclLoaded}</td>
                      <td className={`text-right px-3 py-2 font-mono font-medium ${row.delta === 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                        {row.delta === 0 ? '0' : (row.delta > 0 ? `+${row.delta}` : row.delta)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {(data.inAamNotDcl?.length ?? 0) > 0 && (
          <div>
            <button
              onClick={() => setUnmappedExpanded(!unmappedExpanded)}
              className="flex items-center gap-2 text-xs font-semibold uppercase text-muted-foreground tracking-wide mb-3 hover:text-foreground transition-colors"
            >
              <span>{unmappedExpanded ? '▾' : '▸'}</span>
              <span>{isFarm ? 'In Farm but Not DCL' : 'In AAM but Not DCL'} ({data.inAamNotDcl?.length ?? 0})</span>
            </button>
            {unmappedExpanded && (
              <div className="rounded-lg border border-border overflow-hidden">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-border bg-card/50">
                      <th className="text-left px-3 py-2 text-xs font-medium text-muted-foreground">Source</th>
                      <th className="text-left px-3 py-2 text-xs font-medium text-muted-foreground">Vendor</th>
                      <th className="text-left px-3 py-2 text-xs font-medium text-muted-foreground">Fabric</th>
                      <th className="text-right px-3 py-2 text-xs font-medium text-muted-foreground">Fields</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(data.inAamNotDcl ?? []).slice(0, unmappedLimit).map((item, i) => (
                      <tr key={i} className="border-b border-border/50 last:border-0">
                        <td className="px-3 py-2 font-mono text-xs truncate max-w-[200px]" title={item.sourceName}>{item.sourceName}</td>
                        <td className="px-3 py-2 text-xs text-muted-foreground">{item.vendor}</td>
                        <td className="px-3 py-2 text-xs text-muted-foreground">{item.fabricPlane}</td>
                        <td className="text-right px-3 py-2 font-mono text-xs">{item.fieldCount}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                {unmappedLimit < (data.inAamNotDcl?.length ?? 0) && (
                  <div className="border-t border-border px-3 py-2 text-center">
                    <button
                      onClick={() => setUnmappedLimit(prev => prev + 50)}
                      className="text-xs text-primary hover:text-primary/80"
                    >
                      Show more ({(data.inAamNotDcl?.length ?? 0) - unmappedLimit} remaining)
                    </button>
                  </div>
                )}
              </div>
            )}
          </div>
        )}

        {(data.inDclNotAam?.length ?? 0) > 0 && (
          <div>
            <h3 className="text-xs font-semibold uppercase text-muted-foreground tracking-wide mb-3">
              {isFarm ? 'In DCL but Not Farm' : 'In DCL but Not AAM'} ({data.inDclNotAam?.length ?? 0})
            </h3>
            <div className="rounded-lg border border-border overflow-hidden">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border bg-card/50">
                    <th className="text-left px-3 py-2 text-xs font-medium text-muted-foreground">Source</th>
                    <th className="text-left px-3 py-2 text-xs font-medium text-muted-foreground">Cause</th>
                  </tr>
                </thead>
                <tbody>
                  {(data.inDclNotAam ?? []).map((item, i) => (
                    <tr key={i} className="border-b border-border/50 last:border-0">
                      <td className="px-3 py-2 font-mono text-xs">{item.sourceName}</td>
                      <td className="px-3 py-2 text-xs text-muted-foreground">{item.cause}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {data.trace && (
          <div>
            <button
              onClick={() => setTraceExpanded(!traceExpanded)}
              className="flex items-center gap-2 text-xs font-semibold uppercase text-muted-foreground tracking-wide mb-3 hover:text-foreground transition-colors"
            >
              <span>{traceExpanded ? '▾' : '▸'}</span>
              <span>Trace</span>
            </button>
            {traceExpanded && (
              <div className="rounded-lg border border-border bg-card/30 p-4 space-y-4">
                <div className="rounded-lg border p-3 bg-blue-500/10 border-blue-500/20">
                  <div className="flex items-center gap-2">
                    <span className="text-sm text-blue-400">ℹ</span>
                    <span className="text-xs text-blue-400 font-medium">Data Sources</span>
                  </div>
                  <p className="text-xs text-muted-foreground mt-1">
                    Export-pipes (current): <span className="font-mono text-foreground">{data.trace.exportPipeCount}</span> pipes ({data.trace.unmappedCount} unmapped).
                    {data.trace.pushPipeCount > 0 && data.trace.pushPipeCount !== data.trace.exportPipeCount && (
                      <> Last push history: <span className="font-mono text-amber-400">{data.trace.pushPipeCount}</span> (stale — different from current).</>
                    )}
                  </p>
                </div>
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <h4 className="text-[10px] uppercase text-muted-foreground tracking-wide mb-2">{isFarm ? 'Farm Sources' : 'AAM Pipes'} ({data.trace.aamPipeNames.length})</h4>
                    <div className="space-y-1 max-h-60 overflow-y-auto">
                      {data.trace.aamPipeNames.map((name, i) => (
                        <div key={i} className="text-[11px] font-mono text-foreground/80 truncate" title={name}>{name}</div>
                      ))}
                      {data.trace.aamPipeNames.length === 0 && (
                        <div className="text-[11px] text-muted-foreground italic">None</div>
                      )}
                    </div>
                  </div>
                  <div>
                    <h4 className="text-[10px] uppercase text-muted-foreground tracking-wide mb-2">DCL Loaded Sources ({data.trace.dclLoadedSourceNames.length})</h4>
                    <div className="space-y-1 max-h-60 overflow-y-auto">
                      {data.trace.dclLoadedSourceNames.map((name, i) => (
                        <div key={i} className="text-[11px] font-mono text-foreground/80 truncate" title={name}>{name}</div>
                      ))}
                      {data.trace.dclLoadedSourceNames.length === 0 && (
                        <div className="text-[11px] text-muted-foreground italic">None</div>
                      )}
                    </div>
                  </div>
                </div>
              </div>
            )}
          </div>
        )}
      </div>
    );
  };

  const renderSorTab = () => {
    if (sorLoading) return renderLoading();
    if (sorError) return renderError(sorError, fetchSorData);
    if (!sorData) return null;

    const summaryCards = [
      { label: 'Bindings', value: sorData.summary.totalBindings },
      { label: 'Loaded Sources', value: sorData.summary.loadedSources },
      { label: 'Binding Sources', value: sorData.summary.bindingSources },
      { label: 'Orphan Sources', value: sorData.summary.orphanSources },
      { label: 'Missing Sources', value: sorData.summary.missingSources },
      { label: 'Resolved', value: sorData.summary.resolvedEntities ?? 0 },
      { label: 'SOR Conflicts', value: sorData.summary.sorConflicts },
    ];

    return (
      <div className="flex-1 overflow-y-auto p-6 space-y-6 min-h-0">
        <div className="rounded-lg border border-border bg-card/30 p-4 space-y-2">
          <div className="flex items-center gap-3 flex-wrap">
            <span className={`px-2.5 py-1 text-xs font-medium rounded border ${statusColors[sorData.status] || statusColors.empty}`}>
              {sorData.status.toUpperCase()}
            </span>
            {sorData.reconMeta?.dclRunId && (
              <span className="text-xs font-mono text-muted-foreground bg-secondary/30 px-2 py-0.5 rounded">
                Run {sorData.reconMeta.dclRunId.slice(0, 8)}
              </span>
            )}
            {sorData.reconMeta?.dataMode && (
              <span className="text-xs font-medium text-muted-foreground bg-secondary/30 px-2 py-0.5 rounded">
                {sorData.reconMeta.dataMode}
              </span>
            )}
            {sorData.reconMeta?.snapshotName && (
              <span className="text-xs font-medium text-blue-400 bg-blue-500/10 px-2 py-0.5 rounded border border-blue-500/20">
                {sorData.reconMeta.snapshotName}
              </span>
            )}
            <span className="text-xs text-muted-foreground">
              {sorData.summary.totalEntities} entities, {sorData.summary.totalMetrics} metrics
            </span>
          </div>
          {sorData.reconMeta && (
            <div className="flex items-center gap-3 flex-wrap text-[10px] text-muted-foreground border-t border-border pt-2">
              <span>Reconciled: <span className="text-foreground font-mono">{formatTimestamp(sorData.reconMeta.reconAt)}</span></span>
              {sorData.reconMeta.dclRunAt && (
                <span>Pipeline ran: <span className="text-foreground font-mono">{formatTimestamp(sorData.reconMeta.dclRunAt)}</span></span>
              )}
              <span>Sources: <span className="text-foreground font-mono">{sorData.reconMeta.loadedSourceCount}</span></span>
            </div>
          )}
        </div>

        <div className="grid grid-cols-2 gap-3">
          {summaryCards.map((card) => (
            <div key={card.label} className="rounded-lg border border-border bg-card/30 p-3">
              <div className="text-[10px] uppercase text-muted-foreground tracking-wide">{card.label}</div>
              <div className="text-xl font-mono font-semibold mt-1">{card.value.toLocaleString()}</div>
            </div>
          ))}
        </div>

        {sorData.coverageMatrix.length > 0 && (
          <div>
            <h3 className="text-xs font-semibold uppercase text-muted-foreground tracking-wide mb-3">Coverage Matrix</h3>
            <div className="rounded-lg border border-border overflow-hidden">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border bg-card/50">
                    <th className="text-left px-3 py-2 text-xs font-medium text-muted-foreground">Entity</th>
                    <th className="text-left px-3 py-2 text-xs font-medium text-muted-foreground">Sources</th>
                    <th className="text-center px-3 py-2 text-xs font-medium text-muted-foreground">Covered</th>
                    <th className="text-right px-3 py-2 text-xs font-medium text-muted-foreground">SOR Status</th>
                  </tr>
                </thead>
                <tbody>
                  {sorData.coverageMatrix.map((row) => (
                    <tr key={row.entity} className="border-b border-border/50 last:border-0">
                      <td className="px-3 py-2">
                        <span className="text-xs font-medium">{row.entityName}</span>
                        <span className="text-[10px] text-muted-foreground ml-1">({row.entity})</span>
                      </td>
                      <td className="px-3 py-2">
                        <div className="flex flex-wrap gap-1">
                          {row.sources.map((s, i) => (
                            <span
                              key={i}
                              className={`inline-flex items-center gap-1 px-1.5 py-0.5 text-[10px] rounded border ${
                                s.loaded
                                  ? 'bg-emerald-500/10 border-emerald-500/20 text-emerald-400'
                                  : 'bg-gray-500/10 border-gray-500/20 text-gray-400'
                              }`}
                            >
                              <span className={`inline-block w-1.5 h-1.5 rounded-full ${s.loaded ? 'bg-emerald-400' : 'bg-gray-500'}`} />
                              {s.source}
                            </span>
                          ))}
                        </div>
                      </td>
                      <td className="text-center px-3 py-2">
                        <span className={`inline-block w-2 h-2 rounded-full ${row.isCovered ? 'bg-emerald-400' : 'bg-red-400'}`} />
                      </td>
                      <td className="text-right px-3 py-2 text-xs">
                        {row.resolutionStatus === 'resolved' ? (
                          <span className="text-emerald-400">Resolved</span>
                        ) : row.resolutionStatus === 'needs_resolution' ? (
                          <span className="text-amber-400">Needs SOR</span>
                        ) : (
                          <span className="text-muted-foreground">Single</span>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {(sorData.resolvedEntities ?? []).length > 0 && (
          <div>
            <h3 className="text-xs font-semibold uppercase text-muted-foreground tracking-wide mb-3">
              Resolved Multi-Source ({sorData.resolvedEntities!.length})
            </h3>
            <div className="flex flex-wrap gap-2">
              {sorData.resolvedEntities!.map((r) => (
                <span
                  key={r.entity}
                  className="inline-flex items-center gap-1.5 px-2.5 py-1 text-[11px] rounded-lg border bg-emerald-500/10 border-emerald-500/20 text-emerald-400"
                >
                  {r.entityName}
                  <span className="text-[9px] text-muted-foreground">
                    {r.sourceCount} sources
                  </span>
                  <span className="text-[9px] font-mono">
                    SOR: {r.winner}
                  </span>
                </span>
              ))}
            </div>
          </div>
        )}

        {sorData.sorConflicts.length > 0 && (
          <div>
            <h3 className="text-xs font-semibold uppercase text-muted-foreground tracking-wide mb-3">
              SOR Conflicts ({sorData.sorConflicts.length})
            </h3>
            <div className="space-y-2">
              {sorData.sorConflicts.map((conflict) => (
                <div key={conflict.entity} className="rounded-lg border border-amber-500/20 bg-amber-500/10 p-3">
                  <div className="flex items-center gap-2 mb-2">
                    <span className="text-sm text-amber-400">⚠</span>
                    <span className="text-sm font-medium">{conflict.entityName}</span>
                    <span className="text-[10px] text-muted-foreground">({conflict.entity})</span>
                  </div>
                  <div className="flex flex-wrap gap-1.5 mb-2">
                    {conflict.claimants.map((c, i) => (
                      <span
                        key={i}
                        className={`inline-flex items-center gap-1 px-2 py-0.5 text-[10px] rounded border ${
                          c.source === conflict.recommendation
                            ? 'bg-emerald-500/20 border-emerald-500/30 text-emerald-400'
                            : 'bg-card/50 border-border text-muted-foreground'
                        }`}
                      >
                        {c.source}
                        <span className="font-mono">({(c.qualityScore * 100).toFixed(0)}%)</span>
                        {c.source === conflict.recommendation && (
                          <span className="text-[9px] ml-0.5">★</span>
                        )}
                      </span>
                    ))}
                  </div>
                  <div className="text-[10px] text-muted-foreground">
                    Recommended: <span className="text-emerald-400 font-medium">{conflict.recommendation}</span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        {sorData.entityGaps.length > 0 && (
          <div>
            <h3 className="text-xs font-semibold uppercase text-muted-foreground tracking-wide mb-3">
              Entity Coverage Gaps ({sorData.entityGaps.length})
            </h3>
            <div className="rounded-lg border border-border overflow-hidden">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border bg-card/50">
                    <th className="text-left px-3 py-2 text-xs font-medium text-muted-foreground">Entity</th>
                    <th className="text-left px-3 py-2 text-xs font-medium text-muted-foreground">Referenced By</th>
                    <th className="text-left px-3 py-2 text-xs font-medium text-muted-foreground">Pack</th>
                  </tr>
                </thead>
                <tbody>
                  {sorData.entityGaps.map((gap) => (
                    <tr key={gap.entity} className="border-b border-border/50 last:border-0">
                      <td className="px-3 py-2">
                        <span className="text-xs font-medium">{gap.entityName}</span>
                        <span className="text-[10px] text-muted-foreground ml-1">({gap.entity})</span>
                      </td>
                      <td className="px-3 py-2">
                        <div className="flex flex-wrap gap-1">
                          {gap.referencedBy.map((m) => (
                            <span key={m} className="px-1.5 py-0.5 text-[10px] rounded bg-secondary/30 text-muted-foreground">
                              {m}
                            </span>
                          ))}
                        </div>
                      </td>
                      <td className="px-3 py-2 text-xs text-muted-foreground">{gap.pack || '—'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {sorData.orphanSources.length > 0 && (
          <div>
            <h3 className="text-xs font-semibold uppercase text-muted-foreground tracking-wide mb-3">
              Orphan Sources ({sorData.orphanSources.length})
            </h3>
            <div className="flex flex-wrap gap-1.5">
              {sorData.orphanSources.map((s) => (
                <span key={s} className="px-2 py-1 text-[10px] rounded border border-amber-500/20 bg-amber-500/10 text-amber-400">
                  {s}
                </span>
              ))}
            </div>
          </div>
        )}

        {sorData.missingSources.length > 0 && (
          <div>
            <h3 className="text-xs font-semibold uppercase text-muted-foreground tracking-wide mb-3">
              Missing Sources ({sorData.missingSources.length})
            </h3>
            <div className="flex flex-wrap gap-1.5">
              {sorData.missingSources.map((s) => (
                <span key={s} className="px-2 py-1 text-[10px] rounded border border-red-500/20 bg-red-500/10 text-red-400">
                  {s}
                </span>
              ))}
            </div>
          </div>
        )}
      </div>
    );
  };

  const renderXsysTab = () => {
    if (xsysLoading) return renderLoading();
    if (xsysError) return renderError(xsysError, fetchXsysData);
    if (!xsysData) return null;

    const { systems, deltas, category_breakdown, governance, drops_by_error } = xsysData;
    const { aam, farm, dcl } = systems;

    return (
      <div className="flex-1 overflow-y-auto p-4 space-y-4 min-h-0">
        {/* Snapshot identity bar */}
        {xsysData.snapshot_name && (
          <div className="rounded-lg border border-border bg-card/30 px-4 py-2.5">
            <div className="flex items-center gap-6 text-xs font-mono">
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

        {/* Pipeline flow — 3-column system comparison */}
        <div className="grid grid-cols-3 gap-3">
          {/* AAM column */}
          <div className="rounded-lg border border-blue-500/30 bg-blue-500/5 p-3 space-y-2">
            <div className="text-xs font-semibold text-blue-400 uppercase tracking-wider">AAM</div>
            <div className="space-y-1.5 text-xs">
              <div className="flex justify-between">
                <span className="text-muted-foreground">Total pipes</span>
                <span className="font-mono font-semibold text-foreground">{aam.total_pipes}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-muted-foreground">Dispatched</span>
                <span className="font-mono font-semibold text-emerald-400">{aam.dispatched}</span>
              </div>
              {aam.failed_pre_dispatch > 0 && (
                <div className="flex justify-between">
                  <span className="text-muted-foreground">Failed (pre-dispatch)</span>
                  <span className="font-mono font-semibold text-red-400">{aam.failed_pre_dispatch}</span>
                </div>
              )}
              <div className="border-t border-blue-500/20 pt-1.5 mt-1.5" />
              <div className="flex justify-between">
                <span className="text-muted-foreground" title="Unique vendor names from AAM pipe definitions. This is not the same as DCL's SOR count which uses category-based classification.">SORs (vendors)</span>
                <span className="font-mono text-foreground">{aam.sors}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-muted-foreground">Fabrics</span>
                <span className="font-mono text-foreground">{aam.fabrics}</span>
              </div>
              {aam.fabric_names.length > 0 && (
                <div className="text-[10px] text-muted-foreground/60 font-mono">
                  {aam.fabric_names.join(', ')}
                </div>
              )}
            </div>
          </div>

          {/* Farm column */}
          <div className="rounded-lg border border-emerald-500/30 bg-emerald-500/5 p-3 space-y-2">
            <div className="text-xs font-semibold text-emerald-400 uppercase tracking-wider">Farm</div>
            <div className="space-y-1.5 text-xs">
              <div className="flex justify-between">
                <span className="text-muted-foreground">Received from AAM</span>
                <span className="font-mono font-semibold text-foreground">{farm.total_received}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-muted-foreground">Pushed to DCL</span>
                <span className="font-mono font-semibold text-emerald-400">{farm.pushed_to_dcl}</span>
              </div>
              {farm.failed_execution > 0 && (
                <div className="flex justify-between">
                  <span className="text-muted-foreground">Failed (execution)</span>
                  <span className="font-mono font-semibold text-red-400">{farm.failed_execution}</span>
                </div>
              )}
            </div>
          </div>

          {/* DCL column */}
          <div className="rounded-lg border border-amber-500/30 bg-amber-500/5 p-3 space-y-2">
            <div className="text-xs font-semibold text-amber-400 uppercase tracking-wider">DCL</div>
            <div className="space-y-1.5 text-xs">
              <div className="flex justify-between">
                <span className="text-muted-foreground">Definitions</span>
                <span className="font-mono font-semibold text-foreground">{dcl.total_definitions}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-muted-foreground">Ingested</span>
                <span className="font-mono font-semibold text-emerald-400">{dcl.ingested}</span>
              </div>
              <div className="border-t border-amber-500/20 pt-1.5 mt-1.5" />
              <div className="flex justify-between">
                <span className="text-muted-foreground" title="Unique non-tooling source_systems. Counts sources where category is crm, erp, finops, infra, or aod. Different definition than AAM's vendor-based count.">SORs (category)</span>
                <span className="font-mono text-foreground">{dcl.sors_category}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-muted-foreground" title="Pipes with governance_status='governed'. A third SOR definition based on governance metadata.">SORs (governed)</span>
                <span className="font-mono text-foreground">{dcl.sors_governed}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-muted-foreground">Tooling</span>
                <span className="font-mono text-amber-400">{dcl.tooling_pipes}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-muted-foreground" title={`${dcl.fabrics_active} of ${dcl.fabrics_defined} defined fabrics have pipes that pushed content. Gap is expected if no pipes are currently routed to those fabrics.`}>
                  Fabrics (active)
                  {dcl.fabrics_active < dcl.fabrics_defined && (
                    <span className="text-[10px] text-muted-foreground/50 ml-1">/{dcl.fabrics_defined} defined</span>
                  )}
                </span>
                <span className="font-mono text-foreground">{dcl.fabrics_active}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-muted-foreground">Mapped / Unmapped</span>
                <span className="font-mono">
                  <span className="text-emerald-400">{dcl.mapped_pipes}</span>
                  {' / '}
                  <span className="text-red-400">{dcl.unmapped_pipes}</span>
                </span>
              </div>
              <div className="flex justify-between">
                <span className="text-muted-foreground">Rows</span>
                <span className="font-mono text-foreground">{dcl.rows.toLocaleString()}</span>
              </div>
              {dcl.drops_unique_pipes > 0 && (
                <>
                  <div className="border-t border-red-500/20 pt-1.5 mt-1.5" />
                  <div className="flex justify-between">
                    <span className="text-red-400">Drops (unique pipes)</span>
                    <span className="font-mono font-semibold text-red-400">{dcl.drops_unique_pipes}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Drop events (incl. retries)</span>
                    <span className="font-mono text-muted-foreground">{dcl.drops_total}</span>
                  </div>
                </>
              )}
            </div>
          </div>
        </div>

        {/* Deltas — gap explanations */}
        {deltas.length > 0 && (
          <div className="space-y-2">
            <h3 className="text-xs font-semibold uppercase text-muted-foreground tracking-wide">
              Gap Analysis ({deltas.length})
            </h3>
            {deltas.map((d, idx) => {
              const sev = d.severity === 'error'
                ? 'border-red-500/30 bg-red-500/5'
                : d.severity === 'warning'
                  ? 'border-amber-500/30 bg-amber-500/5'
                  : 'border-blue-500/30 bg-blue-500/5';
              const sevIcon = d.severity === 'error' ? '!' : d.severity === 'warning' ? '~' : 'i';
              const sevColor = d.severity === 'error'
                ? 'text-red-400'
                : d.severity === 'warning'
                  ? 'text-amber-400'
                  : 'text-blue-400';
              return (
                <div key={idx} className={`rounded-lg border ${sev} p-3`}>
                  <div className="flex items-start gap-2">
                    <span className={`w-5 h-5 rounded-full shrink-0 flex items-center justify-center text-[10px] font-bold border ${sevColor} border-current/30 bg-current/10`}>
                      {sevIcon}
                    </span>
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-3 text-xs">
                        <span className="font-semibold text-foreground">{d.label}</span>
                        <span className={`font-mono font-bold ${sevColor}`}>
                          {d.delta > 0 ? `+${d.delta}` : d.delta}
                        </span>
                      </div>
                      <div className="flex items-center gap-2 mt-1 text-[10px] font-mono text-muted-foreground">
                        <span>{d.left}</span>
                        <span className="text-muted-foreground/40">&rarr;</span>
                        <span>{d.right}</span>
                      </div>
                      <p className="text-[11px] text-muted-foreground/80 mt-1.5 leading-relaxed">
                        {d.explanation}
                      </p>
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        )}

        {/* Category breakdown */}
        {Object.keys(category_breakdown).length > 0 && (
          <div className="rounded-lg border border-border bg-card/30 p-3">
            <h3 className="text-xs font-semibold uppercase text-muted-foreground tracking-wide mb-2">
              Pipe Categories (from definitions)
            </h3>
            <div className="grid grid-cols-4 gap-2">
              {Object.entries(category_breakdown).sort((a, b) => b[1] - a[1]).map(([cat, count]) => (
                <div key={cat} className="flex justify-between text-xs px-2 py-1 rounded bg-card/50">
                  <span className="text-muted-foreground font-mono">{cat}</span>
                  <span className="font-mono font-semibold text-foreground">{count}</span>
                </div>
              ))}
            </div>
            <div className="flex gap-4 mt-2 text-[10px] text-muted-foreground">
              <span>Governed: <span className="text-emerald-400 font-semibold">{governance.governed}</span></span>
              <span>Ungoverned: <span className="text-muted-foreground/60">{governance.ungoverned}</span></span>
            </div>
          </div>
        )}

        {/* Drop breakdown by error code */}
        {Object.keys(drops_by_error).length > 0 && (
          <div className="rounded-lg border border-red-500/20 bg-red-500/5 p-3">
            <h3 className="text-xs font-semibold uppercase text-red-400/80 tracking-wide mb-2">
              Drops by Error Code
            </h3>
            <div className="space-y-1">
              {Object.entries(drops_by_error).sort((a, b) => b[1] - a[1]).map(([code, count]) => (
                <div key={code} className="flex justify-between text-xs px-2 py-1 rounded bg-red-500/5">
                  <span className="font-mono text-red-400/80">{code}</span>
                  <span className="font-mono font-semibold text-red-400">{count}</span>
                </div>
              ))}
            </div>
            {dcl.drop_pipe_ids.length > 0 && dcl.drop_pipe_ids.length <= 20 && (
              <div className="mt-2 text-[10px] text-muted-foreground/60 font-mono">
                {dcl.drop_pipe_ids.join(', ')}
              </div>
            )}
          </div>
        )}

        {/* 3-phase activity raw data */}
        <div className="rounded-lg border border-border bg-card/30 p-3">
          <h3 className="text-xs font-semibold uppercase text-muted-foreground tracking-wide mb-2">
            3-Phase Activity
          </h3>
          <div className="grid grid-cols-3 gap-3 text-xs">
            {(['structure', 'dispatch', 'content'] as const).map((phase) => {
              const entry = xsysData.activity[phase];
              const colors = {
                structure: { border: 'border-blue-500/30', text: 'text-blue-400', bg: 'bg-blue-500/10' },
                dispatch: { border: 'border-amber-500/30', text: 'text-amber-400', bg: 'bg-amber-500/10' },
                content: { border: 'border-emerald-500/30', text: 'text-emerald-400', bg: 'bg-emerald-500/10' },
              };
              const c = colors[phase];
              return (
                <div key={phase} className={`rounded border ${c.border} ${c.bg} p-2`}>
                  <div className={`font-semibold ${c.text} uppercase text-[10px] tracking-wider mb-1`}>
                    {phase}
                  </div>
                  {entry ? (
                    <div className="space-y-0.5 font-mono text-[10px]">
                      <div className="text-muted-foreground">
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
    );
  };

  const handleRefresh = () => {
    if (activeTab === 'aam') {
      fetchData();
    } else if (activeTab === 'sor') {
      fetchSorData();
    } else {
      fetchXsysData();
    }
  };

  const handleDownload = () => {
    const dateStr = getDateString();
    if (activeTab === 'aam' && data) {
      downloadJson(data, `dcl-aam-recon-${dateStr}.json`);
    } else if (activeTab === 'sor' && sorData) {
      downloadJson(sorData, `dcl-sor-recon-${dateStr}.json`);
    } else if (activeTab === 'xsys' && xsysData) {
      downloadJson(xsysData, `dcl-xsys-recon-${dateStr}.json`);
    }
  };

  return (
    <div className="h-full flex flex-col min-h-0">
      <div className="shrink-0 flex items-center justify-between px-6 py-3 border-b border-border bg-card/50">
        <h2 className="text-sm font-semibold">Reconciliation</h2>
        <div className="flex items-center gap-2">
          <button
            onClick={handleDownload}
            disabled={(activeTab === 'aam' && !data) || (activeTab === 'sor' && !sorData) || (activeTab === 'xsys' && !xsysData)}
            className="px-3 py-1 text-xs rounded bg-secondary text-secondary-foreground hover:bg-secondary/80 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            Download JSON
          </button>
          <button
            onClick={handleRefresh}
            className="px-3 py-1 text-xs rounded bg-primary text-primary-foreground hover:bg-primary/90"
          >
            Refresh
          </button>
        </div>
      </div>

      <div className="shrink-0 flex border-b border-border bg-card/30">
        <button
          onClick={() => setActiveTab('xsys')}
          className={`px-4 py-2 text-xs font-medium transition-colors ${
            activeTab === 'xsys'
              ? 'text-primary border-b-2 border-primary'
              : 'text-muted-foreground hover:text-foreground'
          }`}
        >
          X-System
        </button>
        <button
          onClick={() => setActiveTab('aam')}
          className={`px-4 py-2 text-xs font-medium transition-colors ${
            activeTab === 'aam'
              ? 'text-primary border-b-2 border-primary'
              : 'text-muted-foreground hover:text-foreground'
          }`}
        >
          {isFarm ? 'Farm Recon' : 'AAM Recon'}
        </button>
        <button
          onClick={() => setActiveTab('sor')}
          className={`px-4 py-2 text-xs font-medium transition-colors ${
            activeTab === 'sor'
              ? 'text-primary border-b-2 border-primary'
              : 'text-muted-foreground hover:text-foreground'
          }`}
        >
          SOR Recon
        </button>
      </div>

      {activeTab === 'xsys' ? renderXsysTab() : activeTab === 'aam' ? renderAamTab() : renderSorTab()}
    </div>
  );
}
