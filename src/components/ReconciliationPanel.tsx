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
  };
  coverageMatrix: SorCoverageRow[];
  sorConflicts: SorConflict[];
  orphanSources: string[];
  missingSources: string[];
  entityGaps: SorEntityGap[];
  reconMeta?: {
    dclRunId: string | null;
    dclRunAt: string | null;
    reconAt: string;
    dataMode: string | null;
    loadedSourceCount: number;
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
}

export function ReconciliationPanel({ runId }: ReconciliationPanelProps) {
  const [activeTab, setActiveTab] = useState<'aam' | 'sor'>('aam');
  const [data, setData] = useState<ReconciliationData | null>(null);
  const [sorData, setSorData] = useState<SorReconciliationData | null>(null);
  const [loading, setLoading] = useState(true);
  const [sorLoading, setSorLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [sorError, setSorError] = useState<string | null>(null);
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

    const summaryCards = [
      { label: 'AAM Connections', value: data.summary.aamConnections ?? 0 },
      { label: 'DCL Loaded', value: data.summary.dclLoadedSources ?? 0 },
      { label: 'Matched', value: data.summary.matched ?? 0 },
      { label: 'In AAM Not DCL', value: data.summary.inAamNotDcl ?? 0 },
      { label: 'In DCL Not AAM', value: data.summary.inDclNotAam ?? 0 },
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
              <span>AAM: <span className="text-foreground font-mono">{data.reconMeta.aamConnectionCount}</span></span>
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
                    <th className="text-right px-3 py-2 text-xs font-medium text-muted-foreground">AAM</th>
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
              <span>In AAM but Not DCL ({data.inAamNotDcl?.length ?? 0})</span>
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
              In DCL but Not AAM ({data.inDclNotAam?.length ?? 0})
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
                    <h4 className="text-[10px] uppercase text-muted-foreground tracking-wide mb-2">AAM Pipes ({data.trace.aamPipeNames.length})</h4>
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
                    <th className="text-right px-3 py-2 text-xs font-medium text-muted-foreground">Conflicts</th>
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
                      <td className="text-right px-3 py-2 font-mono text-xs">
                        {row.conflictCount > 0 ? (
                          <span className="text-amber-400">{row.conflictCount}</span>
                        ) : (
                          <span className="text-muted-foreground">0</span>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
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

  const handleRefresh = () => {
    if (activeTab === 'aam') {
      fetchData();
    } else {
      fetchSorData();
    }
  };

  const handleDownload = () => {
    const dateStr = getDateString();
    if (activeTab === 'aam' && data) {
      downloadJson(data, `dcl-aam-recon-${dateStr}.json`);
    } else if (activeTab === 'sor' && sorData) {
      downloadJson(sorData, `dcl-sor-recon-${dateStr}.json`);
    }
  };

  return (
    <div className="h-full flex flex-col min-h-0">
      <div className="shrink-0 flex items-center justify-between px-6 py-3 border-b border-border bg-card/50">
        <h2 className="text-sm font-semibold">Reconciliation</h2>
        <div className="flex items-center gap-2">
          <button
            onClick={handleDownload}
            disabled={(activeTab === 'aam' && !data) || (activeTab === 'sor' && !sorData)}
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
          onClick={() => setActiveTab('aam')}
          className={`px-4 py-2 text-xs font-medium transition-colors ${
            activeTab === 'aam'
              ? 'text-primary border-b-2 border-primary'
              : 'text-muted-foreground hover:text-foreground'
          }`}
        >
          AAM Recon
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

      {activeTab === 'aam' ? renderAamTab() : renderSorTab()}
    </div>
  );
}
