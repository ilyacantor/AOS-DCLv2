import { useState, useEffect } from 'react';

interface ReconciliationData {
  status: string;
  summary: {
    totalPushed: number;
    mappedPipes: number;
    unmappedPipes: number;
    dclConnections: number;
    dclFabrics: number;
    uniqueSourceSystems: number;
    missingFromDcl: number;
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
    pushedPipes: number;
    dclConnections: number;
    delta: number;
  }>;
  unmappedPipes: Array<{
    pipeId: string;
    displayName: string;
    sourceSystem: string;
    transportKind: string;
    trustLabels: string[];
    hasSchema: boolean;
  }>;
  missingFromDcl: Array<{
    pipeId: string;
    displayName: string;
    sourceSystem: string;
    fabricPlane: string;
  }>;
  pushMeta: {
    pushId: string;
    pushedAt: string;
    pipeCount: number;
    payloadHash: string;
    aodRunId: string | null;
  } | null;
}

const statusColors: Record<string, string> = {
  synced: 'bg-emerald-500/20 text-emerald-400 border-emerald-500/30',
  drifted: 'bg-amber-500/20 text-amber-400 border-amber-500/30',
  critical: 'bg-red-500/20 text-red-400 border-red-500/30',
  empty: 'bg-gray-500/20 text-gray-400 border-gray-500/30',
  no_pushes: 'bg-gray-500/20 text-gray-400 border-gray-500/30',
};

const severityStyles: Record<string, { bg: string; text: string; icon: string }> = {
  warning: { bg: 'bg-amber-500/10 border-amber-500/20', text: 'text-amber-400', icon: '⚠' },
  error: { bg: 'bg-red-500/10 border-red-500/20', text: 'text-red-400', icon: '✕' },
  info: { bg: 'bg-blue-500/10 border-blue-500/20', text: 'text-blue-400', icon: 'ℹ' },
};

export function ReconciliationPanel() {
  const [data, setData] = useState<ReconciliationData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [unmappedExpanded, setUnmappedExpanded] = useState(false);
  const [unmappedLimit, setUnmappedLimit] = useState(50);

  const fetchData = async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch('/api/dcl/reconciliation');
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = await res.json();
      setData(json);
    } catch (e: any) {
      setError(e.message || 'Failed to fetch reconciliation data');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, []);

  if (loading) {
    return (
      <div className="h-full flex items-center justify-center text-muted-foreground">
        <div className="flex flex-col items-center gap-3">
          <div className="w-8 h-8 border-2 border-primary border-t-transparent rounded-full animate-spin" />
          <span className="text-sm">Loading reconciliation data...</span>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="h-full flex items-center justify-center">
        <div className="flex flex-col items-center gap-3 text-center p-8">
          <span className="text-red-400 text-lg">✕</span>
          <span className="text-sm text-muted-foreground">{error}</span>
          <button
            onClick={fetchData}
            className="px-3 py-1.5 text-xs rounded bg-primary text-primary-foreground hover:bg-primary/90"
          >
            Retry
          </button>
        </div>
      </div>
    );
  }

  if (!data) return null;

  const formatTimestamp = (ts: string) => {
    try {
      return new Date(ts).toLocaleString();
    } catch {
      return ts;
    }
  };

  const summaryCards = [
    { label: 'Pushed', value: data.summary.totalPushed },
    { label: 'Mapped', value: data.summary.mappedPipes },
    { label: 'Unmapped', value: data.summary.unmappedPipes },
    { label: 'DCL Loaded', value: data.summary.dclConnections },
    { label: 'Sources', value: data.summary.uniqueSourceSystems },
    { label: 'Missing', value: data.summary.missingFromDcl },
  ];

  const visibleUnmapped = data.unmappedPipes.slice(0, unmappedLimit);

  return (
    <div className="h-full flex flex-col min-h-0">
      <div className="shrink-0 flex items-center justify-between px-6 py-3 border-b border-border bg-card/50">
        <h2 className="text-sm font-semibold">Reconciliation</h2>
        <button
          onClick={fetchData}
          className="px-3 py-1 text-xs rounded bg-primary text-primary-foreground hover:bg-primary/90"
        >
          Refresh
        </button>
      </div>

      <div className="flex-1 overflow-y-auto p-6 space-y-6 min-h-0">
        <div className="rounded-lg border border-border bg-card/30 p-4">
          <div className="flex items-center gap-3 flex-wrap">
            <span className={`px-2.5 py-1 text-xs font-medium rounded border ${statusColors[data.status] || statusColors.empty}`}>
              {data.status.toUpperCase()}
            </span>
            {data.pushMeta ? (
              <>
                <span className="text-xs text-muted-foreground">
                  {formatTimestamp(data.pushMeta.pushedAt)}
                </span>
                <span className="text-xs font-mono text-muted-foreground bg-secondary/30 px-2 py-0.5 rounded">
                  {data.pushMeta.payloadHash.slice(0, 12)}…
                </span>
                <span className="text-xs text-muted-foreground">
                  {data.pushMeta.pipeCount} pipes
                </span>
              </>
            ) : (
              <span className="text-xs text-muted-foreground italic">No push data available</span>
            )}
          </div>
        </div>

        <div className="grid grid-cols-2 gap-3">
          {summaryCards.map((card) => (
            <div key={card.label} className="rounded-lg border border-border bg-card/30 p-3">
              <div className="text-[10px] uppercase text-muted-foreground tracking-wide">{card.label}</div>
              <div className="text-xl font-mono font-semibold mt-1">{card.value.toLocaleString()}</div>
            </div>
          ))}
        </div>

        {data.diffCauses.length > 0 && (
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
                      <span className={`ml-auto text-xs font-mono ${style.text}`}>×{cause.count}</span>
                    </div>
                    <p className="text-xs text-muted-foreground mt-1">{cause.description}</p>
                  </div>
                );
              })}
            </div>
          </div>
        )}

        {data.fabricBreakdown.length > 0 && (
          <div>
            <h3 className="text-xs font-semibold uppercase text-muted-foreground tracking-wide mb-3">Fabric Breakdown</h3>
            <div className="rounded-lg border border-border overflow-hidden">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border bg-card/50">
                    <th className="text-left px-3 py-2 text-xs font-medium text-muted-foreground">Plane</th>
                    <th className="text-right px-3 py-2 text-xs font-medium text-muted-foreground">Pushed</th>
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
                      <td className="text-right px-3 py-2 font-mono">{row.pushedPipes}</td>
                      <td className="text-right px-3 py-2 font-mono">{row.dclConnections}</td>
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

        {data.unmappedPipes.length > 0 && (
          <div>
            <button
              onClick={() => setUnmappedExpanded(!unmappedExpanded)}
              className="flex items-center gap-2 text-xs font-semibold uppercase text-muted-foreground tracking-wide mb-3 hover:text-foreground transition-colors"
            >
              <span>{unmappedExpanded ? '▾' : '▸'}</span>
              <span>Unmapped Pipes ({data.unmappedPipes.length.toLocaleString()})</span>
            </button>
            {unmappedExpanded && (
              <div className="rounded-lg border border-border overflow-hidden">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-border bg-card/50">
                      <th className="text-left px-3 py-2 text-xs font-medium text-muted-foreground">Name</th>
                      <th className="text-left px-3 py-2 text-xs font-medium text-muted-foreground">Source System</th>
                      <th className="text-left px-3 py-2 text-xs font-medium text-muted-foreground">Transport</th>
                      <th className="text-center px-3 py-2 text-xs font-medium text-muted-foreground">Governed</th>
                    </tr>
                  </thead>
                  <tbody>
                    {visibleUnmapped.map((pipe) => (
                      <tr key={pipe.pipeId} className="border-b border-border/50 last:border-0">
                        <td className="px-3 py-2 font-mono text-xs truncate max-w-[200px]" title={pipe.displayName}>{pipe.displayName}</td>
                        <td className="px-3 py-2 text-xs text-muted-foreground">{pipe.sourceSystem}</td>
                        <td className="px-3 py-2 text-xs text-muted-foreground">{pipe.transportKind}</td>
                        <td className="text-center px-3 py-2">
                          <span className={`inline-block w-2 h-2 rounded-full ${pipe.hasSchema ? 'bg-emerald-400' : 'bg-gray-500'}`} />
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                {unmappedLimit < data.unmappedPipes.length && (
                  <div className="border-t border-border px-3 py-2 text-center">
                    <button
                      onClick={() => setUnmappedLimit(prev => prev + 50)}
                      className="text-xs text-primary hover:text-primary/80"
                    >
                      Show more ({data.unmappedPipes.length - unmappedLimit} remaining)
                    </button>
                  </div>
                )}
              </div>
            )}
          </div>
        )}

        {data.missingFromDcl.length > 0 && (
          <div>
            <h3 className="text-xs font-semibold uppercase text-muted-foreground tracking-wide mb-3">
              Missing from DCL ({data.missingFromDcl.length})
            </h3>
            <div className="rounded-lg border border-border overflow-hidden">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border bg-card/50">
                    <th className="text-left px-3 py-2 text-xs font-medium text-muted-foreground">Name</th>
                    <th className="text-left px-3 py-2 text-xs font-medium text-muted-foreground">Source</th>
                    <th className="text-left px-3 py-2 text-xs font-medium text-muted-foreground">Fabric Plane</th>
                  </tr>
                </thead>
                <tbody>
                  {data.missingFromDcl.map((item) => (
                    <tr key={item.pipeId} className="border-b border-border/50 last:border-0">
                      <td className="px-3 py-2 font-mono text-xs">{item.displayName}</td>
                      <td className="px-3 py-2 text-xs text-muted-foreground">{item.sourceSystem}</td>
                      <td className="px-3 py-2 text-xs text-muted-foreground">{item.fabricPlane}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}