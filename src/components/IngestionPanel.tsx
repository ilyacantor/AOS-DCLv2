import { useState, useEffect, useCallback, Fragment } from 'react';

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
}

interface IngestRunsResponse {
  runs: unknown[];
  stats: IngestStats;
}

interface IngestBatch {
  batch_id: string;
  snapshot_name: string;
  tenant_id: string;
  run_count: number;
  total_rows: number;
  unique_sources: number;
  source_list: string[];
  first_run_id: string;
  latest_run_id: string;
  first_received_at: string;
  latest_received_at: string;
  drift_count: number;
}

const POLL_INTERVAL_MS = 5000;

export function IngestionPanel() {
  const [stats, setStats] = useState<IngestStats | null>(null);
  const [batches, setBatches] = useState<IngestBatch[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [expandedId, setExpandedId] = useState<string | null>(null);

  const fetchRuns = async () => {
    try {
      const res = await fetch('/api/dcl/ingest/runs');
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json: IngestRunsResponse = await res.json();
      setStats(json.stats);
      setError(null);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : 'Failed to fetch ingestion data';
      setError(msg);
    } finally {
      setLoading(false);
    }
  };

  const fetchBatches = async () => {
    try {
      const res = await fetch('/api/dcl/ingest/batches');
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = await res.json();
      setBatches(json.batches);
    } catch {}
  };

  const fetchAll = useCallback(() => {
    fetchRuns();
    fetchBatches();
  }, []);

  useEffect(() => {
    fetchAll();
    const interval = setInterval(fetchAll, POLL_INTERVAL_MS);
    return () => clearInterval(interval);
  }, [fetchAll]);

  const fmtDate = (ts: string) => {
    try {
      return new Date(ts).toLocaleString('en-US', {
        month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', hour12: true,
      });
    } catch { return ts; }
  };

  const fmtRows = (n: number) => n >= 1000 ? `${(n / 1000).toFixed(1)}k` : String(n);

  const inferSource = (b: IngestBatch) => {
    if (b.snapshot_name.startsWith('cloudedge-')) return 'Farm';
    if (b.snapshot_name.startsWith('aam') || b.tenant_id.includes('aam')) return 'AAM';
    return 'Push';
  };

  if (loading) {
    return (
      <div className="h-full flex flex-col min-h-0">
        <div className="shrink-0 flex items-center justify-between px-6 py-3 border-b border-border bg-card/50">
          <h2 className="text-sm font-semibold">Recent Ingestions</h2>
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
      <div className="shrink-0 px-6 py-3 border-b border-border bg-card/50">
        <div className="flex items-center justify-between">
          <h2 className="text-sm font-semibold">Ingest Buffer</h2>
          <div className="flex items-center gap-2">
            <span className="text-[10px] text-muted-foreground">
              Auto-refresh {POLL_INTERVAL_MS / 1000}s
            </span>
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

        {stats && stats.total_runs > 0 && (
          <div className="rounded-lg border border-border bg-card/30 px-4 py-2.5">
            <div className="flex items-center gap-6 text-xs font-mono">
              <span><span className="text-foreground font-semibold">{batches.length}</span> <span className="text-muted-foreground">batches</span></span>
              <span><span className="text-foreground font-semibold">{stats.total_runs}</span> <span className="text-muted-foreground">runs</span></span>
              <span><span className="text-foreground font-semibold">{fmtRows(stats.total_rows_buffered)}</span> <span className="text-muted-foreground">rows</span></span>
              <span><span className="text-foreground font-semibold">{stats.unique_sources}</span> <span className="text-muted-foreground">sources</span></span>
              {stats.total_drift_events > 0 && (
                <span><span className="text-amber-400 font-semibold">{stats.total_drift_events}</span> <span className="text-muted-foreground">drift</span></span>
              )}
            </div>
          </div>
        )}

        <div>
          {batches.length === 0 ? (
            <div className="rounded-lg border border-border bg-card/30 p-6 text-center">
              <div className="text-muted-foreground text-sm">No ingestions yet</div>
              <div className="text-muted-foreground text-xs mt-1">Waiting for Farm or AAM to push data...</div>
            </div>
          ) : (
            <div className="rounded-lg border border-border bg-card/30 overflow-hidden">
              <table className="w-full text-xs">
                <thead>
                  <tr className="border-b border-border text-[10px] uppercase tracking-wider text-muted-foreground">
                    <th className="text-left px-3 py-2 font-medium">Snapshot</th>
                    <th className="text-left px-3 py-2 font-medium">Source</th>
                    <th className="text-left px-3 py-2 font-medium">When</th>
                    <th className="text-right px-3 py-2 font-medium">Runs</th>
                    <th className="text-right px-3 py-2 font-medium">Rows</th>
                    <th className="text-right px-3 py-2 font-medium">Pipes</th>
                    <th className="text-center px-3 py-2 font-medium">Drift</th>
                  </tr>
                </thead>
                <tbody>
                  {batches.map((b) => {
                    const isExpanded = expandedId === b.batch_id;
                    const src = inferSource(b);
                    const srcColor = src === 'Farm' ? 'text-emerald-400' : src === 'AAM' ? 'text-blue-400' : 'text-muted-foreground';

                    return (
                      <Fragment key={b.batch_id}>
                        <tr
                          onClick={() => setExpandedId(isExpanded ? null : b.batch_id)}
                          className="border-b border-border/50 hover:bg-card/50 cursor-pointer transition-colors"
                        >
                          <td className="px-3 py-2 font-mono font-medium text-foreground">
                            <div className="flex items-center gap-1.5">
                              <svg
                                className={`w-2.5 h-2.5 shrink-0 transition-transform duration-150 text-muted-foreground ${isExpanded ? 'rotate-90' : ''}`}
                                fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}
                              >
                                <path strokeLinecap="round" strokeLinejoin="round" d="M9 5l7 7-7 7" />
                              </svg>
                              {b.snapshot_name}
                            </div>
                          </td>
                          <td className={`px-3 py-2 font-semibold ${srcColor}`}>{src}</td>
                          <td className="px-3 py-2 text-muted-foreground">{fmtDate(b.latest_received_at)}</td>
                          <td className="px-3 py-2 text-right font-mono text-foreground">{b.run_count}</td>
                          <td className="px-3 py-2 text-right font-mono text-foreground">{fmtRows(b.total_rows)}</td>
                          <td className="px-3 py-2 text-right font-mono text-foreground">{b.unique_sources}</td>
                          <td className="px-3 py-2 text-center">
                            {b.drift_count > 0
                              ? <span className="text-amber-400 font-semibold">{b.drift_count}</span>
                              : <span className="text-muted-foreground/40">-</span>
                            }
                          </td>
                        </tr>
                        {isExpanded && (
                          <tr>
                            <td colSpan={7} className="bg-card/20 px-4 py-3 border-b border-border/50">
                              <div className="space-y-2">
                                <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-[11px] font-mono text-muted-foreground">
                                  <span>tenant: <span className="text-foreground">{b.tenant_id}</span></span>
                                  <span className="text-border">|</span>
                                  <span>batch: <span className="text-foreground">{b.batch_id.slice(0, 12)}</span></span>
                                  <span className="text-border">|</span>
                                  <span>first run: <span className="text-foreground">{b.first_run_id.slice(0, 12)}</span></span>
                                  {b.first_run_id !== b.latest_run_id && (
                                    <>
                                      <span className="text-border">|</span>
                                      <span>latest run: <span className="text-foreground">{b.latest_run_id.slice(0, 12)}</span></span>
                                    </>
                                  )}
                                </div>
                                <div className="flex flex-wrap items-center gap-x-4 text-[11px] text-muted-foreground">
                                  <span>{fmtDate(b.first_received_at)}{b.first_received_at !== b.latest_received_at && ` \u2192 ${fmtDate(b.latest_received_at)}`}</span>
                                </div>
                                {b.source_list.length > 0 && (
                                  <div className="flex flex-wrap gap-1 pt-1">
                                    {b.source_list.map(s => (
                                      <span key={s} className="px-1.5 py-0.5 text-[10px] font-mono rounded bg-card/50 border border-border text-muted-foreground">
                                        {s}
                                      </span>
                                    ))}
                                  </div>
                                )}
                              </div>
                            </td>
                          </tr>
                        )}
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
