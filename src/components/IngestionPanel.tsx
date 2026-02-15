import { useState, useEffect, useCallback } from 'react';

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
const BATCH_DISPLAY_LIMIT = 5;

export function IngestionPanel() {
  const [stats, setStats] = useState<IngestStats | null>(null);
  const [batches, setBatches] = useState<IngestBatch[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [expandedBatchIds, setExpandedBatchIds] = useState<Set<string>>(new Set());

  const fetchRuns = async () => {
    try {
      const res = await fetch('/api/dcl/ingest/runs');
      if (!res.ok) {
        throw new Error(`HTTP ${res.status}`);
      }
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

  const toggleBatchExpand = useCallback((batchId: string) => {
    setExpandedBatchIds(prev => {
      const next = new Set(prev);
      if (next.has(batchId)) {
        next.delete(batchId);
      } else {
        next.add(batchId);
      }
      return next;
    });
  }, []);

  useEffect(() => {
    fetchAll();
    const interval = setInterval(fetchAll, POLL_INTERVAL_MS);
    return () => clearInterval(interval);
  }, [fetchAll]);

  const formatTimestamp = (ts: string) => {
    try {
      return new Date(ts).toLocaleString('en-US', {
        month: 'short',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: true,
      });
    } catch {
      return ts;
    }
  };

  const visibleBatches = batches.slice(0, BATCH_DISPLAY_LIMIT);
  const driftBatches = visibleBatches.filter(b => b.drift_count > 0);

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
      <div className="shrink-0 px-6 py-3 border-b border-border bg-card/50 space-y-2">
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

      <div className="flex-1 overflow-y-auto p-6 space-y-6 min-h-0">
        {error && (
          <div className="rounded-lg border border-red-500/20 bg-red-500/10 p-4 text-center">
            <span className="text-sm text-red-400">{error}</span>
            <button
              onClick={fetchAll}
              className="ml-3 px-3 py-1 text-xs rounded bg-primary text-primary-foreground hover:bg-primary/90"
            >
              Retry
            </button>
          </div>
        )}

        {stats && stats.total_runs > 0 && (
          <div className="rounded-lg border border-border bg-card/30 p-4 space-y-2">
            <h3 className="text-xs font-semibold uppercase text-muted-foreground tracking-wide">
              Run Summary
            </h3>
            <div className="grid grid-cols-3 gap-4 text-center">
              <div>
                <div className="text-lg font-bold text-foreground">{batches.length}</div>
                <div className="text-[10px] text-muted-foreground uppercase tracking-wide">Batches</div>
              </div>
              <div>
                <div className="text-lg font-bold text-foreground">{stats.total_runs}</div>
                <div className="text-[10px] text-muted-foreground uppercase tracking-wide">Runs</div>
              </div>
              <div>
                <div className="text-lg font-bold text-foreground">{stats.total_rows_buffered.toLocaleString()}</div>
                <div className="text-[10px] text-muted-foreground uppercase tracking-wide">Rows</div>
              </div>
            </div>
            <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-[11px] font-mono text-muted-foreground pt-1 border-t border-border/50">
              <span>
                <span className="text-foreground font-semibold">{stats.unique_sources}</span> sources
              </span>
              <span className="text-border">|</span>
              <span>
                tenant: <span className="text-foreground font-semibold">{stats.tenant_names.join(', ')}</span>
              </span>
              {stats.first_run_at && stats.latest_run_at && (
                <>
                  <span className="text-border">|</span>
                  <span>
                    {formatTimestamp(stats.first_run_at)} → {formatTimestamp(stats.latest_run_at)}
                  </span>
                </>
              )}
            </div>
          </div>
        )}

        <div>
          <div className="flex items-center justify-between mb-3">
            <h3 className="text-xs font-semibold uppercase text-muted-foreground tracking-wide">
              Batches ({visibleBatches.length})
            </h3>
            {batches.length > BATCH_DISPLAY_LIMIT && (
              <span className="text-[10px] text-muted-foreground">
                Showing {BATCH_DISPLAY_LIMIT} of {batches.length} batches
              </span>
            )}
          </div>
          {visibleBatches.length === 0 ? (
            <div className="rounded-lg border border-border bg-card/30 p-6 text-center">
              <div className="text-muted-foreground text-sm">No batches yet</div>
              <div className="text-muted-foreground text-xs mt-1">
                Waiting for AAM Runners to push data...
              </div>
            </div>
          ) : (
            <div className="space-y-2">
              {visibleBatches.map((batch) => {
                const isExpanded = expandedBatchIds.has(batch.batch_id);
                return (
                  <div
                    key={batch.batch_id}
                    className="rounded-lg border border-border bg-card/30 overflow-hidden"
                  >
                    <div className="px-4 py-3 space-y-2">
                      <div className="flex items-start justify-between">
                        <div className="flex items-center gap-3">
                          <span className="text-sm font-semibold text-foreground">{batch.snapshot_name}</span>
                          <span className="text-[10px] px-1.5 py-0.5 rounded bg-primary/10 border border-primary/20 text-muted-foreground">
                            {batch.tenant_id}
                          </span>
                        </div>
                      </div>

                      <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs font-mono">
                        <span>
                          <span className="text-emerald-400 font-semibold">{batch.run_count}</span>
                          <span className="text-muted-foreground ml-1">runs</span>
                        </span>
                        <span className="text-border">|</span>
                        <span>
                          <span className="text-emerald-400 font-semibold">{batch.total_rows.toLocaleString()}</span>
                          <span className="text-muted-foreground ml-1">rows</span>
                        </span>
                        <span className="text-border">|</span>
                        <span>
                          <span className="text-emerald-400 font-semibold">{batch.unique_sources}</span>
                          <span className="text-muted-foreground ml-1">sources</span>
                        </span>
                        <span className="text-border">|</span>
                        <span>
                          <span className={`font-semibold ${batch.drift_count > 0 ? 'text-amber-400' : 'text-emerald-400'}`}>
                            {batch.drift_count}
                          </span>
                          <span className="text-muted-foreground ml-1">drift</span>
                        </span>
                      </div>

                      <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-[11px] text-muted-foreground">
                        <span>
                          {formatTimestamp(batch.first_received_at)}
                          {batch.first_received_at !== batch.latest_received_at && (
                            <> → {formatTimestamp(batch.latest_received_at)}</>
                          )}
                        </span>
                      </div>

                      <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-[10px] font-mono text-muted-foreground">
                        <span>first: {batch.first_run_id.slice(0, 12)}...</span>
                        {batch.first_run_id !== batch.latest_run_id && (
                          <span>latest: {batch.latest_run_id.slice(0, 12)}...</span>
                        )}
                      </div>

                      {batch.source_list.length > 0 && (
                        <div>
                          <button
                            onClick={() => toggleBatchExpand(batch.batch_id)}
                            className="flex items-center gap-1 text-[10px] text-primary hover:text-primary/80 transition-colors"
                          >
                            <svg
                              className={`w-2.5 h-2.5 transition-transform duration-200 ${isExpanded ? 'rotate-90' : ''}`}
                              fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}
                            >
                              <path strokeLinecap="round" strokeLinejoin="round" d="M9 5l7 7-7 7" />
                            </svg>
                            {batch.source_list.length} source{batch.source_list.length !== 1 ? 's' : ''}
                          </button>
                          {isExpanded && (
                            <div className="flex flex-wrap gap-1 mt-1.5">
                              {batch.source_list.map(s => (
                                <span
                                  key={s}
                                  className="px-1.5 py-0.5 text-[10px] font-mono rounded bg-card/50 border border-border text-muted-foreground"
                                >
                                  {s}
                                </span>
                              ))}
                            </div>
                          )}
                        </div>
                      )}
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </div>

        {driftBatches.length > 0 && (
          <div>
            <h3 className="text-xs font-semibold uppercase text-muted-foreground tracking-wide mb-3">
              Schema Drift Detected
            </h3>
            <div className="space-y-2">
              {driftBatches.map(b => (
                <div key={b.batch_id} className="rounded-lg border border-amber-500/20 bg-amber-500/10 p-3">
                  <div className="flex items-center gap-2">
                    <span className="text-sm text-amber-400">~</span>
                    <span className="text-xs font-medium">{b.snapshot_name}</span>
                    <span className="text-[10px] font-mono text-muted-foreground">{b.batch_id.slice(0, 8)}</span>
                    <span className="text-[10px] text-amber-400 font-semibold ml-auto">{b.drift_count} drift event{b.drift_count !== 1 ? 's' : ''}</span>
                  </div>
                  <div className="flex flex-wrap gap-1 mt-2">
                    {b.source_list.map(s => (
                      <span key={s} className="px-1.5 py-0.5 text-[10px] rounded bg-card/50 border border-border text-muted-foreground">
                        {s}
                      </span>
                    ))}
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
