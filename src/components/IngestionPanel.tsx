import { useState, useEffect, useRef } from 'react';

interface IngestRun {
  run_id: string;
  pipe_id: string;
  source_system: string;
  canonical_source_id: string;
  tenant_id: string;
  snapshot_name: string;
  run_timestamp: string;
  received_at: string;
  schema_version: string;
  row_count: number;
  schema_drift: boolean;
  drift_fields: string[];
  runner_id: string | null;
}

interface IngestStats {
  total_runs: number;
  total_rows_buffered: number;
  total_drift_events: number;
  pipes_tracked: number;
  max_runs: number;
  max_rows: number;
}

interface IngestRunsResponse {
  runs: IngestRun[];
  stats: IngestStats;
}

const POLL_INTERVAL_MS = 5000;
const DISPLAY_LIMIT = 5;

export function IngestionPanel() {
  const [runs, setRuns] = useState<IngestRun[]>([]);
  const [stats, setStats] = useState<IngestStats | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const prevRunIdsRef = useRef<Set<string>>(new Set());
  const [newRunIds, setNewRunIds] = useState<Set<string>>(new Set());

  const fetchRuns = async () => {
    try {
      const res = await fetch('/api/dcl/ingest/runs');
      if (!res.ok) {
        throw new Error(`HTTP ${res.status}`);
      }
      const json: IngestRunsResponse = await res.json();

      // Detect newly arrived runs for flash animation
      const incomingIds = new Set(json.runs.map(r => r.run_id));
      const prevIds = prevRunIdsRef.current;
      if (prevIds.size > 0) {
        const fresh = new Set<string>();
        incomingIds.forEach(id => {
          if (!prevIds.has(id)) fresh.add(id);
        });
        if (fresh.size > 0) {
          setNewRunIds(fresh);
          setTimeout(() => setNewRunIds(new Set()), 2000);
        }
      }
      prevRunIdsRef.current = incomingIds;

      setRuns(json.runs);
      setStats(json.stats);
      setError(null);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : 'Failed to fetch ingestion data';
      setError(msg);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchRuns();
    const interval = setInterval(fetchRuns, POLL_INTERVAL_MS);
    return () => clearInterval(interval);
  }, []);

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

  const recentRuns = [...runs]
    .sort((a, b) => new Date(b.received_at).getTime() - new Date(a.received_at).getTime())
    .slice(0, DISPLAY_LIMIT);

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
      {/* Header */}
      <div className="shrink-0 flex items-center justify-between px-6 py-3 border-b border-border bg-card/50">
        <div className="flex items-center gap-3">
          <h2 className="text-sm font-semibold">Recent Ingestions</h2>
          {stats && (
            <span className="text-[10px] text-muted-foreground font-mono bg-secondary/30 px-2 py-0.5 rounded">
              {stats.total_runs} runs / {stats.total_rows_buffered.toLocaleString()} rows buffered
            </span>
          )}
        </div>
        <div className="flex items-center gap-2">
          <span className="text-[10px] text-muted-foreground">
            Auto-refresh {POLL_INTERVAL_MS / 1000}s
          </span>
          <button
            onClick={fetchRuns}
            className="px-3 py-1 text-xs rounded bg-primary text-primary-foreground hover:bg-primary/90"
          >
            Refresh
          </button>
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-y-auto p-6 space-y-6 min-h-0">
        {error && (
          <div className="rounded-lg border border-red-500/20 bg-red-500/10 p-4 text-center">
            <span className="text-sm text-red-400">{error}</span>
            <button
              onClick={fetchRuns}
              className="ml-3 px-3 py-1 text-xs rounded bg-primary text-primary-foreground hover:bg-primary/90"
            >
              Retry
            </button>
          </div>
        )}

        {/* Summary Stats */}
        {stats && (
          <div className="grid grid-cols-4 gap-3">
            <div className="rounded-lg border border-border bg-card/30 p-3">
              <div className="text-[10px] uppercase text-muted-foreground tracking-wide">Total Runs</div>
              <div className="text-xl font-mono font-semibold mt-1">{stats.total_runs}</div>
            </div>
            <div className="rounded-lg border border-border bg-card/30 p-3">
              <div className="text-[10px] uppercase text-muted-foreground tracking-wide">Rows Buffered</div>
              <div className="text-xl font-mono font-semibold mt-1">{stats.total_rows_buffered.toLocaleString()}</div>
            </div>
            <div className="rounded-lg border border-border bg-card/30 p-3">
              <div className="text-[10px] uppercase text-muted-foreground tracking-wide">Pipes Tracked</div>
              <div className="text-xl font-mono font-semibold mt-1">{stats.pipes_tracked}</div>
            </div>
            <div className="rounded-lg border border-border bg-card/30 p-3">
              <div className="text-[10px] uppercase text-muted-foreground tracking-wide">Drift Events</div>
              <div className="text-xl font-mono font-semibold mt-1">{stats.total_drift_events}</div>
            </div>
          </div>
        )}

        {/* Recent Runs Table */}
        {recentRuns.length === 0 ? (
          <div className="rounded-lg border border-border bg-card/30 p-8 text-center">
            <div className="text-muted-foreground text-sm">No ingestion runs yet.</div>
            <div className="text-muted-foreground text-xs mt-1">
              Waiting for AAM Runners to push data...
            </div>
          </div>
        ) : (
          <div>
            <h3 className="text-xs font-semibold uppercase text-muted-foreground tracking-wide mb-3">
              Last {recentRuns.length} Runs
            </h3>
            <div className="rounded-lg border border-border overflow-hidden">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border bg-card/50">
                    <th className="text-center px-3 py-2 text-xs font-medium text-muted-foreground w-12">Status</th>
                    <th className="text-left px-3 py-2 text-xs font-medium text-muted-foreground">Run ID</th>
                    <th className="text-left px-3 py-2 text-xs font-medium text-muted-foreground">Source</th>
                    <th className="text-right px-3 py-2 text-xs font-medium text-muted-foreground">Rows</th>
                    <th className="text-left px-3 py-2 text-xs font-medium text-muted-foreground">Received</th>
                    <th className="text-center px-3 py-2 text-xs font-medium text-muted-foreground">Drift</th>
                  </tr>
                </thead>
                <tbody>
                  {recentRuns.map((run) => {
                    const isNew = newRunIds.has(run.run_id);
                    return (
                      <tr
                        key={run.run_id}
                        className={`border-b border-border/50 last:border-0 transition-colors duration-1000 ${
                          isNew ? 'bg-emerald-500/10' : ''
                        }`}
                      >
                        <td className="text-center px-3 py-2">
                          <span className="inline-flex items-center justify-center w-5 h-5 rounded-full bg-emerald-500/20">
                            <svg className="w-3 h-3 text-emerald-400" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={3}>
                              <path strokeLinecap="round" strokeLinejoin="round" d="M5 13l4 4L19 7" />
                            </svg>
                          </span>
                        </td>
                        <td className="px-3 py-2">
                          <span className="font-mono text-xs" title={run.run_id}>
                            {run.run_id.slice(0, 8)}
                          </span>
                        </td>
                        <td className="px-3 py-2">
                          <span className="text-xs font-medium">{run.source_system}</span>
                          {run.snapshot_name && (
                            <span className="text-[10px] text-muted-foreground ml-1">
                              ({run.snapshot_name})
                            </span>
                          )}
                        </td>
                        <td className="text-right px-3 py-2 font-mono text-xs">
                          {run.row_count.toLocaleString()}
                        </td>
                        <td className="px-3 py-2 text-xs text-muted-foreground">
                          {formatTimestamp(run.received_at)}
                        </td>
                        <td className="text-center px-3 py-2">
                          {run.schema_drift ? (
                            <span className="inline-flex items-center gap-1 px-1.5 py-0.5 text-[10px] rounded border bg-amber-500/10 border-amber-500/20 text-amber-400">
                              drift
                            </span>
                          ) : (
                            <span className="text-[10px] text-muted-foreground">-</span>
                          )}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {/* Drift details for runs with drift */}
        {recentRuns.some(r => r.schema_drift) && (
          <div>
            <h3 className="text-xs font-semibold uppercase text-muted-foreground tracking-wide mb-3">
              Schema Drift Detected
            </h3>
            <div className="space-y-2">
              {recentRuns
                .filter(r => r.schema_drift)
                .map(r => (
                  <div key={r.run_id} className="rounded-lg border border-amber-500/20 bg-amber-500/10 p-3">
                    <div className="flex items-center gap-2">
                      <span className="text-sm text-amber-400">~</span>
                      <span className="text-xs font-medium">{r.source_system}</span>
                      <span className="text-[10px] font-mono text-muted-foreground">{r.run_id.slice(0, 8)}</span>
                    </div>
                    {r.drift_fields.length > 0 && (
                      <div className="flex flex-wrap gap-1 mt-2">
                        {r.drift_fields.map(f => (
                          <span key={f} className="px-1.5 py-0.5 text-[10px] rounded bg-card/50 border border-border text-muted-foreground">
                            {f}
                          </span>
                        ))}
                      </div>
                    )}
                  </div>
                ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
