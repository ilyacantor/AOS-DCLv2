import { useEffect, useRef, useState } from 'react';
import { SnapshotSelector, SnapshotState } from './RunSelector';
import { ProposalsPanel } from './ProposalsPanel';

interface ScheduleJob {
  job_name: string;
  interval_seconds: number;
  enabled: boolean;
  last_run_at: string | null;
  last_status: string | null;
  last_detail: string | null;
}

interface MonitoringTabProps {
  snapshot: SnapshotState;
}

function humanInterval(s: number): string {
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.round(s / 60)}m`;
  return `${(s / 3600).toFixed(1)}h`;
}

function humanTime(iso: string | null): string {
  if (!iso) return '—';
  const d = new Date(iso);
  return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

function statusCls(status: string | null): string {
  if (status === 'ok') return 'text-green-400';
  if (status === 'error') return 'text-red-400';
  return 'text-muted-foreground';
}

export function MonitoringTab({ snapshot }: MonitoringTabProps) {
  const { selectedEntityId } = snapshot;
  const [jobs, setJobs] = useState<ScheduleJob[] | null>(null);
  const [loadingJobs, setLoadingJobs] = useState(true);
  const [jobError, setJobError] = useState<string | null>(null);
  // Per-job action state: 'idle' | 'loading'
  const [actionState, setActionState] = useState<Record<string, string>>({});
  // Per-job action error message (shown inline, verbatim from DCL)
  const [actionError, setActionError] = useState<Record<string, string>>({});
  // Per-job interval input value for resume
  const [intervalInput, setIntervalInput] = useState<Record<string, string>>({});
  const pollRef = useRef<number | null>(null);

  const fetchJobs = async () => {
    try {
      const res = await fetch('/api/dcl/monitor/schedule');
      if (!res.ok) {
        setJobError(`GET /api/dcl/monitor/schedule → ${res.status}: ${await res.text()}`);
        setLoadingJobs(false);
        return;
      }
      const body = await res.json();
      setJobs(body.jobs ?? []);
      setJobError(null);
    } catch (e) {
      setJobError(`Network error: ${e}`);
    } finally {
      setLoadingJobs(false);
    }
  };

  useEffect(() => {
    fetchJobs();
    pollRef.current = window.setInterval(fetchJobs, 5000);
    return () => { if (pollRef.current) window.clearInterval(pollRef.current); };
  }, []);

  const doAction = async (jobName: string, action: 'pause' | 'resume') => {
    setActionState(s => ({ ...s, [jobName]: 'loading' }));
    setActionError(s => ({ ...s, [jobName]: '' }));
    try {
      const body: Record<string, unknown> = {};
      if (action === 'resume') {
        const raw = intervalInput[jobName]?.trim();
        if (raw) {
          const parsed = parseInt(raw, 10);
          body.interval_seconds = parsed;
        }
      }
      const res = await fetch(`/api/dcl/monitor/schedule/${jobName}/${action}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      if (!res.ok) {
        const detail = await res.json().then(j => j.detail ?? JSON.stringify(j)).catch(() => res.statusText);
        setActionError(s => ({ ...s, [jobName]: String(detail) }));
      } else {
        await fetchJobs();
      }
    } catch (e) {
      setActionError(s => ({ ...s, [jobName]: `Network error: ${e}` }));
    } finally {
      setActionState(s => ({ ...s, [jobName]: 'idle' }));
    }
  };

  return (
    <div className="h-full flex flex-col p-4 gap-4 overflow-hidden">

      {/* Scheduler Jobs */}
      <div className="shrink-0">
        <div className="flex items-center gap-2 mb-2">
          <h2 className="text-sm font-semibold text-foreground">Drift Monitor Jobs</h2>
          <button
            onClick={fetchJobs}
            className="text-xs text-muted-foreground hover:text-foreground transition-colors"
            title="Refresh"
          >↺</button>
        </div>

        {loadingJobs && (
          <p className="text-xs text-muted-foreground">…</p>
        )}
        {jobError && (
          <p className="text-xs text-red-400" data-testid="schedule-error">{jobError}</p>
        )}
        {jobs && (
          <div className="flex flex-col gap-2">
            {jobs.map(job => {
              const busy = actionState[job.job_name] === 'loading';
              const err = actionError[job.job_name];
              const ivInput = intervalInput[job.job_name] ?? '';
              return (
                <div
                  key={job.job_name}
                  className="flex flex-wrap items-center gap-3 p-3 rounded-lg border border-border bg-card/50"
                  data-testid={`job-row-${job.job_name}`}
                >
                  {/* Name */}
                  <span className="text-sm font-mono text-foreground min-w-[130px]" data-testid={`job-name-${job.job_name}`}>
                    {job.job_name}
                  </span>

                  {/* Enabled badge */}
                  <span
                    className={`px-1.5 py-0.5 rounded text-xs whitespace-nowrap ${job.enabled ? 'bg-green-500/15 text-green-400' : 'bg-muted text-muted-foreground'}`}
                    data-testid={`job-enabled-${job.job_name}`}
                  >
                    {job.enabled ? 'enabled' : 'paused'}
                  </span>

                  {/* Interval */}
                  <span className="text-xs text-muted-foreground" data-testid={`job-interval-${job.job_name}`}>
                    every {humanInterval(job.interval_seconds)}
                  </span>

                  {/* Last run */}
                  <span className="text-xs text-muted-foreground" data-testid={`job-last-run-${job.job_name}`}>
                    last: {job.last_run_at ? humanTime(job.last_run_at) : '—'}
                  </span>

                  {/* Status */}
                  {job.last_status && (
                    <span className={`text-xs ${statusCls(job.last_status)}`} data-testid={`job-status-${job.job_name}`}>
                      {job.last_status}
                    </span>
                  )}

                  {/* Controls */}
                  <div className="flex items-center gap-2 ml-auto">
                    {!job.enabled && (
                      <input
                        type="number"
                        min="1"
                        placeholder="interval (s)"
                        value={ivInput}
                        onChange={e => setIntervalInput(s => ({ ...s, [job.job_name]: e.target.value }))}
                        className="w-24 px-2 py-1 text-xs rounded border border-border bg-background text-foreground"
                        data-testid={`interval-input-${job.job_name}`}
                      />
                    )}
                    {job.enabled ? (
                      <button
                        onClick={() => doAction(job.job_name, 'pause')}
                        disabled={busy}
                        className="px-2 py-1 text-xs rounded bg-muted text-foreground hover:bg-accent disabled:opacity-50 transition-colors"
                        data-testid={`pause-btn-${job.job_name}`}
                      >
                        {busy ? '…' : 'Pause'}
                      </button>
                    ) : (
                      <button
                        onClick={() => doAction(job.job_name, 'resume')}
                        disabled={busy}
                        className="px-2 py-1 text-xs rounded bg-primary text-primary-foreground hover:bg-primary/90 disabled:opacity-50 transition-colors"
                        data-testid={`resume-btn-${job.job_name}`}
                      >
                        {busy ? '…' : 'Resume'}
                      </button>
                    )}
                  </div>

                  {/* Inline action error — DCL detail text verbatim (A1/#77) */}
                  {err && (
                    <p className="w-full text-xs text-red-400 mt-1" data-testid={`job-action-error-${job.job_name}`}>{err}</p>
                  )}
                </div>
              );
            })}
          </div>
        )}
      </div>

      {/* Divider */}
      <div className="shrink-0 border-t border-border" />

      {/* Drift Proposals (existing ProposalsPanel, entity-scoped) */}
      <div className="shrink-0 flex items-center gap-3">
        <SnapshotSelector snapshot={snapshot} />
      </div>
      <div className="flex-1 overflow-auto">
        {selectedEntityId ? (
          <ProposalsPanel entityId={selectedEntityId} />
        ) : (
          <p className="text-xs text-muted-foreground p-2">Select a snapshot to view drift proposals.</p>
        )}
      </div>
    </div>
  );
}
