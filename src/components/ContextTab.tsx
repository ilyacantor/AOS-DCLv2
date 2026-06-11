import { useEffect, useRef, useState } from 'react';
import { SnapshotSelector, SnapshotState } from './RunSelector';

interface DomainInfo {
  domain: string;
  triple_count: number;
  concepts_used: number;
  concepts_available: number;
  source_count: number;
  avg_confidence: number;
}

interface SourceInfo {
  system: string;
  triple_count: number;
  avg_confidence: number;
}

interface ResolutionActivity {
  workspaces_total: number;
  workspaces_pending: number;
  workspaces_resolved: number;
  conflicts_detected: number;
}

interface ContextData {
  domain_coverage: {
    domains_populated: number;
    domains_total: number;
    domains: DomainInfo[];
  };
  confidence_distribution: {
    exact: number;
    high: number;
    medium: number;
    low: number;
  };
  resolution_activity: ResolutionActivity;
  source_system_breakdown: SourceInfo[];
}

interface ConflictClaim {
  source_system: string;
  value?: unknown;
  triple_id?: string;
  confidence_score?: number;
  confidence_tier?: string;
  ingested_at?: string;
  source_table?: string;
  source_field?: string;
  pipe_id?: string;
  row_count?: number;
}

interface ConflictEntry {
  conflict_id: string;
  conflict_type: 'value' | 'structural';
  conflict_class: string;
  concept: string;
  property: string;
  period: string | null;
  status: 'open' | 'dispositioned' | 'escalated';
  claims: ConflictClaim[];
  materiality?: { abs_delta?: number | null; rel_delta?: number | null; material?: boolean };
  recommended?: { action?: string; basis?: string; winner_source?: string | null;
                  precedent?: { decided_by?: string; rationale?: string; winner_source?: string } };
  root_cause_explanation?: string;
  detected_at: string;
}

interface ContextTabProps {
  snapshot: SnapshotState;
}

export function ContextTab({ snapshot }: ContextTabProps) {
  const { selectedEntityId } = snapshot;
  const [data, setData] = useState<ContextData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  // Stale-response guard: the unscoped mount fetch can resolve AFTER a
  // scoped post-selection fetch and overwrite it (entity badge then shows
  // store-wide numbers against a selected entity). Only the latest request
  // may set state.
  const fetchSeq = useRef(0);

  const fetchData = async (entityId?: string) => {
    const seq = ++fetchSeq.current;
    setLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams();
      if (entityId) params.set('entity_id', entityId);
      const qs = params.toString() ? `?${params}` : '';

      const ctxRes = await fetch(`/api/dcl/contextualization-summary${qs}`);
      if (!ctxRes.ok) throw new Error(`Context summary: HTTP ${ctxRes.status}`);
      const body = await ctxRes.json();
      if (seq === fetchSeq.current) setData(body);
    } catch (e) {
      if (seq === fetchSeq.current) setError(e instanceof Error ? e.message : 'Failed to fetch context data');
    } finally {
      if (seq === fetchSeq.current) setLoading(false);
    }
  };

  useEffect(() => { fetchData(selectedEntityId || undefined); }, [selectedEntityId]);

  const handlePurgeStale = async () => {
    if (!window.confirm('Delete all stale-run triples across all tenants? Current run data is preserved.')) return;
    try {
      const res = await fetch('/api/dcl/admin/purge-stale', { method: 'POST' });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const d = await res.json();
      alert(`Purged ${d.deleted.toLocaleString()} stale triples across ${d.tenants_purged} tenant(s).`);
      fetchData(selectedEntityId || undefined);
    } catch (e) {
      console.error('[ContextTab] Purge stale failed:', e);
      alert('Purge failed — check console.');
    }
  };

  if (error) {
    return (
      <div className="h-full flex items-center justify-center">
        <div className="text-center p-6 rounded-lg border border-destructive/30 bg-destructive/5 max-w-md">
          <p className="text-sm text-destructive font-medium">{error}</p>
          <button onClick={() => fetchData(selectedEntityId || undefined)} className="mt-3 px-3 py-1 text-xs rounded bg-primary text-primary-foreground">Retry</button>
        </div>
      </div>
    );
  }

  const conf = data?.confidence_distribution ?? { exact: 0, high: 0, medium: 0, low: 0 };
  const confTotal = conf.exact + conf.high + conf.medium + conf.low;
  const resolution = data?.resolution_activity ?? { workspaces_total: 0, workspaces_pending: 0, workspaces_resolved: 0, conflicts_detected: 0 };
  const showResolution = !selectedEntityId && resolution.workspaces_total > 0;

  return (
    <div className="h-full flex flex-col p-4 gap-3 overflow-hidden">
      {/* Entity selector bar */}
      <div className="shrink-0 flex items-center gap-3">
        <SnapshotSelector snapshot={snapshot} />
        <div className="ml-auto flex items-center gap-2">
          <button
            onClick={handlePurgeStale}
            disabled={loading}
            className="px-2 py-1 text-xs rounded border border-red-500/30 bg-red-500/10 text-red-400 hover:bg-red-500/20 disabled:opacity-50"
          >
            Purge Stale
          </button>
          <button
            onClick={() => fetchData(selectedEntityId || undefined)}
            disabled={loading}
            className="px-2 py-1 text-xs rounded border border-border hover:bg-accent disabled:opacity-50"
          >
            {loading ? '...' : 'Refresh'}
          </button>
        </div>
      </div>

      {/* Top metric row */}
      <div className={`shrink-0 grid gap-3 ${showResolution ? 'grid-cols-4' : 'grid-cols-3'}`}>
        <MetricCard
          label="Domain Coverage"
          value={`${data?.domain_coverage.domains_populated ?? 0} / ${data?.domain_coverage.domains_total ?? 0}`}
          detail="Ontology concepts with data"
        />
        <MetricCard
          label="Triples"
          value={confTotal.toLocaleString()}
          detail={`E:${conf.exact} H:${conf.high} M:${conf.medium} L:${conf.low}`}
        />
        {showResolution && (
          <MetricCard
            label="Resolution"
            value={`${resolution.workspaces_resolved} / ${resolution.workspaces_total}`}
            detail={`${resolution.workspaces_pending} pending`}
          />
        )}
        <MetricCard
          label="Source Systems"
          value={String(data?.source_system_breakdown.length ?? 0)}
          detail="Distinct systems"
        />
      </div>

      {/* Conflict Register drill (Gate 1A) — entity-scoped operator surface */}
      {selectedEntityId && (
        <ConflictsPanel
          entityId={selectedEntityId}
          openCount={resolution.conflicts_detected}
          onDispositioned={() => fetchData(selectedEntityId || undefined)}
        />
      )}

      {/* Two side-by-side panels */}
      <div className="flex-1 min-h-0 flex gap-3">
        {/* Left: Domain coverage table */}
        <div className="flex-[3] min-w-0 border rounded overflow-hidden flex flex-col">
          <div className="shrink-0 bg-muted/50 px-3 py-1.5 border-b text-xs font-medium">
            Domain Coverage
          </div>
          <div className="shrink-0 bg-muted/50">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b">
                  <th className="text-left px-3 py-1.5 font-medium">Domain</th>
                  <th className="text-right px-3 py-1.5 font-medium">Triples</th>
                  <th className="text-right px-3 py-1.5 font-medium">Concepts</th>
                  <th className="text-right px-3 py-1.5 font-medium">Sources</th>
                  <th className="text-right px-3 py-1.5 font-medium">Avg Conf</th>
                </tr>
              </thead>
            </table>
          </div>
          <div className="flex-1 overflow-y-auto">
            <table className="w-full text-xs">
              <tbody>
                {(data?.domain_coverage.domains ?? [])
                  .sort((a, b) => (a.triple_count === 0 ? -1 : b.triple_count === 0 ? 1 : a.triple_count - b.triple_count))
                  .map((d) => (
                    <tr
                      key={d.domain}
                      className={`border-b ${d.triple_count === 0 ? 'bg-destructive/5 text-muted-foreground' : 'hover:bg-accent/50'}`}
                    >
                      <td className="px-3 py-1.5 font-medium">{formatDomain(d.domain)}</td>
                      <td className="px-3 py-1.5 text-right">{d.triple_count.toLocaleString()}</td>
                      <td className="px-3 py-1.5 text-right">{d.concepts_used} / {d.concepts_available}</td>
                      <td className="px-3 py-1.5 text-right">{d.source_count}</td>
                      <td className="px-3 py-1.5 text-right">{d.avg_confidence.toFixed(2)}</td>
                    </tr>
                  ))}
                {data && data.domain_coverage.domains.length === 0 && (
                  <tr><td colSpan={5} className="px-3 py-6 text-center text-muted-foreground">No domain data</td></tr>
                )}
              </tbody>
            </table>
          </div>
        </div>

        {/* Right: Source system breakdown + confidence bar */}
        <div className="flex-[2] min-w-0 flex flex-col gap-3">
          {/* Confidence distribution bar */}
          <div className="shrink-0 border rounded px-3 py-2">
            <div className="text-xs font-medium mb-1.5">Confidence Distribution</div>
            <ConfidenceBar distribution={conf} total={confTotal} />
            <div className="flex justify-between text-xs text-muted-foreground mt-1">
              <span>Exact: {conf.exact}</span>
              <span>High: {conf.high}</span>
              <span>Med: {conf.medium}</span>
              <span>Low: {conf.low}</span>
            </div>
          </div>

          {/* Source system table */}
          <div className="flex-1 min-h-0 border rounded overflow-hidden flex flex-col">
            <div className="shrink-0 bg-muted/50 px-3 py-1.5 border-b text-xs font-medium">
              Source Systems
            </div>
            <div className="shrink-0 bg-muted/50">
              <table className="w-full text-xs">
                <thead>
                  <tr className="border-b">
                    <th className="text-left px-3 py-1.5 font-medium">System</th>
                    <th className="text-right px-3 py-1.5 font-medium">Triples</th>
                    <th className="text-right px-3 py-1.5 font-medium">Avg Conf</th>
                  </tr>
                </thead>
              </table>
            </div>
            <div className="flex-1 overflow-y-auto">
              <table className="w-full text-xs">
                <tbody>
                  {(data?.source_system_breakdown ?? []).map((s) => (
                    <tr key={s.system} className="border-b hover:bg-accent/50">
                      <td className="px-3 py-1.5 font-medium">{s.system}</td>
                      <td className="px-3 py-1.5 text-right">{s.triple_count.toLocaleString()}</td>
                      <td className="px-3 py-1.5 text-right">{s.avg_confidence.toFixed(2)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

function ConflictsPanel({ entityId, openCount, onDispositioned }:
  { entityId: string; openCount: number; onDispositioned: () => void }) {
  const [open, setOpen] = useState(false);
  const [entries, setEntries] = useState<ConflictEntry[]>([]);
  const [expanded, setExpanded] = useState<string | null>(null);
  const [showAll, setShowAll] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [rationale, setRationale] = useState('');
  const [decidedBy, setDecidedBy] = useState('operator');
  const [manualWinner, setManualWinner] = useState('');

  const fetchRegister = async (all: boolean) => {
    setErr(null);
    try {
      const params = new URLSearchParams({ entity_id: entityId, limit: '100' });
      if (!all) params.set('status', 'open');
      const res = await fetch(`/api/dcl/conflicts?${params}`);
      if (!res.ok) throw new Error(`Conflict register: HTTP ${res.status} — ${(await res.json()).detail ?? ''}`);
      const body = await res.json();
      setEntries(body.conflicts);
    } catch (e) {
      setErr(e instanceof Error ? e.message : 'Failed to load conflict register');
    }
  };

  useEffect(() => {
    if (open) fetchRegister(showAll);
    setExpanded(null);
  }, [open, showAll, entityId]);

  const disposition = async (entry: ConflictEntry, action: string, winner?: string) => {
    setBusy(true);
    setErr(null);
    try {
      const res = await fetch(`/api/dcl/conflicts/${entry.conflict_id}/disposition`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          action, decided_by: decidedBy.trim(), rationale: rationale.trim(),
          winner_source: winner ?? null, entity_id: entityId,
        }),
      });
      const body = await res.json();
      if (!res.ok) throw new Error(`Disposition failed: HTTP ${res.status} — ${body.detail ?? ''}`);
      setRationale('');
      setManualWinner('');
      await fetchRegister(showAll);
      onDispositioned();
    } catch (e) {
      setErr(e instanceof Error ? e.message : 'Disposition failed');
    } finally {
      setBusy(false);
    }
  };

  const fmtVal = (v: unknown) => (typeof v === 'number' ? v.toLocaleString() : String(v ?? '—'));

  return (
    <div className="shrink-0 border rounded overflow-hidden" data-testid="conflicts-panel">
      <button
        onClick={() => setOpen(!open)}
        data-testid="conflicts-toggle"
        className="w-full flex items-center justify-between px-3 py-2 bg-muted/50 hover:bg-accent text-xs font-medium"
      >
        <span>
          Conflict Register
          <span className={`ml-2 px-1.5 py-0.5 rounded ${openCount > 0 ? 'bg-amber-500/20 text-amber-500' : 'bg-muted text-muted-foreground'}`}>
            {openCount} open
          </span>
        </span>
        <span className="text-muted-foreground">{open ? 'Hide' : 'Drill'}</span>
      </button>

      {open && (
        <div className="max-h-80 overflow-y-auto border-t">
          <div className="flex items-center gap-3 px-3 py-1.5 border-b bg-card/50 text-xs">
            <label className="flex items-center gap-1.5">
              <input type="checkbox" checked={showAll} onChange={(e) => setShowAll(e.target.checked)} />
              Include dispositioned
            </label>
            <span className="ml-auto text-muted-foreground">{entries.length} shown</span>
          </div>
          {err && <div className="px-3 py-2 text-xs text-destructive" data-testid="conflicts-error">{err}</div>}
          {entries.length === 0 && !err && (
            <div className="px-3 py-4 text-xs text-center text-muted-foreground">No conflicts on the register for this entity.</div>
          )}
          {entries.map((c) => (
            <div key={c.conflict_id} className="border-b" data-testid={`conflict-row-${c.concept}-${c.period ?? ''}`}>
              <button
                onClick={() => setExpanded(expanded === c.conflict_id ? null : c.conflict_id)}
                className="w-full grid grid-cols-[70px_1fr_auto_auto] items-center gap-2 px-3 py-1.5 text-xs hover:bg-accent/50 text-left"
              >
                <span className={`px-1.5 py-0.5 rounded text-center ${c.conflict_type === 'value' ? 'bg-red-500/15 text-red-400' : 'bg-blue-500/15 text-blue-400'}`}>
                  {c.conflict_type}
                </span>
                <span className="font-medium truncate">
                  {c.concept}.{c.property}{c.period ? ` · ${c.period}` : ''}
                  <span className="ml-2 text-muted-foreground font-normal">
                    {c.claims.map((cl) => `${cl.source_system}${cl.value !== undefined ? `=${fmtVal(cl.value)}` : ''}`).join(' vs ')}
                  </span>
                </span>
                {c.materiality?.rel_delta != null && (
                  <span className="text-muted-foreground">Δ {(c.materiality.rel_delta * 100).toFixed(1)}%</span>
                )}
                <span className={`px-1.5 py-0.5 rounded ${c.status === 'open' ? 'bg-amber-500/15 text-amber-500' : c.status === 'escalated' ? 'bg-purple-500/15 text-purple-400' : 'bg-green-500/15 text-green-500'}`}>
                  {c.status}
                </span>
              </button>

              {expanded === c.conflict_id && (
                <div className="px-3 py-2 bg-card/30 border-t text-xs space-y-2" data-testid="conflict-detail">
                  {c.recommended?.basis === 'precedent' && c.recommended.precedent && (
                    <div className="px-2 py-1.5 rounded bg-blue-500/10 border border-blue-500/30" data-testid="precedent-banner">
                      Precedent: <b>{c.recommended.precedent.winner_source}</b> accepted by {c.recommended.precedent.decided_by} — “{c.recommended.precedent.rationale}”.
                      Proposed: <b>{c.recommended.action}</b> (HITL decides).
                    </div>
                  )}
                  {c.recommended?.basis === 'authority' && (
                    <div className="px-2 py-1.5 rounded bg-muted/40">
                      Authority map proposes <b>{c.recommended.winner_source}</b> ({c.recommended.action}).
                    </div>
                  )}
                  <table className="w-full">
                    <thead><tr className="text-muted-foreground border-b">
                      <th className="text-left py-1">Source</th><th className="text-left">Value</th>
                      <th className="text-left">Conf</th><th className="text-left">Ingested</th>
                      <th className="text-left">Field</th><th className="text-left">Triple</th>
                    </tr></thead>
                    <tbody>
                      {c.claims.map((cl) => (
                        <tr key={cl.source_system} className="border-b border-border/40">
                          <td className="py-1 font-medium">{cl.source_system}</td>
                          <td>{cl.row_count != null ? `${cl.row_count} rows` : fmtVal(cl.value)}</td>
                          <td>{cl.confidence_score != null ? `${cl.confidence_score} (${cl.confidence_tier})` : '—'}</td>
                          <td>{cl.ingested_at ? new Date(cl.ingested_at).toLocaleString() : '—'}</td>
                          <td>{cl.source_table ?? '—'}.{cl.source_field ?? '—'}</td>
                          <td className="font-mono">{cl.triple_id ? cl.triple_id.slice(0, 8) : '—'}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                  {c.root_cause_explanation && (
                    <div className="text-muted-foreground">{c.root_cause_explanation}</div>
                  )}
                  {c.status === 'open' || c.status === 'escalated' ? (
                    <div className="space-y-1.5 pt-1 border-t border-border/40">
                      <div className="flex gap-2">
                        <input
                          value={decidedBy} onChange={(e) => setDecidedBy(e.target.value)}
                          placeholder="Decided by" data-testid="decided-by"
                          className="w-32 px-2 py-1 rounded border bg-background"
                        />
                        <input
                          value={rationale} onChange={(e) => setRationale(e.target.value)}
                          placeholder="Rationale (required — this is the decision trace)"
                          data-testid="rationale"
                          className="flex-1 px-2 py-1 rounded border bg-background"
                        />
                      </div>
                      <div className="flex gap-2 items-center">
                        {c.claims.slice(0, 2).map((cl, i) => (
                          <button
                            key={cl.source_system}
                            disabled={busy || !rationale.trim() || !decidedBy.trim()}
                            onClick={() => disposition(c, i === 0 ? 'accept_a' : 'accept_b')}
                            data-testid={`accept-${cl.source_system}`}
                            className="px-2 py-1 rounded bg-green-600/20 text-green-400 border border-green-600/40 hover:bg-green-600/30 disabled:opacity-40"
                          >
                            Accept {cl.source_system}
                          </button>
                        ))}
                        <button
                          disabled={busy || !rationale.trim() || !decidedBy.trim()}
                          onClick={() => disposition(c, 'escalate')}
                          data-testid="escalate"
                          className="px-2 py-1 rounded bg-purple-600/20 text-purple-400 border border-purple-600/40 hover:bg-purple-600/30 disabled:opacity-40"
                        >
                          Escalate
                        </button>
                        {c.claims.length > 2 && (
                          <>
                            <select
                              value={manualWinner} onChange={(e) => setManualWinner(e.target.value)}
                              className="px-2 py-1 rounded border bg-background"
                            >
                              <option value="">Manual winner…</option>
                              {c.claims.map((cl) => (
                                <option key={cl.source_system} value={cl.source_system}>{cl.source_system}</option>
                              ))}
                            </select>
                            <button
                              disabled={busy || !rationale.trim() || !decidedBy.trim() || !manualWinner}
                              onClick={() => disposition(c, 'manual', manualWinner)}
                              className="px-2 py-1 rounded bg-muted border hover:bg-accent disabled:opacity-40"
                            >
                              Accept manual
                            </button>
                          </>
                        )}
                      </div>
                    </div>
                  ) : (
                    <div className="text-green-500" data-testid="dispositioned-note">
                      Dispositioned — losing claims superseded (still visible via as-of reads).
                    </div>
                  )}
                </div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function MetricCard({ label, value, detail }: { label: string; value: string; detail?: string }) {
  return (
    <div className="rounded border bg-card/50 px-3 py-2">
      <div className="text-xs text-muted-foreground">{label}</div>
      <div className="text-lg font-semibold mt-0.5">{value}</div>
      {detail && <div className="text-xs text-muted-foreground mt-0.5">{detail}</div>}
    </div>
  );
}

function ConfidenceBar({ distribution, total }: { distribution: { exact: number; high: number; medium: number; low: number }; total: number }) {
  if (total === 0) return <div className="h-3 rounded bg-muted" />;
  const pct = (n: number) => `${((n / total) * 100).toFixed(1)}%`;
  return (
    <div className="h-3 rounded overflow-hidden flex">
      {distribution.exact > 0 && <div className="bg-blue-500" style={{ width: pct(distribution.exact) }} />}
      {distribution.high > 0 && <div className="bg-green-500" style={{ width: pct(distribution.high) }} />}
      {distribution.medium > 0 && <div className="bg-yellow-500" style={{ width: pct(distribution.medium) }} />}
      {distribution.low > 0 && <div className="bg-red-500" style={{ width: pct(distribution.low) }} />}
    </div>
  );
}

function formatDomain(d: string): string {
  return d.replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase());
}
