import { useState, useEffect, useCallback, Fragment } from 'react';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface OverviewData {
  total_triples: number;
  active_triples: number;
  entities: { entity_id: string; triple_count: number; display_name: string }[];
  domains: { domain: string; count: number; by_entity: Record<string, number> }[];
  periods: string[];
  last_ingest: { run_id: string; timestamp: string; triple_count: number } | null;
}

interface RunData {
  run_id: string;
  tenant_id?: string;
  tenant_label?: string;
  timestamp: string;
  triple_count: number;
  is_active: boolean;
  domain_summary: Record<string, number>;
  entity_summary: Record<string, number>;
}

interface TripleRow {
  id: string;
  entity_id: string;
  concept: string;
  property: string;
  value: unknown;
  period: string;
  source_system: string;
  source_table: string;
  source_field: string;
  pipe_id: string;
  run_id: string;
  confidence_score: number;
  confidence_tier: string;
  canonical_id: string;
  resolution_method: string;
  resolution_confidence: number;
  currency: string;
  unit: string;
  created_at: string;
  tenant_id: string;
}

interface BrowseData {
  triples: TripleRow[];
  total_count: number;
  filters_applied: Record<string, string>;
}

const POLL_INTERVAL_MS = 5000;

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export function TriplesPanel() {
  const [overview, setOverview] = useState<OverviewData | null>(null);
  const [runs, setRuns] = useState<RunData[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [autoRefresh, setAutoRefresh] = useState(false);

  // Runs expand
  const [expandedRun, setExpandedRun] = useState<string | null>(null);
  const [runsOpen, setRunsOpen] = useState(false);

  // Triple browser state
  const [browseData, setBrowseData] = useState<BrowseData | null>(null);
  const [browseLoading, setBrowseLoading] = useState(false);
  // R3: triples/browse is tenant-scoped — browseTenant carries the required
  // tenant_id. Defaulted from the runs list once it loads.
  const [browseTenant, setBrowseTenant] = useState('');
  const [browseDomain, setBrowseDomain] = useState('');
  const [browseEntity, setBrowseEntity] = useState('');
  const [browsePeriod, setBrowsePeriod] = useState('');
  const [browseProperty, setBrowseProperty] = useState('');
  const [browseOffset, setBrowseOffset] = useState(0);
  const [expandedTriple, setExpandedTriple] = useState<string | null>(null);
  const BROWSE_LIMIT = 50;

  // --- Data fetching ---

  const fetchOverview = async () => {
    try {
      const res = await fetch('/api/dcl/triples/overview');
      if (!res.ok) throw new Error(`HTTP ${res.status}: ${(await res.json().catch(() => ({}))).detail || res.statusText}`);
      setOverview(await res.json());
      setError(null);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Failed to fetch triple overview');
    } finally {
      setLoading(false);
    }
  };

  const fetchRuns = async () => {
    try {
      const res = await fetch('/api/dcl/triples/runs');
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      setRuns(data.runs ?? []);
    } catch (e) {
      console.error('[TriplesPanel] Failed to fetch runs:', e);
    }
  };

  const fetchBrowse = useCallback(async (offset = 0) => {
    // R3: /api/dcl/triples/browse requires tenant_id and 422s without it.
    // browseTenant is empty until the runs list resolves a default — skip
    // the call until then rather than fire a guaranteed 422.
    if (!browseTenant) return;
    setBrowseLoading(true);
    try {
      const params = new URLSearchParams();
      params.set('tenant_id', browseTenant);
      if (browseDomain) params.set('domain', browseDomain);
      if (browseEntity) params.set('entity_id', browseEntity);
      if (browsePeriod) params.set('period', browsePeriod);
      if (browseProperty) params.set('property', browseProperty);
      params.set('limit', String(BROWSE_LIMIT));
      params.set('offset', String(offset));
      const res = await fetch(`/api/dcl/triples/browse?${params}`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      setBrowseData(await res.json());
      setBrowseOffset(offset);
    } catch (e) {
      console.error('[TriplesPanel] Failed to fetch triples:', e);
    } finally {
      setBrowseLoading(false);
    }
  }, [browseTenant, browseDomain, browseEntity, browsePeriod, browseProperty]);

  const fetchAll = useCallback(() => {
    fetchOverview();
    fetchRuns();
  }, []);

  useEffect(() => {
    fetchAll();
  }, [fetchAll]);

  useEffect(() => {
    if (!autoRefresh) return;
    const interval = setInterval(fetchAll, POLL_INTERVAL_MS);
    return () => clearInterval(interval);
  }, [autoRefresh, fetchAll]);

  // R3: once runs load, default the browse tenant to the active run's
  // tenant (else the first run carrying one). browse stays idle until set.
  useEffect(() => {
    if (browseTenant || runs.length === 0) return;
    const tid =
      runs.find((r) => r.is_active && r.tenant_id)?.tenant_id ??
      runs.find((r) => r.tenant_id)?.tenant_id;
    if (tid) setBrowseTenant(tid);
  }, [runs, browseTenant]);

  // Auto-load browse when filters change
  useEffect(() => {
    fetchBrowse(0);
  }, [fetchBrowse]);

  const handleDeactivateRun = async (runId: string) => {
    if (!window.confirm(`Deactivate all triples for run ${runId.slice(0, 8)}...? This marks them inactive.`)) return;
    try {
      const res = await fetch(`/api/dcl/triples/deactivate-run?run_id=${encodeURIComponent(runId)}`, { method: 'POST' });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      fetchAll();
    } catch (e) {
      console.error('[TriplesPanel] Deactivate failed:', e);
    }
  };

  // --- Helpers ---

  const fmtDate = (ts: string) => {
    try {
      return new Date(ts).toLocaleString('en-US', {
        month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', hour12: true,
      });
    } catch { return ts; }
  };

  const fmtNum = (n: number) => n.toLocaleString();

  const pct = (n: number, total: number) => total > 0 ? `${((n / total) * 100).toFixed(1)}%` : '-';

  const shortId = (id: string) => id ? id.slice(0, 8) : '-';

  const confidenceBadge = (tier: string) => {
    const cls = tier === 'exact' || tier === 'high'
      ? 'bg-emerald-500/20 text-emerald-400 border-emerald-500/30'
      : tier === 'medium'
        ? 'bg-amber-500/20 text-amber-400 border-amber-500/30'
        : 'bg-red-500/20 text-red-400 border-red-500/30';
    return (
      <span className={`inline-block px-1.5 py-0.5 rounded text-[11px] font-semibold border ${cls}`}>
        {tier}
      </span>
    );
  };

  const fmtValue = (val: unknown): string => {
    if (val === null || val === undefined) return '-';
    if (typeof val === 'number') return val.toLocaleString();
    if (typeof val === 'string') {
      // Try parsing as number
      const n = Number(val);
      if (!isNaN(n) && val.trim() !== '') return n.toLocaleString();
      return val;
    }
    return JSON.stringify(val);
  };

  // Distinct (tenant_id, label) pairs from the runs list — the tenant
  // selector options. browse must always carry one of these (R3).
  const tenantOptions: [string, string][] = Array.from(
    new Map(
      runs
        .filter((r) => r.tenant_id)
        .map((r) => [r.tenant_id as string, r.tenant_label || (r.tenant_id as string)]),
    ).entries(),
  );

  // --- Render ---

  if (loading) {
    return (
      <div className="h-full flex flex-col min-h-0">
        <div className="shrink-0 flex items-center justify-between px-6 py-3 border-b border-border bg-card/50">
          <h2 className="text-base font-semibold">Triple Store</h2>
        </div>
        <div className="flex-1 flex items-center justify-center text-muted-foreground">
          <div className="flex flex-col items-center gap-3">
            <div className="w-8 h-8 border-2 border-primary border-t-transparent rounded-full animate-spin" />
            <span className="text-base">Loading triple store data...</span>
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
          <h2 className="text-base font-semibold">Triple Store</h2>
          <div className="flex items-center gap-2">
            <label className="flex items-center gap-1.5 text-sm text-muted-foreground cursor-pointer">
              <input
                type="checkbox"
                checked={autoRefresh}
                onChange={(e) => setAutoRefresh(e.target.checked)}
                className="rounded border-border"
              />
              Auto-refresh {POLL_INTERVAL_MS / 1000}s
            </label>
            <button
              onClick={fetchAll}
              className="px-3 py-1 text-sm rounded bg-primary text-primary-foreground hover:bg-primary/90"
            >
              Refresh
            </button>
          </div>
        </div>
      </div>

      <div className="flex-1 overflow-y-auto p-4 space-y-4 min-h-0">
        {error && (
          <div className="rounded-lg border border-red-500/20 bg-red-500/10 p-3 text-center">
            <span className="text-base text-red-400">{error}</span>
            <button onClick={fetchAll} className="ml-3 px-3 py-1 text-sm rounded bg-primary text-primary-foreground hover:bg-primary/90">
              Retry
            </button>
          </div>
        )}

        {/* ================================================================
            Section 1: Overview Bar
            ================================================================ */}
        {overview && (
          <div className="rounded-lg border border-border bg-card/30 px-4 py-3">
            <div className="flex items-center gap-6 text-sm font-mono flex-wrap">
              <span>
                <span className="text-emerald-400 font-semibold text-lg">{fmtNum(overview.total_triples)}</span>
                <span className="text-muted-foreground ml-1">triples</span>
              </span>
              <span>
                <span className="text-foreground font-semibold">{overview.entities.map(e => e.display_name).join(' / ')}</span>
              </span>
              <span>
                <span className="text-foreground font-semibold">{overview.domains.length}</span>
                <span className="text-muted-foreground ml-1">domains</span>
              </span>
              <span>
                <span className="text-foreground font-semibold">{overview.periods.length}</span>
                <span className="text-muted-foreground ml-1">periods</span>
              </span>
              {overview.last_ingest && (
                <>
                  <span>
                    <span className="text-muted-foreground">last ingest </span>
                    <span className="text-foreground">{fmtDate(overview.last_ingest.timestamp)}</span>
                  </span>
                  <span>
                    <span className="text-muted-foreground">run </span>
                    <span className="text-muted-foreground/70">{shortId(overview.last_ingest.run_id)}</span>
                  </span>
                </>
              )}
            </div>
          </div>
        )}

        {/* ================================================================
            Section 2: Domain Breakdown
            ================================================================ */}
        {overview && overview.domains.length > 0 && (
          <div className="rounded-lg border border-border bg-card/30 overflow-hidden">
            <div className="px-4 py-2.5 border-b border-border/50">
              <h3 className="text-sm font-semibold uppercase tracking-wider text-muted-foreground">Domain Breakdown</h3>
            </div>
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border text-sm uppercase tracking-wider text-muted-foreground">
                  <th className="text-left px-3 py-2 font-medium">Domain</th>
                  <th className="text-right px-3 py-2 font-medium">Count</th>
                  {overview.entities.map((e) => (
                    <th key={e.entity_id} className="text-right px-3 py-2 font-medium">{e.display_name}</th>
                  ))}
                  <th className="text-right px-3 py-2 font-medium">% of Total</th>
                </tr>
              </thead>
              <tbody>
                {overview.domains.map((d) => (
                  <tr
                    key={d.domain}
                    className="border-t border-border/30 hover:bg-card/20 cursor-pointer transition-colors"
                    onClick={() => {
                      setBrowseDomain(d.domain);
                      setBrowseEntity('');
                      setBrowsePeriod('');
                      setBrowseOffset(0);
                    }}
                  >
                    <td className="px-3 py-1.5 font-mono font-semibold text-foreground">{d.domain}</td>
                    <td className="px-3 py-1.5 text-right font-mono text-foreground">{fmtNum(d.count)}</td>
                    {overview.entities.map((e) => (
                      <td key={e.entity_id} className="px-3 py-1.5 text-right font-mono text-muted-foreground">
                        {d.by_entity[e.entity_id] != null ? fmtNum(d.by_entity[e.entity_id]) : '0'}
                      </td>
                    ))}
                    <td className="px-3 py-1.5 text-right font-mono text-muted-foreground">
                      {pct(d.count, overview.total_triples)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {/* ================================================================
            Section 4: Ingest Runs
            ================================================================ */}
        <div className="rounded-lg border border-border bg-card/30 overflow-hidden">
          <button
            onClick={() => setRunsOpen(!runsOpen)}
            className="w-full flex items-center gap-2 px-4 py-2.5 text-sm hover:bg-card/20 transition-colors"
          >
            <svg
              className={`w-2.5 h-2.5 shrink-0 transition-transform duration-150 text-muted-foreground ${runsOpen ? 'rotate-90' : ''}`}
              fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}
            >
              <path strokeLinecap="round" strokeLinejoin="round" d="M9 5l7 7-7 7" />
            </svg>
            <span className="font-semibold uppercase tracking-wider text-muted-foreground text-sm">Ingest Runs</span>
            <span className="text-muted-foreground/70 font-mono">{runs.length}</span>
          </button>
          {runsOpen && (
            <table className="w-full text-sm">
              <thead>
                <tr className="border-t border-border text-sm uppercase tracking-wider text-muted-foreground">
                  <th className="text-left px-3 py-2 font-medium">Tenant</th>
                  <th className="text-left px-3 py-2 font-medium">Run ID</th>
                  <th className="text-left px-3 py-2 font-medium">Timestamp</th>
                  <th className="text-right px-3 py-2 font-medium">Triples</th>
                  <th className="text-center px-3 py-2 font-medium">Active</th>
                  <th className="text-right px-3 py-2 font-medium">Domains</th>
                  <th className="text-right px-3 py-2 font-medium">Action</th>
                </tr>
              </thead>
              <tbody>
                {runs.map((run) => {
                  const isExpanded = expandedRun === run.run_id;
                  return (
                    <Fragment key={run.run_id}>
                      <tr
                        className="border-t border-border/30 hover:bg-card/20 cursor-pointer transition-colors"
                        onClick={() => setExpandedRun(isExpanded ? null : run.run_id)}
                      >
                        <td className="px-3 py-1.5 text-foreground font-medium">{Object.keys(run.entity_summary).join(' / ') || '-'}</td>
                        <td className="px-3 py-1.5 font-mono text-foreground/80">{shortId(run.run_id)}</td>
                        <td className="px-3 py-1.5 text-muted-foreground">{run.timestamp ? fmtDate(run.timestamp) : '-'}</td>
                        <td className="px-3 py-1.5 text-right font-mono text-foreground">{fmtNum(run.triple_count)}</td>
                        <td className="px-3 py-1.5 text-center">
                          {run.is_active
                            ? <span className="text-emerald-400">&#10003;</span>
                            : <span className="text-muted-foreground/40">-</span>
                          }
                        </td>
                        <td className="px-3 py-1.5 text-right font-mono text-muted-foreground">
                          {Object.keys(run.domain_summary).length}
                        </td>
                        <td className="px-3 py-1.5 text-right">
                          {run.is_active && (
                            <button
                              onClick={(e) => { e.stopPropagation(); handleDeactivateRun(run.run_id); }}
                              className="px-2 py-0.5 text-xs rounded bg-red-500/20 text-red-400 hover:bg-red-500/30 border border-red-500/30"
                            >
                              Deactivate
                            </button>
                          )}
                        </td>
                      </tr>
                      {isExpanded && (
                        <tr className="border-t border-border/10 bg-card/5">
                          <td colSpan={6} className="px-3 py-2 pl-8">
                            <div className="flex flex-wrap gap-x-4 gap-y-1 text-sm font-mono">
                              {Object.entries(run.domain_summary)
                                .sort(([, a], [, b]) => b - a)
                                .map(([domain, count]) => (
                                  <span key={domain}>
                                    <span className="text-muted-foreground/60">{domain} </span>
                                    <span className="text-foreground/80">{fmtNum(count)}</span>
                                  </span>
                                ))
                              }
                            </div>
                            {Object.keys(run.entity_summary).length > 0 && (
                              <div className="flex flex-wrap gap-x-4 gap-y-1 text-sm font-mono mt-1">
                                {Object.entries(run.entity_summary).map(([eid, count]) => (
                                  <span key={eid}>
                                    <span className="text-muted-foreground/60">{eid} </span>
                                    <span className="text-foreground/80">{fmtNum(count)}</span>
                                  </span>
                                ))}
                              </div>
                            )}
                          </td>
                        </tr>
                      )}
                    </Fragment>
                  );
                })}
              </tbody>
            </table>
          )}
        </div>

        {/* ================================================================
            Section 5: Triple Browser
            ================================================================ */}
        <div className="rounded-lg border border-border bg-card/30 overflow-hidden">
          <div className="px-4 py-2.5 border-b border-border/50">
            <h3 className="text-sm font-semibold uppercase tracking-wider text-muted-foreground">Triple Browser</h3>
          </div>

          {/* Filters */}
          <div className="flex items-center gap-3 px-4 py-2 border-b border-border/30 flex-wrap">
            <div className="flex items-center gap-1">
              <span className="text-xs text-muted-foreground">Tenant:</span>
              <select
                value={browseTenant}
                onChange={(e) => { setBrowseTenant(e.target.value); setBrowseOffset(0); }}
                className="px-2 py-1 text-xs rounded border border-border bg-background"
              >
                {tenantOptions.length === 0 && <option value="">(no runs)</option>}
                {tenantOptions.map(([tid, label]) => (
                  <option key={tid} value={tid}>{label}</option>
                ))}
              </select>
            </div>
            <div className="flex items-center gap-1">
              <span className="text-xs text-muted-foreground">Domain:</span>
              <select
                value={browseDomain}
                onChange={(e) => { setBrowseDomain(e.target.value); setBrowseOffset(0); }}
                className="px-2 py-1 text-xs rounded border border-border bg-background"
              >
                <option value="">All</option>
                {(overview?.domains ?? []).map((d) => (
                  <option key={d.domain} value={d.domain}>{d.domain}</option>
                ))}
              </select>
            </div>
            <div className="flex items-center gap-1">
              <span className="text-xs text-muted-foreground">Entity:</span>
              <select
                value={browseEntity}
                onChange={(e) => { setBrowseEntity(e.target.value); setBrowseOffset(0); }}
                className="px-2 py-1 text-xs rounded border border-border bg-background"
              >
                <option value="">All</option>
                {(overview?.entities ?? []).map((e) => (
                  <option key={e.entity_id} value={e.entity_id}>{e.display_name}</option>
                ))}
              </select>
            </div>
            <div className="flex items-center gap-1">
              <span className="text-xs text-muted-foreground">Period:</span>
              <select
                value={browsePeriod}
                onChange={(e) => { setBrowsePeriod(e.target.value); setBrowseOffset(0); }}
                className="px-2 py-1 text-xs rounded border border-border bg-background"
              >
                <option value="">All</option>
                {(overview?.periods ?? []).map((p) => (
                  <option key={p} value={p}>{p}</option>
                ))}
              </select>
            </div>
            <div className="flex items-center gap-1">
              <span className="text-xs text-muted-foreground">Property:</span>
              <input
                type="text"
                value={browseProperty}
                onChange={(e) => { setBrowseProperty(e.target.value); setBrowseOffset(0); }}
                placeholder="e.g. amount"
                className="px-2 py-1 text-xs rounded border border-border bg-background w-24"
              />
            </div>
            {browseData && (
              <span className="text-xs text-muted-foreground ml-auto font-mono">
                {fmtNum(browseData.total_count)} results
              </span>
            )}
          </div>

          {/* Table */}
          {browseLoading ? (
            <div className="p-4 text-center text-muted-foreground text-sm">Loading...</div>
          ) : browseData && browseData.triples.length > 0 ? (
            <>
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-border text-sm uppercase tracking-wider text-muted-foreground">
                      <th className="text-left px-3 py-2 font-medium">Entity</th>
                      <th className="text-left px-3 py-2 font-medium">Concept</th>
                      <th className="text-left px-3 py-2 font-medium">Property</th>
                      <th className="text-right px-3 py-2 font-medium">Value</th>
                      <th className="text-left px-3 py-2 font-medium">Period</th>
                      <th className="text-left px-3 py-2 font-medium">Source</th>
                      <th className="text-left px-3 py-2 font-medium">Confidence</th>
                      <th className="text-left px-3 py-2 font-medium">Run</th>
                    </tr>
                  </thead>
                  <tbody>
                    {browseData.triples.map((t) => {
                      const isExp = expandedTriple === t.id;
                      return (
                        <Fragment key={t.id}>
                          <tr
                            className="border-t border-border/30 hover:bg-card/20 cursor-pointer transition-colors"
                            onClick={() => setExpandedTriple(isExp ? null : t.id)}
                          >
                            <td className="px-3 py-1.5 font-mono text-foreground/80">{t.entity_id}</td>
                            <td className="px-3 py-1.5 font-mono text-foreground">{t.concept}</td>
                            <td className="px-3 py-1.5 text-muted-foreground">{t.property}</td>
                            <td className="px-3 py-1.5 text-right font-mono text-foreground">{fmtValue(t.value)}</td>
                            <td className="px-3 py-1.5 text-muted-foreground">{t.period || '-'}</td>
                            <td className="px-3 py-1.5 text-muted-foreground">{t.source_system}</td>
                            <td className="px-3 py-1.5">{confidenceBadge(t.confidence_tier)}</td>
                            <td className="px-3 py-1.5 font-mono text-muted-foreground/70">{shortId(t.run_id)}</td>
                          </tr>
                          {isExp && (
                            <tr className="border-t border-border/10 bg-card/5">
                              <td colSpan={8} className="px-3 py-2 pl-8">
                                <div className="flex flex-wrap gap-x-6 gap-y-1 text-sm font-mono">
                                  <span><span className="text-muted-foreground/60">id </span><span className="text-foreground/80">{t.id}</span></span>
                                  <span><span className="text-muted-foreground/60">tenant </span><span className="text-foreground/80">{shortId(t.tenant_id)}</span></span>
                                  <span><span className="text-muted-foreground/60">source_table </span><span className="text-foreground/80">{t.source_table || '-'}</span></span>
                                  <span><span className="text-muted-foreground/60">source_field </span><span className="text-foreground/80">{t.source_field || '-'}</span></span>
                                  <span><span className="text-muted-foreground/60">pipe_id </span><span className="text-foreground/80">{t.pipe_id ? shortId(t.pipe_id) : '-'}</span></span>
                                  <span><span className="text-muted-foreground/60">canonical_id </span><span className="text-foreground/80">{t.canonical_id ? shortId(t.canonical_id) : '-'}</span></span>
                                  <span><span className="text-muted-foreground/60">resolution </span><span className="text-foreground/80">{t.resolution_method || '-'}</span></span>
                                  <span><span className="text-muted-foreground/60">confidence </span><span className="text-foreground/80">{t.confidence_score}</span></span>
                                  <span><span className="text-muted-foreground/60">currency </span><span className="text-foreground/80">{t.currency || '-'}</span></span>
                                  <span><span className="text-muted-foreground/60">unit </span><span className="text-foreground/80">{t.unit || '-'}</span></span>
                                  <span><span className="text-muted-foreground/60">created </span><span className="text-foreground/80">{t.created_at ? fmtDate(t.created_at) : '-'}</span></span>
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
              {/* Pagination */}
              <div className="flex items-center justify-between px-4 py-2 border-t border-border/30">
                <button
                  onClick={() => fetchBrowse(Math.max(0, browseOffset - BROWSE_LIMIT))}
                  disabled={browseOffset === 0}
                  className="px-3 py-1 text-sm rounded bg-accent text-foreground hover:bg-accent/80 disabled:opacity-30"
                >
                  Prev
                </button>
                <span className="text-xs text-muted-foreground font-mono">
                  {browseOffset + 1}–{Math.min(browseOffset + BROWSE_LIMIT, browseData.total_count)} of {fmtNum(browseData.total_count)}
                </span>
                <button
                  onClick={() => fetchBrowse(browseOffset + BROWSE_LIMIT)}
                  disabled={browseOffset + BROWSE_LIMIT >= browseData.total_count}
                  className="px-3 py-1 text-sm rounded bg-accent text-foreground hover:bg-accent/80 disabled:opacity-30"
                >
                  Next
                </button>
              </div>
            </>
          ) : (
            <div className="p-4 text-center text-muted-foreground text-sm">
              No triples match the current filters
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
