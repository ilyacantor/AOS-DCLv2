import { useState, useEffect, useCallback, useRef, Fragment } from 'react';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface EntityInfo {
  entity_id: string;
  display_name: string;
}

interface EntityStat extends EntityInfo {
  cofa_count: number;
  last_ingest: string | null;
}

interface ConceptComparison {
  concept: string;
  acquirer_triples: { property: string; value: unknown; period: string }[];
  target_triples: { property: string; value: unknown; period: string }[];
}

interface MatchRow {
  acquirer_concept: string;
  target_concept: string;
  canonical_id: string | null;
  resolution_confidence: number | null;
  source_field: string | null;
  resolution_method: string | null;
}

interface MergeData {
  engagement_id: string | null;
  acquirer: EntityInfo;
  target: EntityInfo;
  overview: {
    entities: EntityStat[];
    total_cofa_count: number;
  };
  comparison: {
    concepts: ConceptComparison[];
  };
  matches: {
    has_matches: boolean;
    rows: MatchRow[];
    message: string;
  };
  orphans: {
    show_section: boolean;
    acquirer_unmatched: string[];
    target_unmatched: string[];
  };
}

interface BrowseTriple {
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
  triples: BrowseTriple[];
  total_count: number;
  filters_applied: Record<string, string>;
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export function MergePanel() {
  const [data, setData] = useState<MergeData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Collapsible sections
  const [comparisonOpen, setComparisonOpen] = useState(true);
  const [matchesOpen, setMatchesOpen] = useState(true);
  const [orphansOpen, setOrphansOpen] = useState(true);
  const [browseOpen, setBrowseOpen] = useState(false);

  // Browse state
  const [browseData, setBrowseData] = useState<BrowseData | null>(null);
  const [browseLoading, setBrowseLoading] = useState(false);
  const [browseEntity, setBrowseEntity] = useState('');
  const [browsePeriod, setBrowsePeriod] = useState('');
  const [browseOffset, setBrowseOffset] = useState(0);
  const BROWSE_LIMIT = 50;

  // Expanded triple in browse
  const [expandedTriple, setExpandedTriple] = useState<string | null>(null);

  // COFA merge action state
  const [mergeRunning, setMergeRunning] = useState(false);
  const [mergeStatus, setMergeStatus] = useState<string | null>(null);
  const [mergeError, setMergeError] = useState<string | null>(null);
  const [mergeCollapsedResponse, setMergeCollapsedResponse] = useState<string | null>(null);
  const [toast, setToast] = useState<{ message: string; type: 'success' | 'error' } | null>(null);
  const pollRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const mergeStartRef = useRef<number>(0);

  // --- Data fetching ---

  const fetchMerge = useCallback(async () => {
    setLoading(true);
    try {
      const res = await fetch('/api/dcl/merge/overview');
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw new Error(body.detail || `HTTP ${res.status}: ${res.statusText}`);
      }
      setData(await res.json());
      setError(null);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Failed to fetch merge overview');
    } finally {
      setLoading(false);
    }
  }, []);

  const fetchBrowse = useCallback(async (offset = 0) => {
    setBrowseLoading(true);
    try {
      const params = new URLSearchParams();
      params.set('domain', 'cofa');
      if (browseEntity) params.set('entity_id', browseEntity);
      if (browsePeriod) params.set('period', browsePeriod);
      params.set('limit', String(BROWSE_LIMIT));
      params.set('offset', String(offset));
      const res = await fetch(`/api/dcl/triples/browse?${params}`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      setBrowseData(await res.json());
      setBrowseOffset(offset);
    } catch (e) {
      console.error('[MergePanel] Failed to fetch browse:', e);
    } finally {
      setBrowseLoading(false);
    }
  }, [browseEntity, browsePeriod]);

  // Cleanup poll on unmount
  useEffect(() => {
    return () => {
      if (pollRef.current) clearTimeout(pollRef.current);
    };
  }, []);

  // Auto-dismiss toast after 8 seconds
  useEffect(() => {
    if (!toast) return;
    const t = setTimeout(() => setToast(null), 8000);
    return () => clearTimeout(t);
  }, [toast]);

  const runCofaMerge = useCallback(async () => {
    if (!data?.engagement_id) {
      setMergeError(
        'No engagement found in engagement_state table. ' +
        'Create an engagement first so Maestra knows which entities to unify.'
      );
      return;
    }

    setMergeRunning(true);
    setMergeError(null);
    setMergeCollapsedResponse(null);
    setMergeStatus('Sending to Maestra...');
    mergeStartRef.current = Date.now();

    // Step 1: POST to Maestra chat
    let maestraOk = false;
    try {
      const res = await fetch('/api/platform/maestra/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          engagement_id: data.engagement_id,
          message:
            "Perform COFA unification for this engagement. Read both entities' charts of accounts " +
            "from DCL, produce a complete mapping table with confidence scores for every GL account, " +
            "identify all conflicts with type and severity, build the unified account structure, and " +
            "write the results using the write_cofa_mapping tool. Every account from both entities " +
            "must appear in the mapping — no orphans.",
        }),
      });

      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw new Error(
          body.detail || body.error || `Maestra returned HTTP ${res.status}: ${res.statusText}`
        );
      }

      const responseData = await res.json();
      maestraOk = true;

      // Check if Maestra actually invoked the tool (response may be text-only)
      const responseText = responseData?.response || responseData?.message || '';
      if (
        responseText &&
        !responseData?.tool_calls?.length &&
        !responseText.includes('write_cofa_mapping') &&
        !responseText.includes('mapping')
      ) {
        // Maestra responded but may not have written results
        setMergeCollapsedResponse(responseText);
      }
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      if (msg.includes('Failed to fetch') || msg.includes('NetworkError') || msg.includes('ERR_CONNECTION_REFUSED')) {
        setMergeError(
          'Cannot reach Maestra (Platform service). Is it running on port 8006?'
        );
      } else {
        setMergeError(msg);
      }
      setMergeRunning(false);
      setMergeStatus(null);
      return;
    }

    if (!maestraOk) return;

    // Step 2: Poll merge overview for results
    setMergeStatus('Maestra is analyzing charts of accounts...');
    const pollForResults = () => {
      pollRef.current = setTimeout(async () => {
        const elapsed = Date.now() - mergeStartRef.current;

        // Update status message based on elapsed time
        if (elapsed > 60000) {
          setMergeStatus(
            'Still working — COFA unification typically takes 30-90 seconds for two entities.'
          );
        } else if (elapsed > 30000) {
          setMergeStatus('Writing mapping to DCL...');
        } else if (elapsed > 10000) {
          setMergeStatus('Maestra is analyzing charts of accounts...');
        }

        try {
          const res = await fetch('/api/dcl/merge/overview');
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          const freshData: MergeData = await res.json();

          if (freshData.matches.has_matches) {
            // Success — matches found
            if (pollRef.current) clearTimeout(pollRef.current);
            setData(freshData);
            setMergeRunning(false);
            setMergeStatus(null);

            const accountCount = freshData.matches.rows.length;
            const conflictCount =
              freshData.orphans.acquirer_unmatched.length +
              freshData.orphans.target_unmatched.length;
            setToast({
              message: `COFA merge complete — ${accountCount} accounts mapped, ${conflictCount} conflicts identified.`,
              type: 'success',
            });
            return;
          }
        } catch (e) {
          // Network error during polling
          if (pollRef.current) clearTimeout(pollRef.current);
          setMergeError(
            'Lost connection while waiting for results. Check services and try again.'
          );
          setMergeRunning(false);
          setMergeStatus(null);
          return;
        }

        // Keep polling — no results yet
        pollForResults();
      }, 3000);
    };
    pollForResults();
  }, [data]);

  useEffect(() => {
    fetchMerge();
  }, [fetchMerge]);

  // Auto-load browse when filters change and section is open
  useEffect(() => {
    if (browseOpen) fetchBrowse(0);
  }, [browseOpen, fetchBrowse]);

  // --- Helpers ---

  const fmtDate = (ts: string) => {
    try {
      return new Date(ts).toLocaleString('en-US', {
        month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', hour12: true,
      });
    } catch { return ts; }
  };

  const fmtNum = (n: number) => n.toLocaleString();

  const fmtValue = (val: unknown): string => {
    if (val === null || val === undefined) return '-';
    if (typeof val === 'number') return val.toLocaleString();
    if (typeof val === 'string') {
      const n = Number(val);
      if (!isNaN(n) && val.trim() !== '') return n.toLocaleString();
      return val;
    }
    return JSON.stringify(val);
  };

  const shortId = (id: string) => id ? id.slice(0, 8) : '-';

  const confidenceBadge = (score: number | null) => {
    if (score === null) return <span className="text-muted-foreground text-xs">-</span>;
    const cls = score >= 0.8
      ? 'bg-emerald-500/20 text-emerald-400 border-emerald-500/30'
      : score >= 0.5
        ? 'bg-amber-500/20 text-amber-400 border-amber-500/30'
        : 'bg-red-500/20 text-red-400 border-red-500/30';
    return (
      <span className={`inline-block px-1.5 py-0.5 rounded text-[11px] font-semibold border ${cls}`}>
        {(score * 100).toFixed(0)}%
      </span>
    );
  };

  const tierBadge = (tier: string) => {
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

  const chevron = (open: boolean) => (
    <svg
      className={`w-2.5 h-2.5 shrink-0 transition-transform duration-150 text-muted-foreground ${open ? 'rotate-90' : ''}`}
      fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}
    >
      <path strokeLinecap="round" strokeLinejoin="round" d="M9 5l7 7-7 7" />
    </svg>
  );

  // --- Render ---

  if (loading) {
    return (
      <div className="h-full flex flex-col min-h-0">
        <div className="shrink-0 flex items-center justify-between px-6 py-3 border-b border-border bg-card/50">
          <h2 className="text-base font-semibold">COFA Merge</h2>
        </div>
        <div className="flex-1 flex items-center justify-center text-muted-foreground">
          <div className="flex flex-col items-center gap-3">
            <div className="w-8 h-8 border-2 border-primary border-t-transparent rounded-full animate-spin" />
            <span className="text-base">Loading merge overview...</span>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="h-full flex flex-col min-h-0">
      {/* Toast notification */}
      {toast && (
        <div className={`fixed top-4 right-4 z-50 px-4 py-3 rounded-lg border shadow-lg max-w-md animate-[fadeIn_0.2s_ease-out] ${
          toast.type === 'success'
            ? 'bg-emerald-500/15 border-emerald-500/30 text-emerald-400'
            : 'bg-red-500/15 border-red-500/30 text-red-400'
        }`}>
          <div className="flex items-start gap-2">
            <span className="text-sm">{toast.message}</span>
            <button
              onClick={() => setToast(null)}
              className="shrink-0 text-muted-foreground hover:text-foreground ml-2"
            >
              &times;
            </button>
          </div>
        </div>
      )}

      {/* Header */}
      <div className="shrink-0 px-6 py-3 border-b border-border bg-card/50">
        <div className="flex items-center justify-between">
          <h2 className="text-base font-semibold">COFA Merge</h2>
          <div className="flex items-center gap-2">
            {/* Run COFA Merge button — visible when two entities have COFA data */}
            {data && data.overview.entities.length >= 2 && (
              <button
                onClick={runCofaMerge}
                disabled={mergeRunning || !data.engagement_id}
                className={`px-3 py-1 text-sm rounded font-medium transition-colors ${
                  mergeRunning
                    ? 'bg-amber-500/20 text-amber-400 border border-amber-500/30 cursor-wait'
                    : data.matches.has_matches
                      ? 'bg-amber-500/20 text-amber-400 border border-amber-500/30 hover:bg-amber-500/30'
                      : 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/30 hover:bg-emerald-500/30'
                } disabled:opacity-40 disabled:cursor-not-allowed`}
                title={
                  !data.engagement_id
                    ? 'No engagement found — create one first'
                    : data.matches.has_matches
                      ? 'Re-run will replace existing mappings'
                      : 'Trigger Maestra to unify COFA accounts'
                }
              >
                {mergeRunning
                  ? 'Running COFA Merge...'
                  : data.matches.has_matches
                    ? 'Re-run COFA Merge'
                    : 'Run COFA Merge'}
              </button>
            )}
            <button
              onClick={fetchMerge}
              className="px-3 py-1 text-sm rounded bg-primary text-primary-foreground hover:bg-primary/90"
            >
              Refresh
            </button>
          </div>
        </div>

        {/* Progress / error bar */}
        {mergeRunning && mergeStatus && (
          <div className="mt-2 flex items-center gap-2 text-sm text-amber-400">
            <div className="w-4 h-4 border-2 border-amber-400 border-t-transparent rounded-full animate-spin shrink-0" />
            <span>{mergeStatus}</span>
          </div>
        )}
        {mergeError && (
          <div className="mt-2 rounded border border-red-500/20 bg-red-500/10 px-3 py-2">
            <span className="text-sm text-red-400">{mergeError}</span>
            <button
              onClick={() => { setMergeError(null); runCofaMerge(); }}
              className="ml-3 px-2 py-0.5 text-xs rounded bg-primary text-primary-foreground hover:bg-primary/90"
            >
              Retry
            </button>
          </div>
        )}
        {mergeCollapsedResponse && !mergeRunning && (
          <details className="mt-2 rounded border border-amber-500/20 bg-amber-500/10 px-3 py-2">
            <summary className="text-sm text-amber-400 cursor-pointer">
              Maestra completed analysis but did not write results. This may require a follow-up message.
            </summary>
            <pre className="mt-2 text-xs text-muted-foreground whitespace-pre-wrap max-h-40 overflow-y-auto">
              {mergeCollapsedResponse}
            </pre>
          </details>
        )}
      </div>

      <div className="flex-1 overflow-y-auto p-4 space-y-4 min-h-0">
        {error && (
          <div className="rounded-lg border border-red-500/20 bg-red-500/10 p-3 text-center">
            <span className="text-base text-red-400">{error}</span>
            <button onClick={fetchMerge} className="ml-3 px-3 py-1 text-sm rounded bg-primary text-primary-foreground hover:bg-primary/90">
              Retry
            </button>
          </div>
        )}

        {data && (
          <>
            {/* ================================================================
                Section 1: Overview Stats
                ================================================================ */}
            <div className="rounded-lg border border-border bg-card/30 px-4 py-3">
              <div className="grid grid-cols-2 gap-4">
                {data.overview.entities.map((entity) => {
                  const isAcquirer = entity.entity_id === data.acquirer.entity_id;
                  const borderColor = isAcquirer ? 'border-blue-500/30' : 'border-purple-500/30';
                  const textColor = isAcquirer ? 'text-blue-400' : 'text-purple-400';
                  const label = isAcquirer ? 'Acquirer' : 'Target';
                  return (
                    <div key={entity.entity_id} className={`rounded-lg border ${borderColor} bg-card/20 p-3`}>
                      <div className="flex items-center justify-between mb-2">
                        <span className={`text-xs font-semibold uppercase tracking-wider ${textColor}`}>{label}</span>
                        <span className="text-sm font-semibold text-foreground">{entity.display_name}</span>
                      </div>
                      <div className="flex items-center gap-4 text-sm font-mono">
                        <span>
                          <span className="text-foreground font-semibold">{fmtNum(entity.cofa_count)}</span>
                          <span className="text-muted-foreground ml-1">COFA triples</span>
                        </span>
                        {entity.last_ingest && (
                          <span className="text-muted-foreground">
                            last: {fmtDate(entity.last_ingest)}
                          </span>
                        )}
                      </div>
                    </div>
                  );
                })}
              </div>
              <div className="mt-2 text-center text-sm font-mono text-muted-foreground">
                <span className="text-foreground font-semibold">{fmtNum(data.overview.total_cofa_count)}</span> total COFA triples across both entities
              </div>
            </div>

            {/* ================================================================
                Section 2: Side-by-Side Comparison
                ================================================================ */}
            <div className="rounded-lg border border-border bg-card/30 overflow-hidden">
              <button
                onClick={() => setComparisonOpen(!comparisonOpen)}
                className="w-full flex items-center gap-2 px-4 py-2.5 text-sm hover:bg-card/20 transition-colors"
              >
                {chevron(comparisonOpen)}
                <span className="font-semibold uppercase tracking-wider text-muted-foreground text-sm">Side-by-Side Comparison</span>
                <span className="text-muted-foreground/70 font-mono">{data.comparison.concepts.length} concepts</span>
              </button>
              {comparisonOpen && (
                <div className="p-4">
                  {data.comparison.concepts.length === 0 ? (
                    <div className="text-center text-muted-foreground text-sm py-4">
                      No COFA concepts found for these entities.
                    </div>
                  ) : (
                    <div className="space-y-3">
                      {data.comparison.concepts.map((c) => (
                        <div key={c.concept} className="rounded-lg border border-border/50 overflow-hidden">
                          <div className="px-3 py-1.5 bg-card/20 border-b border-border/30">
                            <span className="font-mono text-sm font-semibold text-foreground">{c.concept}</span>
                          </div>
                          <div className="grid grid-cols-2 divide-x divide-border/30">
                            {/* Acquirer column */}
                            <div className="p-2">
                              <div className="text-xs font-semibold uppercase tracking-wider text-blue-400 mb-1">
                                {data.acquirer.display_name}
                              </div>
                              {c.acquirer_triples.length === 0 ? (
                                <span className="text-xs text-muted-foreground/50">No data</span>
                              ) : (
                                <table className="w-full text-xs">
                                  <tbody>
                                    {c.acquirer_triples.map((t, i) => (
                                      <tr key={i} className={i > 0 ? 'border-t border-border/20' : ''}>
                                        <td className="py-0.5 text-muted-foreground pr-2">{t.property}</td>
                                        <td className="py-0.5 text-right font-mono text-foreground">{fmtValue(t.value)}</td>
                                        <td className="py-0.5 text-right text-muted-foreground/60 pl-2">{t.period || ''}</td>
                                      </tr>
                                    ))}
                                  </tbody>
                                </table>
                              )}
                            </div>
                            {/* Target column */}
                            <div className="p-2">
                              <div className="text-xs font-semibold uppercase tracking-wider text-purple-400 mb-1">
                                {data.target.display_name}
                              </div>
                              {c.target_triples.length === 0 ? (
                                <span className="text-xs text-muted-foreground/50">No data</span>
                              ) : (
                                <table className="w-full text-xs">
                                  <tbody>
                                    {c.target_triples.map((t, i) => (
                                      <tr key={i} className={i > 0 ? 'border-t border-border/20' : ''}>
                                        <td className="py-0.5 text-muted-foreground pr-2">{t.property}</td>
                                        <td className="py-0.5 text-right font-mono text-foreground">{fmtValue(t.value)}</td>
                                        <td className="py-0.5 text-right text-muted-foreground/60 pl-2">{t.period || ''}</td>
                                      </tr>
                                    ))}
                                  </tbody>
                                </table>
                              )}
                            </div>
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              )}
            </div>

            {/* ================================================================
                Section 3: Resolution Matches
                ================================================================ */}
            <div className="rounded-lg border border-border bg-card/30 overflow-hidden">
              <button
                onClick={() => setMatchesOpen(!matchesOpen)}
                className="w-full flex items-center gap-2 px-4 py-2.5 text-sm hover:bg-card/20 transition-colors"
              >
                {chevron(matchesOpen)}
                <span className="font-semibold uppercase tracking-wider text-muted-foreground text-sm">Resolution Matches</span>
                <span className="text-muted-foreground/70 font-mono">
                  {data.matches.has_matches ? `${data.matches.rows.length} matched` : 'none'}
                </span>
              </button>
              {matchesOpen && (
                <div className="border-t border-border/30">
                  {data.matches.has_matches ? (
                    <div className="overflow-x-auto">
                      <table className="w-full text-sm">
                        <thead>
                          <tr className="border-b border-border text-sm uppercase tracking-wider text-muted-foreground">
                            <th className="text-left px-3 py-2 font-medium">Acquirer Concept</th>
                            <th className="text-left px-3 py-2 font-medium">Target Concept</th>
                            <th className="text-left px-3 py-2 font-medium">Canonical ID</th>
                            <th className="text-left px-3 py-2 font-medium">Confidence</th>
                            <th className="text-left px-3 py-2 font-medium">Method</th>
                            <th className="text-left px-3 py-2 font-medium">Source Field</th>
                          </tr>
                        </thead>
                        <tbody>
                          {data.matches.rows.map((m, i) => (
                            <tr key={i} className="border-t border-border/30 hover:bg-card/20 transition-colors">
                              <td className="px-3 py-1.5 font-mono text-blue-400">{m.acquirer_concept}</td>
                              <td className="px-3 py-1.5 font-mono text-purple-400">{m.target_concept}</td>
                              <td className="px-3 py-1.5 font-mono text-muted-foreground/70">{m.canonical_id ? shortId(m.canonical_id) : '-'}</td>
                              <td className="px-3 py-1.5">{confidenceBadge(m.resolution_confidence)}</td>
                              <td className="px-3 py-1.5 text-muted-foreground">{m.resolution_method || '-'}</td>
                              <td className="px-3 py-1.5 text-muted-foreground">{m.source_field || '-'}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : (
                    <div className="px-4 py-3 bg-amber-500/10 border-amber-500/20">
                      <span className="text-sm text-amber-400">{data.matches.message}</span>
                    </div>
                  )}
                </div>
              )}
            </div>

            {/* ================================================================
                Section 4: Orphans (only when matches exist)
                ================================================================ */}
            {data.orphans.show_section && (
              <div className="rounded-lg border border-border bg-card/30 overflow-hidden">
                <button
                  onClick={() => setOrphansOpen(!orphansOpen)}
                  className="w-full flex items-center gap-2 px-4 py-2.5 text-sm hover:bg-card/20 transition-colors"
                >
                  {chevron(orphansOpen)}
                  <span className="font-semibold uppercase tracking-wider text-muted-foreground text-sm">Unmatched Concepts</span>
                  <span className="text-muted-foreground/70 font-mono">
                    {data.orphans.acquirer_unmatched.length + data.orphans.target_unmatched.length} orphan(s)
                  </span>
                </button>
                {orphansOpen && (
                  <div className="grid grid-cols-2 divide-x divide-border/30 border-t border-border/30">
                    <div className="p-3">
                      <div className="text-xs font-semibold uppercase tracking-wider text-blue-400 mb-2">
                        {data.acquirer.display_name} unmatched
                      </div>
                      {data.orphans.acquirer_unmatched.length === 0 ? (
                        <span className="text-xs text-muted-foreground/50">All matched</span>
                      ) : (
                        <div className="space-y-1">
                          {data.orphans.acquirer_unmatched.map((c) => (
                            <div key={c} className="font-mono text-xs text-foreground/80">{c}</div>
                          ))}
                        </div>
                      )}
                    </div>
                    <div className="p-3">
                      <div className="text-xs font-semibold uppercase tracking-wider text-purple-400 mb-2">
                        {data.target.display_name} unmatched
                      </div>
                      {data.orphans.target_unmatched.length === 0 ? (
                        <span className="text-xs text-muted-foreground/50">All matched</span>
                      ) : (
                        <div className="space-y-1">
                          {data.orphans.target_unmatched.map((c) => (
                            <div key={c} className="font-mono text-xs text-foreground/80">{c}</div>
                          ))}
                        </div>
                      )}
                    </div>
                  </div>
                )}
              </div>
            )}

            {/* ================================================================
                Section 5: Raw COFA Browser
                ================================================================ */}
            <div className="rounded-lg border border-border bg-card/30 overflow-hidden">
              <button
                onClick={() => setBrowseOpen(!browseOpen)}
                className="w-full flex items-center gap-2 px-4 py-2.5 text-sm hover:bg-card/20 transition-colors"
              >
                {chevron(browseOpen)}
                <span className="font-semibold uppercase tracking-wider text-muted-foreground text-sm">COFA Triple Browser</span>
              </button>
              {browseOpen && (
                <>
                  {/* Filters */}
                  <div className="flex items-center gap-3 px-4 py-2 border-t border-border/30 flex-wrap">
                    <div className="flex items-center gap-1">
                      <span className="text-xs text-muted-foreground">Entity:</span>
                      <select
                        value={browseEntity}
                        onChange={(e) => { setBrowseEntity(e.target.value); setBrowseOffset(0); }}
                        className="px-2 py-1 text-xs rounded border border-border bg-background"
                      >
                        <option value="">All</option>
                        {data.overview.entities.map((e) => (
                          <option key={e.entity_id} value={e.entity_id}>{e.display_name}</option>
                        ))}
                      </select>
                    </div>
                    <div className="flex items-center gap-1">
                      <span className="text-xs text-muted-foreground">Period:</span>
                      <input
                        type="text"
                        value={browsePeriod}
                        onChange={(e) => { setBrowsePeriod(e.target.value); setBrowseOffset(0); }}
                        placeholder="e.g. Q1 2024"
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
                              const entityColor = t.entity_id === data.acquirer.entity_id
                                ? 'text-blue-400'
                                : t.entity_id === data.target.entity_id
                                  ? 'text-purple-400'
                                  : 'text-foreground/80';
                              return (
                                <Fragment key={t.id}>
                                  <tr
                                    className="border-t border-border/30 hover:bg-card/20 cursor-pointer transition-colors"
                                    onClick={() => setExpandedTriple(isExp ? null : t.id)}
                                  >
                                    <td className={`px-3 py-1.5 font-mono ${entityColor}`}>{t.entity_id}</td>
                                    <td className="px-3 py-1.5 font-mono text-foreground">{t.concept}</td>
                                    <td className="px-3 py-1.5 text-muted-foreground">{t.property}</td>
                                    <td className="px-3 py-1.5 text-right font-mono text-foreground">{fmtValue(t.value)}</td>
                                    <td className="px-3 py-1.5 text-muted-foreground">{t.period || '-'}</td>
                                    <td className="px-3 py-1.5 text-muted-foreground">{t.source_system}</td>
                                    <td className="px-3 py-1.5">{tierBadge(t.confidence_tier)}</td>
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
                    <div className="p-4 text-center text-muted-foreground text-sm border-t border-border/30">
                      No COFA triples match the current filters
                    </div>
                  )}
                </>
              )}
            </div>
          </>
        )}
      </div>
    </div>
  );
}
