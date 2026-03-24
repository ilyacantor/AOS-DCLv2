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

interface FinancialMetric {
  label: string;
  acquirer: number | null;
  target: number | null;
  consolidated: number | null;
  is_derived?: boolean;
  format?: 'currency' | 'percent' | 'number';
}

interface ConflictItem {
  conflict_id: string;
  concept: string;
  conflict_type: string;
  severity: string;
  description: string;
  dollar_impact: number;
  acquirer_treatment: string;
  target_treatment: string;
  resolution_status: string;
  resolution: string;
  resolved_by: string;
  resolved_at: string;
  resolution_notes: string;
  impact_area: string;
  revenue_impact: number | null;
  expense_impact: number | null;
  ebitda_impact: number | null;
  from_category: string;
  to_category: string;
}

interface ConflictDetail {
  conflict_id: string;
  description: string;
  dollar_impact: number;
  revenue_impact: number;
  expense_impact: number;
  ebitda_impact: number;
  impact_area: string;
  severity: string;
  acquirer_treatment: string;
  target_treatment: string;
  resolution_status: string;
  from_category: string;
  to_category: string;
}

interface CategoryEntry {
  count: number;
  total_dollar_impact: number;
  revenue_impact: number;
  expense_impact: number;
  ebitda_impact: number;
  conflicts: string[];
  conflict_details: ConflictDetail[];
  reclassifications: {
    conflict_id: string;
    from_category: string;
    to_category: string;
    amount: number;
    description: string;
  }[];
}

interface CategorySummary {
  by_type: Record<string, CategoryEntry>;
  combined_impact: { revenue: number; expenses: number; ebitda: number };
}

interface ConflictData {
  conflicts: ConflictItem[];
  summary: { total: number; pending: number; resolved: number };
  category_summary: CategorySummary;
}

interface MaestraEngagement {
  engagement_id: string;
  entity_a: string;
  entity_b: string;
  entity_a_name: string;
  entity_b_name: string;
  state: string;
  created_by: string;
  created_at: string;
  updated_at: string;
}

interface MergeData {
  engagement_id: string | null;
  source_run_tag: string | Record<string, string> | null;
  acquirer: EntityInfo;
  target: EntityInfo;
  overview: {
    entities: EntityStat[];
    total_cofa_count: number;
  };
  financial_summary?: FinancialMetric[];
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
    acquirer_unmatched_count: number;
    target_unmatched_count: number;
    acquirer_coa_total: number;
    acquirer_mapped: number;
    target_coa_total: number;
    target_mapped: number;
    message: string;
  };
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export function MergePanel() {
  const [data, setData] = useState<MergeData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Conflict state
  const [conflictData, setConflictData] = useState<ConflictData | null>(null);
  const [expandedConflictId, setExpandedConflictId] = useState<string | null>(null);
  const [resolutionDrafts, setResolutionDrafts] = useState<Record<string, { resolution: string; notes: string }>>({});
  const [resolvingId, setResolvingId] = useState<string | null>(null);
  const [batchSelected, setBatchSelected] = useState<Set<string>>(new Set());
  const [batchResolution, setBatchResolution] = useState('acquirer');
  const [batchResolving, setBatchResolving] = useState(false);

  // Collapsible sections
  const [conflictsOpen, setConflictsOpen] = useState(true);
  const [matchesOpen, setMatchesOpen] = useState(false);
  const [orphansOpen, setOrphansOpen] = useState(false);
  const [expandedBucket, setExpandedBucket] = useState<string | null>(null);

  // COFA merge action state
  const [mergeRunning, setMergeRunning] = useState(false);
  const [mergeStatus, setMergeStatus] = useState<string | null>(null);
  const [mergeError, setMergeError] = useState<string | null>(null);
  const [mergeCollapsedResponse, setMergeCollapsedResponse] = useState<string | null>(null);
  const [mergeElapsed, setMergeElapsed] = useState(0);
  const [mergeFinishedIn, setMergeFinishedIn] = useState<number | null>(null);
  const [toast, setToast] = useState<{ message: string; type: 'success' | 'error' } | null>(null);
  const [autoRefresh, setAutoRefresh] = useState(true);

  // Engagement selector
  const [engagements, setEngagements] = useState<MaestraEngagement[]>([]);
  const [selectedEngagementId, setSelectedEngagementId] = useState<string | null>(null);
  const [engagementError, setEngagementError] = useState<string | null>(null);

  const pollRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const mergeStartRef = useRef<number>(0);
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // Live elapsed-seconds counter while merge is running
  useEffect(() => {
    if (mergeRunning) {
      setMergeElapsed(0);
      setMergeFinishedIn(null);
      timerRef.current = setInterval(() => {
        if (mergeStartRef.current > 0) {
          const elapsed = Math.floor((Date.now() - mergeStartRef.current) / 1000);
          setMergeElapsed(elapsed);
          if (elapsed >= 60) {
            setMergeStatus('Writing mapping results to DCL...');
          } else if (elapsed >= 30) {
            setMergeStatus('Mapping accounts and identifying conflicts...');
          } else if (elapsed >= 10) {
            setMergeStatus('Maestra is analyzing charts of accounts...');
          }
        }
      }, 1000);
    } else {
      if (timerRef.current) {
        clearInterval(timerRef.current);
        timerRef.current = null;
      }
    }
    return () => {
      if (timerRef.current) clearInterval(timerRef.current);
    };
  }, [mergeRunning]);

  // --- Data fetching ---

  const fetchMerge = useCallback(async (showSpinner = true) => {
    if (showSpinner) setLoading(true);
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
      if (showSpinner) setLoading(false);
    }
  }, []);

  const fetchConflicts = useCallback(async () => {
    try {
      const res = await fetch('/api/dcl/merge/conflicts');
      if (!res.ok) return;
      setConflictData(await res.json());
    } catch {
      // Non-fatal — conflicts section just won't render
    }
  }, []);

  const fetchEngagements = useCallback(async () => {
    try {
      const res = await fetch('/api/platform/maestra/engagements');
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        setEngagementError(body.detail || `Failed to load engagements (HTTP ${res.status})`);
        return;
      }
      const list: MaestraEngagement[] = await res.json();
      setEngagements(list);
      setEngagementError(null);

      // Auto-select: prefer active engagement, then most recent
      if (!selectedEngagementId || !list.find(e => e.engagement_id === selectedEngagementId)) {
        const active = list.find(e => e.state === 'active');
        if (active) {
          setSelectedEngagementId(active.engagement_id);
        } else if (list.length > 0) {
          // Most recent by created_at
          const sorted = [...list].sort((a, b) => b.created_at.localeCompare(a.created_at));
          setSelectedEngagementId(sorted[0].engagement_id);
        }
      }
    } catch {
      setEngagementError('Cannot reach Platform — engagement list unavailable');
    }
  }, [selectedEngagementId]);

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
    if (!selectedEngagementId) {
      setMergeError(
        'No engagement selected. Select an engagement from the dropdown, ' +
        'or create one in Platform first.'
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
      const res = await fetch('/api/platform/maestra/cofa-chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          session_id: `merge-${selectedEngagementId}`,
          engagement_id: selectedEngagementId,
          message: 'Run COFA unification — map all chart-of-accounts entries, identify conflicts, and write results to DCL.',
        }),
      });

      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw new Error(body.detail || `Platform returned HTTP ${res.status}`);
      }

      const result = await res.json();

      if (result.tool_calls && result.tool_calls.length > 0) {
        maestraOk = true;
      } else {
        setMergeCollapsedResponse(result.response || 'Maestra responded but did not invoke mapping tool.');
      }
    } catch (e: unknown) {
      setMergeError(e instanceof Error ? e.message : 'Failed to reach Platform/Maestra');
      setMergeRunning(false);
      setMergeStatus(null);
      return;
    }

    if (!maestraOk) {
      setMergeRunning(false);
      setMergeStatus(null);
      return;
    }

    // Step 2: Poll for results (max 120s — Maestra LLM + write should complete within this)
    setMergeStatus('Waiting for DCL to receive mapping triples...');
    const POLL_TIMEOUT_MS = 120_000;
    const pollForResults = () => {
      pollRef.current = setTimeout(async () => {
        const elapsed = Date.now() - mergeStartRef.current;
        if (elapsed > POLL_TIMEOUT_MS) {
          if (pollRef.current) clearTimeout(pollRef.current);
          setMergeError(
            `Merge timed out after ${Math.floor(elapsed / 1000)}s. ` +
            'Maestra may still be processing — check Platform logs. ' +
            'Refresh the page and try again if needed.'
          );
          setMergeRunning(false);
          setMergeStatus(null);
          return;
        }

        try {
          const res = await fetch('/api/dcl/merge/overview');
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          const freshData: MergeData = await res.json();

          if (freshData.matches.has_matches) {
            if (pollRef.current) clearTimeout(pollRef.current);
            const finalElapsed = Math.floor((Date.now() - mergeStartRef.current) / 1000);
            setMergeFinishedIn(finalElapsed);
            setData(freshData);
            setMergeRunning(false);
            setMergeStatus(null);
            fetchConflicts();

            const accountCount = freshData.matches.rows.length;
            setToast({
              message: `COFA merge complete in ${finalElapsed}s — ${accountCount} accounts mapped.`,
              type: 'success',
            });
            return;
          }

          setMergeStatus(`Waiting for DCL to receive mapping triples... (${Math.floor(elapsed / 1000)}s)`);
        } catch {
          if (pollRef.current) clearTimeout(pollRef.current);
          setMergeError('Lost connection while waiting for results. Check services and try again.');
          setMergeRunning(false);
          setMergeStatus(null);
          return;
        }

        pollForResults();
      }, 3000);
    };
    pollForResults();
  }, [data, fetchConflicts, selectedEngagementId]);

  useEffect(() => {
    fetchMerge();
    fetchConflicts();
    fetchEngagements();
  }, [fetchMerge, fetchConflicts, fetchEngagements]);

  // Auto-polling (same pattern as TriplesPanel)
  useEffect(() => {
    if (!autoRefresh) return;
    const interval = setInterval(() => {
      fetchMerge(false);
      fetchConflicts();
    }, 5000);
    return () => clearInterval(interval);
  }, [autoRefresh, fetchMerge, fetchConflicts]);

  // --- Conflict resolution ---

  const resolveConflict = async (conflictId: string) => {
    const draft = resolutionDrafts[conflictId];
    if (!draft?.resolution) return;

    setResolvingId(conflictId);
    try {
      const res = await fetch(`/api/dcl/merge/conflicts/${conflictId}/resolve`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          resolution: draft.resolution,
          notes: draft.notes || '',
          resolved_by: 'operator',
        }),
      });
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw new Error(body.detail || `HTTP ${res.status}`);
      }
      setToast({ message: `Conflict ${conflictId} resolved.`, type: 'success' });
      setExpandedConflictId(null);
      fetchConflicts();
    } catch (e: unknown) {
      setToast({ message: e instanceof Error ? e.message : 'Failed to resolve conflict', type: 'error' });
    } finally {
      setResolvingId(null);
    }
  };

  const batchResolve = async () => {
    if (batchSelected.size === 0) return;
    setBatchResolving(true);
    try {
      const res = await fetch('/api/dcl/merge/conflicts/batch-resolve', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          conflict_ids: Array.from(batchSelected),
          resolution: batchResolution,
          notes: 'Batch approved',
          resolved_by: 'operator',
        }),
      });
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw new Error(body.detail || `HTTP ${res.status}`);
      }
      const result = await res.json();
      setToast({ message: `${result.resolved} conflicts resolved.`, type: 'success' });
      setBatchSelected(new Set());
      fetchConflicts();
    } catch (e: unknown) {
      setToast({ message: e instanceof Error ? e.message : 'Batch resolve failed', type: 'error' });
    } finally {
      setBatchResolving(false);
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

  const getRunTag = (): string | null => {
    if (!data?.source_run_tag) return null;
    if (typeof data.source_run_tag === 'string') return data.source_run_tag;
    if (typeof data.source_run_tag === 'object') {
      const vals = Object.values(data.source_run_tag);
      return vals.length > 0 ? vals[0] : null;
    }
    return null;
  };

  const fmtDollarImpact = (val: number): string => {
    if (val === 0) return '';
    const abs = Math.abs(val);
    if (abs >= 1e9) return `$${(val / 1e9).toFixed(1)}B`;
    if (abs >= 1e6) return `$${(val / 1e6).toFixed(1)}M`;
    if (abs >= 1e3) return `$${(val / 1e3).toFixed(0)}K`;
    return `$${val.toFixed(0)}`;
  };

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

  const chevron = (open: boolean) => (
    <svg
      className={`w-2.5 h-2.5 shrink-0 transition-transform duration-150 text-muted-foreground ${open ? 'rotate-90' : ''}`}
      fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}
    >
      <path strokeLinecap="round" strokeLinejoin="round" d="M9 5l7 7-7 7" />
    </svg>
  );

  const statusBadge = (status: string) => {
    const cls = status === 'resolved'
      ? 'bg-emerald-500/20 text-emerald-400 border-emerald-500/30'
      : 'bg-amber-500/20 text-amber-400 border-amber-500/30';
    return (
      <span className={`inline-block px-1.5 py-0.5 rounded text-[11px] font-semibold border ${cls}`}>
        {status === 'resolved' ? 'Resolved' : 'Pending'}
      </span>
    );
  };

  // --- Download helpers ---

  const triggerDownload = (content: string, filename: string, mime: string) => {
    const blob = new Blob([content], { type: mime });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
  };

  const downloadJson = useCallback(() => {
    if (!data) return;
    const exportData = { ...data, conflicts: conflictData?.conflicts || [] };
    const ts = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
    triggerDownload(JSON.stringify(exportData, null, 2), `cofa-merge-${ts}.json`, 'application/json');
  }, [data, conflictData]);

  const downloadReport = useCallback(() => {
    if (!data) return;
    const lines: string[] = [];
    const hr = '\u2500'.repeat(72);

    lines.push('COFA MERGE REPORT');
    lines.push(hr);
    lines.push(`Generated: ${new Date().toLocaleString()}`);
    lines.push(`Engagement: ${selectedEngagementId || data.engagement_id || 'N/A'}`);
    const runTag = typeof data.source_run_tag === 'string'
      ? data.source_run_tag
      : data.source_run_tag ? JSON.stringify(data.source_run_tag) : 'N/A';
    lines.push(`Source Run Tag: ${runTag}`);
    if (mergeFinishedIn !== null) lines.push(`Merge duration: ${mergeFinishedIn}s`);
    lines.push('');

    // Entities
    lines.push('ENTITIES');
    lines.push(hr);
    for (const e of data.overview.entities) {
      const role = e.entity_id === data.acquirer.entity_id ? 'Acquirer' : 'Target';
      lines.push(`  ${role}: ${e.display_name} (${e.entity_id})`);
      lines.push(`    COFA triples: ${e.cofa_count}`);
      lines.push(`    Last ingest: ${e.last_ingest || 'N/A'}`);
    }
    lines.push('');

    // Conflicts
    if (conflictData && conflictData.conflicts.length > 0) {
      lines.push(`CONFLICTS (${conflictData.summary.total} total | ${conflictData.summary.resolved} resolved | ${conflictData.summary.pending} pending)`);
      lines.push(hr);
      for (const c of conflictData.conflicts) {
        const impact = c.dollar_impact > 0 ? fmtDollarImpact(c.dollar_impact) : 'Not estimated';
        lines.push(`  ${c.conflict_id}: ${c.description || c.conflict_type}`);
        lines.push(`    Type: ${c.conflict_type} | Severity: ${c.severity} | Impact: ${impact}`);
        lines.push(`    Acquirer: ${c.acquirer_treatment}`);
        lines.push(`    Target: ${c.target_treatment}`);
        lines.push(`    Status: ${c.resolution_status}${c.resolution ? ` (${c.resolution})` : ''}`);
        if (c.resolution_notes) lines.push(`    Notes: ${c.resolution_notes}`);
      }
      lines.push('');
    }

    // Coverage
    lines.push('MAPPING COVERAGE');
    lines.push(hr);
    lines.push(`  ${data.acquirer.display_name}: ${data.orphans.acquirer_mapped}/${data.orphans.acquirer_coa_total} accounts mapped`);
    lines.push(`  ${data.target.display_name}: ${data.orphans.target_mapped}/${data.orphans.target_coa_total} accounts mapped`);
    if (data.orphans.acquirer_unmatched_count + data.orphans.target_unmatched_count > 0) {
      lines.push(`  Unmapped: ${data.orphans.acquirer_unmatched_count} acquirer, ${data.orphans.target_unmatched_count} target`);
    } else {
      lines.push('  Status: COMPLETE \u2014 all accounts mapped');
    }
    lines.push('');

    // Resolution matches
    lines.push(`RESOLUTION MATCHES (${data.matches.rows.length})`);
    lines.push(hr);
    if (data.matches.rows.length === 0) {
      lines.push('  No cross-entity resolution matches found.');
    } else {
      const acqW = 28, tgtW = 28, confW = 12, methW = 14;
      lines.push(`  ${'Acquirer Account'.padEnd(acqW)} ${'Target Account'.padEnd(tgtW)} ${'Confidence'.padEnd(confW)} ${'Method'.padEnd(methW)}`);
      lines.push(`  ${'\u2500'.repeat(acqW)} ${'\u2500'.repeat(tgtW)} ${'\u2500'.repeat(confW)} ${'\u2500'.repeat(methW)}`);
      for (const r of data.matches.rows) {
        const acq = (r.acquirer_concept || '-').replace('cofa_mapping.', '').replace('cofa.', '');
        const tgt = (r.target_concept || '-').replace('cofa_mapping.', '').replace('cofa.', '');
        const conf = r.resolution_confidence !== null ? `${(r.resolution_confidence * 100).toFixed(0)}%` : '-';
        const meth = r.resolution_method || '-';
        lines.push(`  ${acq.padEnd(acqW)} ${tgt.padEnd(tgtW)} ${conf.padEnd(confW)} ${meth.padEnd(methW)}`);
      }
    }

    const ts = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
    triggerDownload(lines.join('\n'), `cofa-merge-report-${ts}.txt`, 'text/plain');
  }, [data, conflictData, mergeFinishedIn]);

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

  // Compute conflict summary for header
  const conflictSummary = conflictData?.summary;
  const withImpact = conflictData?.conflicts.filter(c => c.dollar_impact > 0).length || 0;
  const withoutImpact = (conflictSummary?.total || 0) - withImpact;

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

      {/* ================================================================
          Header Bar — single compact line
          ================================================================ */}
      <div className="shrink-0 px-6 py-2.5 border-b border-border bg-card/50">
        <div className="flex items-center justify-between gap-3">
          <div className="flex items-center gap-3 text-sm min-w-0">
            <h2 className="text-base font-semibold shrink-0">COFA Merge</h2>
            {engagements.length > 0 ? (
              <select
                value={selectedEngagementId || ''}
                onChange={e => setSelectedEngagementId(e.target.value || null)}
                className="px-2 py-1 text-xs font-mono rounded border border-border bg-background text-foreground max-w-[220px] truncate"
                title="Select Maestra engagement"
              >
                {engagements.map(eng => (
                  <option key={eng.engagement_id} value={eng.engagement_id}>
                    {eng.engagement_id} ({eng.state})
                  </option>
                ))}
              </select>
            ) : engagementError ? (
              <span className="text-xs text-red-400 truncate" title={engagementError}>No engagements</span>
            ) : (
              <span className="text-xs text-muted-foreground">Loading engagements...</span>
            )}
            {getRunTag() && (
              <span className="text-xs font-mono font-semibold text-amber-400 bg-amber-500/10 border border-amber-500/20 px-1.5 py-0.5 rounded shrink-0">
                {getRunTag()}
              </span>
            )}
            {mergeFinishedIn !== null && !mergeRunning && (
              <span className="text-xs text-emerald-400 shrink-0">{mergeFinishedIn}s</span>
            )}
          </div>
          <div className="flex items-center gap-2 shrink-0">
            {data && data.matches.has_matches && (
              <>
                <button
                  onClick={downloadReport}
                  className="px-2.5 py-1 text-xs rounded font-medium bg-zinc-700/50 text-zinc-300 border border-zinc-600/40 hover:bg-zinc-600/50 transition-colors"
                  title="Download formatted merge report"
                >
                  Report
                </button>
                <button
                  onClick={downloadJson}
                  className="px-2.5 py-1 text-xs rounded font-medium bg-zinc-700/50 text-zinc-300 border border-zinc-600/40 hover:bg-zinc-600/50 transition-colors"
                  title="Download raw merge data as JSON"
                >
                  JSON
                </button>
              </>
            )}
            {data && data.overview.entities.length >= 2 && (
              <button
                onClick={runCofaMerge}
                disabled={mergeRunning || !selectedEngagementId}
                className={`px-3 py-1 text-sm rounded font-medium transition-colors ${
                  mergeRunning
                    ? 'bg-amber-500/20 text-amber-400 border border-amber-500/30 cursor-wait'
                    : data.matches.has_matches
                      ? 'bg-amber-500/20 text-amber-400 border border-amber-500/30 hover:bg-amber-500/30'
                      : 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/30 hover:bg-emerald-500/30'
                } disabled:opacity-40 disabled:cursor-not-allowed`}
                title={
                  !selectedEngagementId
                    ? 'No engagement selected \u2014 select or create one first'
                    : data.matches.has_matches
                      ? 'Re-run will replace existing mappings'
                      : 'Trigger Maestra to unify COFA accounts'
                }
              >
                {mergeRunning ? 'Running...' : data.matches.has_matches ? 'Re-run' : 'Run COFA Merge'}
              </button>
            )}
            <label className="flex items-center gap-1.5 text-sm text-muted-foreground cursor-pointer">
              <input
                type="checkbox"
                checked={autoRefresh}
                onChange={(e) => setAutoRefresh(e.target.checked)}
                className="rounded border-border"
              />
              Auto-refresh 5s
            </label>
            <button
              onClick={() => { fetchMerge(true); fetchConflicts(); }}
              className="px-3 py-1 text-sm rounded bg-primary text-primary-foreground hover:bg-primary/90"
            >
              Refresh
            </button>
          </div>
        </div>

        {/* Progress bar */}
        {mergeRunning && (
          <div className="mt-1.5 flex items-center gap-2 text-sm text-amber-400">
            <div className="w-3.5 h-3.5 border-2 border-amber-400 border-t-transparent rounded-full animate-spin shrink-0" />
            <span className="tabular-nums font-mono">{mergeElapsed}s</span>
            {mergeStatus && <span className="text-xs">{mergeStatus}</span>}
          </div>
        )}
        {mergeError && (
          <div className="mt-1.5 rounded border border-red-500/20 bg-red-500/10 px-3 py-1.5">
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
          <details className="mt-1.5 rounded border border-amber-500/20 bg-amber-500/10 px-3 py-1.5">
            <summary className="text-sm text-amber-400 cursor-pointer">
              Maestra completed analysis but did not write results.
            </summary>
            <pre className="mt-2 text-xs text-muted-foreground whitespace-pre-wrap max-h-40 overflow-y-auto">
              {mergeCollapsedResponse}
            </pre>
          </details>
        )}
      </div>

      <div className="flex-1 overflow-y-auto min-h-0">
        <div className="max-w-[1100px] mx-auto p-4 space-y-4">
          {error && (
            <div className="rounded-lg border border-red-500/20 bg-red-500/10 p-3 text-center">
              <span className="text-base text-red-400">{error}</span>
              <button onClick={() => fetchMerge()} className="ml-3 px-3 py-1 text-sm rounded bg-primary text-primary-foreground hover:bg-primary/90">
                Retry
              </button>
            </div>
          )}

          {data && (
            <>
              {/* ================================================================
                  Entity Summary — compact cards
                  ================================================================ */}
              <div className="grid grid-cols-2 gap-3">
                {data.overview.entities.map((entity) => {
                  const isAcquirer = entity.entity_id === data.acquirer.entity_id;
                  const borderColor = isAcquirer ? 'border-blue-500/30' : 'border-purple-500/30';
                  const textColor = isAcquirer ? 'text-blue-400' : 'text-purple-400';
                  const label = isAcquirer ? 'Acquirer' : 'Target';
                  return (
                    <div key={entity.entity_id} className={`rounded-lg border ${borderColor} bg-card/20 p-3`}>
                      <div className="flex items-center justify-between mb-1">
                        <span className={`text-xs font-semibold uppercase tracking-wider ${textColor}`}>{label}</span>
                        <span className="text-sm font-semibold text-foreground">{entity.display_name}</span>
                      </div>
                      <div className="flex items-center gap-3 text-xs font-mono text-muted-foreground">
                        <span>
                          <span className="text-foreground font-semibold">{fmtNum(entity.cofa_count)}</span> triples
                        </span>
                        <span>
                          <span className="text-foreground font-semibold">{isAcquirer ? data.orphans.acquirer_coa_total : data.orphans.target_coa_total}</span> CoA accounts
                        </span>
                        {entity.last_ingest && (
                          <span>last: {fmtDate(entity.last_ingest)}</span>
                        )}
                      </div>
                    </div>
                  );
                })}
              </div>

              {/* ================================================================
                  Categorized Impact Summary
                  ================================================================ */}
              {conflictData && conflictData.category_summary && Object.keys(conflictData.category_summary.by_type).length > 0 && (
                <div className="rounded-lg border border-border bg-card/30 overflow-hidden">
                  <div className="px-4 py-2.5">
                    <span className="font-semibold uppercase tracking-wider text-muted-foreground text-sm">Financial Statement Impact</span>
                  </div>
                  <div className="border-t border-border/30 overflow-x-auto">
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="border-b border-border text-xs uppercase tracking-wider text-muted-foreground">
                          <th className="text-left px-3 py-2 font-medium">Category</th>
                          <th className="text-center px-3 py-2 font-medium">#</th>
                          <th className="text-right px-3 py-2 font-medium">Dollar Impact</th>
                          <th className="text-right px-3 py-2 font-medium">Revenue</th>
                          <th className="text-right px-3 py-2 font-medium">Expenses</th>
                          <th className="text-right px-3 py-2 font-medium">EBITDA</th>
                        </tr>
                      </thead>
                      <tbody>
                        {Object.entries(conflictData.category_summary.by_type).map(([type, cat]) => {
                          const isBucketExpanded = expandedBucket === type;
                          return (
                            <Fragment key={type}>
                              <tr
                                className="border-b border-border/10 hover:bg-card/20 cursor-pointer select-none"
                                onClick={() => setExpandedBucket(isBucketExpanded ? null : type)}
                              >
                                <td className="px-3 py-2 font-medium capitalize">
                                  <span className="inline-flex items-center gap-1.5">
                                    {chevron(isBucketExpanded)}
                                    {type}
                                  </span>
                                </td>
                                <td className="px-3 py-2 text-center font-mono">{cat.count}</td>
                                <td className="px-3 py-2 text-right font-mono">{fmtDollarImpact(cat.total_dollar_impact)}</td>
                                <td className={`px-3 py-2 text-right font-mono ${cat.revenue_impact !== 0 ? 'text-amber-400' : 'text-muted-foreground/40'}`}>
                                  {cat.revenue_impact !== 0 ? fmtDollarImpact(cat.revenue_impact) : '—'}
                                </td>
                                <td className={`px-3 py-2 text-right font-mono ${cat.expense_impact !== 0 ? 'text-amber-400' : 'text-muted-foreground/40'}`}>
                                  {cat.expense_impact !== 0 ? fmtDollarImpact(cat.expense_impact) : '—'}
                                </td>
                                <td className={`px-3 py-2 text-right font-mono ${cat.ebitda_impact !== 0 ? (cat.ebitda_impact > 0 ? 'text-emerald-400' : 'text-red-400') : 'text-muted-foreground/40'}`}>
                                  {cat.ebitda_impact !== 0 ? fmtDollarImpact(cat.ebitda_impact) : '—'}
                                </td>
                              </tr>
                              {isBucketExpanded && cat.conflict_details && cat.conflict_details.length > 0 && (
                                <tr className="bg-card/5">
                                  <td colSpan={6} className="px-0 py-0">
                                    <table className="w-full text-xs">
                                      <thead>
                                        <tr className="border-b border-border/20 text-[10px] uppercase tracking-wider text-muted-foreground">
                                          <th className="text-left px-3 py-1.5 font-medium">Description</th>
                                          <th className="text-center px-2 py-1.5 font-medium">Severity</th>
                                          <th className="text-right px-3 py-1.5 font-medium">Dollar Impact</th>
                                          <th className="text-right px-3 py-1.5 font-medium">Revenue</th>
                                          <th className="text-right px-3 py-1.5 font-medium">Expenses</th>
                                          <th className="text-right px-3 py-1.5 font-medium">EBITDA</th>
                                        </tr>
                                      </thead>
                                      <tbody>
                                        {cat.conflict_details.map(d => {
                                          const sevCls = d.severity === 'critical'
                                            ? 'bg-red-500/20 text-red-400 border-red-500/30'
                                            : d.severity === 'medium'
                                              ? 'bg-amber-500/20 text-amber-400 border-amber-500/30'
                                              : 'bg-zinc-500/20 text-zinc-400 border-zinc-500/30';
                                          return (
                                            <tr
                                              key={d.conflict_id}
                                              className="border-b border-border/5 hover:bg-card/10"
                                              title={`Acquirer: ${d.acquirer_treatment}\nTarget: ${d.target_treatment}${d.impact_area ? `\nImpact Area: ${d.impact_area}` : ''}`}
                                            >
                                              <td className="px-3 py-1.5 text-foreground/80 max-w-[280px] truncate" title={d.description}>
                                                <span className="flex items-center gap-1.5">
                                                  {statusBadge(d.resolution_status)}
                                                  {d.description || d.conflict_id}
                                                </span>
                                              </td>
                                              <td className="px-2 py-1.5 text-center">
                                                <span className={`inline-block px-1.5 py-0.5 rounded text-[10px] font-semibold border ${sevCls}`}>
                                                  {d.severity || '—'}
                                                </span>
                                              </td>
                                              <td className="px-3 py-1.5 text-right font-mono">
                                                {d.dollar_impact > 0 ? fmtDollarImpact(d.dollar_impact) : '—'}
                                              </td>
                                              <td className={`px-3 py-1.5 text-right font-mono ${d.revenue_impact !== 0 ? 'text-amber-400' : 'text-muted-foreground/40'}`}>
                                                {d.revenue_impact !== 0 ? fmtDollarImpact(d.revenue_impact) : '—'}
                                              </td>
                                              <td className={`px-3 py-1.5 text-right font-mono ${d.expense_impact !== 0 ? 'text-amber-400' : 'text-muted-foreground/40'}`}>
                                                {d.expense_impact !== 0 ? fmtDollarImpact(d.expense_impact) : '—'}
                                              </td>
                                              <td className={`px-3 py-1.5 text-right font-mono ${d.ebitda_impact !== 0 ? (d.ebitda_impact > 0 ? 'text-emerald-400' : 'text-red-400') : 'text-muted-foreground/40'}`}>
                                                {d.ebitda_impact !== 0 ? fmtDollarImpact(d.ebitda_impact) : '—'}
                                              </td>
                                            </tr>
                                          );
                                        })}
                                      </tbody>
                                    </table>
                                  </td>
                                </tr>
                              )}
                            </Fragment>
                          );
                        })}
                        <tr className="border-t border-border font-semibold bg-card/10">
                          <td className="px-3 py-2">Combined</td>
                          <td className="px-3 py-2 text-center font-mono">{conflictData.summary.total}</td>
                          <td className="px-3 py-2 text-right font-mono">
                            {fmtDollarImpact(Object.values(conflictData.category_summary.by_type).reduce((s, c) => s + c.total_dollar_impact, 0))}
                          </td>
                          <td className={`px-3 py-2 text-right font-mono ${conflictData.category_summary.combined_impact.revenue !== 0 ? 'text-amber-400' : 'text-muted-foreground/40'}`}>
                            {conflictData.category_summary.combined_impact.revenue !== 0 ? fmtDollarImpact(conflictData.category_summary.combined_impact.revenue) : '—'}
                          </td>
                          <td className={`px-3 py-2 text-right font-mono ${conflictData.category_summary.combined_impact.expenses !== 0 ? 'text-amber-400' : 'text-muted-foreground/40'}`}>
                            {conflictData.category_summary.combined_impact.expenses !== 0 ? fmtDollarImpact(conflictData.category_summary.combined_impact.expenses) : '—'}
                          </td>
                          <td className={`px-3 py-2 text-right font-mono ${conflictData.category_summary.combined_impact.ebitda !== 0 ? (conflictData.category_summary.combined_impact.ebitda > 0 ? 'text-emerald-400' : 'text-red-400') : 'text-muted-foreground/40'}`}>
                            {conflictData.category_summary.combined_impact.ebitda !== 0 ? fmtDollarImpact(conflictData.category_summary.combined_impact.ebitda) : '—'}
                          </td>
                        </tr>
                      </tbody>
                    </table>
                  </div>

                  {/* Expense Reclassification Detail */}
                  {Object.values(conflictData.category_summary.by_type).some(cat => cat.reclassifications.length > 0) && (
                    <>
                      <div className="px-4 py-2 border-t border-border/30">
                        <span className="font-semibold uppercase tracking-wider text-muted-foreground text-xs">Expense Reclassification Detail</span>
                      </div>
                      <div className="overflow-x-auto">
                        <table className="w-full text-sm">
                          <thead>
                            <tr className="border-b border-border text-xs uppercase tracking-wider text-muted-foreground">
                              <th className="text-left px-3 py-2 font-medium">Conflict</th>
                              <th className="text-left px-3 py-2 font-medium">Description</th>
                              <th className="text-left px-3 py-2 font-medium">From</th>
                              <th className="text-center px-3 py-2 font-medium">→</th>
                              <th className="text-left px-3 py-2 font-medium">To</th>
                              <th className="text-right px-3 py-2 font-medium">Amount</th>
                            </tr>
                          </thead>
                          <tbody>
                            {Object.values(conflictData.category_summary.by_type)
                              .flatMap(cat => cat.reclassifications)
                              .map(r => (
                                <tr key={r.conflict_id} className="border-b border-border/10 hover:bg-card/20">
                                  <td className="px-3 py-2 font-mono text-xs">{r.conflict_id}</td>
                                  <td className="px-3 py-2 text-muted-foreground">{r.description}</td>
                                  <td className="px-3 py-2">
                                    <span className="px-1.5 py-0.5 rounded text-xs bg-red-500/10 text-red-400 border border-red-500/20">{r.from_category}</span>
                                  </td>
                                  <td className="px-3 py-2 text-center text-muted-foreground">→</td>
                                  <td className="px-3 py-2">
                                    <span className="px-1.5 py-0.5 rounded text-xs bg-blue-500/10 text-blue-400 border border-blue-500/20">{r.to_category}</span>
                                  </td>
                                  <td className="px-3 py-2 text-right font-mono">{fmtDollarImpact(r.amount)}</td>
                                </tr>
                              ))}
                          </tbody>
                        </table>
                      </div>
                    </>
                  )}
                </div>
              )}

              {/* ================================================================
                  Conflict Resolution Queue
                  ================================================================ */}
              {conflictData && conflictData.conflicts.length > 0 && (
                <div className="rounded-lg border border-border bg-card/30 overflow-hidden">
                  <button
                    onClick={() => setConflictsOpen(!conflictsOpen)}
                    className="w-full flex items-center gap-2 px-4 py-2.5 text-sm hover:bg-card/20 transition-colors"
                  >
                    {chevron(conflictsOpen)}
                    <span className="font-semibold uppercase tracking-wider text-muted-foreground text-sm">Conflict Resolution</span>
                    <span className="text-muted-foreground/70 font-mono text-xs">
                      {conflictSummary?.total} total | {conflictSummary?.resolved} resolved | {conflictSummary?.pending} pending
                      {withoutImpact > 0 && ` | ${withoutImpact} not estimated`}
                    </span>
                  </button>
                  {conflictsOpen && (
                    <div className="border-t border-border/30">
                      {/* Batch controls */}
                      {batchSelected.size > 0 && (
                        <div className="flex items-center gap-2 px-4 py-2 bg-card/10 border-b border-border/20">
                          <span className="text-xs text-muted-foreground">{batchSelected.size} selected</span>
                          <select
                            value={batchResolution}
                            onChange={e => setBatchResolution(e.target.value)}
                            className="px-2 py-1 text-xs rounded border border-border bg-background"
                          >
                            <option value="acquirer">Use Acquirer</option>
                            <option value="target">Use Target</option>
                            <option value="keep_both">Keep Both</option>
                            <option value="post_close">Post-Close</option>
                          </select>
                          <button
                            onClick={batchResolve}
                            disabled={batchResolving}
                            className="px-2.5 py-1 text-xs rounded font-medium bg-emerald-500/20 text-emerald-400 border border-emerald-500/30 hover:bg-emerald-500/30 disabled:opacity-40"
                          >
                            {batchResolving ? 'Resolving...' : 'Batch Approve'}
                          </button>
                          <button
                            onClick={() => setBatchSelected(new Set())}
                            className="px-2 py-1 text-xs text-muted-foreground hover:text-foreground"
                          >
                            Clear
                          </button>
                        </div>
                      )}

                      <div className="overflow-x-auto">
                        <table className="w-full text-sm">
                          <thead>
                            <tr className="border-b border-border text-xs uppercase tracking-wider text-muted-foreground">
                              <th className="px-2 py-2 w-8">
                                <input
                                  type="checkbox"
                                  checked={batchSelected.size === conflictData.conflicts.filter(c => c.resolution_status !== 'resolved').length && batchSelected.size > 0}
                                  onChange={e => {
                                    if (e.target.checked) {
                                      setBatchSelected(new Set(conflictData.conflicts.filter(c => c.resolution_status !== 'resolved').map(c => c.conflict_id)));
                                    } else {
                                      setBatchSelected(new Set());
                                    }
                                  }}
                                  className="rounded"
                                />
                              </th>
                              <th className="text-left px-2 py-2 font-medium">Type</th>
                              <th className="text-left px-2 py-2 font-medium">Description</th>
                              <th className="text-right px-2 py-2 font-medium">Annual Impact</th>
                              <th className="text-left px-2 py-2 font-medium">Status</th>
                              <th className="text-left px-2 py-2 font-medium w-20">Action</th>
                            </tr>
                          </thead>
                          <tbody>
                            {conflictData.conflicts.map((c) => {
                              const isExpanded = expandedConflictId === c.conflict_id;
                              const isResolved = c.resolution_status === 'resolved';
                              return (
                                <Fragment key={c.conflict_id}>
                                  <tr className={`border-t border-border/30 hover:bg-card/20 transition-colors ${isResolved ? 'opacity-60' : ''}`}>
                                    <td className="px-2 py-1.5">
                                      {!isResolved && (
                                        <input
                                          type="checkbox"
                                          checked={batchSelected.has(c.conflict_id)}
                                          onChange={e => {
                                            const next = new Set(batchSelected);
                                            if (e.target.checked) next.add(c.conflict_id);
                                            else next.delete(c.conflict_id);
                                            setBatchSelected(next);
                                          }}
                                          className="rounded"
                                        />
                                      )}
                                    </td>
                                    <td className="px-2 py-1.5">
                                      <span className="text-xs font-mono text-foreground/80">{c.conflict_type}</span>
                                    </td>
                                    <td className="px-2 py-1.5 text-foreground max-w-[300px] truncate" title={c.description}>
                                      {c.description || c.conflict_id}
                                    </td>
                                    <td className="px-2 py-1.5 text-right font-mono">
                                      {c.dollar_impact > 0
                                        ? <span className="text-foreground">{fmtDollarImpact(c.dollar_impact)}</span>
                                        : <span className="text-muted-foreground/50 text-xs">Not estimated</span>
                                      }
                                    </td>
                                    <td className="px-2 py-1.5">{statusBadge(c.resolution_status)}</td>
                                    <td className="px-2 py-1.5">
                                      <button
                                        onClick={() => setExpandedConflictId(isExpanded ? null : c.conflict_id)}
                                        className="px-2 py-0.5 text-xs rounded bg-primary/10 text-primary hover:bg-primary/20 transition-colors"
                                      >
                                        {isExpanded ? 'Close' : 'Review'}
                                      </button>
                                    </td>
                                  </tr>
                                  {isExpanded && (
                                    <tr className="border-t border-border/10 bg-card/5">
                                      <td colSpan={6} className="px-4 py-3">
                                        <div className="space-y-3">
                                          {/* Treatment comparison */}
                                          <div className="grid grid-cols-2 gap-3">
                                            <div className="rounded border border-blue-500/20 p-2">
                                              <div className="text-xs font-semibold text-blue-400 mb-1">Acquirer Treatment</div>
                                              <div className="text-sm text-foreground/80">{c.acquirer_treatment || '-'}</div>
                                            </div>
                                            <div className="rounded border border-purple-500/20 p-2">
                                              <div className="text-xs font-semibold text-purple-400 mb-1">Target Treatment</div>
                                              <div className="text-sm text-foreground/80">{c.target_treatment || '-'}</div>
                                            </div>
                                          </div>

                                          {/* Resolution form */}
                                          {isResolved ? (
                                            <div className="text-xs text-muted-foreground space-y-1">
                                              <div>Resolution: <span className="text-foreground">{c.resolution}</span></div>
                                              <div>By: <span className="text-foreground">{c.resolved_by}</span> at {c.resolved_at ? fmtDate(c.resolved_at) : '-'}</div>
                                              {c.resolution_notes && <div>Notes: <span className="text-foreground">{c.resolution_notes}</span></div>}
                                            </div>
                                          ) : (
                                            <div className="space-y-2">
                                              <div className="flex flex-wrap gap-3">
                                                {(['acquirer', 'target', 'keep_both', 'post_close'] as const).map(opt => (
                                                  <label key={opt} className="flex items-center gap-1.5 text-sm cursor-pointer">
                                                    <input
                                                      type="radio"
                                                      name={`resolution-${c.conflict_id}`}
                                                      value={opt}
                                                      checked={resolutionDrafts[c.conflict_id]?.resolution === opt}
                                                      onChange={() => setResolutionDrafts(prev => ({
                                                        ...prev,
                                                        [c.conflict_id]: { ...prev[c.conflict_id], resolution: opt, notes: prev[c.conflict_id]?.notes || '' },
                                                      }))}
                                                      className="rounded"
                                                    />
                                                    <span className="text-foreground/80">
                                                      {opt === 'acquirer' ? 'Use Acquirer' : opt === 'target' ? 'Use Target' : opt === 'keep_both' ? 'Keep Both' : 'Post-Close'}
                                                    </span>
                                                  </label>
                                                ))}
                                              </div>
                                              <textarea
                                                placeholder="Notes (optional)"
                                                value={resolutionDrafts[c.conflict_id]?.notes || ''}
                                                onChange={e => setResolutionDrafts(prev => ({
                                                  ...prev,
                                                  [c.conflict_id]: { ...prev[c.conflict_id], resolution: prev[c.conflict_id]?.resolution || '', notes: e.target.value },
                                                }))}
                                                className="w-full px-2 py-1.5 text-sm rounded border border-border bg-background resize-none"
                                                rows={2}
                                              />
                                              <button
                                                onClick={() => resolveConflict(c.conflict_id)}
                                                disabled={!resolutionDrafts[c.conflict_id]?.resolution || resolvingId === c.conflict_id}
                                                className="px-3 py-1 text-sm rounded font-medium bg-emerald-500/20 text-emerald-400 border border-emerald-500/30 hover:bg-emerald-500/30 disabled:opacity-40 disabled:cursor-not-allowed"
                                              >
                                                {resolvingId === c.conflict_id ? 'Saving...' : 'Save Decision'}
                                              </button>
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
                    </div>
                  )}
                </div>
              )}

              {/* ================================================================
                  Account Mapping (Resolution Matches)
                  ================================================================ */}
              <div className="rounded-lg border border-border bg-card/30 overflow-hidden">
                <button
                  onClick={() => setMatchesOpen(!matchesOpen)}
                  className="w-full flex items-center gap-2 px-4 py-2.5 text-sm hover:bg-card/20 transition-colors"
                >
                  {chevron(matchesOpen)}
                  <span className="font-semibold uppercase tracking-wider text-muted-foreground text-sm">Account Mapping</span>
                  <span className="text-muted-foreground/70 font-mono text-xs">
                    {data.matches.has_matches ? `${data.matches.rows.length} matched` : 'none'}
                    {data.orphans.show_section && ` | ${data.orphans.acquirer_unmatched_count + data.orphans.target_unmatched_count} unmapped`}
                  </span>
                </button>
                {matchesOpen && (
                  <div className="border-t border-border/30">
                    {data.matches.has_matches ? (
                      <div className="overflow-x-auto">
                        <table className="w-full text-sm">
                          <thead>
                            <tr className="border-b border-border text-xs uppercase tracking-wider text-muted-foreground">
                              <th className="text-left px-3 py-2 font-medium">Acquirer Account</th>
                              <th className="text-left px-3 py-2 font-medium">Target Account</th>
                              <th className="text-left px-3 py-2 font-medium">Confidence</th>
                              <th className="text-left px-3 py-2 font-medium">Method</th>
                            </tr>
                          </thead>
                          <tbody>
                            {data.matches.rows.map((m, i) => (
                              <tr key={i} className="border-t border-border/30 hover:bg-card/20 transition-colors">
                                <td className="px-3 py-1.5 font-mono text-blue-400">
                                  {m.acquirer_concept.replace('cofa_mapping.', '')}
                                </td>
                                <td className="px-3 py-1.5 font-mono text-purple-400">
                                  {m.target_concept.replace('cofa_mapping.', '')}
                                </td>
                                <td className="px-3 py-1.5">{confidenceBadge(m.resolution_confidence)}</td>
                                <td className="px-3 py-1.5 text-muted-foreground">{m.resolution_method || '-'}</td>
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

                    {/* Orphans / coverage inline */}
                    {data.orphans.show_section && (
                      <div className="border-t border-border/20">
                        <button
                          onClick={() => setOrphansOpen(!orphansOpen)}
                          className="w-full flex items-center gap-2 px-4 py-2 text-sm hover:bg-card/20 transition-colors"
                        >
                          {chevron(orphansOpen)}
                          <span className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Coverage</span>
                          <span className="text-muted-foreground/70 font-mono text-xs">
                            {data.orphans.acquirer_unmatched_count + data.orphans.target_unmatched_count} unmapped
                          </span>
                        </button>
                        {orphansOpen && (
                          <div className="grid grid-cols-2 divide-x divide-border/30 border-t border-border/20">
                            <div className="p-3">
                              <div className="text-xs font-semibold uppercase tracking-wider text-blue-400 mb-1">
                                {data.acquirer.display_name}
                              </div>
                              <span className="font-mono text-xs text-foreground/80">
                                {data.orphans.acquirer_mapped}/{data.orphans.acquirer_coa_total} mapped
                              </span>
                            </div>
                            <div className="p-3">
                              <div className="text-xs font-semibold uppercase tracking-wider text-purple-400 mb-1">
                                {data.target.display_name}
                              </div>
                              <span className="font-mono text-xs text-foreground/80">
                                {data.orphans.target_mapped}/{data.orphans.target_coa_total} mapped
                              </span>
                            </div>
                          </div>
                        )}
                      </div>
                    )}
                  </div>
                )}
              </div>

            </>
          )}
        </div>
      </div>
    </div>
  );
}
