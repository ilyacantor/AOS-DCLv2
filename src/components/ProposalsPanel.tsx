import { useEffect, useRef, useState } from 'react';

// ── types ────────────────────────────────────────────────────────────────────

interface Proposal {
  proposal_id: string;
  tenant_id: string;
  entity_id: string | null;
  proposal_type: string;
  natural_key: string;
  payload: Record<string, unknown>;
  confidence: number;
  provenance: Record<string, unknown>;
  status: 'pending' | 'approved' | 'rejected';
  created_at: string;
  decided_at: string | null;
  decided_by: string | null;
  decision_note: string | null;
  canonical_artifact_id: string | null;
}

interface AuthorityEntry {
  concept_prefix: string;
  ranked_sources: string[];
}

type StatusFilter = 'pending' | 'approved' | 'rejected';

// ── helpers ──────────────────────────────────────────────────────────────────

function payloadSummary(ptype: string, payload: Record<string, unknown>): string {
  const s = (v: unknown) => String(v ?? '?');
  switch (ptype) {
    case 'authority_map':
      return `${s(payload.concept_prefix)}: ${((payload.ranked_sources as string[]) ?? []).join(' > ')}`;
    case 'vocabulary_alias':
      return `${s(payload.alias)} → ${s(payload.concept_id)}`;
    case 'conflict_candidate':
      return `${s(payload.concept)}.${s(payload.property)}${payload.period ? ` · ${s(payload.period)}` : ''}`;
    case 'org_hierarchy':
      return `${s(payload.dimension)}`;
    case 'management_overlay':
      return `${s(payload.board_segment)} → ${((payload.maps_to as string[]) ?? []).join(', ')}`;
    case 'priority_query':
      return `${s(payload.query_label)}`;
    case 'structural_drift': {
      const added = (payload.added as Array<{concept: string; property: string}>) ?? [];
      const removed = (payload.removed as Array<{concept: string; property: string}>) ?? [];
      const addParts = added.slice(0, 3).map(a => `+${a.concept}.${a.property}`);
      const remParts = removed.slice(0, 3).map(r => `-${r.concept}.${r.property}`);
      const more = (added.length + removed.length > 6) ? ' …' : '';
      return [...addParts, ...remParts].join(', ') + more || 'no delta';
    }
    case 'value_drift': {
      const claims = (payload.claims as Array<{source_system: string; value?: unknown}>) ?? [];
      const trend = payload.trend as {prior_count: number; current_count: number} | undefined;
      const key = `${s(payload.concept)}.${s(payload.property)}${payload.period ? ` (${s(payload.period)})` : ''}`;
      const claimStr = claims.map(c => `${c.source_system}:${c.value}`).join(' vs ');
      const trendStr = trend ? ` trend:${trend.prior_count}→${trend.current_count}` : '';
      return `${key} — ${claimStr}${trendStr}`;
    }
    default:
      return JSON.stringify(payload).slice(0, 80);
  }
}

function typeBadgeCls(ptype: string): string {
  switch (ptype) {
    case 'authority_map':     return 'bg-blue-500/15 text-blue-400';
    case 'vocabulary_alias':  return 'bg-purple-500/15 text-purple-400';
    case 'conflict_candidate':return 'bg-red-500/15 text-red-400';
    case 'org_hierarchy':     return 'bg-green-500/15 text-green-400';
    case 'management_overlay':return 'bg-amber-500/15 text-amber-500';
    case 'priority_query':    return 'bg-cyan-500/15 text-cyan-400';
    case 'structural_drift':  return 'bg-orange-500/15 text-orange-400';
    case 'value_drift':       return 'bg-rose-500/15 text-rose-400';
    default:                  return 'bg-muted text-muted-foreground';
  }
}

function ProvenanceBadge({ prov }: { prov: Record<string, unknown> }) {
  const basis = String(prov.basis ?? '');
  if (basis === 'confirmed') {
    const by = String(prov.confirmed_by ?? '');
    return (
      <span
        className="px-1.5 py-0.5 rounded bg-green-500/15 text-green-400 text-xs whitespace-nowrap"
        data-testid="provenance-badge"
      >
        confirmed by {by || '?'}
      </span>
    );
  }
  return (
    <span
      className="px-1.5 py-0.5 rounded bg-muted text-muted-foreground text-xs whitespace-nowrap"
      data-testid="provenance-badge"
    >
      inferred
    </span>
  );
}

function StatusChip({ status }: { status: string }) {
  const cls =
    status === 'pending'  ? 'bg-amber-500/15 text-amber-500' :
    status === 'approved' ? 'bg-green-500/15 text-green-500' :
                            'bg-muted text-muted-foreground';
  return <span className={`px-1.5 py-0.5 rounded text-xs whitespace-nowrap ${cls}`}>{status}</span>;
}

// ── main panel ───────────────────────────────────────────────────────────────

export function ProposalsPanel({ entityId }: { entityId: string }) {
  const [open, setOpen] = useState(false);
  const [statusFilter, setStatusFilter] = useState<StatusFilter>('pending');
  const [proposals, setProposals] = useState<Proposal[]>([]);
  const [totalCount, setTotalCount] = useState<number | null>(null);   // null = loading
  const [pendingCount, setPendingCount] = useState<number | null>(null);
  const [tenantId, setTenantId] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [expanded, setExpanded] = useState<string | null>(null);

  // Decision controls — shared across the open panel, one active decision at a time.
  const [decidedBy, setDecidedBy] = useState('operator');
  const [note, setNote] = useState('');
  const [busy, setBusy] = useState(false);
  const [decideError, setDecideError] = useState<string | null>(null);

  // Authority map
  const [authorityMap, setAuthorityMap] = useState<AuthorityEntry[] | null>(null);
  const [authLoading, setAuthLoading] = useState(false);

  const fetchSeq = useRef(0);
  // Tracks the latest statusFilter for async callbacks (decide()) that outlive
  // the render that created them — avoids the stale-closure race where the
  // decide POST completes after the operator switches the filter.
  const statusFilterRef = useRef<StatusFilter>('pending');
  statusFilterRef.current = statusFilter;

  // ── data fetches ────────────────────────────────────────────────────────────

  const fetchProposals = async (status: StatusFilter) => {
    const seq = ++fetchSeq.current;
    setLoading(true);
    setErr(null);
    try {
      const params = new URLSearchParams({ entity_id: entityId, status, limit: '100' });
      const res = await fetch(`/api/dcl/proposals?${params}`);
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw new Error(`Change proposals: HTTP ${res.status} — ${(body as { detail?: string }).detail ?? ''}`);
      }
      const body = await res.json();
      if (seq !== fetchSeq.current) return;
      setProposals(body.proposals ?? []);
      setTotalCount(body.total_count ?? (body.proposals?.length ?? 0));
      if (body.tenant_id && !tenantId) setTenantId(body.tenant_id);
    } catch (e) {
      if (seq === fetchSeq.current)
        setErr(e instanceof Error ? e.message : 'Failed to load proposals');
    } finally {
      if (seq === fetchSeq.current) setLoading(false);
    }
  };

  const fetchPendingCount = async () => {
    try {
      const params = new URLSearchParams({ entity_id: entityId, status: 'pending', limit: '1' });
      const res = await fetch(`/api/dcl/proposals?${params}`);
      if (!res.ok) return;
      const body = await res.json();
      setPendingCount(body.total_count ?? 0);
      if (body.tenant_id && !tenantId) setTenantId(body.tenant_id);
    } catch {
      // badge fetch is non-blocking
    }
  };

  const fetchAuthorityMap = async () => {
    setAuthLoading(true);
    try {
      const res = await fetch(`/api/dcl/conflicts/authority-map?entity_id=${encodeURIComponent(entityId)}`);
      if (!res.ok) return;
      const body = await res.json();
      setAuthorityMap(body.authority_map ?? []);
    } catch {
      // non-blocking
    } finally {
      setAuthLoading(false);
    }
  };

  // Reset + re-fetch when entity changes.
  useEffect(() => {
    setPendingCount(null);
    setTotalCount(null);
    setProposals([]);
    setTenantId(null);
    setAuthorityMap(null);
    setExpanded(null);
    setErr(null);
    setDecideError(null);
    fetchPendingCount();
    if (open) {
      fetchProposals(statusFilter);
      fetchAuthorityMap();
    }
  }, [entityId]); // eslint-disable-line react-hooks/exhaustive-deps

  // Load on open.
  useEffect(() => {
    if (open) {
      fetchProposals(statusFilter);
      fetchAuthorityMap();
    }
    setExpanded(null);
    setDecideError(null);
  }, [open]); // eslint-disable-line react-hooks/exhaustive-deps

  // Reload when filter changes.
  useEffect(() => {
    if (open) fetchProposals(statusFilter);
    setExpanded(null);
    setDecideError(null);
  }, [statusFilter]); // eslint-disable-line react-hooks/exhaustive-deps

  // ── decide action ────────────────────────────────────────────────────────────

  const decide = async (proposal: Proposal, decision: 'approve' | 'reject') => {
    const tid = tenantId;
    if (!tid) { setDecideError('Tenant not resolved — re-open the panel.'); return; }
    if (!decidedBy.trim()) { setDecideError('decided_by is required.'); return; }
    setBusy(true);
    setDecideError(null);
    try {
      const res = await fetch(`/api/dcl/proposals/${proposal.proposal_id}/decide`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          tenant_id: tid,
          decision,
          decided_by: decidedBy.trim(),
          note: note.trim() || undefined,
        }),
      });
      const body = await res.json().catch(() => ({}));
      if (!res.ok) {
        throw new Error((body as { detail?: string }).detail ?? `HTTP ${res.status}`);
      }
      await fetchProposals(statusFilterRef.current);
      await fetchPendingCount();
      if (decision === 'approve') await fetchAuthorityMap();
    } catch (e) {
      setDecideError(e instanceof Error ? e.message : 'Decision failed');
    } finally {
      setBusy(false);
    }
  };

  // ── render ───────────────────────────────────────────────────────────────────

  const badgeCls = pendingCount !== null && pendingCount > 0
    ? 'bg-amber-500/20 text-amber-500'
    : 'bg-muted text-muted-foreground';

  return (
    <div className="shrink-0 border rounded overflow-hidden" data-testid="proposals-panel">
      {/* Toggle header */}
      <button
        onClick={() => setOpen(!open)}
        data-testid="proposals-toggle"
        className="w-full flex items-center justify-between px-3 py-2 bg-muted/50 hover:bg-accent text-xs font-medium"
      >
        <span>
          Change Proposals
          <span className={`ml-2 px-1.5 py-0.5 rounded ${badgeCls}`} data-testid="proposals-pending-count">
            {pendingCount === null ? '…' : `${pendingCount} pending`}
          </span>
        </span>
        <span className="text-muted-foreground">{open ? 'Hide' : 'Review'}</span>
      </button>

      {open && (
        <div className="border-t">
          {/* Status filter bar */}
          <div className="flex items-center gap-2 px-3 py-1.5 border-b bg-card/50 text-xs">
            {(['pending', 'approved', 'rejected'] as const).map((s) => (
              <button
                key={s}
                data-testid={`proposals-status-filter-${s}`}
                onClick={() => setStatusFilter(s)}
                className={`px-2 py-0.5 rounded border text-xs transition-colors ${
                  statusFilter === s
                    ? 'bg-primary text-primary-foreground border-primary'
                    : 'border-border hover:bg-accent'
                }`}
              >
                {s}
              </button>
            ))}
            <span className="ml-auto text-muted-foreground">
              {loading ? '…' : totalCount !== null ? `${totalCount} total` : ''}
            </span>
          </div>

          {/* Error displays */}
          {err && (
            <div className="px-3 py-2 text-xs text-destructive border-b" data-testid="proposals-list-error">
              {err}
            </div>
          )}
          {decideError && (
            <div className="px-3 py-2 text-xs text-destructive border-b" data-testid="proposals-decide-error">
              {decideError}
            </div>
          )}

          {/* Proposals list */}
          <div className="max-h-64 overflow-y-auto">
            {proposals.length === 0 && !loading && !err && (
              <div className="px-3 py-4 text-xs text-center text-muted-foreground">
                No {statusFilter} proposals for this entity.
              </div>
            )}
            {proposals.map((p) => (
              <div
                key={p.proposal_id}
                className="border-b"
                data-testid={`proposals-proposal-row-${p.proposal_type}-${p.natural_key}`}
              >
                {/* Row summary */}
                <button
                  onClick={() => setExpanded(expanded === p.proposal_id ? null : p.proposal_id)}
                  className="w-full grid grid-cols-[90px_1fr_40px_auto_auto] items-center gap-2 px-3 py-1.5 text-xs hover:bg-accent/50 text-left"
                >
                  <span className={`px-1.5 py-0.5 rounded text-center truncate ${typeBadgeCls(p.proposal_type)}`}>
                    {p.proposal_type.replace(/_/g, '·')}
                  </span>
                  <span className="font-medium truncate" data-testid={`proposals-payload-summary-${p.natural_key}`}>
                    {payloadSummary(p.proposal_type, p.payload)}
                  </span>
                  <span className="text-right text-muted-foreground" data-testid={`proposals-confidence-${p.natural_key}`}>
                    {(p.confidence * 100).toFixed(0)}%
                  </span>
                  <ProvenanceBadge prov={p.provenance} />
                  <StatusChip status={p.status} />
                </button>

                {/* Expanded detail */}
                {expanded === p.proposal_id && (
                  <div
                    className="px-3 py-2 bg-card/30 border-t text-xs space-y-2"
                    data-testid="proposals-proposal-detail"
                  >
                    {/* Payload */}
                    <pre className="font-mono bg-muted/30 rounded p-2 text-xs overflow-x-auto whitespace-pre-wrap">
                      {JSON.stringify(p.payload, null, 2)}
                    </pre>

                    {/* Provenance + session id */}
                    <div className="text-muted-foreground space-y-0.5">
                      <div>Basis: <b>{String(p.provenance.basis ?? '—')}</b>
                        {Boolean(p.provenance.confirmed_by) && <> · confirmed by <b>{String(p.provenance.confirmed_by)}</b></>}
                      </div>
                      {Boolean(p.provenance.onboarding_session_id) && (
                        <div>
                          Session: <span className="font-mono" data-testid="proposals-session-id">
                            {String(p.provenance.onboarding_session_id)}
                          </span>
                        </div>
                      )}
                    </div>

                    {/* Decision trace (if decided) */}
                    {p.status !== 'pending' && (
                      <div className="space-y-0.5">
                        {p.canonical_artifact_id && (
                          <div className="text-green-500" data-testid="proposals-canonical-id">
                            Canonical: <span className="font-mono">{p.canonical_artifact_id}</span>
                          </div>
                        )}
                        {p.decided_by && (
                          <div className="text-muted-foreground">
                            {p.status === 'approved' ? 'Approved' : 'Rejected'} by <b>{p.decided_by}</b>
                            {p.decided_at && ` · ${new Date(p.decided_at).toLocaleString()}`}
                            {p.decision_note && ` — "${p.decision_note}"`}
                          </div>
                        )}
                      </div>
                    )}

                    {/* Decision controls for pending proposals */}
                    {p.status === 'pending' && (
                      <div className="space-y-1.5 pt-1 border-t border-border/40">
                        <div className="flex gap-2">
                          <input
                            value={decidedBy}
                            onChange={(e) => setDecidedBy(e.target.value)}
                            placeholder="Decided by"
                            data-testid="proposals-decided-by"
                            className="w-32 px-2 py-1 rounded border bg-background"
                          />
                          <input
                            value={note}
                            onChange={(e) => setNote(e.target.value)}
                            placeholder="Note (optional)"
                            data-testid="proposals-note"
                            className="flex-1 px-2 py-1 rounded border bg-background"
                          />
                        </div>
                        <div className="flex gap-2">
                          <button
                            disabled={busy || !decidedBy.trim()}
                            onClick={() => decide(p, 'approve')}
                            data-testid={`proposals-approve-btn-${p.natural_key}`}
                            className="px-3 py-1 rounded bg-green-600/20 text-green-400 border border-green-600/40 hover:bg-green-600/30 disabled:opacity-40"
                          >
                            Approve
                          </button>
                          <button
                            disabled={busy || !decidedBy.trim()}
                            onClick={() => decide(p, 'reject')}
                            data-testid={`proposals-reject-btn-${p.natural_key}`}
                            className="px-3 py-1 rounded bg-red-600/20 text-red-400 border border-red-600/40 hover:bg-red-600/30 disabled:opacity-40"
                          >
                            Reject
                          </button>
                        </div>
                      </div>
                    )}
                  </div>
                )}
              </div>
            ))}
          </div>

          {/* Authority map — always visible when panel is open; refreshes on approve */}
          <div className="border-t bg-muted/20" data-testid="authority-map-section">
            <div className="flex items-center justify-between px-3 py-1.5 text-xs font-medium border-b border-border/40">
              <span>Authority Map</span>
              <button
                onClick={fetchAuthorityMap}
                disabled={authLoading}
                className="text-xs text-muted-foreground hover:text-foreground disabled:opacity-50"
              >
                {authLoading ? '…' : 'Refresh'}
              </button>
            </div>
            {authorityMap === null ? (
              <div className="px-3 py-2 text-xs text-muted-foreground">…</div>
            ) : authorityMap.length === 0 ? (
              <div className="px-3 py-2 text-xs text-muted-foreground">No authority entries for this tenant.</div>
            ) : (
              <div className="px-3 py-2 space-y-0.5 max-h-32 overflow-y-auto">
                {authorityMap.map((entry) => (
                  <div
                    key={entry.concept_prefix}
                    className="flex items-center gap-2 text-xs"
                    data-testid={`authority-entry-${entry.concept_prefix}`}
                  >
                    <span className="font-medium w-32 truncate">{entry.concept_prefix}</span>
                    <span className="text-muted-foreground">
                      {entry.ranked_sources.join(' > ')}
                    </span>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
