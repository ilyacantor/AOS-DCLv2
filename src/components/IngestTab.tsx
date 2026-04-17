import { useEffect, useState, useMemo } from 'react';
import { EntitySelector, EntityInfo } from './RunSelector';

interface IngestLogEntry {
  id: string;
  dcl_ingest_id: string;
  entity_id: string | null;
  tenant_id: string;
  triples_received: number;
  triples_written: number;
  triples_rejected: number;
  rejection_reasons: Array<{ concept?: string; reason?: string }>;
  source_systems: string[];
  duration_ms: number;
  created_at: string;
}

interface RefreshIngested {
  entity_id: string;
  tenant_id: string;
  farm_manifest_id: string;
  dcl_ingest_id: string | null;
  triples_written: number | null;
  farm_timestamp: string | null;
}

interface RefreshSkipped {
  entity_id: string;
  tenant_id: string | null;
  farm_manifest_id: string | null;
  reason: string;
}

interface RefreshEvicted {
  tenant_id: string;
  entity_id: string;
}

interface RefreshResponse {
  ingested: RefreshIngested[];
  skipped: RefreshSkipped[];
  evicted_sample: RefreshEvicted[];
  evicted_total: number;
  message: string;
}

interface IngestTabProps {
  entities: EntityInfo[];
  selectedEntityId: string;
  onEntityChange: (id: string) => void;
  entitiesLoading?: boolean;
  entitiesError?: string | null;
  refetchEntities: () => Promise<void>;
}

export function IngestTab({ entities, selectedEntityId, onEntityChange, entitiesLoading, entitiesError, refetchEntities }: IngestTabProps) {
  const [logs, setLogs] = useState<IngestLogEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [expandedRow, setExpandedRow] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState(false);
  const [refreshResult, setRefreshResult] = useState<RefreshResponse | null>(null);

  // Post–store-rebuild there is no "store-wide" count to display: the UI
  // works on one (tenant, entity) selection at a time. Read the selected
  // entity's triple_count straight from the entities list so the label
  // matches what the Context and Dashboard tabs show for the same entity.
  const selectedEntity = useMemo(
    () => entities.find((e) => e.entity_id === selectedEntityId) ?? null,
    [entities, selectedEntityId],
  );
  const entityTripleCount = selectedEntity?.triple_count ?? 0;
  const entityLabel = selectedEntity?.entity_id ?? 'entity';

  const fetchData = async (entityId?: string) => {
    setLoading(true);
    setError(null);
    try {
      const logParams = new URLSearchParams({ limit: '50' });
      if (entityId) logParams.set('entity_id', entityId);

      const logRes = await fetch(`/api/dcl/ingest-log?${logParams}`);
      if (!logRes.ok) throw new Error(`Ingest log: HTTP ${logRes.status}`);
      setLogs(await logRes.json());
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to fetch ingest data');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { fetchData(selectedEntityId || undefined); }, [selectedEntityId]);

  const handleRefresh = async () => {
    setRefreshing(true);
    setError(null);
    setRefreshResult(null);
    try {
      const res = await fetch('/api/dcl/refresh-from-farm', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
      });
      if (!res.ok) {
        const body = await res.json().catch(() => null);
        const detail = body?.detail || `HTTP ${res.status}`;
        throw new Error(typeof detail === 'string' ? detail : JSON.stringify(detail));
      }
      const data = (await res.json()) as RefreshResponse;
      await refetchEntities();
      await fetchData(selectedEntityId || undefined);
      setRefreshResult(data);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Refresh failed');
    } finally {
      setRefreshing(false);
    }
  };

  const lastLog = logs[0] || null;
  const totalReceived = logs.reduce((s, l) => s + l.triples_received, 0);
  const totalRejected = logs.reduce((s, l) => s + l.triples_rejected, 0);
  const rejectionRate = totalReceived > 0 ? ((totalRejected / totalReceived) * 100).toFixed(1) : '0.0';

  const formatTs = (iso: string) => {
    const d = new Date(iso);
    return d.toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit' });
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

  return (
    <div className="h-full flex flex-col p-4 gap-3 overflow-hidden">
      {/* Entity selector bar */}
      <div className="shrink-0 flex items-center gap-3">
        <EntitySelector entities={entities} selectedEntityId={selectedEntityId} onEntityChange={onEntityChange} loading={entitiesLoading} error={entitiesError} />
        <div className="ml-auto flex items-center gap-2">
          <button
            onClick={handleRefresh}
            disabled={loading || refreshing}
            title="Pull newer Farm runs into DCL and refresh the dropdown"
            className="px-2 py-1 text-xs rounded border border-border hover:bg-accent disabled:opacity-50"
          >
            {refreshing ? 'Refreshing…' : loading ? 'Loading…' : 'Refresh'}
          </button>
          <span className="text-xs text-muted-foreground">
            {logs.length} entries
          </span>
        </div>
      </div>

      {refreshResult && (
        <div className="shrink-0 rounded border border-border bg-muted/30 px-3 py-2 text-xs text-muted-foreground space-y-2">
          <div data-role="refresh-message">{refreshResult.message}</div>
          {refreshResult.ingested.length > 0 && (
            <div data-role="refresh-ingested">
              <div className="font-medium text-foreground">Ingested ({refreshResult.ingested.length})</div>
              <div className="mt-1">
                {refreshResult.ingested
                  .map((i) => `${i.entity_id} (${(i.triples_written ?? 0).toLocaleString()} triples)`)
                  .join(', ')}
              </div>
            </div>
          )}
          {refreshResult.evicted_total > 0 && (
            <div data-role="refresh-evicted">
              <div className="font-medium text-foreground">
                Evicted ({refreshResult.evicted_total})
                {refreshResult.evicted_total > refreshResult.evicted_sample.length
                  ? ` — showing first ${refreshResult.evicted_sample.length}`
                  : ''}
              </div>
              <div className="mt-1">
                {refreshResult.evicted_sample.map((e) => e.entity_id).join(', ')}
              </div>
            </div>
          )}
          {refreshResult.skipped.length > 0 && (
            <div data-role="refresh-skipped">
              <div className="font-medium text-foreground">Skipped ({refreshResult.skipped.length})</div>
              <div className="mt-1">
                {refreshResult.skipped.map((s) => `${s.entity_id} — ${s.reason}`).join('; ')}
              </div>
            </div>
          )}
        </div>
      )}

      {/* Top metric row */}
      <div className="shrink-0 grid grid-cols-4 gap-3">
        <MetricCard
          label="Last Ingest"
          value={lastLog ? formatTs(lastLog.created_at) : '—'}
          detail={lastLog ? `${lastLog.dcl_ingest_id.slice(0, 8)} | ${lastLog.entity_id || 'multi'} | ${lastLog.duration_ms}ms` : undefined}
        />
        <MetricCard
          label={`Current triples for ${entityLabel}`}
          value={entityTripleCount.toLocaleString()}
          detail={selectedEntity ? 'Live slice from current_triples' : 'Select an entity'}
        />
        <MetricCard
          label="Rejection Rate"
          value={`${rejectionRate}%`}
          detail={`${totalRejected} / ${totalReceived} across ${logs.length} runs`}
          warn={totalRejected > 0}
        />
        <MetricCard
          label="Ingest Count"
          value={String(logs.length)}
          detail="Recent operations"
        />
      </div>

      {/* Table */}
      <div className="flex-1 min-h-0 border rounded overflow-hidden flex flex-col">
        <div className="shrink-0 bg-muted/50">
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b">
                <th className="text-left px-3 py-2 font-medium">Timestamp</th>
                <th className="text-left px-3 py-2 font-medium">Run ID</th>
                <th className="text-left px-3 py-2 font-medium">Entity</th>
                <th className="text-right px-3 py-2 font-medium">Received</th>
                <th className="text-right px-3 py-2 font-medium">Written</th>
                <th className="text-right px-3 py-2 font-medium">Rejected</th>
                <th className="text-left px-3 py-2 font-medium">Sources</th>
                <th className="text-right px-3 py-2 font-medium">Duration</th>
              </tr>
            </thead>
          </table>
        </div>
        <div className="flex-1 overflow-y-auto">
          <table className="w-full text-xs">
            <tbody>
              {logs.map((log) => (
                <IngestRow
                  key={log.id}
                  log={log}
                  expanded={expandedRow === log.id}
                  onToggle={() => setExpandedRow(expandedRow === log.id ? null : log.id)}
                  formatTs={formatTs}
                />
              ))}
              {logs.length === 0 && !loading && (
                <tr>
                  <td colSpan={8} className="px-3 py-8 text-center text-muted-foreground">
                    No ingest activity recorded yet
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

function MetricCard({ label, value, detail, warn }: { label: string; value: string; detail?: string; warn?: boolean }) {
  return (
    <div className={`rounded border px-3 py-2 ${warn ? 'border-yellow-500/50 bg-yellow-500/5' : 'bg-card/50'}`}>
      <div className="text-xs text-muted-foreground">{label}</div>
      <div className="text-lg font-semibold mt-0.5">{value}</div>
      {detail && <div className="text-xs text-muted-foreground mt-0.5 truncate">{detail}</div>}
    </div>
  );
}

function IngestRow({
  log,
  expanded,
  onToggle,
  formatTs,
}: {
  log: IngestLogEntry;
  expanded: boolean;
  onToggle: () => void;
  formatTs: (s: string) => string;
}) {
  const hasRejections = log.triples_rejected > 0;
  return (
    <>
      <tr
        className={`border-b hover:bg-accent/50 cursor-pointer ${hasRejections ? 'bg-yellow-500/5' : ''}`}
        onClick={hasRejections ? onToggle : undefined}
      >
        <td className="px-3 py-1.5">{formatTs(log.created_at)}</td>
        <td className="px-3 py-1.5 font-mono">{log.dcl_ingest_id.slice(0, 8)}</td>
        <td className="px-3 py-1.5">{log.entity_id || '—'}</td>
        <td className="px-3 py-1.5 text-right">{log.triples_received.toLocaleString()}</td>
        <td className="px-3 py-1.5 text-right">{log.triples_written.toLocaleString()}</td>
        <td className="px-3 py-1.5 text-right">
          {log.triples_rejected > 0 ? (
            <span className="text-yellow-600 font-medium">{log.triples_rejected}</span>
          ) : (
            <span className="text-muted-foreground">0</span>
          )}
        </td>
        <td className="px-3 py-1.5">{log.source_systems.join(', ') || '—'}</td>
        <td className="px-3 py-1.5 text-right font-mono">{log.duration_ms}ms</td>
      </tr>
      {expanded && hasRejections && (
        <tr className="bg-yellow-500/5">
          <td colSpan={8} className="px-6 py-2">
            <div className="text-xs">
              <span className="font-medium">Rejection reasons:</span>
              <pre className="mt-1 p-2 rounded bg-background/80 border overflow-x-auto">
                {JSON.stringify(log.rejection_reasons, null, 2)}
              </pre>
            </div>
          </td>
        </tr>
      )}
    </>
  );
}
