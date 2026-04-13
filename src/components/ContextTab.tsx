import { useEffect, useState } from 'react';
import { EntitySelector, EntityInfo } from './RunSelector';

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

interface ContextTabProps {
  entities: EntityInfo[];
  selectedEntityId: string;
  onEntityChange: (id: string) => void;
  entitiesLoading?: boolean;
  entitiesError?: string | null;
}

export function ContextTab({ entities, selectedEntityId, onEntityChange, entitiesLoading, entitiesError }: ContextTabProps) {
  const [data, setData] = useState<ContextData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchData = async (entityId?: string) => {
    setLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams();
      if (entityId) params.set('entity_id', entityId);
      const qs = params.toString() ? `?${params}` : '';

      const ctxRes = await fetch(`/api/dcl/contextualization-summary${qs}`);
      if (!ctxRes.ok) throw new Error(`Context summary: HTTP ${ctxRes.status}`);
      setData(await ctxRes.json());
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to fetch context data');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { fetchData(selectedEntityId || undefined); }, [selectedEntityId]);

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
        <EntitySelector entities={entities} selectedEntityId={selectedEntityId} onEntityChange={onEntityChange} loading={entitiesLoading} error={entitiesError} />
        <div className="ml-auto flex items-center gap-2">
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
