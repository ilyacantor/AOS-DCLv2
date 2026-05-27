import { useEffect, useState, useCallback } from 'react';
import { SnapshotSelector, SnapshotState } from './RunSelector';

interface TripleRow {
  id: string;
  entity_id: string;
  concept: string;
  property: string;
  value: unknown;
  period: string | null;
  source_system: string;
  confidence_score: number;
  confidence_tier: string;
  pipe_id: string | null;
  run_id: string;
}

interface AggItem {
  domain?: string;
  system?: string;
  period?: string;
  count: number;
}

interface DashboardData {
  rows: TripleRow[];
  total_count: number;
  page: number;
  page_size: number;
  filters_applied: Record<string, string>;
  aggregations: {
    by_domain: AggItem[];
    by_source: AggItem[];
    by_period: AggItem[];
  };
}

interface Filters {
  domain: string;
  source_system: string;
  period: string;
}

const EMPTY_FILTERS: Filters = { domain: '', source_system: '', period: '' };

interface DashboardTabProps {
  snapshot: SnapshotState;
}

export function DashboardTab({ snapshot }: DashboardTabProps) {
  const { selectedEntityId } = snapshot;
  const [data, setData] = useState<DashboardData | null>(null);
  const [filters, setFilters] = useState<Filters>(EMPTY_FILTERS);
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const pageSize = 50;

  const fetchData = useCallback(async (f: Filters, p: number, entityId?: string) => {
    setLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams();
      if (entityId) params.set('entity_id', entityId);
      if (f.domain) params.set('domain', f.domain);
      if (f.source_system) params.set('source_system', f.source_system);
      if (f.period) params.set('period', f.period);
      params.set('page', String(p));
      params.set('page_size', String(pageSize));

      const dashRes = await fetch(`/api/dcl/dashboard-data?${params}`);
      if (!dashRes.ok) throw new Error(`Dashboard data: HTTP ${dashRes.status}`);
      setData(await dashRes.json());
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to fetch dashboard data');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { fetchData(filters, page, selectedEntityId || undefined); }, [fetchData, filters, page, selectedEntityId]);

  const applyFilter = (key: keyof Filters, value: string) => {
    setPage(1);
    setFilters((prev) => ({ ...prev, [key]: value }));
  };

  const clearFilters = () => {
    setPage(1);
    setFilters(EMPTY_FILTERS);
  };

  const totalPages = data ? Math.ceil(data.total_count / pageSize) : 0;
  const hasFilters = Object.values(filters).some(Boolean);

  if (error) {
    return (
      <div className="h-full flex items-center justify-center">
        <div className="text-center p-6 rounded-lg border border-destructive/30 bg-destructive/5 max-w-md">
          <p className="text-sm text-destructive font-medium">{error}</p>
          <button onClick={() => fetchData(filters, page, selectedEntityId || undefined)} className="mt-3 px-3 py-1 text-xs rounded bg-primary text-primary-foreground">Retry</button>
        </div>
      </div>
    );
  }

  return (
    <div className="h-full flex flex-col p-4 gap-3 overflow-hidden">
      {/* Entity selector bar */}
      <div className="shrink-0 flex items-center gap-3">
        <SnapshotSelector snapshot={snapshot} />
      </div>

      {/* Filter bar */}
      <div className="shrink-0 flex items-center gap-2 flex-wrap">
        <FilterDropdown
          label="Domain"
          value={filters.domain}
          options={(data?.aggregations.by_domain ?? []).map((d) => ({ value: d.domain!, label: `${formatDomain(d.domain!)} (${d.count})` }))}
          onChange={(v) => applyFilter('domain', v)}
          placeholder="All Domains"
        />
        <FilterDropdown
          label="Source"
          value={filters.source_system}
          options={(data?.aggregations.by_source ?? []).map((s) => ({ value: s.system!, label: `${s.system!} (${s.count})` }))}
          onChange={(v) => applyFilter('source_system', v)}
          placeholder="All Sources"
        />
        <FilterDropdown
          label="Period"
          value={filters.period}
          options={(data?.aggregations.by_period ?? []).map((p) => ({ value: p.period!, label: `${p.period!} (${p.count})` }))}
          onChange={(v) => applyFilter('period', v)}
          placeholder="All Periods"
        />
        {hasFilters && (
          <button
            onClick={clearFilters}
            className="px-2 py-1 text-xs rounded border border-border hover:bg-accent"
          >
            Clear
          </button>
        )}
        <span className="text-xs text-muted-foreground ml-auto">
          {data?.total_count.toLocaleString() ?? 0} triples
          {loading && ' (loading...)'}
        </span>
      </div>

      {/* Sidebar + Table */}
      <div className="flex-1 min-h-0 flex gap-3">
        {/* Left sidebar: aggregations */}
        <div className="w-48 shrink-0 flex flex-col gap-2 overflow-y-auto">
          <AggSection
            title="Domains"
            items={(data?.aggregations.by_domain ?? []).map((d) => ({ label: formatDomain(d.domain!), count: d.count, value: d.domain! }))}
            activeValue={filters.domain}
            onSelect={(v) => applyFilter('domain', v === filters.domain ? '' : v)}
          />
          <AggSection
            title="Sources"
            items={(data?.aggregations.by_source ?? []).map((s) => ({ label: s.system!, count: s.count, value: s.system! }))}
            activeValue={filters.source_system}
            onSelect={(v) => applyFilter('source_system', v === filters.source_system ? '' : v)}
          />
          <AggSection
            title="Periods"
            items={(data?.aggregations.by_period ?? []).map((p) => ({ label: p.period!, count: p.count, value: p.period! }))}
            activeValue={filters.period}
            onSelect={(v) => applyFilter('period', v === filters.period ? '' : v)}
          />
        </div>

        {/* Main table */}
        <div className="flex-1 min-w-0 border rounded overflow-hidden flex flex-col">
          <div className="shrink-0 bg-muted/50">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b">
                  <th className="text-left px-3 py-2 font-medium">Concept</th>
                  <th className="text-left px-3 py-2 font-medium">Property</th>
                  <th className="text-left px-3 py-2 font-medium">Value</th>
                  <th className="text-left px-3 py-2 font-medium">Period</th>
                  <th className="text-left px-3 py-2 font-medium">Source</th>
                  <th className="text-left px-3 py-2 font-medium">Confidence</th>
                  <th className="text-left px-3 py-2 font-medium">Entity</th>
                </tr>
              </thead>
            </table>
          </div>
          <div className="flex-1 overflow-y-auto">
            <table className="w-full text-xs">
              <tbody>
                {(data?.rows ?? []).map((row) => (
                  <tr key={row.id} className="border-b hover:bg-accent/50">
                    <td className="px-3 py-1.5 font-mono truncate max-w-[200px]" title={row.concept}>{row.concept}</td>
                    <td className="px-3 py-1.5">{row.property}</td>
                    <td className="px-3 py-1.5 font-mono truncate max-w-[120px]" title={String(row.value)}>{formatValue(row.value)}</td>
                    <td className="px-3 py-1.5">{row.period ?? '—'}</td>
                    <td className="px-3 py-1.5">{row.source_system}</td>
                    <td className="px-3 py-1.5">
                      <ConfBadge tier={row.confidence_tier} score={row.confidence_score} />
                    </td>
                    <td className="px-3 py-1.5">{row.entity_id}</td>
                  </tr>
                ))}
                {data && data.rows.length === 0 && (
                  <tr><td colSpan={7} className="px-3 py-8 text-center text-muted-foreground">No triples match filters</td></tr>
                )}
              </tbody>
            </table>
          </div>
          {/* Pagination */}
          {totalPages > 1 && (
            <div className="shrink-0 flex items-center justify-between px-3 py-2 border-t bg-muted/30 text-xs">
              <span>Page {page} of {totalPages}</span>
              <div className="flex gap-1">
                <button
                  onClick={() => setPage(Math.max(1, page - 1))}
                  disabled={page <= 1}
                  className="px-2 py-0.5 rounded border border-border hover:bg-accent disabled:opacity-30"
                >Prev</button>
                <button
                  onClick={() => setPage(Math.min(totalPages, page + 1))}
                  disabled={page >= totalPages}
                  className="px-2 py-0.5 rounded border border-border hover:bg-accent disabled:opacity-30"
                >Next</button>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

function FilterDropdown({
  label,
  value,
  options,
  onChange,
  placeholder,
}: {
  label: string;
  value: string;
  options: { value: string; label: string }[];
  onChange: (v: string) => void;
  placeholder: string;
}) {
  return (
    <div className="flex items-center gap-1">
      <span className="text-xs text-muted-foreground">{label}:</span>
      <select
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="px-2 py-1 text-xs rounded border border-border bg-background max-w-[160px]"
      >
        <option value="">{placeholder}</option>
        {options.map((o) => (
          <option key={o.value} value={o.value}>{o.label}</option>
        ))}
      </select>
    </div>
  );
}

function AggSection({
  title,
  items,
  activeValue,
  onSelect,
}: {
  title: string;
  items: { label: string; count: number; value: string }[];
  activeValue: string;
  onSelect: (v: string) => void;
}) {
  return (
    <div className="border rounded">
      <div className="px-2 py-1 bg-muted/50 border-b text-xs font-medium">{title}</div>
      <div className="max-h-36 overflow-y-auto">
        {items.map((item) => (
          <button
            key={item.value}
            onClick={() => onSelect(item.value)}
            className={`w-full text-left px-2 py-1 text-xs flex justify-between hover:bg-accent/50 ${
              activeValue === item.value ? 'bg-primary/10 text-primary font-medium' : ''
            }`}
          >
            <span className="truncate">{item.label}</span>
            <span className="text-muted-foreground ml-1 shrink-0">{item.count}</span>
          </button>
        ))}
      </div>
    </div>
  );
}

function ConfBadge({ tier, score }: { tier: string; score: number }) {
  const colors: Record<string, string> = {
    exact: 'bg-blue-500/20 text-blue-700',
    high: 'bg-green-500/20 text-green-700',
    medium: 'bg-yellow-500/20 text-yellow-700',
    low: 'bg-red-500/20 text-red-700',
  };
  return (
    <span className={`inline-block px-1.5 py-0.5 rounded text-[10px] font-medium ${colors[tier] ?? 'bg-muted text-muted-foreground'}`}>
      {tier} ({score.toFixed(2)})
    </span>
  );
}

function formatValue(v: unknown): string {
  if (v === null || v === undefined) return '—';
  if (typeof v === 'number') return v.toLocaleString(undefined, { maximumFractionDigits: 2 });
  if (typeof v === 'string') return v;
  return JSON.stringify(v);
}

function formatDomain(d: string): string {
  return d.replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase());
}
