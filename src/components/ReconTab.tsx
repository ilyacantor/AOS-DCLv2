import { useState } from 'react';
import { SnapshotSelector, SnapshotState } from './RunSelector';

interface ReconCheck {
  check: string;
  status: 'pass' | 'fail' | 'warn' | 'skip';
  expected?: number | string[] | string;
  actual?: number | string[] | string;
  detail?: string | null;
  entities?: string[];
  missing?: string[];
  rejected?: number;
  reasons?: unknown[];
  populated?: number;
  total?: number;
  gaps?: string[];
}

interface ReconResult {
  run_id: string;
  entity_id: string | null;
  timestamp: string;
  overall: 'pass' | 'warn' | 'fail';
  checks: ReconCheck[];
  detail?: string;
}

interface ReconTabProps {
  snapshot: SnapshotState;
}

export function ReconTab({ snapshot }: ReconTabProps) {
  const { selectedEntityId } = snapshot;
  const [result, setResult] = useState<ReconResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [expandedCheck, setExpandedCheck] = useState<string | null>(null);

  const runRecon = async () => {
    setLoading(true);
    setError(null);
    setResult(null);
    try {
      const params = selectedEntityId ? `?entity_id=${encodeURIComponent(selectedEntityId)}` : '';
      const res = await fetch(`/api/dcl/recon${params}`);
      if (!res.ok) throw new Error(`Recon: HTTP ${res.status}`);
      setResult(await res.json());
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Recon failed');
    } finally {
      setLoading(false);
    }
  };

  const statusIcon = (s: string) => {
    switch (s) {
      case 'pass': return <span className="text-green-600 font-bold">PASS</span>;
      case 'fail': return <span className="text-red-600 font-bold">FAIL</span>;
      case 'warn': return <span className="text-yellow-600 font-bold">WARN</span>;
      case 'skip': return <span className="text-muted-foreground font-medium">SKIP</span>;
      default: return <span>{s}</span>;
    }
  };

  const overallBadge = (s: string) => {
    const colors: Record<string, string> = {
      pass: 'bg-green-500/20 text-green-700 border-green-500/30',
      warn: 'bg-yellow-500/20 text-yellow-700 border-yellow-500/30',
      fail: 'bg-red-500/20 text-red-700 border-red-500/30',
    };
    return (
      <span className={`inline-block px-3 py-1 rounded border text-sm font-bold ${colors[s] ?? 'bg-muted'}`}>
        {s.toUpperCase()}
      </span>
    );
  };

  const checkNames: Record<string, string> = {
    farm_dcl_count: 'Farm \u2192 DCL Count',
    entity_consistency: 'Entity Consistency',
    source_coverage: 'Source Coverage',
    validation_rejections: 'Validation Rejections',
    domain_completeness: 'Domain Completeness',
  };

  const checkSummary = (c: ReconCheck): string => {
    switch (c.check) {
      case 'farm_dcl_count':
        if (c.status === 'skip') return c.detail || 'Skipped';
        return `Expected: ${c.expected}, Actual: ${c.actual}`;
      case 'entity_consistency':
        return c.entities?.join(', ') || 'No entities';
      case 'source_coverage':
        if (c.missing && c.missing.length > 0) return `Missing: ${c.missing.join(', ')}`;
        // The recon endpoint always sends `actual` as the source-system list (a
        // real 0 only when the run genuinely has none — a fail). Don't coerce a
        // missing list to "0 sources present"; that would read absent data as a
        // fact (deferred #77, never zero-default-as-fact).
        return Array.isArray(c.actual)
          ? `${c.actual.length} sources present`
          : 'Source count unavailable';
      case 'validation_rejections':
        return c.rejected === 0 ? 'No rejections' : `${c.rejected} rejected`;
      case 'domain_completeness':
        return `${c.populated} / ${c.total} domains populated`;
      default:
        return c.detail || '';
    }
  };

  return (
    <div className="h-full flex flex-col p-4 gap-4 overflow-hidden">
      {/* Top: Entity selector + Run Recon button */}
      <div className="shrink-0 flex items-center gap-3">
        <SnapshotSelector snapshot={snapshot} />
        <button
          onClick={runRecon}
          disabled={loading || !selectedEntityId}
          className="px-4 py-1.5 text-xs rounded bg-primary text-primary-foreground hover:bg-primary/90 disabled:opacity-50 font-medium"
        >
          {loading ? 'Running...' : 'Run Recon'}
        </button>
        {result && <div className="ml-2">{overallBadge(result.overall)}</div>}
      </div>

      {/* Error */}
      {error && (
        <div className="shrink-0 p-3 rounded border border-destructive/30 bg-destructive/5 text-sm text-destructive">
          {error}
        </div>
      )}

      {/* Results */}
      {result && (
        <div className="flex-1 min-h-0 overflow-y-auto">
          <div className="grid gap-2">
            {result.checks.map((check) => {
              const isExpanded = expandedCheck === check.check;
              const hasDetail = check.status === 'warn' || check.status === 'fail' || check.status === 'skip';
              return (
                <div
                  key={check.check}
                  className={`border rounded ${
                    check.status === 'fail' ? 'border-red-500/30' :
                    check.status === 'warn' ? 'border-yellow-500/30' :
                    check.status === 'skip' ? 'border-muted' :
                    'border-green-500/30'
                  }`}
                >
                  <div
                    className={`flex items-center gap-3 px-4 py-2.5 ${hasDetail ? 'cursor-pointer hover:bg-accent/30' : ''}`}
                    onClick={hasDetail ? () => setExpandedCheck(isExpanded ? null : check.check) : undefined}
                  >
                    <div className="w-12 shrink-0">{statusIcon(check.status)}</div>
                    <div className="flex-1 min-w-0">
                      <div className="text-sm font-medium">{checkNames[check.check] || check.check}</div>
                      <div className="text-xs text-muted-foreground truncate">{checkSummary(check)}</div>
                    </div>
                    {hasDetail && (
                      <span className="text-xs text-muted-foreground shrink-0">
                        {isExpanded ? '\u25B2' : '\u25BC'}
                      </span>
                    )}
                  </div>
                  {isExpanded && hasDetail && (
                    <div className="px-4 py-2 border-t bg-muted/20 text-xs">
                      {check.detail && <div className="mb-1">{check.detail}</div>}
                      {check.gaps && check.gaps.length > 0 && (
                        <div>
                          <span className="font-medium">Gaps: </span>
                          {check.gaps.join(', ')}
                        </div>
                      )}
                      {check.reasons && check.reasons.length > 0 && (
                        <pre className="mt-1 p-2 rounded bg-background border overflow-x-auto">
                          {JSON.stringify(check.reasons, null, 2)}
                        </pre>
                      )}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* Empty state */}
      {!result && !loading && !error && (
        <div className="flex-1 flex items-center justify-center">
          <div className="text-center text-muted-foreground">
            <div className="text-sm">
              {selectedEntityId
                ? 'Click "Run Recon" to validate the data chain for this entity'
                : 'Select a specific entity to run recon checks'}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
