/**
 * Graph v2 tab — data-driven Sankey visualization.
 *
 * Fetches entity-scoped graph data via POST /api/dcl/run with entity_id.
 * Re-fetches when the selected entity changes.
 */

import { useEffect, useState, useRef } from 'react';
import { GraphSnapshot, PersonaId } from '../types';
import { SnapshotSelector, type SnapshotState } from './RunSelector';
import { DataDrivenSankey } from './graph-v2';
import { EntityEdgeGraph } from './graph-v2/EntityEdgeGraph';

type GraphMode = 'fabric' | 'relationships';

interface GraphV2TabProps {
  graphData: GraphSnapshot | null;
  snapshot: SnapshotState;
  selectedPersonas: PersonaId[];
}

export function GraphV2Tab({
  graphData,
  snapshot,
  selectedPersonas,
}: GraphV2TabProps) {
  const { selectedEntityId, selectedTenantId } = snapshot;
  const [mode, setMode] = useState<GraphMode>('fabric');
  const [entityGraphData, setEntityGraphData] = useState<GraphSnapshot | null>(null);
  const [loading, setLoading] = useState(false);
  const [fetchError, setFetchError] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  useEffect(() => {
    if (!selectedEntityId) {
      setEntityGraphData(null);
      setFetchError(null);
      return;
    }

    // Cancel any in-flight request
    if (abortRef.current) {
      abortRef.current.abort();
    }
    const controller = new AbortController();
    abortRef.current = controller;

    setLoading(true);
    setFetchError(null);

    fetch('/api/dcl/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        mode: 'Farm',
        run_mode: 'Dev',
        entity_id: selectedEntityId,
        // Carry the identity pair (I2): the snapshot knows its tenant, so pass it
        // rather than forcing the server to re-guess from entity_id alone — which
        // 422s when the same entity_id exists under more than one tenant. null
        // preserves the single-tenant auto-resolve path.
        tenant_id: selectedTenantId,
      }),
      signal: controller.signal,
    })
      .then(async (res) => {
        if (!res.ok) {
          const body = await res.json().catch(() => null);
          throw new Error(body?.detail || `HTTP ${res.status}`);
        }
        return res.json();
      })
      .then((data) => {
        if (controller.signal.aborted) return;
        if (!data?.graph) {
          throw new Error('API returned OK but response contained no graph data');
        }
        setEntityGraphData(data.graph);
        setLoading(false);
      })
      .catch((err) => {
        if (controller.signal.aborted) return;
        setFetchError(err instanceof Error ? err.message : 'Failed to load graph');
        setLoading(false);
      });

    return () => { controller.abort(); };
  }, [selectedEntityId, selectedTenantId]);

  // Use entity-scoped data when available, fall back to global graphData
  const displayData = entityGraphData ?? graphData;
  const hasData = displayData !== null && displayData.nodes.length > 0;

  return (
    <div className="h-full flex flex-col overflow-hidden">
      {/* Top bar — entity selector + mode toggle + snapshot provenance */}
      <div className="shrink-0 flex items-center gap-4 px-4 py-2 border-b border-border bg-card/30">
        <SnapshotSelector snapshot={snapshot} />
        {/* Fabric | Relationships toggle */}
        <div
          role="tablist"
          aria-label="Graph mode"
          data-testid="graph-mode-toggle"
          className="inline-flex rounded-md border border-border overflow-hidden"
        >
          {(['fabric', 'relationships'] as GraphMode[]).map((m) => (
            <button
              key={m}
              role="tab"
              aria-selected={mode === m}
              data-testid={`graph-mode-${m}`}
              onClick={() => setMode(m)}
              className={`px-3 py-1 text-xs font-medium transition-colors ${
                mode === m
                  ? 'bg-primary/15 text-primary'
                  : 'bg-transparent text-muted-foreground hover:text-foreground'
              }`}
            >
              {m === 'fabric' ? 'Fabric' : 'Relationships'}
            </button>
          ))}
        </div>
        {mode === 'fabric' && displayData?.meta?.snapshotName && (
          <span className="text-xs text-muted-foreground font-mono">
            {displayData.meta.snapshotName}
          </span>
        )}
        {mode === 'fabric' && loading && (
          <span className="text-xs text-muted-foreground">Loading graph...</span>
        )}
      </div>

      {/* Main content — graph or empty state */}
      <div className="flex-1 overflow-hidden p-4">
        <div className="h-full w-full rounded-xl border bg-card/30 overflow-hidden shadow-inner">
          {mode === 'relationships' ? (
            selectedEntityId ? (
              <EntityEdgeGraph entityId={selectedEntityId} />
            ) : (
              <div className="w-full h-full flex items-center justify-center" style={{ backgroundColor: '#080d18' }}>
                <p className="text-sm text-slate-400">Select an entity to load relationships.</p>
              </div>
            )
          ) : fetchError ? (
            <div className="w-full h-full flex items-center justify-center" style={{ backgroundColor: '#080d18' }}>
              <div className="text-center p-6 rounded-lg border border-destructive/30 bg-destructive/5 max-w-md">
                <p className="text-sm text-destructive font-medium">{fetchError}</p>
              </div>
            </div>
          ) : hasData ? (
            <DataDrivenSankey data={displayData} selectedPersonas={selectedPersonas} />
          ) : (
            <div className="w-full h-full flex items-center justify-center" style={{ backgroundColor: '#080d18' }}>
              <div className="text-center">
                <p className="text-sm text-slate-400">
                  {loading ? 'Loading graph data...' : 'No pipeline data'}
                </p>
                {!loading && (
                  <p className="text-xs text-slate-600 mt-1">
                    Select an entity to load the graph.
                  </p>
                )}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
