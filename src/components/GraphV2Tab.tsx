/**
 * Graph v2 tab — data-driven Sankey visualization.
 *
 * Fetches entity-scoped graph data via POST /api/dcl/run with entity_id.
 * Re-fetches when the selected entity changes.
 */

import { useEffect, useState, useRef } from 'react';
import { GraphSnapshot } from '../types';
import { EntitySelector, type EntityInfo } from './RunSelector';
import { DataDrivenSankey } from './graph-v2';

interface GraphV2TabProps {
  graphData: GraphSnapshot | null;
  entities: EntityInfo[];
  selectedEntityId: string;
  onEntityChange: (id: string) => void;
  entitiesLoading: boolean;
  entitiesError: string | null;
}

export function GraphV2Tab({
  graphData,
  entities,
  selectedEntityId,
  onEntityChange,
  entitiesLoading,
  entitiesError,
}: GraphV2TabProps) {
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
  }, [selectedEntityId]);

  // Use entity-scoped data when available, fall back to global graphData
  const displayData = entityGraphData ?? graphData;
  const hasData = displayData !== null && displayData.nodes.length > 0;

  return (
    <div className="h-full flex flex-col overflow-hidden">
      {/* Top bar — entity selector + snapshot provenance */}
      <div className="shrink-0 flex items-center gap-4 px-4 py-2 border-b border-border bg-card/30">
        <EntitySelector
          entities={entities}
          selectedEntityId={selectedEntityId}
          onEntityChange={onEntityChange}
          loading={entitiesLoading}
          error={entitiesError}
        />
        {displayData?.meta?.snapshotName && (
          <span className="text-xs text-muted-foreground font-mono">
            {displayData.meta.snapshotName}
          </span>
        )}
        {loading && (
          <span className="text-xs text-muted-foreground">Loading graph...</span>
        )}
      </div>

      {/* Main content — graph or empty state */}
      <div className="flex-1 overflow-hidden p-4">
        <div className="h-full w-full rounded-xl border bg-card/30 overflow-hidden shadow-inner">
          {fetchError ? (
            <div className="w-full h-full flex items-center justify-center" style={{ backgroundColor: '#080d18' }}>
              <div className="text-center p-6 rounded-lg border border-destructive/30 bg-destructive/5 max-w-md">
                <p className="text-sm text-destructive font-medium">{fetchError}</p>
              </div>
            </div>
          ) : hasData ? (
            <DataDrivenSankey data={displayData} />
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
