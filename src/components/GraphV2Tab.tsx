/**
 * Graph v2 tab — data-driven Sankey visualization.
 *
 * Thin orchestration wrapper: entity selector at top,
 * DataDrivenSankey in main area, clean empty state.
 */

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
  const hasData = graphData !== null && graphData.nodes.length > 0;

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
        {graphData?.meta?.snapshotName && (
          <span className="text-xs text-muted-foreground font-mono">
            {graphData.meta.snapshotName}
          </span>
        )}
      </div>

      {/* Main content — graph or empty state */}
      <div className="flex-1 overflow-hidden p-4">
        <div className="h-full w-full rounded-xl border bg-card/30 overflow-hidden shadow-inner">
          {hasData ? (
            <DataDrivenSankey data={graphData} />
          ) : (
            <div className="w-full h-full flex items-center justify-center" style={{ backgroundColor: '#080d18' }}>
              <div className="text-center">
                <p className="text-sm text-slate-400">No pipeline data</p>
                <p className="text-xs text-slate-600 mt-1">
                  Run the pipeline to generate the graph.
                </p>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
