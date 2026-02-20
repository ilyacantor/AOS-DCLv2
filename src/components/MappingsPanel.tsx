import { GraphSnapshot, GraphNode } from '../types';

interface MappingsPanelProps {
  data: GraphSnapshot | null;
}

interface MappingSummary {
  targetConcept: string;
  confidence: number;
  infoSummary: string;
}

export function MappingsPanel({ data }: MappingsPanelProps) {
  if (!data) {
    return (
      <div className="h-full flex items-center justify-center text-muted-foreground">
        No data loaded
      </div>
    );
  }

  const nodeMap = Object.fromEntries((data?.nodes || []).map(n => [n.id, n]));
  const sourceToMappings = new Map<string, MappingSummary[]>();

  // Group mappings by source system
  data.links
    .filter(link => {
      const srcNode = typeof link.source === 'string' ? nodeMap[link.source] : link.source;
      const tgtNode = typeof link.target === 'string' ? nodeMap[link.target] : link.target;
      return srcNode?.level === 'L1' && tgtNode?.level === 'L2';
    })
    .forEach(link => {
      const sourceId = typeof link.source === 'string' ? link.source : (link.source as GraphNode).id;
      const targetId = typeof link.target === 'string' ? link.target : (link.target as GraphNode).id;
      const targetNode = nodeMap[targetId];

      if (!sourceToMappings.has(sourceId)) {
        sourceToMappings.set(sourceId, []);
      }

      sourceToMappings.get(sourceId)!.push({
        targetConcept: targetNode?.label || 'Unknown',
        confidence: link.confidence || 0,
        infoSummary: link.infoSummary || `Mapped to ${targetNode?.label || 'Unknown'}`
      });
    });
  
  const sortedSources = Array.from(sourceToMappings.entries())
    .map(([sourceId, mappings]) => ({
      source: nodeMap[sourceId],
      mappings: mappings.sort((a, b) => b.confidence - a.confidence)
    }))
    .sort((a, b) => a.source.label.localeCompare(b.source.label));
  
  if (sortedSources.length === 0) {
    return (
      <div className="h-full flex items-center justify-center text-muted-foreground text-sm">
        No mappings available. Run the pipeline to see field mappings.
      </div>
    );
  }
  
  return (
    <div className="h-full bg-sidebar border-t">
      <div className="h-full flex flex-col">
        <div className="px-4 py-3 border-b">
          <h2 className="text-sm font-semibold">Field Mappings</h2>
          <p className="text-xs text-muted-foreground mt-0.5">
            Source fields mapped to ontology concepts
          </p>
        </div>
        
        <div className="flex-1 overflow-y-auto p-4">
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-3">
            {sortedSources.map(({ source, mappings }) => (
              <div key={source.id} className="bg-card/50 rounded-lg border border-border/50">
                <div className="p-3 border-b border-border/30 bg-secondary/20">
                  <div className="flex items-center justify-between">
                    <div>
                      <h3 className="text-xs font-semibold text-primary">{source.label}</h3>
                      <p className="text-[10px] text-muted-foreground mt-0.5">
                        {mappings.length} field{mappings.length === 1 ? '' : 's'} mapped
                      </p>
                    </div>
                    <div className="flex items-center gap-1">
                      <div className="w-2 h-2 rounded-full bg-emerald-500" />
                    </div>
                  </div>
                </div>
                
                <div className="p-2 space-y-1.5">
                  {mappings.slice(0, 5).map((mapping, idx) => (
                    <div key={idx} className="p-2 rounded bg-secondary/30 border border-border/20">
                      <div className="flex items-start justify-between gap-2">
                        <div className="flex-1 min-w-0">
                          <div className="text-[10px] font-medium text-foreground truncate">
                            → {mapping.targetConcept}
                          </div>
                          <div className="text-[9px] text-muted-foreground mt-0.5 truncate">
                            {mapping.infoSummary}
                          </div>
                        </div>
                        <div className={`text-[9px] font-medium px-1.5 py-0.5 rounded ${
                          mapping.confidence > 0.8 ? 'bg-green-500/20 text-green-600' :
                          mapping.confidence > 0.5 ? 'bg-yellow-500/20 text-yellow-600' :
                          'bg-red-500/20 text-red-600'
                        }`}>
                          {(mapping.confidence * 100).toFixed(0)}%
                        </div>
                      </div>
                    </div>
                  ))}
                  {mappings.length > 5 && (
                    <div className="text-[9px] text-muted-foreground text-center py-1">
                      +{mappings.length - 5} more mapping{mappings.length - 5 === 1 ? '' : 's'}
                    </div>
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}