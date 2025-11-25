import { useState, useMemo } from 'react';
import { GraphSnapshot } from '../types';
import { 
  Database, 
  ArrowRight, 
  CheckCircle2, 
  AlertCircle, 
  AlertTriangle,
  Search,
  Filter,
  ChevronDown,
  Layers,
  GitBranch
} from 'lucide-react';

interface EnterpriseDashboardProps {
  data: GraphSnapshot | null;
  runId?: string;
}

interface MappingItem {
  id: string;
  sourceSystem: string;
  sourceTable: string;
  sourceField: string;
  targetConcept: string;
  confidence: number;
  method: string;
}

export function EnterpriseDashboard({ data, runId }: EnterpriseDashboardProps) {
  const [searchTerm, setSearchTerm] = useState('');
  const [confidenceFilter, setConfidenceFilter] = useState<'all' | 'high' | 'medium' | 'low'>('all');
  const [sourceFilter, setSourceFilter] = useState<string>('all');

  const { mappings, sources, stats } = useMemo(() => {
    if (!data) return { mappings: [], sources: [], stats: { total: 0, high: 0, medium: 0, low: 0 } };

    const mappingItems: MappingItem[] = [];
    const sourceSet = new Set<string>();

    data.links.forEach(link => {
      if (link.flowType === 'mapping' && link.infoSummary) {
        const sourceNode = data.nodes.find(n => n.id === link.source);
        const targetNode = data.nodes.find(n => n.id === link.target);
        
        if (sourceNode && targetNode) {
          const parts = link.infoSummary.split(' → ');
          const fieldPart = parts[0] || '';
          const confidence = link.confidence || 0;
          
          sourceSet.add(sourceNode.label);
          
          mappingItems.push({
            id: link.id,
            sourceSystem: sourceNode.label,
            sourceTable: fieldPart.includes('.') ? fieldPart.split('.')[0] : 'default',
            sourceField: fieldPart.includes('.') ? fieldPart.split('.').slice(1).join('.') : fieldPart,
            targetConcept: targetNode.label,
            confidence,
            method: link.infoSummary.includes('llm') ? 'llm' : 'heuristic'
          });
        }
      }
    });

    const high = mappingItems.filter(m => m.confidence >= 0.85).length;
    const medium = mappingItems.filter(m => m.confidence >= 0.6 && m.confidence < 0.85).length;
    const low = mappingItems.filter(m => m.confidence < 0.6).length;

    return {
      mappings: mappingItems,
      sources: Array.from(sourceSet).sort(),
      concepts: Array.from(conceptSet).sort(),
      stats: { total: mappingItems.length, high, medium, low }
    };
  }, [data]);

  const filteredMappings = useMemo(() => {
    return mappings.filter(m => {
      const matchesSearch = searchTerm === '' || 
        m.sourceSystem.toLowerCase().includes(searchTerm.toLowerCase()) ||
        m.sourceTable.toLowerCase().includes(searchTerm.toLowerCase()) ||
        m.sourceField.toLowerCase().includes(searchTerm.toLowerCase()) ||
        m.targetConcept.toLowerCase().includes(searchTerm.toLowerCase());
      
      const matchesConfidence = confidenceFilter === 'all' ||
        (confidenceFilter === 'high' && m.confidence >= 0.85) ||
        (confidenceFilter === 'medium' && m.confidence >= 0.6 && m.confidence < 0.85) ||
        (confidenceFilter === 'low' && m.confidence < 0.6);
      
      const matchesSource = sourceFilter === 'all' || m.sourceSystem === sourceFilter;
      
      return matchesSearch && matchesConfidence && matchesSource;
    });
  }, [mappings, searchTerm, confidenceFilter, sourceFilter]);

  const getConfidenceColor = (confidence: number) => {
    if (confidence >= 0.85) return 'text-green-400';
    if (confidence >= 0.6) return 'text-yellow-400';
    return 'text-red-400';
  };

  const getConfidenceBg = (confidence: number) => {
    if (confidence >= 0.85) return 'bg-green-500/10 border-green-500/30';
    if (confidence >= 0.6) return 'bg-yellow-500/10 border-yellow-500/30';
    return 'bg-red-500/10 border-red-500/30';
  };

  const getConfidenceIcon = (confidence: number) => {
    if (confidence >= 0.85) return <CheckCircle2 className="w-3.5 h-3.5 text-green-400" />;
    if (confidence >= 0.6) return <AlertTriangle className="w-3.5 h-3.5 text-yellow-400" />;
    return <AlertCircle className="w-3.5 h-3.5 text-red-400" />;
  };

  if (!data) {
    return (
      <div className="h-full flex items-center justify-center text-muted-foreground">
        <div className="text-center">
          <Database className="w-12 h-12 mx-auto mb-3 opacity-30" />
          <p>No data loaded</p>
          <p className="text-sm mt-1">Run the pipeline to see mappings</p>
        </div>
      </div>
    );
  }

  return (
    <div className="h-full flex flex-col bg-background">
      <div className="grid grid-cols-4 gap-3 p-4 border-b shrink-0">
        <div className="bg-card rounded-lg p-3 border">
          <div className="flex items-center gap-2 text-muted-foreground text-xs mb-1">
            <Layers className="w-3.5 h-3.5" />
            <span>Sources</span>
          </div>
          <div className="text-2xl font-bold">{sources.length}</div>
        </div>
        <div className="bg-card rounded-lg p-3 border">
          <div className="flex items-center gap-2 text-muted-foreground text-xs mb-1">
            <GitBranch className="w-3.5 h-3.5" />
            <span>Total Mappings</span>
          </div>
          <div className="text-2xl font-bold">{stats.total}</div>
        </div>
        <div className="bg-card rounded-lg p-3 border">
          <div className="flex items-center gap-2 text-green-400 text-xs mb-1">
            <CheckCircle2 className="w-3.5 h-3.5" />
            <span>High Confidence</span>
          </div>
          <div className="text-2xl font-bold text-green-400">{stats.high}</div>
        </div>
        <div className="bg-card rounded-lg p-3 border">
          <div className="flex items-center gap-2 text-yellow-400 text-xs mb-1">
            <AlertTriangle className="w-3.5 h-3.5" />
            <span>Needs Review</span>
          </div>
          <div className="text-2xl font-bold text-yellow-400">{stats.medium + stats.low}</div>
        </div>
      </div>

      <div className="flex items-center gap-3 p-3 border-b shrink-0 bg-card/30">
        <div className="relative flex-1">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
          <input
            type="text"
            placeholder="Search sources, tables, fields, concepts..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            className="w-full pl-9 pr-3 py-2 text-sm bg-background border rounded-md focus:outline-none focus:ring-2 focus:ring-primary/50"
          />
        </div>
        
        <div className="flex items-center gap-2">
          <Filter className="w-4 h-4 text-muted-foreground" />
          <select
            value={confidenceFilter}
            onChange={(e) => setConfidenceFilter(e.target.value as any)}
            className="px-3 py-2 text-sm bg-background border rounded-md cursor-pointer"
          >
            <option value="all">All Confidence</option>
            <option value="high">High (≥85%)</option>
            <option value="medium">Medium (60-84%)</option>
            <option value="low">Low (&lt;60%)</option>
          </select>
          
          <select
            value={sourceFilter}
            onChange={(e) => setSourceFilter(e.target.value)}
            className="px-3 py-2 text-sm bg-background border rounded-md cursor-pointer max-w-[160px]"
          >
            <option value="all">All Sources</option>
            {sources.map(s => (
              <option key={s} value={s}>{s}</option>
            ))}
          </select>
        </div>
      </div>

      <div className="flex-1 overflow-auto min-h-0">
        <div className="divide-y divide-border/50">
          {filteredMappings.length === 0 ? (
            <div className="p-8 text-center text-muted-foreground">
              <p>No mappings match your filters</p>
            </div>
          ) : (
            filteredMappings.map((mapping) => (
              <div 
                key={mapping.id}
                className={`flex items-center gap-3 px-4 py-2.5 hover:bg-card/50 transition-colors ${getConfidenceBg(mapping.confidence)} border-l-2`}
              >
                <div className="shrink-0">
                  {getConfidenceIcon(mapping.confidence)}
                </div>
                
                <div className="flex items-center gap-2 flex-1 min-w-0 text-sm">
                  <span className="font-medium text-primary truncate max-w-[120px]" title={mapping.sourceSystem}>
                    {mapping.sourceSystem}
                  </span>
                  <ChevronDown className="w-3 h-3 text-muted-foreground rotate-[-90deg] shrink-0" />
                  <span className="text-muted-foreground truncate max-w-[100px]" title={mapping.sourceTable}>
                    {mapping.sourceTable}
                  </span>
                  <ChevronDown className="w-3 h-3 text-muted-foreground rotate-[-90deg] shrink-0" />
                  <span className="font-mono text-xs bg-secondary/50 px-1.5 py-0.5 rounded truncate max-w-[140px]" title={mapping.sourceField}>
                    {mapping.sourceField}
                  </span>
                  
                  <ArrowRight className="w-4 h-4 text-muted-foreground shrink-0 mx-1" />
                  
                  <span className="font-semibold text-accent-foreground bg-accent/20 px-2 py-0.5 rounded truncate max-w-[120px]" title={mapping.targetConcept}>
                    {mapping.targetConcept}
                  </span>
                </div>
                
                <div className={`shrink-0 font-mono text-xs font-bold ${getConfidenceColor(mapping.confidence)}`}>
                  {(mapping.confidence * 100).toFixed(0)}%
                </div>
                
                <div className="shrink-0 text-xs text-muted-foreground bg-secondary/30 px-1.5 py-0.5 rounded">
                  {mapping.method}
                </div>
              </div>
            ))
          )}
        </div>
      </div>

      <div className="border-t p-2 text-xs text-muted-foreground flex items-center justify-between bg-card/30 shrink-0">
        <span>Showing {filteredMappings.length} of {stats.total} mappings</span>
        {runId && <span className="font-mono">Run: {runId.slice(0, 8)}</span>}
      </div>
    </div>
  );
}
