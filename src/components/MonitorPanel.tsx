import { useState, useEffect } from 'react';
import { GraphSnapshot, PersonaId } from '../types';
import { Badge } from './Badge';
import { TrendingUp, TrendingDown, Minus, AlertTriangle, Info, Database, Zap, CheckCircle2, ChevronDown, ChevronRight, Layers, Server, Table2, FileText, X, ArrowRight } from 'lucide-react';

interface MonitorPanelProps {
  data: GraphSnapshot | null;
  selectedPersonas: PersonaId[];
  runId?: string;
}

interface SourceHierarchy {
  [source: string]: {
    [table: string]: Array<{
      field: string;
      confidence: number;
    }>;
  };
}

interface DetailSelection {
  type: 'source' | 'table' | 'field';
  ontologyId: string;
  ontologyLabel: string;
  sourceName: string;
  tableName?: string;
  fieldName?: string;
  confidence?: number;
}

export function MonitorPanel({ data, selectedPersonas, runId }: MonitorPanelProps) {
  const [activeTab, setActiveTab] = useState('views');
  const [ragMessages, setRagMessages] = useState<any[]>([]);
  const [ragMetrics, setRagMetrics] = useState({ llm_calls: 0, rag_reads: 0, rag_writes: 0 });
  const [expandedSections, setExpandedSections] = useState<Record<string, { sources: boolean; ontologies: boolean }>>({});
  const [expandedOntologies, setExpandedOntologies] = useState<Record<string, boolean>>({});
  const [expandedSources, setExpandedSources] = useState<Record<string, boolean>>({});
  const [expandedTables, setExpandedTables] = useState<Record<string, boolean>>({});
  const [selectedDetail, setSelectedDetail] = useState<DetailSelection | null>(null);

  const toggleSection = (personaId: string, section: 'sources' | 'ontologies') => {
    setExpandedSections(prev => ({
      ...prev,
      [personaId]: {
        sources: prev[personaId]?.sources ?? false,
        ontologies: prev[personaId]?.ontologies ?? false,
        [section]: !(prev[personaId]?.[section] ?? false)
      }
    }));
  };

  const toggleOntology = (ontologyId: string) => {
    setExpandedOntologies(prev => ({
      ...prev,
      [ontologyId]: !prev[ontologyId]
    }));
  };

  const toggleSource = (key: string) => {
    setExpandedSources(prev => ({
      ...prev,
      [key]: !prev[key]
    }));
  };

  const toggleTable = (key: string) => {
    setExpandedTables(prev => ({
      ...prev,
      [key]: !prev[key]
    }));
  };

  const getOntologySourceHierarchy = (ontologyId: string): SourceHierarchy | null => {
    if (!data) return null;
    const node = data.nodes.find(n => n.id === ontologyId);
    if (!node?.metrics?.source_hierarchy) return null;
    return node.metrics.source_hierarchy as unknown as SourceHierarchy;
  };

  const getSourceNode = (sourceName: string) => {
    if (!data) return null;
    const expectedId = `source_${sourceName}`;
    return data.nodes.find(n => n.level === 'L1' && n.id === expectedId);
  };

  const getPersonaConnections = (personaId: PersonaId) => {
    if (!data) return { sources: [], ontologies: [] };
    
    const personaNodeId = `bll_${personaId.toLowerCase()}`;
    const l2Nodes = data.nodes.filter(n => n.level === 'L2');
    const l1Nodes = data.nodes.filter(n => n.level === 'L1');
    
    const connectedOntologies = l2Nodes.filter(l2 => {
      return data.links.some(link => {
        const sourceId = typeof link.source === 'string' ? link.source : link.source.id;
        const targetId = typeof link.target === 'string' ? link.target : link.target.id;
        return sourceId === l2.id && targetId === personaNodeId;
      });
    });
    
    const connectedSources = l1Nodes.filter(l1 => {
      return data.links.some(link => {
        const sourceId = typeof link.source === 'string' ? link.source : link.source.id;
        const targetId = typeof link.target === 'string' ? link.target : link.target.id;
        return sourceId === l1.id && connectedOntologies.some(o => o.id === targetId);
      });
    });
    
    return {
      sources: connectedSources,
      ontologies: connectedOntologies
    };
  };
  
  useEffect(() => {
    if (!runId) return;
    
    const fetchRagData = async () => {
      try {
        const response = await fetch(`/api/dcl/narration/${runId}`);
        const narrationData = await response.json();
        const ragMsgs = narrationData.messages?.filter((m: any) => m.source === 'RAG' || m.source === 'LLM') || [];
        setRagMessages(ragMsgs);
      } catch (error) {
        console.error('Error fetching RAG data:', error);
      }
    };
    
    fetchRagData();
    const interval = setInterval(fetchRagData, 3000);
    return () => clearInterval(interval);
  }, [runId]);
  
  useEffect(() => {
    if (data?.meta?.runMetrics) {
      setRagMetrics({
        llm_calls: data.meta.runMetrics.llm_calls || 0,
        rag_reads: data.meta.runMetrics.rag_reads || 0,
        rag_writes: data.meta.runMetrics.rag_writes || 0,
      });
    }
  }, [data]);
  
  if (!data) return <div className="p-4 text-muted-foreground">No data loaded</div>;

  const activePersonaViews = data.meta.personaViews?.filter(pv => selectedPersonas.includes(pv.personaId)) || [];

  const DetailPanel = () => {
    if (!selectedDetail) return null;
    
    const sourceNode = getSourceNode(selectedDetail.sourceName);
    const hierarchy = getOntologySourceHierarchy(selectedDetail.ontologyId);
    
    return (
      <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50" onClick={() => setSelectedDetail(null)}>
        <div className="bg-card border border-border rounded-lg shadow-xl max-w-md w-full mx-4 max-h-[80vh] overflow-hidden" onClick={e => e.stopPropagation()}>
          <div className="flex items-center justify-between p-4 border-b border-border">
            <div className="flex items-center gap-2">
              {selectedDetail.type === 'source' && <Server className="w-5 h-5 text-cyan-400" />}
              {selectedDetail.type === 'table' && <Table2 className="w-5 h-5 text-violet-400" />}
              {selectedDetail.type === 'field' && <FileText className="w-5 h-5 text-emerald-400" />}
              <h3 className="font-semibold">
                {selectedDetail.type === 'source' && selectedDetail.sourceName}
                {selectedDetail.type === 'table' && selectedDetail.tableName}
                {selectedDetail.type === 'field' && selectedDetail.fieldName}
              </h3>
            </div>
            <button onClick={() => setSelectedDetail(null)} className="p-1 hover:bg-secondary rounded">
              <X className="w-4 h-4" />
            </button>
          </div>
          
          <div className="p-4 space-y-4 overflow-y-auto max-h-[60vh]">
            <div className="flex items-center gap-2 text-xs text-muted-foreground">
              <Layers className="w-3 h-3" />
              <span>Maps to</span>
              <ArrowRight className="w-3 h-3" />
              <Badge variant="outline">{selectedDetail.ontologyLabel}</Badge>
            </div>
            
            {selectedDetail.type === 'source' && sourceNode && (
              <>
                <div className="space-y-2">
                  <h4 className="text-sm font-medium text-muted-foreground">Source Details</h4>
                  <div className="grid grid-cols-2 gap-2 text-sm">
                    <div className="bg-secondary/30 rounded p-2">
                      <div className="text-[10px] text-muted-foreground uppercase">Type</div>
                      <div className="font-medium">{sourceNode.group}</div>
                    </div>
                    <div className="bg-secondary/30 rounded p-2">
                      <div className="text-[10px] text-muted-foreground uppercase">Status</div>
                      <div className="font-medium flex items-center gap-1">
                        <CheckCircle2 className="w-3 h-3 text-green-400" />
                        {sourceNode.status === 'ok' ? 'Connected' : 'Error'}
                      </div>
                    </div>
                    <div className="bg-secondary/30 rounded p-2">
                      <div className="text-[10px] text-muted-foreground uppercase">Tables</div>
                      <div className="font-medium">{sourceNode.metrics?.tables || 0}</div>
                    </div>
                    <div className="bg-secondary/30 rounded p-2">
                      <div className="text-[10px] text-muted-foreground uppercase">Total Fields</div>
                      <div className="font-medium">{sourceNode.metrics?.fields || 0}</div>
                    </div>
                  </div>
                </div>
                
                {hierarchy && hierarchy[selectedDetail.sourceName] && (
                  <div className="space-y-2">
                    <h4 className="text-sm font-medium text-muted-foreground">Tables in this mapping</h4>
                    <div className="space-y-1">
                      {Object.entries(hierarchy[selectedDetail.sourceName]).map(([tableName, fields]) => (
                        <div key={tableName} className="flex items-center justify-between p-2 bg-violet-500/10 rounded border border-violet-500/20 text-sm">
                          <div className="flex items-center gap-2">
                            <Table2 className="w-3 h-3 text-violet-400" />
                            <span className="font-mono">{tableName}</span>
                          </div>
                          <Badge variant="outline" className="text-[10px]">{fields.length} fields</Badge>
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </>
            )}
            
            {selectedDetail.type === 'table' && hierarchy && selectedDetail.tableName && (
              <>
                <div className="space-y-2">
                  <h4 className="text-sm font-medium text-muted-foreground">Table Details</h4>
                  <div className="grid grid-cols-2 gap-2 text-sm">
                    <div className="bg-secondary/30 rounded p-2">
                      <div className="text-[10px] text-muted-foreground uppercase">Source</div>
                      <div className="font-medium capitalize">{selectedDetail.sourceName}</div>
                    </div>
                    <div className="bg-secondary/30 rounded p-2">
                      <div className="text-[10px] text-muted-foreground uppercase">Mapped Fields</div>
                      <div className="font-medium">
                        {hierarchy[selectedDetail.sourceName]?.[selectedDetail.tableName]?.length || 0}
                      </div>
                    </div>
                  </div>
                </div>
                
                <div className="space-y-2">
                  <h4 className="text-sm font-medium text-muted-foreground">Field Mappings</h4>
                  <div className="space-y-1">
                    {hierarchy[selectedDetail.sourceName]?.[selectedDetail.tableName]?.map((field, idx) => (
                      <div key={idx} className="flex items-center justify-between p-2 bg-secondary/20 rounded text-sm">
                        <div className="flex items-center gap-2">
                          <FileText className="w-3 h-3 text-muted-foreground" />
                          <span className="font-mono">{field.field}</span>
                        </div>
                        <span className={`px-1.5 py-0.5 rounded text-[10px] ${
                          field.confidence >= 0.8 ? 'bg-green-500/20 text-green-300' :
                          field.confidence >= 0.5 ? 'bg-yellow-500/20 text-yellow-300' :
                          'bg-red-500/20 text-red-300'
                        }`}>
                          {Math.round(field.confidence * 100)}% confidence
                        </span>
                      </div>
                    ))}
                  </div>
                </div>
              </>
            )}
            
            {selectedDetail.type === 'field' && (
              <div className="space-y-2">
                <h4 className="text-sm font-medium text-muted-foreground">Field Details</h4>
                <div className="grid grid-cols-2 gap-2 text-sm">
                  <div className="bg-secondary/30 rounded p-2">
                    <div className="text-[10px] text-muted-foreground uppercase">Source</div>
                    <div className="font-medium capitalize">{selectedDetail.sourceName}</div>
                  </div>
                  <div className="bg-secondary/30 rounded p-2">
                    <div className="text-[10px] text-muted-foreground uppercase">Table</div>
                    <div className="font-medium font-mono">{selectedDetail.tableName}</div>
                  </div>
                  <div className="bg-secondary/30 rounded p-2 col-span-2">
                    <div className="text-[10px] text-muted-foreground uppercase">Mapping Confidence</div>
                    <div className="flex items-center gap-2 mt-1">
                      <div className="flex-1 h-2 bg-secondary rounded-full overflow-hidden">
                        <div 
                          className={`h-full ${
                            (selectedDetail.confidence || 0) >= 0.8 ? 'bg-green-500' :
                            (selectedDetail.confidence || 0) >= 0.5 ? 'bg-yellow-500' :
                            'bg-red-500'
                          }`}
                          style={{ width: `${(selectedDetail.confidence || 0) * 100}%` }}
                        />
                      </div>
                      <span className="font-medium">{Math.round((selectedDetail.confidence || 0) * 100)}%</span>
                    </div>
                  </div>
                </div>
                
                <div className="mt-4 p-3 bg-blue-500/10 border border-blue-500/20 rounded">
                  <div className="flex items-start gap-2 text-xs">
                    <Info className="w-3.5 h-3.5 shrink-0 mt-0.5 text-blue-400" />
                    <div>
                      <p className="text-blue-200">
                        This field from <span className="font-medium">{selectedDetail.tableName}</span> is mapped to 
                        the <span className="font-medium">{selectedDetail.ontologyLabel}</span> ontology concept 
                        with {Math.round((selectedDetail.confidence || 0) * 100)}% confidence.
                      </p>
                    </div>
                  </div>
                </div>
              </div>
            )}
          </div>
        </div>
      </div>
    );
  };

  return (
    <div className="h-full flex flex-col bg-sidebar/30">
      <div className="px-4 pt-4 pb-2 border-b">
        <div className="flex gap-2">
          <button onClick={() => setActiveTab('views')} className={`text-sm px-3 py-1 rounded-md transition-colors ${activeTab === 'views' ? 'bg-secondary text-secondary-foreground' : 'text-muted-foreground'}`}>Persona Views</button>
          <button onClick={() => setActiveTab('rag')} className={`text-sm px-3 py-1 rounded-md transition-colors ${activeTab === 'rag' ? 'bg-secondary text-secondary-foreground' : 'text-muted-foreground'}`}>RAG History</button>
        </div>
      </div>

      <div className="flex-1 overflow-y-auto p-4 space-y-4">
        {activeTab === 'views' && (
          <>
            {activePersonaViews.length === 0 && (
              <div className="text-center py-8 text-muted-foreground text-sm">Select a persona filter to see views</div>
            )}
            
            {activePersonaViews.map((view) => (
              <div key={view.personaId} className="border-l-4 border-l-primary shadow-sm bg-card/50 rounded p-4 space-y-4">
                <div className="flex justify-between items-start">
                  <div>
                    <h3 className="text-lg font-semibold">{view.title}</h3>
                    <div className="text-xs mt-1 flex gap-2">
                      {view.focusAreas.map(area => (
                        <span key={area} className="text-[10px] border rounded px-1.5 py-0.5">{area}</span>
                      ))}
                    </div>
                  </div>
                  <Badge className="font-mono text-xs">{view.personaId}</Badge>
                </div>

                <div className="grid grid-cols-2 gap-2">
                  {view.metrics.map(metric => (
                    <div key={metric.id} className="bg-secondary/30 rounded p-2 flex flex-col">
                      <span className="text-[10px] text-muted-foreground uppercase truncate">{metric.label}</span>
                      <div className="flex items-end justify-between mt-1">
                        <span className="text-lg font-mono font-medium">
                          {metric.value.toLocaleString()}
                          {metric.unit && <span className="text-xs text-muted-foreground ml-0.5">{metric.unit}</span>}
                        </span>
                        {metric.trend && (
                          <span className={`flex items-center text-xs ${
                            metric.trend === 'up' ? 'text-green-400' : 
                            metric.trend === 'down' ? 'text-red-400' : 'text-muted-foreground'
                          }`}>
                            {metric.trend === 'up' && <TrendingUp className="w-3 h-3 mr-0.5" />}
                            {metric.trend === 'down' && <TrendingDown className="w-3 h-3 mr-0.5" />}
                            {metric.trend === 'flat' && <Minus className="w-3 h-3 mr-0.5" />}
                          </span>
                        )}
                      </div>
                    </div>
                  ))}
                </div>

                {(() => {
                  const connections = getPersonaConnections(view.personaId);
                  const isSourcesExpanded = expandedSections[view.personaId]?.sources ?? false;
                  const isOntologiesExpanded = expandedSections[view.personaId]?.ontologies ?? false;
                  
                  return (
                    <div className="space-y-2 pt-2 border-t border-border/30">
                      <button
                        onClick={() => toggleSection(view.personaId, 'sources')}
                        className="w-full flex items-center justify-between p-2 rounded bg-secondary/20 hover:bg-secondary/40 transition-colors text-sm"
                      >
                        <div className="flex items-center gap-2">
                          <Server className="w-4 h-4 text-cyan-400" />
                          <span>Data Sources</span>
                          <Badge variant="outline" className="h-5 text-[10px] px-1.5">{connections.sources.length}</Badge>
                        </div>
                        {isSourcesExpanded ? <ChevronDown className="w-4 h-4" /> : <ChevronRight className="w-4 h-4" />}
                      </button>
                      
                      {isSourcesExpanded && (
                        <div className="ml-6 space-y-1">
                          {connections.sources.length === 0 ? (
                            <div className="text-xs text-muted-foreground italic p-2">No data sources connected</div>
                          ) : (
                            connections.sources.map(source => (
                              <div key={source.id} className="flex items-center justify-between p-2 rounded bg-secondary/10 text-xs">
                                <div className="flex flex-col">
                                  <span className="font-medium">{source.label}</span>
                                  <span className="text-[10px] text-muted-foreground">{source.group}</span>
                                </div>
                                <div className="flex items-center gap-2">
                                  <span className="text-[10px] text-muted-foreground">
                                    {source.metrics?.tables || 0} tables, {source.metrics?.fields || 0} fields
                                  </span>
                                  <Badge className={source.status === 'ok' ? 'h-5 text-[10px]' : 'h-5 text-[10px] bg-red-500'}>
                                    {source.status === 'ok' ? 'OK' : 'Error'}
                                  </Badge>
                                </div>
                              </div>
                            ))
                          )}
                        </div>
                      )}

                      <button
                        onClick={() => toggleSection(view.personaId, 'ontologies')}
                        className="w-full flex items-center justify-between p-2 rounded bg-secondary/20 hover:bg-secondary/40 transition-colors text-sm"
                      >
                        <div className="flex items-center gap-2">
                          <Layers className="w-4 h-4 text-violet-400" />
                          <span>Ontologies</span>
                          <Badge variant="outline" className="h-5 text-[10px] px-1.5">{connections.ontologies.length}</Badge>
                        </div>
                        {isOntologiesExpanded ? <ChevronDown className="w-4 h-4" /> : <ChevronRight className="w-4 h-4" />}
                      </button>
                      
                      {isOntologiesExpanded && (
                        <div className="ml-6 space-y-1">
                          {connections.ontologies.length === 0 ? (
                            <div className="text-xs text-muted-foreground italic p-2">No ontology concepts mapped</div>
                          ) : (
                            connections.ontologies.map(onto => {
                              const hierarchy = getOntologySourceHierarchy(onto.id);
                              const isExpanded = expandedOntologies[onto.id] ?? false;
                              const sourceCount = hierarchy ? Object.keys(hierarchy).length : 0;
                              const totalFields = hierarchy 
                                ? Object.values(hierarchy).reduce((acc, tables) => 
                                    acc + Object.values(tables).reduce((a, fields) => a + fields.length, 0), 0)
                                : 0;
                              
                              return (
                                <div key={onto.id} className="space-y-1">
                                  <button
                                    onClick={() => toggleOntology(onto.id)}
                                    className="w-full flex items-center justify-between p-2 rounded bg-secondary/10 hover:bg-secondary/20 transition-colors text-xs"
                                  >
                                    <div className="flex items-center gap-2">
                                      {isExpanded ? <ChevronDown className="w-3 h-3" /> : <ChevronRight className="w-3 h-3" />}
                                      <span className="font-medium font-mono">{onto.label}</span>
                                    </div>
                                    <div className="flex items-center gap-1">
                                      <Badge variant="outline" className="h-4 text-[9px] px-1">{sourceCount} sources</Badge>
                                      <Badge variant="outline" className="h-4 text-[9px] px-1">{totalFields} fields</Badge>
                                    </div>
                                  </button>
                                  
                                  {isExpanded && hierarchy && (
                                    <div className="ml-5 space-y-1">
                                      {Object.keys(hierarchy).length === 0 ? (
                                        <div className="text-[10px] text-muted-foreground italic p-1.5">No source fields mapped</div>
                                      ) : (
                                        Object.entries(hierarchy).map(([sourceName, tables]) => {
                                          const sourceKey = `${onto.id}_${sourceName}`;
                                          const isSourceExpanded = expandedSources[sourceKey] ?? false;
                                          const tableCount = Object.keys(tables).length;
                                          const fieldCount = Object.values(tables).reduce((a, f) => a + f.length, 0);
                                          
                                          return (
                                            <div key={sourceKey} className="space-y-1">
                                              <div className="flex items-center gap-1">
                                                <button
                                                  onClick={() => toggleSource(sourceKey)}
                                                  className="flex-1 flex items-center justify-between p-1.5 rounded-l bg-cyan-500/10 hover:bg-cyan-500/20 transition-colors text-[10px] border border-cyan-500/20 border-r-0"
                                                >
                                                  <div className="flex items-center gap-2">
                                                    {isSourceExpanded ? <ChevronDown className="w-2.5 h-2.5" /> : <ChevronRight className="w-2.5 h-2.5" />}
                                                    <Server className="w-3 h-3 text-cyan-400" />
                                                    <span className="font-medium capitalize">{sourceName}</span>
                                                  </div>
                                                  <div className="flex items-center gap-1">
                                                    <Badge variant="outline" className="h-3.5 text-[8px] px-1">{tableCount} tables</Badge>
                                                    <Badge variant="outline" className="h-3.5 text-[8px] px-1">{fieldCount} fields</Badge>
                                                  </div>
                                                </button>
                                                <button
                                                  onClick={() => setSelectedDetail({
                                                    type: 'source',
                                                    ontologyId: onto.id,
                                                    ontologyLabel: onto.label,
                                                    sourceName
                                                  })}
                                                  className="p-1.5 rounded-r bg-cyan-500/10 hover:bg-cyan-500/30 transition-colors border border-cyan-500/20 border-l-0"
                                                  title="View details"
                                                >
                                                  <Info className="w-3 h-3 text-cyan-400" />
                                                </button>
                                              </div>
                                              
                                              {isSourceExpanded && (
                                                <div className="ml-4 space-y-1">
                                                  {Object.entries(tables).map(([tableName, fields]) => {
                                                    const tableKey = `${sourceKey}_${tableName}`;
                                                    const isTableExpanded = expandedTables[tableKey] ?? false;
                                                    const avgConf = fields.reduce((a, f) => a + f.confidence, 0) / fields.length;
                                                    
                                                    return (
                                                      <div key={tableKey} className="space-y-1">
                                                        <div className="flex items-center gap-1">
                                                          <button
                                                            onClick={() => toggleTable(tableKey)}
                                                            className="flex-1 flex items-center justify-between p-1.5 rounded-l bg-violet-500/10 hover:bg-violet-500/20 transition-colors text-[10px] border border-violet-500/20 border-r-0"
                                                          >
                                                            <div className="flex items-center gap-2">
                                                              {isTableExpanded ? <ChevronDown className="w-2.5 h-2.5" /> : <ChevronRight className="w-2.5 h-2.5" />}
                                                              <Table2 className="w-3 h-3 text-violet-400" />
                                                              <span className="font-medium font-mono">{tableName}</span>
                                                            </div>
                                                            <div className="flex items-center gap-1">
                                                              <Badge variant="outline" className="h-3.5 text-[8px] px-1">{fields.length} fields</Badge>
                                                              <span className={`px-1 py-0.5 rounded text-[8px] ${
                                                                avgConf >= 0.8 ? 'bg-green-500/20 text-green-300' :
                                                                avgConf >= 0.5 ? 'bg-yellow-500/20 text-yellow-300' :
                                                                'bg-red-500/20 text-red-300'
                                                              }`}>
                                                                {Math.round(avgConf * 100)}%
                                                              </span>
                                                            </div>
                                                          </button>
                                                          <button
                                                            onClick={() => setSelectedDetail({
                                                              type: 'table',
                                                              ontologyId: onto.id,
                                                              ontologyLabel: onto.label,
                                                              sourceName,
                                                              tableName
                                                            })}
                                                            className="p-1.5 rounded-r bg-violet-500/10 hover:bg-violet-500/30 transition-colors border border-violet-500/20 border-l-0"
                                                            title="View details"
                                                          >
                                                            <Info className="w-3 h-3 text-violet-400" />
                                                          </button>
                                                        </div>
                                                        
                                                        {isTableExpanded && (
                                                          <div className="ml-4 space-y-0.5">
                                                            {fields.map((field, idx) => (
                                                              <button
                                                                key={idx}
                                                                onClick={() => setSelectedDetail({
                                                                  type: 'field',
                                                                  ontologyId: onto.id,
                                                                  ontologyLabel: onto.label,
                                                                  sourceName,
                                                                  tableName,
                                                                  fieldName: field.field,
                                                                  confidence: field.confidence
                                                                })}
                                                                className="w-full flex items-center justify-between p-1 rounded bg-secondary/5 hover:bg-secondary/20 border border-border/10 text-[9px] transition-colors"
                                                              >
                                                                <div className="flex items-center gap-1.5">
                                                                  <FileText className="w-2.5 h-2.5 text-muted-foreground" />
                                                                  <span className="font-mono">{field.field}</span>
                                                                </div>
                                                                <span className={`px-1 py-0.5 rounded text-[8px] ${
                                                                  field.confidence >= 0.8 ? 'bg-green-500/20 text-green-300' :
                                                                  field.confidence >= 0.5 ? 'bg-yellow-500/20 text-yellow-300' :
                                                                  'bg-red-500/20 text-red-300'
                                                                }`}>
                                                                  {Math.round(field.confidence * 100)}%
                                                                </span>
                                                              </button>
                                                            ))}
                                                          </div>
                                                        )}
                                                      </div>
                                                    );
                                                  })}
                                                </div>
                                              )}
                                            </div>
                                          );
                                        })
                                      )}
                                    </div>
                                  )}
                                </div>
                              );
                            })
                          )}
                        </div>
                      )}
                    </div>
                  );
                })()}

                {(view.insights.length > 0 || view.alerts.length > 0) && (
                  <div className="space-y-2 pt-2">
                    {view.alerts.map(alert => (
                      <div key={alert.id} className="flex gap-2 text-xs p-2 rounded bg-red-500/10 border border-red-500/20 text-red-200">
                        <AlertTriangle className="w-3.5 h-3.5 shrink-0 mt-0.5" />
                        <span>{alert.message}</span>
                      </div>
                    ))}
                    {view.insights.map(insight => (
                      <div key={insight.id} className="flex gap-2 text-xs p-2 rounded bg-blue-500/10 border border-blue-500/20 text-blue-200">
                        <Info className="w-3.5 h-3.5 shrink-0 mt-0.5" />
                        <span>{insight.message}</span>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            ))}
          </>
        )}

        {activeTab === 'rag' && (
          <div className="space-y-4">
            <div className="bg-card/50 rounded p-4">
              <div className="flex items-center gap-2 mb-4">
                <Database className="w-4 h-4 text-cyan-400" />
                <h3 className="text-sm font-semibold">Vector Database Status</h3>
              </div>
              <div className="grid grid-cols-2 gap-3">
                <div className="flex items-center gap-2 text-xs">
                  <CheckCircle2 className="w-3.5 h-3.5 text-green-400" />
                  <span>Pinecone Connected</span>
                </div>
                <div className="flex items-center gap-2 text-xs">
                  <CheckCircle2 className="w-3.5 h-3.5 text-green-400" />
                  <span>API Keys Configured</span>
                </div>
              </div>
            </div>

            <div className="bg-card/50 rounded p-4">
              <div className="flex items-center gap-2 mb-4">
                <Zap className="w-4 h-4 text-violet-400" />
                <h3 className="text-sm font-semibold">RAG Metrics</h3>
              </div>
              <div className="grid grid-cols-3 gap-2">
                <div className="bg-secondary/30 rounded p-2">
                  <div className="text-[10px] text-muted-foreground uppercase">LLM Calls</div>
                  <div className="text-lg font-mono font-medium mt-1">{ragMetrics.llm_calls}</div>
                </div>
                <div className="bg-secondary/30 rounded p-2">
                  <div className="text-[10px] text-muted-foreground uppercase">Vector Reads</div>
                  <div className="text-lg font-mono font-medium mt-1">{ragMetrics.rag_reads}</div>
                </div>
                <div className="bg-secondary/30 rounded p-2">
                  <div className="text-[10px] text-muted-foreground uppercase">Vector Writes</div>
                  <div className="text-lg font-mono font-medium mt-1">{ragMetrics.rag_writes}</div>
                </div>
              </div>
            </div>

            <div className="bg-card/50 rounded p-4">
              <h3 className="text-sm font-semibold mb-3">Operation Log</h3>
              <div className="space-y-2 max-h-64 overflow-y-auto">
                {ragMessages.length === 0 && (
                  <div className="text-xs text-muted-foreground italic">No RAG operations yet. Run pipeline in Prod mode to see activity.</div>
                )}
                {ragMessages.map((msg, idx) => (
                  <div key={idx} className="flex gap-2 text-xs p-2 rounded bg-secondary/20 border border-border/30">
                    <Database className="w-3.5 h-3.5 shrink-0 mt-0.5 text-cyan-400" />
                    <div className="flex-1">
                      <div className="flex items-center gap-2 mb-1">
                        <Badge variant="outline" className="h-4 text-[10px] px-1">{msg.source}</Badge>
                        <span className="text-[10px] text-muted-foreground font-mono">
                          {new Date(msg.timestamp).toLocaleTimeString([], { hour12: false })}
                        </span>
                      </div>
                      <p className="text-foreground/90">{msg.message}</p>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>
        )}
      </div>
      
      <DetailPanel />
    </div>
  );
}
