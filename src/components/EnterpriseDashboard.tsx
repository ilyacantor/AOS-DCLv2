import { useState, useMemo } from 'react';
import { GraphSnapshot, GraphLink } from '../types';
import { CONFIDENCE } from '../constants';
import Database from 'lucide-react/dist/esm/icons/database';
import ArrowRight from 'lucide-react/dist/esm/icons/arrow-right';
import CheckCircle2 from 'lucide-react/dist/esm/icons/check-circle-2';
import AlertCircle from 'lucide-react/dist/esm/icons/alert-circle';
import AlertTriangle from 'lucide-react/dist/esm/icons/alert-triangle';
import Search from 'lucide-react/dist/esm/icons/search';
import Filter from 'lucide-react/dist/esm/icons/filter';
import ChevronDown from 'lucide-react/dist/esm/icons/chevron-down';
import Layers from 'lucide-react/dist/esm/icons/layers';
import GitBranch from 'lucide-react/dist/esm/icons/git-branch';
import Shield from 'lucide-react/dist/esm/icons/shield';
import HelpCircle from 'lucide-react/dist/esm/icons/help-circle';
import Building2 from 'lucide-react/dist/esm/icons/building-2';

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

/**
 * Extract mapping item from a link using structured mappingDetail (preferred)
 * or falling back to string parsing for backward compatibility.
 */
function extractMappingFromLink(
  link: GraphLink,
  sourceLabel: string,
  targetLabel: string
): MappingItem | null {
  // Prefer structured mappingDetail if available
  if (link.mappingDetail) {
    return {
      id: link.id,
      sourceSystem: sourceLabel,
      sourceTable: link.mappingDetail.sourceTable,
      sourceField: link.mappingDetail.sourceField,
      targetConcept: link.mappingDetail.targetConcept,
      confidence: link.mappingDetail.confidence,
      method: link.mappingDetail.method,
    };
  }

  // Fallback: parse infoSummary for backward compatibility
  const infoSummary = link.infoSummary;
  if (!infoSummary) return null;

  const parts = infoSummary.split(' → ');
  const fieldPart = parts[0] || '';
  const confidence = link.confidence || 0;

  return {
    id: link.id,
    sourceSystem: sourceLabel,
    sourceTable: fieldPart.includes('.') ? fieldPart.split('.')[0] : 'default',
    sourceField: fieldPart.includes('.') ? fieldPart.split('.').slice(1).join('.') : fieldPart,
    targetConcept: targetLabel,
    confidence,
    method: infoSummary.includes('llm') ? 'llm' : 'heuristic',
  };
}

type KpiPanel = 'sources' | 'canonical' | 'totalMappings' | 'highConfidence' | 'needsReview';

export function EnterpriseDashboard({ data, runId }: EnterpriseDashboardProps) {
  const [searchTerm, setSearchTerm] = useState('');
  const [confidenceFilter, setConfidenceFilter] = useState<'all' | 'high' | 'medium' | 'low'>('all');
  const [sourceFilter, setSourceFilter] = useState<string>('all');
  const [expandedKpi, setExpandedKpi] = useState<KpiPanel | null>(null);

  const toggleKpi = (panel: KpiPanel) => {
    setExpandedKpi(prev => prev === panel ? null : panel);
  };

  const { mappings, sources, sourceNodes, stats, fabricStats } = useMemo(() => {
    if (!data) return { 
      mappings: [], 
      sources: [], 
      sourceNodes: [], 
      stats: { total: 0, high: 0, medium: 0, low: 0, canonical: 0, pending: 0 },
      fabricStats: null
    };

    const mappingItems: MappingItem[] = [];
    const sourceSet = new Set<string>();

    // Check for fabric nodes (they carry the absolute truth in their metrics)
    const fabricNodes = data.nodes.filter(n => n.kind === 'fabric');
    
    // Sources can be at L1 (individual sources) OR fabrics can be at L1 (aggregated mode)
    const srcNodes = data.nodes
      .filter(n => (n.level === 'L1' && (n.kind === 'source' || n.kind === 'fabric')))
      .sort((a, b) => {
        const metrics = a.metrics as Record<string, unknown> | undefined;
        const metricsB = b.metrics as Record<string, unknown> | undefined;
        const trustA = (metrics?.trustScore ?? metrics?.trust_score ?? 50) as number;
        const trustB = (metricsB?.trustScore ?? metricsB?.trust_score ?? 50) as number;
        return trustB - trustA;
      });

    const canonicalCount = srcNodes.filter(n => {
      const metrics = n.metrics as Record<string, unknown> | undefined;
      const status = metrics?.discoveryStatus ?? metrics?.discovery_status;
      return status === 'canonical';
    }).length;

    const pendingCount = srcNodes.filter(n => {
      const metrics = n.metrics as Record<string, unknown> | undefined;
      const status = metrics?.discoveryStatus ?? metrics?.discovery_status;
      return status === 'pending_triage';
    }).length;

    // Extract absolute fabric stats (always, regardless of graph aggregation)
    let fabricStatsData = null;
    if (fabricNodes.length > 0) {
      const totalCandidates = fabricNodes.reduce((sum, fn) => {
        const metrics = fn.metrics as Record<string, unknown> | undefined;
        return sum + ((metrics?.source_count as number) || 0);
      }, 0);

      // Extract fabric breakdown directly from nodes (no further grouping needed)
      const fabricBreakdown = fabricNodes.map(fn => {
        const metrics = fn.metrics as Record<string, unknown> | undefined;
        const fabricType = (metrics?.fabric_type as string) || 'unknown';
        const sourceCount = (metrics?.source_count as number) || 0;
        const vendorsArray = (metrics?.vendors as string[]) || [];
        const sources = (metrics?.sources as string[]) || [];
        
        return {
          type: fabricType,
          instanceCount: vendorsArray.length,  // Number of vendor instances
          count: sourceCount,
          vendors: vendorsArray,
          sources: sources
        };
      }).filter(f => f.count > 0);  // Filter out empty fabrics

      fabricStatsData = {
        totalCandidates,
        fabrics: fabricBreakdown
      };
    }

    data.links.forEach(link => {
      const flowType = link.flowType;

      if (flowType === 'mapping') {
        const sourceNode = data.nodes.find(n => n.id === link.source);
        const targetNode = data.nodes.find(n => n.id === link.target);

        if (sourceNode && targetNode) {
          const mappingItem = extractMappingFromLink(link, sourceNode.label, targetNode.label);
          if (mappingItem) {
            sourceSet.add(sourceNode.label);
            mappingItems.push(mappingItem);
          }
        }
      }
    });

    const high = mappingItems.filter(m => m.confidence >= CONFIDENCE.HIGH).length;
    const medium = mappingItems.filter(m => m.confidence >= CONFIDENCE.MEDIUM && m.confidence < CONFIDENCE.HIGH).length;
    const low = mappingItems.filter(m => m.confidence < CONFIDENCE.MEDIUM).length;

    return {
      mappings: mappingItems,
      sources: Array.from(sourceSet).sort(),
      sourceNodes: srcNodes,
      stats: { total: mappingItems.length, high, medium, low, canonical: canonicalCount, pending: pendingCount },
      fabricStats: fabricStatsData
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
        (confidenceFilter === 'high' && m.confidence >= CONFIDENCE.HIGH) ||
        (confidenceFilter === 'medium' && m.confidence >= CONFIDENCE.MEDIUM && m.confidence < CONFIDENCE.HIGH) ||
        (confidenceFilter === 'low' && m.confidence < CONFIDENCE.MEDIUM);

      const matchesSource = sourceFilter === 'all' || m.sourceSystem === sourceFilter;

      return matchesSearch && matchesConfidence && matchesSource;
    });
  }, [mappings, searchTerm, confidenceFilter, sourceFilter]);

  const getConfidenceColor = (confidence: number) => {
    if (confidence >= CONFIDENCE.HIGH) return 'text-green-400';
    if (confidence >= CONFIDENCE.MEDIUM) return 'text-yellow-400';
    return 'text-red-400';
  };

  const getConfidenceBg = (confidence: number) => {
    if (confidence >= CONFIDENCE.HIGH) return 'bg-green-500/10 border-green-500/30';
    if (confidence >= CONFIDENCE.MEDIUM) return 'bg-yellow-500/10 border-yellow-500/30';
    return 'bg-red-500/10 border-red-500/30';
  };

  const getConfidenceIcon = (confidence: number) => {
    if (confidence >= CONFIDENCE.HIGH) return <CheckCircle2 className="w-3.5 h-3.5 text-green-400" />;
    if (confidence >= CONFIDENCE.MEDIUM) return <AlertTriangle className="w-3.5 h-3.5 text-yellow-400" />;
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
      {/* KPI Row - Always shows absolute truth */}
      {fabricStats ? (
        // AAM Mode - Absolute fabric stats (regardless of graph aggregation)
        <div className="grid grid-cols-6 gap-3 p-4 border-b shrink-0">
          <div className="bg-card rounded-lg p-3 border border-primary/30">
            <div className="flex items-center gap-2 text-primary text-xs mb-1">
              <Database className="w-3.5 h-3.5" />
              <span>Total Candidates</span>
            </div>
            <div className="text-2xl font-bold">{fabricStats.totalCandidates}</div>
            <div className="text-[10px] text-muted-foreground mt-1">Imported assets</div>
          </div>
          {fabricStats.fabrics.map(fabric => (
            <div 
              key={fabric.type} 
              className="bg-card rounded-lg p-3 border cursor-pointer hover:border-primary/50 transition-colors"
              title={`${fabric.instanceCount} instance${fabric.instanceCount > 1 ? 's' : ''}\nVendors: ${fabric.vendors.join(', ')}\nSources: ${fabric.sources.slice(0, 5).join(', ')}${fabric.sources.length > 5 ? '...' : ''}`}
            >
              <div className="flex items-center gap-2 text-blue-400 text-xs mb-1">
                <Layers className="w-3.5 h-3.5" />
                <span>{fabric.type.toUpperCase()}</span>
                {fabric.instanceCount > 1 && (
                  <span className="text-[10px] opacity-70">x{fabric.instanceCount}</span>
                )}
              </div>
              <div className="text-2xl font-bold">{fabric.count}</div>
              <div className="text-[10px] text-muted-foreground mt-1 truncate">{fabric.vendors.join(', ')}</div>
            </div>
          ))}
          <div className="bg-card rounded-lg p-3 border">
            <div className="flex items-center gap-2 text-green-400 text-xs mb-1">
              <Shield className="w-3.5 h-3.5" />
              <span>SORs</span>
            </div>
            <div className="text-2xl font-bold text-green-400">{stats.canonical}</div>
            <div className="text-[10px] text-muted-foreground mt-1">Systems of Record</div>
          </div>
        </div>
      ) : (
        // Demo/Farm Mode - Standard KPIs (clickable accordion)
        <>
        <div className="grid grid-cols-5 gap-3 p-4 border-b shrink-0">
          <div
            className={`bg-card rounded-lg p-3 border cursor-pointer transition-all hover:border-primary/50 ${
              expandedKpi === 'sources' ? 'border-primary ring-1 ring-primary/30' : ''
            }`}
            onClick={() => toggleKpi('sources')}
          >
            <div className="flex items-center justify-between text-xs mb-1">
              <div className="flex items-center gap-2 text-muted-foreground">
                <Layers className="w-3.5 h-3.5" />
                <span>Sources</span>
              </div>
              <ChevronDown className={`w-3.5 h-3.5 text-muted-foreground transition-transform ${expandedKpi === 'sources' ? 'rotate-180' : ''}`} />
            </div>
            <div className="text-2xl font-bold">{sourceNodes.length}</div>
          </div>
          <div
            className={`bg-card rounded-lg p-3 border cursor-pointer transition-all hover:border-green-500/50 ${
              expandedKpi === 'canonical' ? 'border-green-500 ring-1 ring-green-500/30' : ''
            }`}
            onClick={() => toggleKpi('canonical')}
          >
            <div className="flex items-center justify-between text-xs mb-1">
              <div className="flex items-center gap-2 text-green-400">
                <Shield className="w-3.5 h-3.5" />
                <span>Canonical</span>
              </div>
              <ChevronDown className={`w-3.5 h-3.5 text-muted-foreground transition-transform ${expandedKpi === 'canonical' ? 'rotate-180' : ''}`} />
            </div>
            <div className="text-2xl font-bold text-green-400">{stats.canonical}</div>
          </div>
          <div
            className={`bg-card rounded-lg p-3 border cursor-pointer transition-all hover:border-primary/50 ${
              expandedKpi === 'totalMappings' ? 'border-primary ring-1 ring-primary/30' : ''
            }`}
            onClick={() => toggleKpi('totalMappings')}
          >
            <div className="flex items-center justify-between text-xs mb-1">
              <div className="flex items-center gap-2 text-muted-foreground">
                <GitBranch className="w-3.5 h-3.5" />
                <span>Total Mappings</span>
              </div>
              <ChevronDown className={`w-3.5 h-3.5 text-muted-foreground transition-transform ${expandedKpi === 'totalMappings' ? 'rotate-180' : ''}`} />
            </div>
            <div className="text-2xl font-bold">{stats.total}</div>
          </div>
          <div
            className={`bg-card rounded-lg p-3 border cursor-pointer transition-all hover:border-green-500/50 ${
              expandedKpi === 'highConfidence' ? 'border-green-500 ring-1 ring-green-500/30' : ''
            }`}
            onClick={() => toggleKpi('highConfidence')}
          >
            <div className="flex items-center justify-between text-xs mb-1">
              <div className="flex items-center gap-2 text-green-400">
                <CheckCircle2 className="w-3.5 h-3.5" />
                <span>High Confidence</span>
              </div>
              <ChevronDown className={`w-3.5 h-3.5 text-muted-foreground transition-transform ${expandedKpi === 'highConfidence' ? 'rotate-180' : ''}`} />
            </div>
            <div className="text-2xl font-bold text-green-400">{stats.high}</div>
          </div>
          <div
            className={`bg-card rounded-lg p-3 border cursor-pointer transition-all hover:border-yellow-500/50 ${
              expandedKpi === 'needsReview' ? 'border-yellow-500 ring-1 ring-yellow-500/30' : ''
            }`}
            onClick={() => toggleKpi('needsReview')}
          >
            <div className="flex items-center justify-between text-xs mb-1">
              <div className="flex items-center gap-2 text-yellow-400">
                <AlertTriangle className="w-3.5 h-3.5" />
                <span>Needs Review</span>
              </div>
              <ChevronDown className={`w-3.5 h-3.5 text-muted-foreground transition-transform ${expandedKpi === 'needsReview' ? 'rotate-180' : ''}`} />
            </div>
            <div className="text-2xl font-bold text-yellow-400">{stats.medium + stats.low}</div>
          </div>
        </div>

        {/* Accordion Detail Panel */}
        {expandedKpi && (
          <div className="border-b shrink-0 bg-card/30 overflow-auto" style={{ maxHeight: '300px' }}>
            {/* Sources Panel */}
            {expandedKpi === 'sources' && (
              <div className="p-3">
                <div className="flex items-center gap-2 text-xs text-muted-foreground mb-2">
                  <Building2 className="w-3.5 h-3.5" />
                  <span>Source Registry — {sourceNodes.length} discovered systems</span>
                  {stats.pending > 0 && (
                    <span className="flex items-center gap-1 text-yellow-400">
                      <HelpCircle className="w-3 h-3" />
                      {stats.pending} pending triage
                    </span>
                  )}
                </div>
                <div className="flex flex-wrap gap-2">
                  {sourceNodes.map(node => {
                    const metrics = node.metrics as Record<string, unknown> | undefined;
                    const trustScore = (metrics?.trustScore ?? metrics?.trust_score ?? 50) as number;
                    const discoveryStatus = (metrics?.discoveryStatus ?? metrics?.discovery_status) as string | undefined;
                    const vendor = metrics?.vendor as string | undefined;
                    const isCanonical = discoveryStatus === 'canonical';
                    return (
                      <div
                        key={node.id}
                        className={`flex items-center gap-2 px-2 py-1 rounded text-xs border ${
                          isCanonical
                            ? 'bg-green-500/10 border-green-500/30'
                            : 'bg-yellow-500/10 border-yellow-500/30'
                        }`}
                        title={`${node.label}\nVendor: ${vendor || 'Unknown'}\nTrust: ${trustScore}%\nStatus: ${discoveryStatus || 'unknown'}`}
                      >
                        {isCanonical ? (
                          <Shield className="w-3 h-3 text-green-400" />
                        ) : (
                          <HelpCircle className="w-3 h-3 text-yellow-400" />
                        )}
                        <span className="font-medium">{node.label}</span>
                        <span className={`font-mono ${trustScore >= 80 ? 'text-green-400' : trustScore >= 60 ? 'text-yellow-400' : 'text-red-400'}`}>
                          {trustScore}
                        </span>
                      </div>
                    );
                  })}
                </div>
              </div>
            )}

            {/* Canonical Panel */}
            {expandedKpi === 'canonical' && (
              <div className="p-3">
                <div className="text-xs text-muted-foreground mb-2">
                  <span className="text-green-400 font-medium">Canonical sources</span> are verified Systems of Record — the authoritative data source for each domain.
                </div>
                <div className="flex flex-wrap gap-2">
                  {sourceNodes
                    .filter(node => {
                      const metrics = node.metrics as Record<string, unknown> | undefined;
                      const status = metrics?.discoveryStatus ?? metrics?.discovery_status;
                      return status === 'canonical';
                    })
                    .map(node => {
                      const metrics = node.metrics as Record<string, unknown> | undefined;
                      const trustScore = (metrics?.trustScore ?? metrics?.trust_score ?? 50) as number;
                      const vendor = metrics?.vendor as string | undefined;
                      return (
                        <div
                          key={node.id}
                          className="flex items-center gap-2 px-2 py-1 rounded text-xs border bg-green-500/10 border-green-500/30"
                          title={`${node.label}\nVendor: ${vendor || 'Unknown'}\nTrust: ${trustScore}%`}
                        >
                          <Shield className="w-3 h-3 text-green-400" />
                          <span className="font-medium">{node.label}</span>
                          <span className="font-mono text-green-400">{trustScore}</span>
                        </div>
                      );
                    })}
                  {stats.canonical === 0 && (
                    <div className="text-xs text-muted-foreground">No canonical sources identified yet.</div>
                  )}
                </div>
              </div>
            )}

            {/* Total Mappings Panel */}
            {expandedKpi === 'totalMappings' && (
              <div className="p-3">
                <div className="text-xs text-muted-foreground mb-3">Confidence distribution across {stats.total} field-to-concept mappings</div>
                {/* Stacked bar */}
                {stats.total > 0 && (
                  <div className="mb-3">
                    <div className="flex h-4 rounded overflow-hidden">
                      {stats.high > 0 && (
                        <div
                          className="bg-green-500/70 flex items-center justify-center text-[10px] font-mono text-white"
                          style={{ width: `${(stats.high / stats.total) * 100}%` }}
                          title={`High: ${stats.high}`}
                        >{stats.high}</div>
                      )}
                      {stats.medium > 0 && (
                        <div
                          className="bg-yellow-500/70 flex items-center justify-center text-[10px] font-mono text-white"
                          style={{ width: `${(stats.medium / stats.total) * 100}%` }}
                          title={`Medium: ${stats.medium}`}
                        >{stats.medium}</div>
                      )}
                      {stats.low > 0 && (
                        <div
                          className="bg-red-500/70 flex items-center justify-center text-[10px] font-mono text-white"
                          style={{ width: `${(stats.low / stats.total) * 100}%` }}
                          title={`Low: ${stats.low}`}
                        >{stats.low}</div>
                      )}
                    </div>
                    <div className="flex gap-4 mt-2 text-[10px]">
                      <span className="flex items-center gap-1"><span className="w-2 h-2 rounded bg-green-500/70" /> High ({'\u2265'}85%): {stats.high}</span>
                      <span className="flex items-center gap-1"><span className="w-2 h-2 rounded bg-yellow-500/70" /> Medium (60-84%): {stats.medium}</span>
                      <span className="flex items-center gap-1"><span className="w-2 h-2 rounded bg-red-500/70" /> Low ({'<'}60%): {stats.low}</span>
                    </div>
                  </div>
                )}
                {/* Per-source breakdown */}
                <div className="text-xs text-muted-foreground mb-1.5">Per-source breakdown</div>
                <div className="grid grid-cols-2 gap-2">
                  {sources.map(source => {
                    const sourceMappings = mappings.filter(m => m.sourceSystem === source);
                    const sHigh = sourceMappings.filter(m => m.confidence >= CONFIDENCE.HIGH).length;
                    const sMed = sourceMappings.filter(m => m.confidence >= CONFIDENCE.MEDIUM && m.confidence < CONFIDENCE.HIGH).length;
                    const sLow = sourceMappings.filter(m => m.confidence < CONFIDENCE.MEDIUM).length;
                    return (
                      <div key={source} className="flex items-center gap-2 text-xs bg-secondary/20 rounded px-2 py-1">
                        <span className="font-medium truncate max-w-[100px]" title={source}>{source}</span>
                        <span className="font-mono text-muted-foreground">{sourceMappings.length}</span>
                        <span className="text-green-400 font-mono">{sHigh}</span>
                        <span className="text-yellow-400 font-mono">{sMed}</span>
                        <span className="text-red-400 font-mono">{sLow}</span>
                      </div>
                    );
                  })}
                </div>
              </div>
            )}

            {/* High Confidence Panel */}
            {expandedKpi === 'highConfidence' && (
              <div className="p-3">
                <div className="text-xs text-muted-foreground mb-2">
                  High-confidence mappings ({'\u2265'}85%) — verified field-to-concept links grouped by source
                </div>
                {sources.map(source => {
                  const highMappings = mappings.filter(m => m.sourceSystem === source && m.confidence >= CONFIDENCE.HIGH);
                  if (highMappings.length === 0) return null;
                  return (
                    <div key={source} className="mb-2">
                      <div className="text-xs font-medium text-primary mb-1">{source} ({highMappings.length})</div>
                      <div className="flex flex-wrap gap-1.5">
                        {highMappings.slice(0, 10).map(m => (
                          <div key={m.id} className="flex items-center gap-1 text-[11px] bg-green-500/10 border border-green-500/20 rounded px-1.5 py-0.5">
                            <span className="font-mono text-muted-foreground">{m.sourceField}</span>
                            <ArrowRight className="w-3 h-3 text-green-400" />
                            <span className="font-medium text-green-400">{m.targetConcept}</span>
                          </div>
                        ))}
                        {highMappings.length > 10 && (
                          <span className="text-[10px] text-muted-foreground self-center">+{highMappings.length - 10} more</span>
                        )}
                      </div>
                    </div>
                  );
                })}
                {stats.high === 0 && (
                  <div className="text-xs text-muted-foreground">No high-confidence mappings yet.</div>
                )}
              </div>
            )}

            {/* Needs Review Panel */}
            {expandedKpi === 'needsReview' && (
              <div className="p-3">
                <div className="bg-yellow-500/10 border border-yellow-500/20 rounded-md p-2.5 mb-3 text-xs">
                  <div className="font-medium text-yellow-400 mb-1">What are these {stats.medium + stats.low} items?</div>
                  <div className="text-muted-foreground">
                    These are field-to-concept semantic mappings where the system's confidence is below 85%.
                    They need human verification before promotion to the canonical catalog.
                  </div>
                </div>
                <div className="grid grid-cols-2 gap-3">
                  {/* Medium confidence column */}
                  <div>
                    <div className="flex items-center gap-1.5 text-xs font-medium text-yellow-400 mb-1.5">
                      <AlertTriangle className="w-3 h-3" />
                      Medium (60-84%) — {stats.medium}
                    </div>
                    <div className="text-[10px] text-muted-foreground mb-2">Likely correct, needs human verification</div>
                    <div className="space-y-1">
                      {mappings
                        .filter(m => m.confidence >= CONFIDENCE.MEDIUM && m.confidence < CONFIDENCE.HIGH)
                        .slice(0, 8)
                        .map(m => (
                          <div key={m.id} className="flex items-center gap-1 text-[11px] bg-yellow-500/10 border border-yellow-500/20 rounded px-1.5 py-0.5">
                            <span className="font-mono text-muted-foreground truncate max-w-[80px]" title={m.sourceField}>{m.sourceField}</span>
                            <ArrowRight className="w-3 h-3 text-yellow-400 shrink-0" />
                            <span className="font-medium text-yellow-400 truncate max-w-[80px]" title={m.targetConcept}>{m.targetConcept}</span>
                            <span className="font-mono text-[10px] text-yellow-400/70 shrink-0">{(m.confidence * 100).toFixed(0)}%</span>
                          </div>
                        ))}
                      {stats.medium > 8 && (
                        <div className="text-[10px] text-muted-foreground">+{stats.medium - 8} more</div>
                      )}
                    </div>
                  </div>
                  {/* Low confidence column */}
                  <div>
                    <div className="flex items-center gap-1.5 text-xs font-medium text-red-400 mb-1.5">
                      <AlertCircle className="w-3 h-3" />
                      Low ({'<'}60%) — {stats.low}
                    </div>
                    <div className="text-[10px] text-muted-foreground mb-2">Likely incorrect, needs manual mapping</div>
                    <div className="space-y-1">
                      {mappings
                        .filter(m => m.confidence < CONFIDENCE.MEDIUM)
                        .slice(0, 8)
                        .map(m => (
                          <div key={m.id} className="flex items-center gap-1 text-[11px] bg-red-500/10 border border-red-500/20 rounded px-1.5 py-0.5">
                            <span className="font-mono text-muted-foreground truncate max-w-[80px]" title={m.sourceField}>{m.sourceField}</span>
                            <ArrowRight className="w-3 h-3 text-red-400 shrink-0" />
                            <span className="font-medium text-red-400 truncate max-w-[80px]" title={m.targetConcept}>{m.targetConcept}</span>
                            <span className="font-mono text-[10px] text-red-400/70 shrink-0">{(m.confidence * 100).toFixed(0)}%</span>
                          </div>
                        ))}
                      {stats.low > 8 && (
                        <div className="text-[10px] text-muted-foreground">+{stats.low - 8} more</div>
                      )}
                      {stats.low === 0 && (
                        <div className="text-[10px] text-muted-foreground">No low-confidence mappings.</div>
                      )}
                    </div>
                  </div>
                </div>
              </div>
            )}
          </div>
        )}
        </>
      )}

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
            onChange={(e) => setConfidenceFilter(e.target.value as 'all' | 'high' | 'medium' | 'low')}
            className="px-3 py-2 text-sm bg-background border rounded-md cursor-pointer"
          >
            <option value="all">All Confidence</option>
            <option value="high">High (&ge;85%)</option>
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
