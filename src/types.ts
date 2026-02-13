export type PersonaId = 'CFO' | 'CRO' | 'COO' | 'CTO';

export type Severity = 'info' | 'low' | 'medium' | 'high' | 'critical';
export type Trend = 'up' | 'down' | 'flat' | 'unknown';

export type DiscoveryStatus = 'canonical' | 'pending_triage' | 'custom' | 'rejected';
export type ResolutionType = 'exact' | 'alias' | 'pattern' | 'fuzzy' | 'discovered';

export interface SourceMetrics {
  tables?: number;
  fields?: number;
  type?: string;
  canonicalId?: string;
  rawId?: string;
  discoveryStatus?: DiscoveryStatus;
  resolutionType?: ResolutionType;
  trustScore?: number;
  dataQualityScore?: number;
  vendor?: string;
  category?: string;
}

export interface GraphNode {
  id: string;
  label: string;
  level: 'L0' | 'L1' | 'L2' | 'L3';
  kind: 'pipe' | 'source' | 'ontology' | 'bll' | 'fabric';
  group?: string;
  status?: string;
  metrics?: SourceMetrics & Record<string, unknown>;
  personaId?: PersonaId;
  x0?: number;
  y0?: number;
  x1?: number;
  y1?: number;
}

/**
 * Structured mapping information for graph links.
 * Available when flowType === 'mapping'.
 */
export interface MappingDetail {
  sourceField: string;
  sourceTable: string;
  targetConcept: string;
  method: 'heuristic' | 'rag' | 'llm' | 'llm_validated';
  confidence: number;
}

export interface GraphLink {
  id: string;
  source: string | GraphNode;
  target: string | GraphNode;
  value: number;
  confidence?: number;
  flowType?: string;
  infoSummary?: string;
  mappingDetail?: MappingDetail;
  width?: number;
}

export interface PersonaMetric {
  id: string;
  label: string;
  value: number;
  unit?: string;
  trend?: Trend;
  trendDeltaPct?: number;
}

export interface PersonaInsight {
  id: string;
  severity: Severity;
  message: string;
  relatedOntology?: string[];
  relatedSources?: string[];
}

export interface PersonaAlert {
  id: string;
  severity: Severity;
  message: string;
  relatedOntology?: string[];
  relatedSources?: string[];
}

export interface PersonaView {
  personaId: PersonaId;
  title: string;
  focusAreas: string[];
  keyEntities: string[];
  metrics: PersonaMetric[];
  insights: PersonaInsight[];
  alerts: PersonaAlert[];
}

export interface PayloadKpis {
  planesReceived: number;
  totalConnections: number;
  totalFields: number;
  emptyPlanes: number;
  planesDetail: Array<{ planeType: string; vendor: string; connections: number; fields: number }>;
  governedPct: number;
}

export interface RunMetrics {
  llmCalls: number;
  ragReads: number;
  ragWrites: number;
  totalMappings: number;
  processingMs: number;
  renderMs: number;
  dataStatus?: string | null;
  payloadKpis?: PayloadKpis | null;
}

export interface GraphSnapshot {
  nodes: GraphNode[];
  links: GraphLink[];
  meta: {
    mode: 'Demo' | 'Farm' | 'AAM';
    runId: string;
    generatedAt: string;
    stats?: Record<string, unknown>;
    personaViews?: PersonaView[];
    runMetrics?: RunMetrics;
  };
}
