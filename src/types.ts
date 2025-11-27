export type PersonaId = 'CFO' | 'CRO' | 'COO' | 'CTO';

export type Severity = 'info' | 'low' | 'medium' | 'high' | 'critical';
export type Trend = 'up' | 'down' | 'flat' | 'unknown';

export type DiscoveryStatus = 'canonical' | 'pending_triage' | 'custom' | 'rejected';
export type ResolutionType = 'exact' | 'alias' | 'pattern' | 'fuzzy' | 'discovered';

export interface SourceMetrics {
  tables?: number;
  fields?: number;
  type?: string;
  canonical_id?: string;
  raw_id?: string;
  discovery_status?: DiscoveryStatus;
  resolution_type?: ResolutionType;
  trust_score?: number;
  data_quality_score?: number;
  vendor?: string;
  category?: string;
}

export interface GraphNode {
  id: string;
  label: string;
  level: 'L0' | 'L1' | 'L2' | 'L3';
  kind: 'pipe' | 'source' | 'ontology' | 'bll';
  group?: string;
  status?: string;
  metrics?: SourceMetrics & Record<string, unknown>;
  personaId?: PersonaId;
  x0?: number;
  y0?: number;
  x1?: number;
  y1?: number;
}

export interface GraphLink {
  id: string;
  source: string | GraphNode;
  target: string | GraphNode;
  value: number;
  confidence?: number;
  flowType?: string;
  flow_type?: string;
  infoSummary?: string;
  info_summary?: string;
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

export interface GraphSnapshot {
  nodes: GraphNode[];
  links: GraphLink[];
  meta: {
    mode: 'Demo' | 'Farm';
    runId: string;
    generatedAt: string;
    stats?: Record<string, unknown>;
    personaViews?: PersonaView[];
    runMetrics?: {
      llm_calls: number;
      rag_reads: number;
      rag_writes: number;
      processing_ms: number;
      render_ms: number;
    };
  };
}
