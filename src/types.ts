export type PersonaId = 'CFO' | 'CRO' | 'COO' | 'CTO';

export type Severity = 'info' | 'low' | 'medium' | 'high' | 'critical';
export type Trend = 'up' | 'down' | 'flat' | 'unknown';

export interface GraphNode {
  id: string;
  label: string;
  level: 'L0' | 'L1' | 'L2' | 'L3';
  kind: 'pipe' | 'source' | 'ontology' | 'bll';
  group?: string;
  status?: string;
  metrics?: Record<string, any>;
  personaId?: PersonaId;
}

export interface GraphLink {
  id: string;
  source: string;
  target: string;
  value: number;
  confidence?: number;
  flowType?: string;
  infoSummary?: string;
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
      llmCalls: number;
      ragReads: number;
      ragWrites: number;
      processingMs: number;
      totalMs: number;
    };
  };
}
