/**
 * NLQ (Natural Language Query) Types
 * Types for the Ask UI and NLQ API interactions
 */

export interface NLQAskRequest {
  question: string;
  tenant_id?: string;
  dataset_id?: string;
}

export interface NLQWarning {
  type: string;
  message: string;
}

export interface NLQAggregations {
  population_total?: number;
  population_count?: number;
  topn_total?: number;
  shown_total?: number;
  share_of_total_pct?: number;
  [key: string]: unknown;
}

export interface NLQSummary {
  aggregations: NLQAggregations;
  warnings?: string[];
  debug_summary?: string;
  answer?: string;
}

export interface NLQClarificationCandidate {
  definition_id: string;
  name: string;
  description: string;
  score: number;
}

export interface NLQAskResponse {
  question: string;
  definition_id: string;
  confidence_score: number;
  execution_args: Record<string, unknown>;
  data: Record<string, unknown>[];
  metadata: {
    dataset_id: string;
    definition_id: string;
    row_count: number;
    total_available?: number;
    execution_time_ms: number;
    matched_keywords?: string[];
    effective_limit?: number;
  };
  summary?: NLQSummary;
  caveats: string[];
  needs_clarification: boolean;
  clarification_prompt?: string;
  candidates?: NLQClarificationCandidate[];
}

export interface HistoryEntry {
  id: string;
  timestamp: string;
  question: string;
  dataset_id: string;
  definition_id: string;
  extracted_params: Record<string, unknown>;
  response: NLQAskResponse;
  latency_ms: number;
  status: string;
}

export interface DatasetInfo {
  dataset_id: string;
  snapshot_ts?: string;
  source: 'demo' | 'farm' | 'env';
  description?: string;
}

export interface Preset {
  id: string;
  label: string;
  question: string;
  category: string;
}

// Response payload type for debug calls
export interface DebugResponsePayload {
  definition_id?: string;
  confidence_score?: number;
  caveats?: string[];
  metadata?: {
    row_count?: number;
    [key: string]: unknown;
  };
  [key: string]: unknown;
}

export interface DebugCall {
  timestamp: string;
  endpoint: string;
  method: string;
  request_payload: Record<string, unknown>;
  response_payload: DebugResponsePayload;
  latency_ms: number;
  definition_id?: string;
  warnings: string[];
  curl?: string;
}
