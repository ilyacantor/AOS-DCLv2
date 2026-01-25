/**
 * AskPage - Google-like NLQ interface for DCL
 *
 * Features:
 * - Single input, minimal chrome (Google-like)
 * - Preset chips that execute immediately on click
 * - Enter key runs query
 * - Results with data_summary at top
 * - Collapsible aggregations and debug trace
 * - History panel with clickable replay
 * - Debug panel with request/response and Copy curl
 */
import { useState, useEffect, useCallback } from 'react';
import { Tabs, TabsList, TabsTrigger, TabsContent } from './ui/tabs';
import type {
  NLQAskRequest,
  NLQAskResponse,
  HistoryEntry,
  DatasetInfo,
  Preset,
  DebugCall,
} from '../nlq-types';

// ============================================================================
// API Functions
// ============================================================================

async function fetchPresets(): Promise<Preset[]> {
  try {
    const res = await fetch('/api/presets');
    if (!res.ok) return [];
    const data = await res.json();
    return data.presets || [];
  } catch {
    return [];
  }
}

async function fetchDataset(): Promise<DatasetInfo | null> {
  try {
    const res = await fetch('/api/datasets/current');
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  }
}

async function fetchHistory(limit = 20): Promise<HistoryEntry[]> {
  try {
    const res = await fetch(`/api/history?limit=${limit}`);
    if (!res.ok) return [];
    const data = await res.json();
    return data.entries || [];
  } catch {
    return [];
  }
}

async function askQuestion(question: string, datasetId?: string): Promise<NLQAskResponse> {
  const body: NLQAskRequest = {
    question,
    dataset_id: datasetId || 'demo9',
  };

  const res = await fetch('/api/nlq/ask', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });

  if (!res.ok) {
    throw new Error(`API error: ${res.status}`);
  }

  return await res.json();
}

// ============================================================================
// Helper Functions
// ============================================================================

function formatCurrency(amount: number): string {
  if (Math.abs(amount) >= 1_000_000) {
    return `$${(amount / 1_000_000).toFixed(2)}M`;
  } else if (Math.abs(amount) >= 1_000) {
    return `$${(amount / 1_000).toFixed(1)}K`;
  } else {
    return `$${amount.toFixed(2)}`;
  }
}

function formatTimestamp(ts: string): string {
  try {
    const date = new Date(ts);
    return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  } catch {
    return ts;
  }
}

function generateCurl(question: string, datasetId: string): string {
  const body = JSON.stringify({ question, dataset_id: datasetId });
  return `curl -X POST http://localhost:8000/api/nlq/ask \\
  -H "Content-Type: application/json" \\
  -d '${body}'`;
}

// ============================================================================
// Components
// ============================================================================

interface PresetChipProps {
  preset: Preset;
  onClick: (preset: Preset) => void;
  isLoading: boolean;
}

function PresetChip({ preset, onClick, isLoading }: PresetChipProps) {
  return (
    <button
      onClick={() => onClick(preset)}
      disabled={isLoading}
      className="px-3 py-1.5 text-sm rounded-full border border-border/60 bg-card/50 hover:bg-accent hover:border-primary/50 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
    >
      {preset.label}
    </button>
  );
}

interface ResultsTableProps {
  data: Record<string, unknown>[];
  maxRows?: number;
}

function ResultsTable({ data, maxRows = 10 }: ResultsTableProps) {
  if (!data || data.length === 0) {
    return <p className="text-muted-foreground text-sm">No data returned.</p>;
  }

  const columns = Object.keys(data[0]);
  const displayData = data.slice(0, maxRows);

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm border-collapse">
        <thead>
          <tr className="border-b border-border">
            {columns.map((col) => (
              <th key={col} className="text-left px-2 py-1.5 font-medium text-muted-foreground">
                {col}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {displayData.map((row, i) => (
            <tr key={i} className="border-b border-border/50 hover:bg-accent/30">
              {columns.map((col) => (
                <td key={col} className="px-2 py-1.5">
                  {typeof row[col] === 'number'
                    ? (col.toLowerCase().includes('revenue') ||
                       col.toLowerCase().includes('amount') ||
                       col.toLowerCase().includes('cost'))
                      ? formatCurrency(row[col] as number)
                      : (row[col] as number).toLocaleString()
                    : String(row[col] ?? '')}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {data.length > maxRows && (
        <p className="text-xs text-muted-foreground mt-2">
          Showing {maxRows} of {data.length} rows
        </p>
      )}
    </div>
  );
}

interface AggregationsCardProps {
  aggregations: Record<string, unknown>;
}

function AggregationsCard({ aggregations }: AggregationsCardProps) {
  const entries = Object.entries(aggregations).filter(
    ([k]) => !k.startsWith('_') && k !== 'limitations'
  );

  if (entries.length === 0) return null;

  return (
    <div className="grid grid-cols-2 md:grid-cols-4 gap-2">
      {entries.slice(0, 8).map(([key, value]) => (
        <div key={key} className="p-2 rounded bg-accent/30 text-sm">
          <div className="text-muted-foreground text-xs">{key.replace(/_/g, ' ')}</div>
          <div className="font-medium">
            {typeof value === 'number'
              ? key.toLowerCase().includes('pct')
                ? `${value.toFixed(1)}%`
                : key.toLowerCase().includes('total') ||
                  key.toLowerCase().includes('revenue') ||
                  key.toLowerCase().includes('spend')
                ? formatCurrency(value)
                : value.toLocaleString()
              : String(value)}
          </div>
        </div>
      ))}
    </div>
  );
}

interface HistoryPanelProps {
  entries: HistoryEntry[];
  onSelect: (entry: HistoryEntry) => void;
  onRefresh: () => void;
}

function HistoryPanel({ entries, onSelect, onRefresh }: HistoryPanelProps) {
  return (
    <div className="h-full flex flex-col">
      <div className="flex items-center justify-between px-3 py-2 border-b border-border">
        <span className="text-sm font-medium">History</span>
        <button
          onClick={onRefresh}
          className="text-xs text-muted-foreground hover:text-foreground"
        >
          Refresh
        </button>
      </div>
      <div className="flex-1 overflow-y-auto">
        {entries.length === 0 ? (
          <p className="p-3 text-sm text-muted-foreground">No history yet.</p>
        ) : (
          <ul className="divide-y divide-border/50">
            {entries.map((entry) => (
              <li key={entry.id}>
                <button
                  onClick={() => onSelect(entry)}
                  className="w-full text-left px-3 py-2 hover:bg-accent/30 transition-colors"
                >
                  <div className="text-sm truncate">{entry.question}</div>
                  <div className="flex items-center gap-2 mt-0.5">
                    <span className="text-xs text-muted-foreground">
                      {formatTimestamp(entry.timestamp)}
                    </span>
                    <span className="text-xs text-muted-foreground">
                      {entry.latency_ms}ms
                    </span>
                    <span className="text-xs px-1 py-0.5 rounded bg-accent/50">
                      {entry.definition_id}
                    </span>
                  </div>
                </button>
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}

interface DebugPanelProps {
  calls: DebugCall[];
  onCopyCurl: (curl: string) => void;
}

function DebugPanel({ calls, onCopyCurl }: DebugPanelProps) {
  const [expandedIdx, setExpandedIdx] = useState<number | null>(null);

  return (
    <div className="h-full flex flex-col">
      <div className="px-3 py-2 border-b border-border">
        <span className="text-sm font-medium">Debug Trace</span>
        <span className="text-xs text-muted-foreground ml-2">
          Last {calls.length} calls
        </span>
      </div>
      <div className="flex-1 overflow-y-auto">
        {calls.length === 0 ? (
          <p className="p-3 text-sm text-muted-foreground">No calls yet.</p>
        ) : (
          <ul className="divide-y divide-border/50">
            {calls.map((call, idx) => (
              <li key={idx} className="p-3">
                <div
                  className="cursor-pointer"
                  onClick={() => setExpandedIdx(expandedIdx === idx ? null : idx)}
                >
                  <div className="flex items-center justify-between">
                    <span className="text-sm font-medium">{call.endpoint}</span>
                    <span className="text-xs text-muted-foreground">
                      {call.latency_ms}ms
                    </span>
                  </div>
                  <div className="text-xs text-muted-foreground mt-0.5">
                    {formatTimestamp(call.timestamp)}
                    {call.definition_id && ` | ${call.definition_id}`}
                  </div>
                </div>

                {expandedIdx === idx && (
                  <div className="mt-2 space-y-2">
                    <div>
                      <div className="text-xs font-medium text-muted-foreground mb-1">
                        Request
                      </div>
                      <pre className="text-xs bg-accent/30 p-2 rounded overflow-x-auto">
                        {JSON.stringify(call.request_payload, null, 2)}
                      </pre>
                    </div>
                    <div>
                      <div className="text-xs font-medium text-muted-foreground mb-1">
                        Response (summary)
                      </div>
                      <pre className="text-xs bg-accent/30 p-2 rounded overflow-x-auto max-h-40">
                        {JSON.stringify(
                          {
                            row_count: call.response_payload?.metadata?.row_count,
                            definition_id: call.response_payload?.definition_id,
                            confidence: call.response_payload?.confidence_score,
                            caveats: call.response_payload?.caveats,
                          },
                          null,
                          2
                        )}
                      </pre>
                    </div>
                    {call.warnings.length > 0 && (
                      <div>
                        <div className="text-xs font-medium text-yellow-500 mb-1">
                          Warnings
                        </div>
                        <ul className="text-xs">
                          {call.warnings.map((w, wi) => (
                            <li key={wi} className="text-yellow-600">
                              {w}
                            </li>
                          ))}
                        </ul>
                      </div>
                    )}
                    {call.curl && (
                      <button
                        onClick={() => onCopyCurl(call.curl!)}
                        className="text-xs px-2 py-1 rounded border border-border hover:bg-accent"
                      >
                        Copy curl
                      </button>
                    )}
                  </div>
                )}
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}

// ============================================================================
// Main AskPage Component
// ============================================================================

export function AskPage() {
  const [query, setQuery] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [result, setResult] = useState<NLQAskResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [presets, setPresets] = useState<Preset[]>([]);
  const [dataset, setDataset] = useState<DatasetInfo | null>(null);
  const [history, setHistory] = useState<HistoryEntry[]>([]);
  const [debugCalls, setDebugCalls] = useState<DebugCall[]>([]);
  const [showAggregations, setShowAggregations] = useState(true);
  const [showDebug, setShowDebug] = useState(false);

  // Load initial data
  useEffect(() => {
    fetchPresets().then(setPresets);
    fetchDataset().then(setDataset);
    fetchHistory().then(setHistory);
  }, []);

  const refreshHistory = useCallback(() => {
    fetchHistory().then(setHistory);
  }, []);

  const executeQuery = useCallback(async (questionText: string) => {
    if (!questionText.trim()) return;

    setIsLoading(true);
    setError(null);
    setResult(null);

    const startTime = Date.now();

    try {
      const response = await askQuestion(questionText, dataset?.dataset_id);
      const latency = Date.now() - startTime;

      setResult(response);

      // Add to debug calls
      setDebugCalls((prev) => [
        {
          timestamp: new Date().toISOString(),
          endpoint: '/api/nlq/ask',
          method: 'POST',
          request_payload: {
            question: questionText,
            dataset_id: dataset?.dataset_id || 'demo9',
          },
          response_payload: response as unknown as Record<string, unknown>,
          latency_ms: latency,
          definition_id: response.definition_id,
          warnings: response.caveats || [],
          curl: generateCurl(questionText, dataset?.dataset_id || 'demo9'),
        },
        ...prev.slice(0, 9), // Keep last 10
      ]);

      // Refresh history after successful query
      setTimeout(refreshHistory, 500);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unknown error');
    } finally {
      setIsLoading(false);
    }
  }, [dataset, refreshHistory]);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    executeQuery(query);
  };

  const handlePresetClick = (preset: Preset) => {
    setQuery(preset.question);
    executeQuery(preset.question);
  };

  const handleHistorySelect = (entry: HistoryEntry) => {
    // Load stored result directly (replay without re-running)
    setQuery(entry.question);
    setResult(entry.response);
    setError(null);
  };

  const handleCopyCurl = (curl: string) => {
    navigator.clipboard.writeText(curl);
  };

  return (
    <div className="h-full flex bg-background">
      {/* Main Content */}
      <div className="flex-1 flex flex-col overflow-hidden">
        {/* Search Header */}
        <div className="shrink-0 px-6 pt-8 pb-4">
          <div className="max-w-2xl mx-auto">
            {/* Dataset Indicator */}
            {dataset && (
              <div className="text-xs text-muted-foreground mb-2 text-center">
                Using dataset: <span className="font-medium">{dataset.dataset_id}</span>
                <span className="text-muted-foreground/60 ml-1">({dataset.source})</span>
              </div>
            )}

            {/* Search Input */}
            <form onSubmit={handleSubmit}>
              <div className="relative">
                <input
                  type="text"
                  value={query}
                  onChange={(e) => setQuery(e.target.value)}
                  placeholder="Ask a question..."
                  className="w-full px-4 py-3 text-lg rounded-xl border border-border bg-card shadow-sm focus:outline-none focus:ring-2 focus:ring-primary/50 focus:border-primary/50 transition-shadow"
                  disabled={isLoading}
                />
                {isLoading && (
                  <div className="absolute right-4 top-1/2 -translate-y-1/2">
                    <div className="w-5 h-5 border-2 border-primary border-t-transparent rounded-full animate-spin" />
                  </div>
                )}
              </div>
            </form>

            {/* Presets */}
            {presets.length > 0 && !result && (
              <div className="flex flex-wrap gap-2 mt-4 justify-center">
                {presets.map((preset) => (
                  <PresetChip
                    key={preset.id}
                    preset={preset}
                    onClick={handlePresetClick}
                    isLoading={isLoading}
                  />
                ))}
              </div>
            )}
          </div>
        </div>

        {/* Results Area */}
        <div className="flex-1 overflow-y-auto px-6 pb-6">
          <div className="max-w-3xl mx-auto">
            {error && (
              <div className="p-4 rounded-lg bg-destructive/10 border border-destructive/30 text-destructive">
                <div className="font-medium">Error</div>
                <div className="text-sm mt-1">{error}</div>
              </div>
            )}

            {result && (
              <div className="space-y-4">
                {/* Data Summary */}
                {result.summary?.answer && (
                  <div className="p-4 rounded-lg bg-primary/5 border border-primary/20">
                    <div className="text-lg">{result.summary.answer}</div>
                  </div>
                )}

                {/* Metadata Bar */}
                <div className="flex items-center gap-3 text-xs text-muted-foreground">
                  <span>Definition: {result.definition_id}</span>
                  <span>Confidence: {(result.confidence_score * 100).toFixed(0)}%</span>
                  <span>Rows: {result.data.length}</span>
                  <span>{result.metadata.execution_time_ms}ms</span>
                </div>

                {/* Caveats */}
                {result.caveats.length > 0 && (
                  <div className="flex flex-wrap gap-2">
                    {result.caveats.map((caveat, i) => (
                      <span
                        key={i}
                        className={`text-xs px-2 py-0.5 rounded ${
                          caveat.includes('MISSING_LIMIT')
                            ? 'bg-yellow-100 text-yellow-800'
                            : 'bg-accent/50'
                        }`}
                      >
                        {caveat}
                      </span>
                    ))}
                  </div>
                )}

                {/* Aggregations (collapsible) */}
                {result.summary?.aggregations && Object.keys(result.summary.aggregations).length > 0 && (
                  <div>
                    <button
                      onClick={() => setShowAggregations(!showAggregations)}
                      className="text-sm text-muted-foreground hover:text-foreground flex items-center gap-1 mb-2"
                    >
                      <span>{showAggregations ? '▼' : '▶'}</span>
                      Aggregations
                    </button>
                    {showAggregations && (
                      <AggregationsCard aggregations={result.summary.aggregations} />
                    )}
                  </div>
                )}

                {/* Results Table */}
                <div className="rounded-lg border border-border overflow-hidden">
                  <ResultsTable data={result.data} maxRows={20} />
                </div>

                {/* Debug Trace (collapsible) */}
                <div>
                  <button
                    onClick={() => setShowDebug(!showDebug)}
                    className="text-sm text-muted-foreground hover:text-foreground flex items-center gap-1 mb-2"
                  >
                    <span>{showDebug ? '▼' : '▶'}</span>
                    Debug Trace
                  </button>
                  {showDebug && result.metadata && (
                    <pre className="text-xs bg-accent/30 p-3 rounded overflow-x-auto">
                      {JSON.stringify(
                        {
                          definition_id: result.definition_id,
                          confidence_score: result.confidence_score,
                          execution_args: result.execution_args,
                          metadata: result.metadata,
                          matched_keywords: result.metadata.matched_keywords,
                        },
                        null,
                        2
                      )}
                    </pre>
                  )}
                </div>
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Side Panel - History & Debug */}
      <div className="w-80 border-l border-border bg-sidebar flex flex-col">
        <Tabs defaultValue="history" className="flex-1 flex flex-col">
          <div className="shrink-0 border-b border-border px-2 pt-2">
            <TabsList className="w-full">
              <TabsTrigger value="history" className="flex-1">
                History
              </TabsTrigger>
              <TabsTrigger value="debug" className="flex-1">
                Debug
              </TabsTrigger>
            </TabsList>
          </div>

          <TabsContent value="history" className="flex-1 mt-0">
            <HistoryPanel
              entries={history}
              onSelect={handleHistorySelect}
              onRefresh={refreshHistory}
            />
          </TabsContent>

          <TabsContent value="debug" className="flex-1 mt-0">
            <DebugPanel calls={debugCalls} onCopyCurl={handleCopyCurl} />
          </TabsContent>
        </Tabs>
      </div>
    </div>
  );
}

export default AskPage;
