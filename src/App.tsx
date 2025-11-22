import React, { useState } from 'react';
import ControlPanel from './components/ControlPanel';
import SankeyGraph from './components/SankeyGraph';
import NarrationPanel from './components/NarrationPanel';
import MonitorPanel from './components/MonitorPanel';
import './App.css';

export interface RunMetrics {
  llm_calls: number;
  rag_reads: number;
  rag_writes: number;
  processing_ms: number;
  render_ms: number;
}

export interface GraphData {
  nodes: any[];
  links: any[];
  meta: any;
}

function App() {
  const [activeTab, setActiveTab] = useState<'graph' | 'monitor'>('graph');
  const [graphData, setGraphData] = useState<GraphData | null>(null);
  const [metrics, setMetrics] = useState<RunMetrics | null>(null);
  const [runId, setRunId] = useState<string>('');
  const [isLoading, setIsLoading] = useState(false);

  const handleRun = async (mode: string, runMode: string, personas: string[]) => {
    setIsLoading(true);
    try {
      const response = await fetch('/api/dcl/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ mode, run_mode: runMode, personas }),
      });

      if (!response.ok) throw new Error('Failed to run DCL');

      const data = await response.json();
      setGraphData(data.graph);
      setMetrics(data.run_metrics);
      setRunId(data.run_id);
    } catch (error) {
      console.error('Error running DCL:', error);
      alert('Failed to run DCL engine. Check console for details.');
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="app">
      <header className="app-header">
        <h1>DCL Engine</h1>
        <p>Data Connectivity Layer Visualization</p>
      </header>

      <ControlPanel onRun={handleRun} isLoading={isLoading} />

      {metrics && (
        <div className="metrics-bar">
          <div className="metric">
            <span className="metric-label">LLM Calls:</span>
            <span className="metric-value">{metrics.llm_calls}</span>
          </div>
          <div className="metric">
            <span className="metric-label">RAG Reads:</span>
            <span className="metric-value">{metrics.rag_reads}</span>
          </div>
          <div className="metric">
            <span className="metric-label">RAG Writes:</span>
            <span className="metric-value">{metrics.rag_writes}</span>
          </div>
          <div className="metric">
            <span className="metric-label">Processing:</span>
            <span className="metric-value">{Math.round(metrics.processing_ms)}ms</span>
          </div>
          <div className="metric">
            <span className="metric-label">Total:</span>
            <span className="metric-value">{Math.round(metrics.processing_ms + metrics.render_ms)}ms</span>
          </div>
        </div>
      )}

      <div className="tabs">
        <button
          className={`tab ${activeTab === 'graph' ? 'active' : ''}`}
          onClick={() => setActiveTab('graph')}
        >
          Graph
        </button>
        <button
          className={`tab ${activeTab === 'monitor' ? 'active' : ''}`}
          onClick={() => setActiveTab('monitor')}
        >
          Monitor
        </button>
      </div>

      <div className="main-content">
        <div className="content-area">
          {activeTab === 'graph' && (
            <div className="graph-container">
              {graphData ? (
                <SankeyGraph data={graphData} />
              ) : (
                <div className="placeholder">
                  <p>Click "Run" to generate graph</p>
                </div>
              )}
            </div>
          )}

          {activeTab === 'monitor' && (
            <div className="monitor-container">
              <MonitorPanel runId={runId} />
            </div>
          )}
        </div>

        <div className="narration-area">
          <NarrationPanel runId={runId} />
        </div>
      </div>
    </div>
  );
}

export default App;
