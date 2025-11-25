import { GraphSnapshot, PersonaId } from '../types';
import { Play, Activity, Database, Cpu, Clock, LayoutGrid, GitBranch } from 'lucide-react';

interface ControlsBarProps {
  runMode: 'Dev' | 'Prod';
  setRunMode: (m: 'Dev' | 'Prod') => void;
  dataMode: 'Demo' | 'Farm';
  setDataMode: (m: 'Demo' | 'Farm') => void;
  sourceLimit: number;
  setSourceLimit: (n: number) => void;
  selectedPersonas: PersonaId[];
  togglePersona: (p: PersonaId) => void;
  onRun: () => void;
  metrics?: GraphSnapshot['meta']['runMetrics'];
  isRunning: boolean;
  elapsedTime: number;
  mainView: 'graph' | 'dashboard';
  setMainView: (v: 'graph' | 'dashboard') => void;
}

const SOURCE_LIMITS = [5, 10, 20, 50, 100];

export function ControlsBar({
  runMode,
  setRunMode,
  dataMode,
  setDataMode,
  sourceLimit,
  setSourceLimit,
  selectedPersonas,
  togglePersona,
  onRun,
  metrics,
  isRunning,
  elapsedTime,
  mainView,
  setMainView
}: ControlsBarProps) {
  return (
    <div className="h-16 border-b bg-card/50 backdrop-blur-sm flex items-center px-6 justify-between shrink-0 z-10 relative">
      <div className="flex items-center gap-6">
        <div className="flex items-center gap-1 font-bold text-xl tracking-tight">
          <div className="w-6 h-6 rounded bg-primary flex items-center justify-center text-primary-foreground">
            <Activity className="w-4 h-4" />
          </div>
          <span>DCL<span className="text-muted-foreground font-normal">Monitor</span></span>
        </div>

        <div className="h-6 w-px bg-border" />

        <div className="flex items-center gap-2">
          <span className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Env</span>
          <div className="gap-0 rounded-lg border p-0.5 flex">
            <button onClick={() => setRunMode('Dev')} className={`h-7 px-3 text-xs rounded-md transition-colors ${runMode === 'Dev' ? 'bg-secondary text-secondary-foreground' : 'text-muted-foreground'}`}>Dev</button>
            <button onClick={() => setRunMode('Prod')} className={`h-7 px-3 text-xs rounded-md transition-colors ${runMode === 'Prod' ? 'bg-secondary text-secondary-foreground' : 'text-muted-foreground'}`}>Prod</button>
          </div>
        </div>

        <div className="flex items-center gap-2">
          <span className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Data</span>
          <div className="gap-0 rounded-lg border p-0.5 flex">
            <button onClick={() => setDataMode('Demo')} className={`h-7 px-3 text-xs rounded-md transition-colors ${dataMode === 'Demo' ? 'bg-secondary text-secondary-foreground' : 'text-muted-foreground'}`}>Demo</button>
            <button onClick={() => setDataMode('Farm')} className={`h-7 px-3 text-xs rounded-md transition-colors ${dataMode === 'Farm' ? 'bg-secondary text-secondary-foreground' : 'text-muted-foreground'}`}>Farm</button>
          </div>
          {dataMode === 'Farm' && (
            <select
              value={sourceLimit}
              onChange={(e) => setSourceLimit(Number(e.target.value))}
              className="h-7 px-2 text-xs rounded-md border bg-background text-foreground cursor-pointer"
              title="Number of sources"
            >
              {SOURCE_LIMITS.map(n => (
                <option key={n} value={n}>{n} src</option>
              ))}
            </select>
          )}
        </div>

        <div className="h-6 w-px bg-border" />

        <div className="flex items-center gap-2">
          <span className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Display</span>
          <div className="gap-0 rounded-lg border p-0.5 flex">
            <button 
              onClick={() => setMainView('graph')} 
              className={`h-7 px-3 text-xs rounded-md transition-colors flex items-center gap-1.5 ${mainView === 'graph' ? 'bg-secondary text-secondary-foreground' : 'text-muted-foreground'}`}
              title="Sankey Graph (best for <20 sources)"
            >
              <GitBranch className="w-3 h-3" />
              Graph
            </button>
            <button 
              onClick={() => setMainView('dashboard')} 
              className={`h-7 px-3 text-xs rounded-md transition-colors flex items-center gap-1.5 ${mainView === 'dashboard' ? 'bg-secondary text-secondary-foreground' : 'text-muted-foreground'}`}
              title="Enterprise Dashboard (scales to 100s of sources)"
            >
              <LayoutGrid className="w-3 h-3" />
              Dashboard
            </button>
          </div>
        </div>

        <div className="h-6 w-px bg-border" />

        <div className="flex items-center gap-2">
          <span className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Persona</span>
          <div className="flex gap-1">
            {(['CFO', 'CRO', 'COO', 'CTO'] as PersonaId[]).map(p => (
              <button
                key={p}
                onClick={() => togglePersona(p)}
                className={`
                  px-3 py-1 rounded-md text-xs font-medium transition-all border
                  ${selectedPersonas.includes(p) 
                    ? 'bg-primary text-primary-foreground border-primary shadow-sm' 
                    : 'bg-transparent text-muted-foreground border-transparent hover:bg-secondary/50'}
                `}
              >
                {p}
              </button>
            ))}
          </div>
        </div>
      </div>

      <div className="flex items-center gap-4">
        {(metrics || isRunning) && (
          <div className="flex items-center gap-3 text-xs text-muted-foreground mr-2">
            <div className="flex items-center gap-1.5 bg-secondary/30 px-2 py-1 rounded">
              <Cpu className="w-3 h-3" />
              <span>LLM: <span className="text-foreground font-mono">{metrics?.llm_calls || 0}</span></span>
            </div>
            <div className="flex items-center gap-1.5 bg-secondary/30 px-2 py-1 rounded">
              <Database className="w-3 h-3" />
              <span>RAG: <span className="text-foreground font-mono">{metrics?.rag_reads || 0}</span></span>
            </div>
            <div className="flex items-center gap-1.5 bg-secondary/30 px-2 py-1 rounded">
              <Clock className="w-3 h-3" />
              <span className="text-foreground font-mono">
                {isRunning 
                  ? `${(elapsedTime / 1000).toFixed(2)}s` 
                  : `${((metrics?.processing_ms || 0) / 1000).toFixed(2)}s`}
              </span>
            </div>
          </div>
        )}
        <button 
          onClick={onRun} 
          disabled={isRunning}
          className="gap-2 shadow-lg shadow-primary/20 px-4 py-2 bg-primary text-primary-foreground rounded-md text-sm font-medium hover:opacity-90 transition-opacity flex items-center disabled:opacity-50 disabled:cursor-not-allowed"
        >
          <Play className="w-3.5 h-3.5 fill-current" />
          {isRunning ? 'Running...' : 'Run Pipeline'}
        </button>
      </div>
    </div>
  );
}
