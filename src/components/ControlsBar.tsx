import { useState } from 'react';
import { GraphSnapshot, PersonaId } from '../types';
import { Play, Activity, Database, Cpu, Clock, LayoutGrid, GitBranch, Menu, X } from 'lucide-react';

interface ControlsBarProps {
  runMode: 'Dev' | 'Prod';
  setRunMode: (m: 'Dev' | 'Prod') => void;
  dataMode: 'Demo' | 'Farm' | 'AAM';
  setDataMode: (m: 'Demo' | 'Farm' | 'AAM') => void;
  selectedPersonas: PersonaId[];
  togglePersona: (p: PersonaId) => void;
  onRun: () => void;
  metrics?: GraphSnapshot['meta']['runMetrics'];
  isRunning: boolean;
  elapsedTime: number;
  mainView: 'graph' | 'dashboard';
  setMainView: (v: 'graph' | 'dashboard') => void;
}

export function ControlsBar({
  runMode,
  setRunMode,
  dataMode,
  setDataMode,
  selectedPersonas,
  togglePersona,
  onRun,
  metrics,
  isRunning,
  elapsedTime,
  mainView,
  setMainView
}: ControlsBarProps) {
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);

  const ToggleGroup = ({ label, children }: { label: string; children: React.ReactNode }) => (
    <div className="flex items-center gap-2">
      <span className="text-xs font-medium text-muted-foreground uppercase tracking-wider whitespace-nowrap">{label}</span>
      <div className="gap-0 rounded-lg border p-0.5 flex">
        {children}
      </div>
    </div>
  );

  const ToggleButton = ({ active, onClick, children }: { active: boolean; onClick: () => void; children: React.ReactNode }) => (
    <button
      onClick={onClick}
      className={`h-7 px-2 sm:px-3 text-xs rounded-md transition-colors flex items-center gap-1 whitespace-nowrap ${
        active ? 'bg-secondary text-secondary-foreground' : 'text-muted-foreground'
      }`}
    >
      {children}
    </button>
  );

  const MetricsDisplay = () => (
    (metrics || isRunning) ? (
      <div className="flex items-center gap-2 sm:gap-3 text-xs text-muted-foreground flex-wrap">
        <div className="flex items-center gap-1.5 bg-secondary/30 px-2 py-1 rounded">
          <Cpu className="w-3 h-3" />
          <span>LLM: <span className="text-foreground font-mono">{metrics?.llmCalls || 0}</span></span>
        </div>
        <div className="flex items-center gap-1.5 bg-secondary/30 px-2 py-1 rounded">
          <Database className="w-3 h-3" />
          <span>RAG: <span className="text-foreground font-mono">{metrics?.ragReads || 0}</span></span>
        </div>
        <div className="flex items-center gap-1.5 bg-secondary/30 px-2 py-1 rounded">
          <Clock className="w-3 h-3" />
          <span className="text-foreground font-mono">
            {isRunning
              ? `${(elapsedTime / 1000).toFixed(1)}s`
              : `${((metrics?.processingMs || 0) / 1000).toFixed(1)}s`}
          </span>
        </div>
      </div>
    ) : null
  );

  const ControlsContent = () => (
    <>
      <ToggleGroup label="Env">
        <ToggleButton active={runMode === 'Dev'} onClick={() => setRunMode('Dev')}>Dev</ToggleButton>
        <ToggleButton active={runMode === 'Prod'} onClick={() => setRunMode('Prod')}>Prod</ToggleButton>
      </ToggleGroup>

      <ToggleGroup label="Data">
        <ToggleButton active={dataMode === 'Demo'} onClick={() => setDataMode('Demo')}>Demo</ToggleButton>
        <ToggleButton active={dataMode === 'Farm'} onClick={() => setDataMode('Farm')}>Farm</ToggleButton>
        <ToggleButton active={dataMode === 'AAM'} onClick={() => setDataMode('AAM')}>AAM</ToggleButton>
      </ToggleGroup>

      <ToggleGroup label="Display">
        <ToggleButton active={mainView === 'graph'} onClick={() => setMainView('graph')}>
          <GitBranch className="w-3 h-3" />
          <span className="hidden sm:inline">Graph</span>
        </ToggleButton>
        <ToggleButton active={mainView === 'dashboard'} onClick={() => setMainView('dashboard')}>
          <LayoutGrid className="w-3 h-3" />
          <span className="hidden sm:inline">Dashboard</span>
        </ToggleButton>
      </ToggleGroup>

      <div className="flex items-center gap-2">
        <span className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Persona</span>
        <div className="flex gap-1 flex-wrap">
          {(['CFO', 'CRO', 'COO', 'CTO'] as PersonaId[]).map(p => (
            <button
              key={p}
              onClick={() => togglePersona(p)}
              className={`
                px-2 sm:px-3 py-1 rounded-md text-xs font-medium transition-all border
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
    </>
  );

  return (
    <div className="border-b bg-card/50 backdrop-blur-sm shrink-0 z-10 relative">
      <div className="flex items-center px-3 sm:px-6 py-2 sm:py-0 min-h-[56px] sm:min-h-[64px] justify-between gap-2">
        <div className="flex items-center gap-2 sm:gap-4 flex-1 min-w-0">
          <div className="flex items-center gap-1 font-bold text-lg sm:text-xl tracking-tight shrink-0">
            <div className="w-5 h-5 sm:w-6 sm:h-6 rounded bg-primary flex items-center justify-center text-primary-foreground">
              <Activity className="w-3 h-3 sm:w-4 sm:h-4" />
            </div>
            <span className="hidden xs:inline">DCL<span className="text-muted-foreground font-normal">Monitor</span></span>
            <span className="xs:hidden">DCL</span>
          </div>

          <div className="h-6 w-px bg-border hidden lg:block" />

          <div className="hidden lg:flex items-center gap-4 xl:gap-6 flex-wrap">
            <ControlsContent />
          </div>
        </div>

        <div className="flex items-center gap-2 sm:gap-4 shrink-0">
          <div className="hidden md:flex">
            <MetricsDisplay />
          </div>
          
          <button 
            onClick={onRun} 
            disabled={isRunning}
            className="gap-1.5 sm:gap-2 shadow-lg shadow-primary/20 px-3 sm:px-4 py-1.5 sm:py-2 bg-primary text-primary-foreground rounded-md text-xs sm:text-sm font-medium hover:opacity-90 transition-opacity flex items-center disabled:opacity-50 disabled:cursor-not-allowed whitespace-nowrap"
          >
            <Play className="w-3 h-3 sm:w-3.5 sm:h-3.5 fill-current" />
            <span className="hidden sm:inline">{isRunning ? 'Running...' : 'Run Pipeline'}</span>
            <span className="sm:hidden">{isRunning ? '...' : 'Run'}</span>
          </button>

          <button
            onClick={() => setMobileMenuOpen(!mobileMenuOpen)}
            className="lg:hidden p-2 rounded-md hover:bg-secondary/50 transition-colors"
            aria-label="Toggle menu"
          >
            {mobileMenuOpen ? <X className="w-5 h-5" /> : <Menu className="w-5 h-5" />}
          </button>
        </div>
      </div>

      {mobileMenuOpen && (
        <div className="lg:hidden border-t bg-card/95 backdrop-blur-sm px-4 py-3 space-y-3">
          <div className="flex flex-wrap items-center gap-3">
            <ControlsContent />
          </div>
          <div className="md:hidden pt-2 border-t">
            <MetricsDisplay />
          </div>
        </div>
      )}
    </div>
  );
}
