import { GraphSnapshot, PersonaId } from '../types';
import { Play, Activity, Database, Cpu, Clock } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { ToggleGroup, ToggleGroupItem } from '@/components/ui/toggle-group';
import { Separator } from '@/components/ui/separator';

interface ControlsBarProps {
  runMode: 'Dev' | 'Prod';
  setRunMode: (m: 'Dev' | 'Prod') => void;
  dataMode: 'Demo' | 'Farm';
  setDataMode: (m: 'Demo' | 'Farm') => void;
  selectedPersonas: PersonaId[];
  togglePersona: (p: PersonaId) => void;
  onRun: () => void;
  isLoading?: boolean;
  metrics?: GraphSnapshot['meta']['runMetrics'];
}

export function ControlsBar({
  runMode,
  setRunMode,
  dataMode,
  setDataMode,
  selectedPersonas,
  togglePersona,
  onRun,
  isLoading = false,
  metrics
}: ControlsBarProps) {
  return (
    <div className="h-16 border-b bg-card/50 backdrop-blur-sm flex items-center px-6 justify-between shrink-0 z-10 relative">
      <div className="flex items-center gap-6">
        <div className="flex items-center gap-1 font-display font-bold text-xl tracking-tight">
          <div className="w-6 h-6 rounded bg-primary flex items-center justify-center text-primary-foreground">
            <Activity className="w-4 h-4" />
          </div>
          <span>DCL<span className="text-muted-foreground font-normal">Monitor</span></span>
        </div>

        <Separator orientation="vertical" className="h-6" />

        <div className="flex items-center gap-2">
          <span className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Env</span>
          <ToggleGroup type="single" value={runMode} onValueChange={(v) => v && setRunMode(v as any)} className="gap-0 rounded-lg border border-cyan-500/50 p-0.5">
            <ToggleGroupItem value="Dev" size="sm" className="h-7 px-3 text-xs rounded-md data-[state=on]:bg-secondary">Dev</ToggleGroupItem>
            <ToggleGroupItem value="Prod" size="sm" className="h-7 px-3 text-xs rounded-md data-[state=on]:bg-secondary">Prod</ToggleGroupItem>
          </ToggleGroup>
        </div>

        <div className="flex items-center gap-2">
          <span className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Data</span>
          <ToggleGroup type="single" value={dataMode} onValueChange={(v) => v && setDataMode(v as any)} className="gap-0 rounded-lg border border-cyan-500/50 p-0.5">
            <ToggleGroupItem value="Demo" size="sm" className="h-7 px-3 text-xs rounded-md data-[state=on]:bg-secondary">Demo</ToggleGroupItem>
            <ToggleGroupItem value="Farm" size="sm" className="h-7 px-3 text-xs rounded-md data-[state=on]:bg-secondary">Farm</ToggleGroupItem>
          </ToggleGroup>
        </div>

        <Separator orientation="vertical" className="h-6" />

        <div className="flex items-center gap-2">
          <span className="text-xs font-medium text-muted-foreground uppercase tracking-wider">View</span>
          <div className="flex gap-1 border border-cyan-500/50 rounded-lg p-0.5">
            {(['CFO', 'CRO', 'COO', 'CTO'] as PersonaId[]).map(p => (
              <button
                key={p}
                onClick={() => togglePersona(p)}
                className={`
                  px-3 py-1 rounded-md text-xs font-medium transition-all cursor-pointer
                  ${selectedPersonas.includes(p) 
                    ? 'bg-cyan-500/20 text-cyan-300 shadow-sm' 
                    : 'bg-transparent text-muted-foreground hover:bg-secondary/50'}
                `}
              >
                {p}
              </button>
            ))}
          </div>
        </div>
      </div>

      <div className="flex items-center gap-4">
        {metrics && (
          <div className="flex items-center gap-3 text-xs text-muted-foreground mr-2">
            <div className="flex items-center gap-1.5 bg-secondary/30 px-2 py-1 rounded border border-cyan-500/40">
              <Cpu className="w-3 h-3" />
              <span>LLM: <span className="text-foreground font-mono">{metrics.llmCalls}</span></span>
            </div>
            <div className="flex items-center gap-1.5 bg-secondary/30 px-2 py-1 rounded border border-cyan-500/40">
              <Database className="w-3 h-3" />
              <span>RAG: <span className="text-foreground font-mono">{metrics.ragReads}</span></span>
            </div>
            <div className="flex items-center gap-1.5 bg-secondary/30 px-2 py-1 rounded border border-cyan-500/40">
              <Clock className="w-3 h-3" />
              <span>{(metrics.totalMs / 1000).toFixed(1)}s</span>
            </div>
          </div>
        )}
        <Button 
          onClick={onRun} 
          size="sm" 
          className="gap-2 shadow-lg shadow-primary/20" 
          disabled={isLoading}
        >
          <Play className="w-3.5 h-3.5 fill-current" />
          {isLoading ? 'Running...' : 'Run Pipeline'}
        </Button>
      </div>
    </div>
  );
}
