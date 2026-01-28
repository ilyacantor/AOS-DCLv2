import { useEffect, useState } from 'react';
import { GraphSnapshot, PersonaId } from './types';
import { MonitorPanel } from './components/MonitorPanel';
import { NarrationPanel } from './components/NarrationPanel';
import { SankeyGraph } from './components/SankeyGraph';
import { EnterpriseDashboard } from './components/EnterpriseDashboard';
import { AskPage } from './components/AskPage';
import { ResizablePanelGroup, ResizablePanel, ResizableHandle } from './components/ui/resizable';
import { Tabs, TabsList, TabsTrigger, TabsContent } from './components/ui/tabs';
import { Toaster } from './components/ui/toaster';
import { useToast } from './hooks/use-toast';

type MainView = 'ask' | 'graph' | 'dashboard';

const ALL_PERSONAS: PersonaId[] = ['CFO', 'CRO', 'COO', 'CTO'];

function App() {
  const [graphData, setGraphData] = useState<GraphSnapshot | null>(null);
  const [runMode, setRunMode] = useState<'Dev' | 'Prod'>('Dev');
  const [dataMode, setDataMode] = useState<'Demo' | 'Farm'>('Demo');
  const [sourceLimit, setSourceLimit] = useState<number>(5);
  const [selectedPersonas, setSelectedPersonas] = useState<PersonaId[]>([]);
  const [runId, setRunId] = useState<string | undefined>(undefined);
  const [isRunning, setIsRunning] = useState(false);
  const [elapsedTime, setElapsedTime] = useState(0);
  const [mainView, setMainView] = useState<MainView>('graph');
  const { toast } = useToast();

  useEffect(() => {
    // Only load DCL graph data when not on Ask view
    if (mainView !== 'ask') {
      loadData();
    }
  }, [mainView]);

  useEffect(() => {
    if (!isRunning) return;

    const startTime = Date.now();
    const interval = setInterval(() => {
      setElapsedTime(Date.now() - startTime);
    }, 100);

    return () => clearInterval(interval);
  }, [isRunning]);

  const generatePersonaViews = (graphData: any, personas: PersonaId[]) => {
    if (!graphData || personas.length === 0) return [];

    const personaTitles: Record<PersonaId, string> = {
      CFO: 'Chief Financial Officer',
      CRO: 'Chief Revenue Officer',
      COO: 'Chief Operating Officer',
      CTO: 'Chief Technology Officer',
    };

    const personaFocusAreas: Record<PersonaId, string[]> = {
      CFO: ['Revenue', 'Cost', 'Budget'],
      CRO: ['Pipeline', 'Revenue', 'Accounts'],
      COO: ['Operations', 'Health', 'Usage'],
      CTO: ['Infrastructure', 'Resources', 'Cost'],
    };

    const personaOntologies: Record<PersonaId, string[]> = {
      CFO: ['revenue', 'cost'],
      CRO: ['account', 'opportunity', 'revenue'],
      COO: ['usage', 'health'],
      CTO: ['aws_resource', 'usage', 'cost'],
    };

    return personas.map((persona) => {
      const ontologies = personaOntologies[persona];
      const ontologyNodes = graphData.nodes.filter((n: any) =>
        n.level === 'L2' && ontologies.some(ont => n.id.includes(ont))
      );

      const sourceConnections = graphData.links.filter((l: any) =>
        ontologyNodes.some((n: any) => l.target === n.id)
      );

      return {
        personaId: persona,
        title: personaTitles[persona],
        focusAreas: personaFocusAreas[persona],
        keyEntities: ontologyNodes.map((n: any) => n.label),
        metrics: [
          { id: 'm1', label: 'Data Sources', value: sourceConnections.length, unit: '', trend: 'up' as const },
          { id: 'm2', label: 'Ontologies', value: ontologyNodes.length, unit: '', trend: 'flat' as const },
        ],
        insights: [
          { id: 'i1', severity: 'info' as const, message: `Receiving data from ${sourceConnections.length} sources` },
        ],
        alerts: [],
      };
    });
  };

  const loadData = async () => {
    console.log('[App] loadData started');
    try {
      const res = await fetch('/api/dcl/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ mode: 'Demo', run_mode: 'Dev', personas: [] }),
      });

      console.log('[App] fetch response:', res.status);
      if (!res.ok) throw new Error('Failed to load graph');
      const data = await res.json();
      console.log('[App] data received, nodes:', data.graph?.nodes?.length, 'links:', data.graph?.links?.length);
      const graphWithViews = {
        ...data.graph,
        meta: {
          ...(data.graph.meta ?? {}),
          personaViews: generatePersonaViews(data.graph, []),
        },
      };
      setGraphData(graphWithViews);
      setRunId(data.run_id);
      console.log('[App] graphData set');
    } catch (e) {
      console.error('[App] Failed to load data:', e);
      toast({ title: 'Error', description: 'Failed to load initial data', variant: 'destructive' });
    }
  };

  const handleRun = async () => {
    setIsRunning(true);
    setElapsedTime(0);
    toast({ title: 'Pipeline Started', description: `Running in ${runMode} mode on ${dataMode} data...` });

    try {
      const res = await fetch('/api/dcl/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          mode: dataMode,
          run_mode: runMode,
          personas: selectedPersonas,
          source_limit: sourceLimit
        }),
      });

      if (!res.ok) throw new Error('Failed to run pipeline');
      const data = await res.json();
      const graphWithViews = {
        ...data.graph,
        meta: {
          ...(data.graph.meta ?? {}),
          personaViews: generatePersonaViews(data.graph, selectedPersonas),
          runMetrics: data.run_metrics,
        },
      };
      setGraphData(graphWithViews);
      setRunId(data.run_id);
      setIsRunning(false);
      toast({ title: 'Pipeline Complete', description: 'New graph snapshot generated.' });
    } catch (e) {
      console.error('Failed to run pipeline:', e);
      setIsRunning(false);
      toast({ title: 'Error', description: 'Failed to run pipeline', variant: 'destructive' });
    }
  };

  const togglePersona = (p: PersonaId) => {
    setSelectedPersonas(prev =>
      prev.includes(p) ? prev.filter(x => x !== p) : [...prev, p]
    );
  };

  // Format elapsed time for display
  const formatElapsedTime = (ms: number) => {
    const seconds = Math.floor(ms / 1000);
    const tenths = Math.floor((ms % 1000) / 100);
    return `${seconds}.${tenths}s`;
  };

  // Top-level navigation tabs
  // NOTE: 'ask' tab hidden - NLQ functionality moved to AOS-NLQ repository
  const navTabs: { id: MainView; label: string }[] = [
    // { id: 'ask', label: 'Ask' },
    { id: 'graph', label: 'Graph' },
    { id: 'dashboard', label: 'Dashboard' },
  ];

  return (
    <div className="h-screen w-screen flex flex-col overflow-hidden bg-background text-foreground">
      {/* Top Navigation */}
      <div className="shrink-0 border-b border-border bg-card/50">
        <div className="flex items-center h-12 px-4 gap-4">
          {/* Logo/Title */}
          <div className="flex items-center gap-2 pr-4 border-r border-border">
            <span className="font-semibold text-primary">DCL</span>
            <span className="text-sm text-muted-foreground">Data Connectivity Layer</span>
          </div>

          {/* Main Navigation Tabs */}
          <nav className="flex items-center gap-1">
            {navTabs.map((tab) => (
              <button
                key={tab.id}
                onClick={() => setMainView(tab.id)}
                className={`px-3 py-1.5 text-sm rounded transition-colors ${
                  mainView === tab.id
                    ? 'bg-primary text-primary-foreground'
                    : 'text-muted-foreground hover:text-foreground hover:bg-accent'
                }`}
              >
                {tab.label}
              </button>
            ))}
          </nav>

          {/* Spacer */}
          <div className="flex-1" />

          {/* Controls for graph/dashboard views */}
          {mainView !== 'ask' && (
            <div className="flex items-center gap-3 text-sm">
              {/* Data Mode */}
              <div className="flex items-center gap-1">
                <span className="text-xs text-muted-foreground">Data:</span>
                <select
                  value={dataMode}
                  onChange={(e) => setDataMode(e.target.value as 'Demo' | 'Farm')}
                  className="px-2 py-1 text-xs rounded border border-border bg-background"
                >
                  <option value="Demo">Demo</option>
                  <option value="Farm">Farm</option>
                </select>
              </div>

              {/* Run Mode */}
              <div className="flex items-center gap-1">
                <span className="text-xs text-muted-foreground">Mode:</span>
                <select
                  value={runMode}
                  onChange={(e) => setRunMode(e.target.value as 'Dev' | 'Prod')}
                  className="px-2 py-1 text-xs rounded border border-border bg-background"
                >
                  <option value="Dev">Dev</option>
                  <option value="Prod">Prod</option>
                </select>
              </div>

              {/* Source Limit (only for Farm mode) */}
              {dataMode === 'Farm' && (
                <div className="flex items-center gap-1">
                  <span className="text-xs text-muted-foreground">Sources:</span>
                  <input
                    type="number"
                    min={1}
                    max={50}
                    value={sourceLimit}
                    onChange={(e) => setSourceLimit(parseInt(e.target.value) || 5)}
                    className="w-12 px-1 py-1 text-xs rounded border border-border bg-background text-center"
                  />
                </div>
              )}

              {/* Persona Selector */}
              <div className="flex items-center gap-1 pl-2 border-l border-border">
                <span className="text-xs text-muted-foreground">Personas:</span>
                <div className="flex gap-1">
                  {ALL_PERSONAS.map((p) => (
                    <button
                      key={p}
                      onClick={() => togglePersona(p)}
                      className={`px-2 py-0.5 text-xs rounded transition-colors ${
                        selectedPersonas.includes(p)
                          ? 'bg-primary text-primary-foreground'
                          : 'bg-accent/50 text-muted-foreground hover:bg-accent'
                      }`}
                    >
                      {p}
                    </button>
                  ))}
                </div>
              </div>

              {/* Run Button & Timer */}
              <div className="flex items-center gap-2 pl-2 border-l border-border">
                <button
                  onClick={handleRun}
                  disabled={isRunning}
                  className="px-3 py-1 text-xs rounded bg-primary text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
                >
                  {isRunning ? 'Running...' : 'Run'}
                </button>
                {isRunning && (
                  <span className="text-xs font-mono text-muted-foreground min-w-[3rem]">
                    {formatElapsedTime(elapsedTime)}
                  </span>
                )}
                {!isRunning && graphData?.meta.runMetrics && (
                  <span className="text-xs text-muted-foreground">
                    {graphData.meta.runMetrics.processingMs}ms
                  </span>
                )}
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Main Content */}
      <div className="flex-1 overflow-hidden">
        {mainView === 'ask' ? (
          <AskPage />
        ) : mainView === 'dashboard' ? (
          <ResizablePanelGroup direction="horizontal">
            <ResizablePanel defaultSize={75} minSize={50}>
              <div className="h-full w-full">
                <EnterpriseDashboard data={graphData} runId={runId} />
              </div>
            </ResizablePanel>

            <ResizableHandle className="bg-border/50 w-1.5 hover:bg-primary/50 transition-colors" />

            <ResizablePanel defaultSize={25} minSize={15}>
              <div className="h-full border-l bg-sidebar">
                <NarrationPanel runId={runId} />
              </div>
            </ResizablePanel>
          </ResizablePanelGroup>
        ) : (
          <ResizablePanelGroup direction="horizontal">
            <ResizablePanel defaultSize={70} minSize={40}>
              <div className="h-full w-full relative">
                <div className="absolute inset-0 p-4">
                  <div className="h-full w-full rounded-xl border bg-card/30 overflow-hidden shadow-inner">
                    <SankeyGraph
                      data={graphData}
                      selectedPersonas={selectedPersonas}
                    />
                  </div>
                </div>
              </div>
            </ResizablePanel>

            <ResizableHandle className="bg-border/50 w-1.5 hover:bg-primary/50 transition-colors" />

            <ResizablePanel defaultSize={30} minSize={20}>
              <div className="h-full border-l bg-sidebar flex flex-col min-h-0">
                <Tabs defaultValue="monitor" className="flex-1 flex flex-col min-h-0">
                   <div className="border-b px-4 pt-2 shrink-0">
                     <TabsList className="w-full">
                       <TabsTrigger value="monitor" className="flex-1">Monitor</TabsTrigger>
                       <TabsTrigger value="narration" className="flex-1">Narration</TabsTrigger>
                     </TabsList>
                   </div>

                   <div className="flex-1 overflow-hidden flex flex-col min-h-0">
                     <TabsContent value="monitor" className="flex-1 flex flex-col mt-0 min-h-0">
                       <MonitorPanel data={graphData} selectedPersonas={selectedPersonas} runId={runId} />
                     </TabsContent>
                     <TabsContent value="narration" className="flex-1 flex flex-col mt-0 min-h-0">
                       <NarrationPanel runId={runId} />
                     </TabsContent>
                   </div>
                </Tabs>
              </div>
            </ResizablePanel>
          </ResizablePanelGroup>
        )}
      </div>
      <Toaster />
    </div>
  );
}

export default App;
