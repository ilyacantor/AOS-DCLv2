import { useEffect, useState, useRef } from 'react';
import { GraphSnapshot, PersonaId } from './types';
import { MonitorPanel } from './components/MonitorPanel';
import { NarrationPanel } from './components/NarrationPanel';
import { SankeyGraph } from './components/SankeyGraph';
import { EnterpriseDashboard } from './components/EnterpriseDashboard';
import { ResizablePanelGroup, ResizablePanel, ResizableHandle } from './components/ui/resizable';
import { Tabs, TabsList, TabsTrigger, TabsContent } from './components/ui/tabs';
import { Toaster } from './components/ui/toaster';
import { useToast } from './hooks/use-toast';
import { ChevronLeft, ChevronRight } from 'lucide-react';
import { UserGuide } from './components/UserGuide';
import { ReconciliationPanel } from './components/ReconciliationPanel';
import { IngestionPanel } from './components/IngestionPanel';

type MainView = 'graph' | 'dashboard' | 'guide' | 'recon' | 'ingest';

const ALL_PERSONAS: PersonaId[] = ['CFO', 'CRO', 'COO', 'CTO'];

function App() {
  const [graphData, setGraphData] = useState<GraphSnapshot | null>(null);
  const [runMode, setRunMode] = useState<'Dev' | 'Prod'>('Dev');
  const [dataMode, setDataMode] = useState<'Demo' | 'Farm' | 'AAM'>('Demo');
  const [selectedPersonas, setSelectedPersonas] = useState<PersonaId[]>(['CFO', 'CRO', 'COO', 'CTO']);
  const [runId, setRunId] = useState<string | undefined>(undefined);
  const [isRunning, setIsRunning] = useState(false);
  const [elapsedTime, setElapsedTime] = useState(0);
  const [mainView, setMainView] = useState<MainView>('graph');
  const [rightPanelCollapsed, setRightPanelCollapsed] = useState(false);
  const { toast } = useToast();

  useEffect(() => {
    if (!isRunning) return;

    const startTime = Date.now();
    const interval = setInterval(() => {
      setElapsedTime(Date.now() - startTime);
    }, 100);

    return () => clearInterval(interval);
  }, [isRunning]);

  const generatePersonaViews = (graph: GraphSnapshot, personas: PersonaId[]) => {
    if (!graph || personas.length === 0) return [];

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
      const ontologyNodes = graph.nodes.filter(n =>
        n.level === 'L2' && ontologies.some(ont => n.id.includes(ont))
      );

      const sourceConnections = graph.links.filter(l =>
        ontologyNodes.some(n => l.target === n.id)
      );

      return {
        personaId: persona,
        title: personaTitles[persona],
        focusAreas: personaFocusAreas[persona],
        keyEntities: ontologyNodes.map(n => n.label),
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

  const autoLoadedRef = useRef(false);
  useEffect(() => {
    if (autoLoadedRef.current) return;
    autoLoadedRef.current = true;
    fetch('/api/dcl/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ mode: 'Demo', run_mode: 'Dev', personas: ALL_PERSONAS }),
    })
      .then(r => r.ok ? r.json() : null)
      .then(data => {
        if (!data?.graph) return;
        const gv = {
          ...data.graph,
          meta: {
            ...(data.graph.meta ?? {}),
            personaViews: generatePersonaViews(data.graph, ALL_PERSONAS),
            runMetrics: data.run_metrics,
          },
        };
        setGraphData(prev => prev || gv);
        setRunId(prev => prev || data.run_id);
      })
      .catch(() => {});
  }, []);

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
          personas: selectedPersonas.length > 0 ? selectedPersonas : undefined,
          force_refresh: true,
        }),
      });

      if (!res.ok) {
        const errBody = await res.json().catch(() => null);
        const detail = errBody?.detail || `HTTP ${res.status}`;
        throw new Error(detail);
      }
      const data = await res.json();
      console.log('[handleRun] Received data:', {
        nodes: data.graph?.nodes?.length,
        links: data.graph?.links?.length,
        mode: dataMode,
        personas: selectedPersonas,
        runMetrics: data.run_metrics
      });
      
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
      toast({ title: 'Pipeline Complete', description: `${data.graph.nodes.length} nodes, ${data.graph.links.length} links` });
    } catch (e) {
      const msg = e instanceof Error ? e.message : 'Failed to run pipeline';
      console.error('Failed to run pipeline:', msg);
      setIsRunning(false);
      toast({ title: 'Pipeline Error', description: msg, variant: 'destructive' });
    }
  };

  const togglePersona = (p: PersonaId) => {
    setSelectedPersonas(prev =>
      prev.includes(p) ? prev.filter(x => x !== p) : [...prev, p]
    );
  };

  // Format elapsed time for display
  const formatElapsedTime = (ms: number) => {
    return `${(ms / 1000).toFixed(1)}s`;
  };

  // Top-level navigation tabs
  const navTabs: { id: MainView; label: string }[] = [
    { id: 'graph', label: 'Graph' },
    { id: 'dashboard', label: 'Dashboard' },
    { id: 'guide', label: 'Guide' },
    { id: 'recon', label: 'Recon' },
    { id: 'ingest', label: 'Ingest' },
  ];

  return (
    <div className="h-screen w-screen flex flex-col overflow-hidden bg-background text-foreground">
      {/* Top Navigation */}
      <div className="shrink-0 border-b border-border bg-card/50">
        <div className="flex items-center h-12 px-4 gap-4">
          {/* Logo/Title */}
          <div className="flex items-center gap-2 pr-4 border-r border-border">
            <span className="font-semibold text-primary">AOS DCL</span>
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

          {/* AAM Payload KPIs */}
          {!isRunning && graphData?.meta.runMetrics?.dataStatus && (
            <div className="flex items-center gap-1.5 px-2">
              <span className={`inline-block w-2 h-2 rounded-full ${
                graphData.meta.runMetrics.dataStatus === 'ok' ? 'bg-emerald-400' :
                graphData.meta.runMetrics.dataStatus === 'partial' ? 'bg-amber-400' :
                'bg-red-400'
              }`} />
              {graphData.meta.runMetrics.payloadKpis && (
                <span className="text-xs text-muted-foreground font-mono">
                  {graphData.meta.runMetrics.payloadKpis.fabrics}F·{graphData.meta.runMetrics.payloadKpis.pipes}P·{graphData.meta.runMetrics.payloadKpis.sources}S
                </span>
              )}
              {graphData.meta.runMetrics.payloadKpis && graphData.meta.runMetrics.payloadKpis.unpipedCount > 0 && (
                <span className="text-xs text-amber-400 font-mono">
                  {graphData.meta.runMetrics.payloadKpis.unpipedCount}!
                </span>
              )}
            </div>
          )}

          {/* Spacer */}
          <div className="flex-1" />

          {/* Controls for graph/dashboard views */}
          <div className="flex items-center gap-3 text-sm">
              {/* Data Mode */}
              <div className="flex items-center gap-1">
                <span className="text-xs text-muted-foreground">Data:</span>
                <select
                  value={dataMode}
                  onChange={(e) => setDataMode(e.target.value as 'Demo' | 'Farm' | 'AAM')}
                  className="px-2 py-1 text-xs rounded border border-border bg-background"
                >
                  <option value="Demo">Demo</option>
                  <option value="Farm">Farm</option>
                  <option value="AAM">AAM</option>
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
                    {(graphData.meta.runMetrics.processingMs / 1000).toFixed(1)}s
                  </span>
                )}
              </div>
            </div>
        </div>
      </div>

      {/* Main Content */}
      <div className="flex-1 overflow-hidden">
        {mainView === 'ingest' ? (
          <IngestionPanel />
        ) : mainView === 'recon' ? (
          <ReconciliationPanel runId={runId} dataMode={dataMode} />
        ) : mainView === 'guide' ? (
          <UserGuide />
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

            <div className="relative flex h-full">
              <button
                onClick={() => setRightPanelCollapsed(!rightPanelCollapsed)}
                className="absolute left-0 top-1/2 -translate-y-1/2 -translate-x-1/2 z-10 w-6 h-12 bg-sidebar border border-border rounded-md flex items-center justify-center hover:bg-accent transition-colors"
                title={rightPanelCollapsed ? "Expand panel" : "Collapse panel"}
              >
                {rightPanelCollapsed ? <ChevronLeft className="w-4 h-4" /> : <ChevronRight className="w-4 h-4" />}
              </button>

              <div className={`h-full border-l bg-sidebar flex flex-col min-h-0 transition-all duration-200 ${rightPanelCollapsed ? 'w-0 overflow-hidden' : 'w-80'}`}>
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
            </div>
          </ResizablePanelGroup>
        )}
      </div>
      <Toaster />
    </div>
  );
}

export default App;
