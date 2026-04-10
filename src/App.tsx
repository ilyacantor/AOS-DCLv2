import { useEffect, useState, useRef } from 'react';
import { GraphSnapshot, PersonaId, PersonaStats } from './types';
import { MonitorPanel } from './components/MonitorPanel';
import { SnapshotPanel } from './components/SnapshotPanel';
import { SankeyGraph } from './components/SankeyGraph';
import { ResizablePanelGroup, ResizablePanel } from './components/ui/resizable';
import { Tabs, TabsList, TabsTrigger, TabsContent } from './components/ui/tabs';
import { Toaster } from './components/ui/toaster';
import { useToast } from './hooks/use-toast';
import ChevronLeft from 'lucide-react/dist/esm/icons/chevron-left';
import ChevronRight from 'lucide-react/dist/esm/icons/chevron-right';
import { UserGuide } from './components/UserGuide';
import { IngestTab } from './components/IngestTab';
import { ContextTab } from './components/ContextTab';
import { DashboardTab } from './components/DashboardTab';
import { ReconTab } from './components/ReconTab';
import { GraphV2Tab } from './components/GraphV2Tab';
import { useEntities } from './components/RunSelector';

type MainView = 'graph' | 'graph_v2' | 'dashboard' | 'context' | 'guide' | 'recon' | 'ingest';

const ALL_PERSONAS: PersonaId[] = ['CFO', 'CRO', 'COO', 'CTO', 'CHRO'];

const CACHE_KEY = 'dcl_last_run';

function loadCachedRun(): { graph: GraphSnapshot; runId: string } | null {
  try {
    const raw = localStorage.getItem(CACHE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (parsed?.graph?.nodes && parsed?.runId) return parsed;
    return null;
  } catch {
    return null;
  }
}

function saveCachedRun(graph: GraphSnapshot, runId: string): void {
  try {
    localStorage.setItem(CACHE_KEY, JSON.stringify({ graph, runId }));
  } catch {
    // localStorage full or unavailable — non-critical
  }
}

function App() {
  const cached = useRef(loadCachedRun());
  const [graphData, setGraphData] = useState<GraphSnapshot | null>(cached.current?.graph ?? null);
  const [runMode, setRunMode] = useState<'Dev' | 'Prod'>('Dev');
  const [selectedPersonas, setSelectedPersonas] = useState<PersonaId[]>(['CFO', 'CRO', 'COO', 'CTO', 'CHRO']);
  const [runId, setRunId] = useState<string | undefined>(cached.current?.runId);
  const [isRunning, setIsRunning] = useState(false);
  const [elapsedTime, setElapsedTime] = useState(0);
  const [mainView, setMainView] = useState<MainView>('graph');
  const [loadError, setLoadError] = useState<string | null>(null);
  const [isCachedView, setIsCachedView] = useState(cached.current !== null);
  const [rightPanelCollapsed, setRightPanelCollapsed] = useState(false);
  const [personaDropdownOpen, setPersonaDropdownOpen] = useState(false);
  const [selectedSnapshotName, setSelectedSnapshotName] = useState<string | undefined>(undefined);
  const personaDropdownRef = useRef<HTMLDivElement>(null);
  const { toast } = useToast();

  // Shared entity state — all 4 monitoring tabs use this
  const { entities, selectedEntityId, setSelectedEntityId, loading: entitiesLoading, error: entitiesError } = useEntities();

  useEffect(() => {
    if (!isRunning) return;

    const startTime = Date.now();
    const interval = setInterval(() => {
      setElapsedTime(Date.now() - startTime);
    }, 100);

    return () => clearInterval(interval);
  }, [isRunning]);

  // Close persona dropdown on outside click or Escape
  useEffect(() => {
    if (!personaDropdownOpen) return;
    const handleMouseDown = (e: MouseEvent) => {
      if (personaDropdownRef.current && !personaDropdownRef.current.contains(e.target as Node)) {
        setPersonaDropdownOpen(false);
      }
    };
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setPersonaDropdownOpen(false);
    };
    document.addEventListener('mousedown', handleMouseDown);
    document.addEventListener('keydown', handleKeyDown);
    return () => {
      document.removeEventListener('mousedown', handleMouseDown);
      document.removeEventListener('keydown', handleKeyDown);
    };
  }, [personaDropdownOpen]);

  const personaTitles: Record<PersonaId, string> = {
    CFO: 'Chief Financial Officer',
    CRO: 'Chief Revenue Officer',
    COO: 'Chief Operating Officer',
    CTO: 'Chief Technology Officer',
    CHRO: 'Chief Human Resources Officer',
  };

  const formatDomainLabel = (domain: string): string =>
    domain.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());

  const generatePersonaViews = (personas: PersonaId[], stats: Record<string, PersonaStats>) => {
    if (personas.length === 0) return [];

    return personas.map((persona) => {
      const ps = stats[persona];
      if (!ps) {
        return {
          personaId: persona,
          title: personaTitles[persona],
          focusAreas: [],
          keyEntities: [],
          metrics: [
            { id: 'm1', label: 'Data Sources', value: 0, unit: '', trend: 'flat' as const },
            { id: 'm2', label: 'Domains', value: 0, unit: '', trend: 'flat' as const },
          ],
          insights: [
            { id: 'i1', severity: 'info' as const, message: 'No matching triples found' },
          ],
          alerts: [],
        };
      }

      return {
        personaId: persona,
        title: personaTitles[persona],
        focusAreas: ps.domain_list.map(formatDomainLabel),
        keyEntities: ps.domain_list,
        metrics: [
          { id: 'm1', label: 'Data Sources', value: ps.data_sources, unit: '', trend: ps.data_sources > 0 ? 'up' as const : 'flat' as const },
          { id: 'm2', label: 'Domains', value: ps.domains, unit: '', trend: 'flat' as const },
          { id: 'm3', label: 'Triples', value: ps.triple_count, unit: '', trend: ps.triple_count > 0 ? 'up' as const : 'flat' as const },
        ],
        insights: [
          { id: 'i1', severity: 'info' as const, message: `${ps.triple_count.toLocaleString()} triples across ${ps.data_sources} source${ps.data_sources !== 1 ? 's' : ''}` },
        ],
        alerts: [],
      };
    });
  };

  const fetchPersonaStats = async (): Promise<Record<string, PersonaStats>> => {
    const res = await fetch('/api/dcl/triples/persona-stats');
    if (!res.ok) {
      console.warn(`[App] persona-stats returned ${res.status}`);
      return {};
    }
    return res.json();
  };

  const autoLoadedRef = useRef(false);
  useEffect(() => {
    if (autoLoadedRef.current) return;
    if (entitiesLoading) return; // Wait for entity list before auto-loading
    autoLoadedRef.current = true;

    // If we have cached data, show it immediately — no spinner needed
    const hasCached = cached.current !== null;
    if (!hasCached) {
      setIsRunning(true);
      setElapsedTime(0);
    }

    // Multi-tenant: include entity_id so the backend can resolve the correct tenant.
    // Single-tenant: entity_id is optional, backend falls back to resolve_single_tenant.
    const runBody: Record<string, unknown> = { mode: 'Farm', run_mode: 'Dev', personas: ALL_PERSONAS };
    if (selectedEntityId) {
      runBody.entity_id = selectedEntityId;
    }

    const runPromise = fetch('/api/dcl/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(runBody),
    }).then(async (r) => {
      if (!r.ok) {
        const errBody = await r.json().catch(() => null);
        const detail = errBody?.detail || `HTTP ${r.status}`;
        throw new Error(`DCL Engine returned ${r.status}: ${detail}`);
      }
      return r.json();
    });

    Promise.all([runPromise, fetchPersonaStats()])
      .then(([data, personaStats]) => {
        if (!data?.graph) {
          throw new Error('DCL Engine returned OK but response contained no graph data');
        }
        const gv = {
          ...data.graph,
          meta: {
            ...(data.graph.meta ?? {}),
            personaViews: generatePersonaViews(ALL_PERSONAS, personaStats),
            runMetrics: data.run_metrics,
          },
        };
        setGraphData(gv);
        setRunId(data.run_id);
        setIsCachedView(false);
        setIsRunning(false);
        setLoadError(null);
        saveCachedRun(gv, data.run_id);
      })
      .catch((err) => {
        console.error('[App] Auto-load failed:', err);
        if (!hasCached) {
          setLoadError(`Auto-load failed: ${err instanceof Error ? err.message : 'Could not connect to DCL Engine'}. Start the backend and click Run.`);
        }
        setIsRunning(false);
      });
  }, [entitiesLoading, selectedEntityId]);

  // Auto re-render when snapshot selection changes
  const snapshotInitRef = useRef(true);
  useEffect(() => {
    if (snapshotInitRef.current) {
      snapshotInitRef.current = false;
      return;
    }
    if (selectedSnapshotName) {
      handleRun();
    }
  }, [selectedSnapshotName]);

  const handleRun = async () => {
    setIsRunning(true);
    setElapsedTime(0);
    toast({ title: 'Pipeline Started', description: `Running in ${runMode} mode...` });

    try {
      const [res, personaStats] = await Promise.all([
        fetch('/api/dcl/run', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            mode: 'Farm',
            run_mode: runMode,
            personas: selectedPersonas.length > 0 ? selectedPersonas : undefined,
            force_refresh: true,
            snapshot_name: selectedSnapshotName,
            entity_id: selectedEntityId || undefined,
          }),
        }),
        fetchPersonaStats(),
      ]);

      if (!res.ok) {
        const errBody = await res.json().catch(() => null);
        const detail = errBody?.detail || `HTTP ${res.status}`;
        throw new Error(detail);
      }
      const data = await res.json();
      console.log('[handleRun] Received data:', {
        nodes: data.graph?.nodes?.length,
        links: data.graph?.links?.length,
        mode: 'Farm',
        personas: selectedPersonas,
        runMetrics: data.run_metrics
      });

      const graphWithViews = {
        ...data.graph,
        meta: {
          ...(data.graph.meta ?? {}),
          personaViews: generatePersonaViews(selectedPersonas, personaStats),
          runMetrics: data.run_metrics,
        },
      };
      setGraphData(graphWithViews);
      setRunId(data.run_id);
      setIsCachedView(false);
      setIsRunning(false);
      saveCachedRun(graphWithViews, data.run_id);
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
    { id: 'graph_v2', label: 'Graph v2' },
    { id: 'dashboard', label: 'Dashboard' },
    { id: 'context', label: 'Context' },
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

          {/* Spacer */}
          <div className="flex-1" />

          {/* Guide button — far right */}
          <button
            onClick={() => setMainView('guide')}
            className={`px-2.5 py-1.5 text-sm rounded transition-colors ${
              mainView === 'guide'
                ? 'bg-primary text-primary-foreground'
                : 'text-muted-foreground hover:text-foreground hover:bg-accent'
            }`}
            title="Guide"
          >
            ?
          </button>

          {/* Controls for graph/dashboard views */}
          <div className="flex items-center gap-3 text-sm">
              {/* Run Mode */}
              <div className="flex items-center gap-1">
                <span className="text-xs text-muted-foreground">Mode:</span>
                <div className="flex rounded border border-border overflow-hidden">
                  {(['Dev', 'Prod'] as const).map((mode) => (
                    <button
                      key={mode}
                      onClick={() => setRunMode(mode)}
                      className={`px-2 py-1 text-xs transition-colors ${
                        runMode === mode
                          ? 'bg-primary text-primary-foreground'
                          : 'bg-background text-muted-foreground hover:bg-accent'
                      }`}
                    >
                      {mode}
                    </button>
                  ))}
                </div>
              </div>

              {/* Persona Selector — Dropdown */}
              <div className="relative pl-2 border-l border-border" ref={personaDropdownRef}>
                <button
                  onClick={() => setPersonaDropdownOpen(prev => !prev)}
                  className="flex items-center gap-1 px-2 py-1 text-xs rounded border border-border bg-background hover:bg-accent transition-colors"
                >
                  <span>Personas ({selectedPersonas.length}/{ALL_PERSONAS.length})</span>
                  <svg className={`w-3 h-3 transition-transform ${personaDropdownOpen ? 'rotate-180' : ''}`} viewBox="0 0 12 12" fill="none" stroke="currentColor" strokeWidth="2"><path d="M3 4.5l3 3 3-3" /></svg>
                </button>
                {personaDropdownOpen && (
                  <div className="absolute top-full right-0 mt-1 z-50 min-w-[140px] rounded border border-border bg-background shadow-lg py-1">
                    {ALL_PERSONAS.map((p) => (
                      <label
                        key={p}
                        className="flex items-center gap-2 px-3 py-1 text-xs cursor-pointer hover:bg-accent transition-colors"
                      >
                        <input
                          type="checkbox"
                          checked={selectedPersonas.includes(p)}
                          onChange={() => togglePersona(p)}
                          className="rounded border-border"
                        />
                        <span className={selectedPersonas.includes(p) ? 'text-foreground font-medium' : 'text-muted-foreground'}>{p}</span>
                      </label>
                    ))}
                  </div>
                )}
              </div>

              {/* Run Button & Timer */}
              <div className="flex items-center gap-2 pl-2 border-l border-border">
                <button
                  onClick={handleRun}
                  disabled={isRunning}
                  data-role="run-primary"
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
                {isCachedView && !isRunning && (
                  <span className="text-xs text-yellow-500" title="Displaying last successful run from cache">cached</span>
                )}
              </div>
            </div>
        </div>
      </div>

      {/* Main Content */}
      <div className="flex-1 overflow-hidden">
        {mainView === 'context' ? (
          <ContextTab entities={entities} selectedEntityId={selectedEntityId} onEntityChange={setSelectedEntityId} entitiesLoading={entitiesLoading} entitiesError={entitiesError} />
        ) : mainView === 'ingest' ? (
          <IngestTab entities={entities} selectedEntityId={selectedEntityId} onEntityChange={setSelectedEntityId} entitiesLoading={entitiesLoading} entitiesError={entitiesError} />
        ) : mainView === 'recon' ? (
          <ReconTab entities={entities} selectedEntityId={selectedEntityId} onEntityChange={setSelectedEntityId} entitiesLoading={entitiesLoading} entitiesError={entitiesError} />
        ) : mainView === 'guide' ? (
          <UserGuide />
        ) : mainView === 'dashboard' ? (
          <DashboardTab entities={entities} selectedEntityId={selectedEntityId} onEntityChange={setSelectedEntityId} entitiesLoading={entitiesLoading} entitiesError={entitiesError} />
        ) : mainView === 'graph_v2' ? (
          <GraphV2Tab graphData={graphData} entities={entities} selectedEntityId={selectedEntityId} onEntityChange={setSelectedEntityId} entitiesLoading={entitiesLoading} entitiesError={entitiesError} selectedPersonas={selectedPersonas} />
        ) : (
          <ResizablePanelGroup direction="horizontal">
            <ResizablePanel defaultSize={70} minSize={40}>
              <div className="h-full w-full relative">
                <div className="absolute inset-0 p-4">
                  <div className="h-full w-full rounded-xl border bg-card/30 overflow-hidden shadow-inner">
                    {loadError && !graphData && (
                      <div className="flex items-center justify-center h-full">
                        <div className="text-center p-6 rounded-lg border border-destructive/30 bg-destructive/5 max-w-md">
                          <p className="text-sm text-destructive font-medium">{loadError}</p>
                        </div>
                      </div>
                    )}
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
                       <TabsTrigger value="snapshot" className="flex-1">Snapshot</TabsTrigger>
                     </TabsList>
                   </div>

                   <div className="flex-1 overflow-hidden flex flex-col min-h-0">
                     <TabsContent value="monitor" className="flex-1 flex flex-col mt-0 min-h-0">
                       <MonitorPanel data={graphData} selectedPersonas={selectedPersonas} runId={runId} />
                     </TabsContent>
                     <TabsContent value="snapshot" className="flex-1 flex flex-col mt-0 min-h-0">
                       <SnapshotPanel
                         currentSnapshotName={graphData?.meta?.snapshotName}
                         runMetrics={graphData?.meta?.runMetrics}
                         aodRunId={graphData?.meta?.aodRunId}
                         onSnapshotSelect={setSelectedSnapshotName}
                       />
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
