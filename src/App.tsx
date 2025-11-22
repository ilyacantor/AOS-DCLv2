import React, { useState, useEffect } from 'react';
import { GraphSnapshot, PersonaId, PersonaView } from './types';
import { ControlsBar } from './components/ControlsBar';
import { MonitorPanel } from './components/MonitorPanel';
import { NarrationPanel } from './components/NarrationPanel';
import { SankeyGraph } from './components/SankeyGraph';
import { ResizablePanelGroup, ResizablePanel, ResizableHandle } from "@/components/ui/resizable";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { Toaster } from '@/components/ui/toaster';
import { useToast } from '@/hooks/use-toast';

// Convert snake_case backend response to camelCase frontend types
function mapNode(backendNode: any): any {
  return {
    ...backendNode,
    personaId: backendNode.persona_id
  };
}

function mapPersonaView(backendView: any): PersonaView {
  return {
    personaId: backendView.persona_id,
    title: backendView.title,
    focusAreas: backendView.focus_areas || [],
    keyEntities: backendView.key_entities || [],
    metrics: (backendView.metrics || []).map((m: any) => ({
      id: m.id,
      label: m.label,
      value: m.value,
      unit: m.unit,
      trend: m.trend,
      trendDeltaPct: m.trend_delta_pct
    })),
    insights: (backendView.insights || []).map((i: any) => ({
      id: i.id,
      severity: i.severity,
      message: i.message,
      relatedOntology: i.related_ontology,
      relatedSources: i.related_sources
    })),
    alerts: (backendView.alerts || []).map((a: any) => ({
      id: a.id,
      severity: a.severity,
      message: a.message,
      relatedOntology: a.related_ontology,
      relatedSources: a.related_sources
    }))
  };
}

function App() {
  const [graphData, setGraphData] = useState<GraphSnapshot | null>(null);
  const [runMode, setRunMode] = useState<'Dev' | 'Prod'>('Dev');
  const [dataMode, setDataMode] = useState<'Demo' | 'Farm'>('Demo');
  const [selectedPersonas, setSelectedPersonas] = useState<PersonaId[]>(['CFO', 'CRO', 'COO', 'CTO']);
  const [isRunLoading, setIsRunLoading] = useState(false);
  const [runId, setRunId] = useState<string | undefined>(undefined);
  const { toast } = useToast();

  useEffect(() => {
    loadInitialData();
  }, []);

  const loadInitialData = async () => {
    setIsRunLoading(true);
    try {
      const response = await fetch('/api/dcl/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ 
          mode: 'Demo', 
          run_mode: 'Dev', 
          personas: ['CFO', 'CRO', 'COO', 'CTO']
        }),
      });

      if (!response.ok) throw new Error('Failed to load initial data');

      const data = await response.json();
      
      const snapshot: GraphSnapshot = {
        nodes: data.graph.nodes.map(mapNode),
        links: data.graph.links,
        meta: {
          mode: 'Demo',
          runId: data.run_id,
          generatedAt: data.graph.meta.generated_at,
          stats: data.graph.meta.stats,
          personaViews: (data.graph.meta.persona_views || []).map(mapPersonaView),
          runMetrics: {
            llmCalls: data.run_metrics.llm_calls,
            ragReads: data.run_metrics.rag_reads,
            ragWrites: data.run_metrics.rag_writes,
            processingMs: data.run_metrics.processing_ms,
            totalMs: data.run_metrics.processing_ms + (data.run_metrics.render_ms || 0)
          }
        }
      };
      
      setGraphData(snapshot);
      setRunId(data.run_id);
    } catch (error) {
      console.error('Error loading initial data:', error);
      toast({ 
        title: "Initial Load Failed", 
        description: "Click 'Run Pipeline' to generate graph data.", 
        variant: "destructive" 
      });
    } finally {
      setIsRunLoading(false);
    }
  };

  const handleRun = async () => {
    setIsRunLoading(true);
    toast({ title: "Pipeline Started", description: `Running in ${runMode} mode on ${dataMode} data...` });
    
    try {
      const response = await fetch('/api/dcl/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ 
          mode: dataMode, 
          run_mode: runMode, 
          personas: selectedPersonas.length > 0 ? selectedPersonas : ['CFO', 'CRO', 'COO', 'CTO']
        }),
      });

      if (!response.ok) throw new Error('Failed to run DCL');

      const data = await response.json();
      
      const snapshot: GraphSnapshot = {
        nodes: data.graph.nodes.map(mapNode),
        links: data.graph.links,
        meta: {
          mode: dataMode,
          runId: data.run_id,
          generatedAt: data.graph.meta.generated_at,
          stats: data.graph.meta.stats,
          personaViews: (data.graph.meta.persona_views || []).map(mapPersonaView),
          runMetrics: {
            llmCalls: data.run_metrics.llm_calls,
            ragReads: data.run_metrics.rag_reads,
            ragWrites: data.run_metrics.rag_writes,
            processingMs: data.run_metrics.processing_ms,
            totalMs: data.run_metrics.processing_ms + (data.run_metrics.render_ms || 0)
          }
        }
      };
      
      setGraphData(snapshot);
      setRunId(data.run_id);
      
      toast({ 
        title: "Pipeline Complete", 
        description: "New graph snapshot generated.",
        className: "bg-green-500/10 border-green-500/20 text-green-100"
      });
    } catch (error) {
      console.error('Error running DCL:', error);
      toast({ 
        title: "Error", 
        description: "Failed to run DCL engine. Check console for details.", 
        variant: "destructive" 
      });
    } finally {
      setIsRunLoading(false);
    }
  };

  const togglePersona = (p: PersonaId) => {
    setSelectedPersonas(prev => 
      prev.includes(p) ? prev.filter(x => x !== p) : [...prev, p]
    );
  };

  return (
    <div className="h-screen w-screen flex flex-col overflow-hidden bg-background text-foreground">
      <ControlsBar 
        runMode={runMode}
        setRunMode={setRunMode}
        dataMode={dataMode}
        setDataMode={setDataMode}
        selectedPersonas={selectedPersonas}
        togglePersona={togglePersona}
        onRun={handleRun}
        isLoading={isRunLoading}
        metrics={graphData?.meta.runMetrics}
      />

      <div className="flex-1 overflow-hidden">
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
            <div className="h-full border-l bg-sidebar flex flex-col">
              <Tabs defaultValue="monitor" className="flex-1 flex flex-col">
                 <div className="border-b px-4 pt-2">
                   <TabsList className="w-full">
                     <TabsTrigger value="monitor" className="flex-1">Monitor</TabsTrigger>
                     <TabsTrigger value="narration" className="flex-1">Narration</TabsTrigger>
                   </TabsList>
                 </div>
                 
                 <div className="flex-1 overflow-hidden">
                   <TabsContent value="monitor" className="h-full mt-0">
                     <MonitorPanel data={graphData} selectedPersonas={selectedPersonas} />
                   </TabsContent>
                   <TabsContent value="narration" className="h-full mt-0">
                     <NarrationPanel runId={runId} />
                   </TabsContent>
                 </div>
              </Tabs>
            </div>
          </ResizablePanel>
        </ResizablePanelGroup>
      </div>
      <Toaster />
    </div>
  );
}

export default App;
