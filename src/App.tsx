import { useEffect, useState } from 'react';
import { GraphSnapshot, PersonaId } from './types';
import { ControlsBar } from './components/ControlsBar';
import { MonitorPanel } from './components/MonitorPanel';
import { NarrationPanel } from './components/NarrationPanel';
import { SankeyGraph } from './components/SankeyGraph';
import { ResizablePanelGroup, ResizablePanel, ResizableHandle } from './components/ui/resizable';
import { Tabs, TabsList, TabsTrigger, TabsContent } from './components/ui/tabs';
import { Toaster } from './components/ui/toaster';
import { useToast } from './hooks/use-toast';

function App() {
  const [graphData, setGraphData] = useState<GraphSnapshot | null>(null);
  const [runMode, setRunMode] = useState<'Dev' | 'Prod'>('Dev');
  const [dataMode, setDataMode] = useState<'Demo' | 'Farm'>('Demo');
  const [selectedPersonas, setSelectedPersonas] = useState<PersonaId[]>([]);
  const [runId, setRunId] = useState<string | undefined>(undefined);
  const { toast } = useToast();

  useEffect(() => {
    loadData();
  }, []);

  const loadData = async () => {
    try {
      const res = await fetch('/api/dcl/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ mode: 'Demo', run_mode: 'Dev', personas: [] }),
      });
      
      if (!res.ok) throw new Error('Failed to load graph');
      const data = await res.json();
      setGraphData(data.graph);
      setRunId(data.run_id);
    } catch (e) {
      console.error(e);
    }
  };

  const handleRun = async () => {
    toast({ title: 'Pipeline Started', description: `Running in ${runMode} mode on ${dataMode} data...` });
    
    try {
      const res = await fetch('/api/dcl/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ 
          mode: dataMode, 
          run_mode: runMode, 
          personas: selectedPersonas 
        }),
      });
      
      if (!res.ok) throw new Error('Failed to run pipeline');
      const data = await res.json();
      setGraphData(data.graph);
      setRunId(data.run_id);
      toast({ title: 'Pipeline Complete', description: 'New graph snapshot generated.' });
    } catch (e) {
      console.error(e);
      toast({ title: 'Error', description: 'Failed to run pipeline', variant: 'destructive' });
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
