import React from 'react';
import { GraphSnapshot, PersonaId } from '../types';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Tabs, TabsList, TabsTrigger, TabsContent } from '@/components/ui/tabs';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { TrendingUp, TrendingDown, Minus, AlertTriangle, Info } from 'lucide-react';

interface MonitorPanelProps {
  data: GraphSnapshot | null;
  selectedPersonas: PersonaId[];
}

export function MonitorPanel({ data, selectedPersonas }: MonitorPanelProps) {
  if (!data) return <div className="p-4 text-muted-foreground">No data loaded</div>;

  const activePersonaViews = data.meta.personaViews?.filter(pv => selectedPersonas.includes(pv.personaId)) || [];

  return (
    <div className="h-full flex flex-col bg-sidebar/30">
      <Tabs defaultValue="views" className="flex-1 flex flex-col">
        <div className="px-4 pt-4 pb-2">
          <TabsList className="w-full grid grid-cols-3">
            <TabsTrigger value="views">Persona Views</TabsTrigger>
            <TabsTrigger value="sources">Sources</TabsTrigger>
            <TabsTrigger value="ontology">Ontology</TabsTrigger>
          </TabsList>
        </div>

        <ScrollArea className="flex-1">
          <div className="p-4 space-y-4">
            <TabsContent value="views" className="mt-0 space-y-4">
              {activePersonaViews.length === 0 && (
                <div className="text-center py-8 text-muted-foreground text-sm">
                  Select a persona filter to see views
                </div>
              )}
              
              {activePersonaViews.map((view) => (
                <Card key={view.personaId} className="border-l-4 border-l-primary shadow-sm bg-card/50">
                  <CardHeader className="pb-2 pt-4 px-4">
                    <div className="flex justify-between items-start">
                      <div>
                        <CardTitle className="text-lg font-display">{view.title}</CardTitle>
                        <div className="text-xs mt-1 flex gap-2 text-muted-foreground">
                          {view.focusAreas.map(area => (
                            <Badge key={area} variant="outline" className="text-[10px] h-5 px-1.5 font-normal">
                              {area}
                            </Badge>
                          ))}
                        </div>
                      </div>
                      <Badge variant="secondary" className="font-mono text-xs">{view.personaId}</Badge>
                    </div>
                  </CardHeader>
                  <CardContent className="px-4 pb-4 pt-2 space-y-4">
                    <div className="grid grid-cols-2 gap-2">
                      {view.metrics.map(metric => (
                        <div key={metric.id} className="bg-secondary/30 rounded p-2 flex flex-col">
                          <span className="text-[10px] text-muted-foreground uppercase truncate">{metric.label}</span>
                          <div className="flex items-end justify-between mt-1">
                            <span className="text-lg font-mono font-medium leading-none">
                              {metric.value.toLocaleString()}
                              {metric.unit && <span className="text-xs text-muted-foreground ml-0.5">{metric.unit}</span>}
                            </span>
                            {metric.trend && (
                              <span className={`flex items-center text-xs ${
                                metric.trend === 'up' ? 'text-green-400' : 
                                metric.trend === 'down' ? 'text-red-400' : 'text-muted-foreground'
                              }`}>
                                {metric.trend === 'up' && <TrendingUp className="w-3 h-3 mr-0.5" />}
                                {metric.trend === 'down' && <TrendingDown className="w-3 h-3 mr-0.5" />}
                                {metric.trend === 'flat' && <Minus className="w-3 h-3 mr-0.5" />}
                                {metric.trendDeltaPct ? `${Math.abs(metric.trendDeltaPct)}%` : ''}
                              </span>
                            )}
                          </div>
                        </div>
                      ))}
                    </div>

                    {(view.insights.length > 0 || view.alerts.length > 0) && (
                      <div className="space-y-2 pt-2">
                         {view.alerts.map(alert => (
                           <div key={alert.id} className="flex gap-2 text-xs p-2 rounded bg-red-500/10 border border-red-500/20 text-red-200">
                             <AlertTriangle className="w-3.5 h-3.5 shrink-0 mt-0.5" />
                             <span>{alert.message}</span>
                           </div>
                         ))}
                         {view.insights.map(insight => (
                           <div key={insight.id} className="flex gap-2 text-xs p-2 rounded bg-blue-500/10 border border-blue-500/20 text-blue-200">
                             <Info className="w-3.5 h-3.5 shrink-0 mt-0.5" />
                             <span>{insight.message}</span>
                           </div>
                         ))}
                      </div>
                    )}
                  </CardContent>
                </Card>
              ))}
            </TabsContent>

            <TabsContent value="sources" className="mt-0">
              <Card className="bg-card/50">
                <div className="grid grid-cols-1 divide-y">
                  {data.nodes.filter(n => n.level === 'L1').map(node => (
                    <div key={node.id} className="p-3 flex items-center justify-between text-sm hover:bg-secondary/20 transition-colors">
                      <div className="flex flex-col">
                        <span className="font-medium">{node.label}</span>
                        <span className="text-[10px] text-muted-foreground">{node.group}</span>
                      </div>
                      <div className="flex items-center gap-2">
                        <Badge variant={node.status === 'ok' ? 'outline' : 'destructive'} className="text-[10px] h-5">
                          {node.status === 'ok' ? 'Connected' : 'Error'}
                        </Badge>
                      </div>
                    </div>
                  ))}
                </div>
              </Card>
            </TabsContent>

            <TabsContent value="ontology" className="mt-0">
              <Card className="bg-card/50">
                <div className="grid grid-cols-1 divide-y">
                  {data.nodes.filter(n => n.level === 'L2').map(node => (
                    <div key={node.id} className="p-3 flex items-center justify-between text-sm hover:bg-secondary/20 transition-colors">
                      <span className="font-medium font-mono text-xs">{node.label}</span>
                      <div className="flex items-center gap-2">
                         <span className="text-[10px] text-muted-foreground">
                           {data.links.filter(l => l.target === node.id).length} in / {data.links.filter(l => l.source === node.id).length} out
                         </span>
                      </div>
                    </div>
                  ))}
                </div>
              </Card>
            </TabsContent>
          </div>
        </ScrollArea>
      </Tabs>
    </div>
  );
}
