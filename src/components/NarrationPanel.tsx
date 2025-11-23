import { useState, useEffect } from 'react';
import { Badge } from './Badge';
import { Bot, Database, Activity, Terminal } from 'lucide-react';

interface Message {
  id: string;
  seq: number;
  timestamp: string;
  source: 'Engine' | 'RAG' | 'LLM' | 'Monitor';
  text: string;
}

interface NarrationPanelProps {
  runId?: string;
}

export function NarrationPanel({ runId }: NarrationPanelProps) {
  const [messages, setMessages] = useState<Message[]>([]);

  useEffect(() => {
    setMessages([]);
    if (!runId) return;

    const fetchMessages = async () => {
      try {
        const response = await fetch(`/api/dcl/narration/${runId}`);
        const data = await response.json();
        const apiMessages = data.messages || [];
        setMessages(apiMessages.map((m: any, idx: number) => ({
          id: m.id || `msg-${idx}`,
          seq: m.number || idx + 1,
          timestamp: m.timestamp,
          source: m.source,
          text: m.message
        })));
      } catch (error) {
        console.error('Error fetching narration:', error);
      }
    };

    fetchMessages();
    const interval = setInterval(fetchMessages, 2000);

    return () => clearInterval(interval);
  }, [runId]);

  return (
    <div className="h-full flex flex-col bg-sidebar/30">
      <div className="p-4 border-b bg-card/50">
        <h3 className="font-semibold text-sm">System Narration</h3>
        <p className="text-xs text-muted-foreground">Live execution log</p>
      </div>
      <div className="flex-1 overflow-y-auto p-4">
        <div className="space-y-6 relative pl-4 border-l border-border/50 ml-2">
          {messages.map((msg) => (
            <div key={msg.id} className="relative group">
              <div className="absolute -left-[21px] top-1 w-2.5 h-2.5 rounded-full bg-border ring-4 ring-background" />
              
              <div className="flex flex-col gap-1">
                <div className="flex items-center gap-2">
                  <span className="font-mono text-[10px] text-muted-foreground">#{msg.seq}</span>
                  <span className="font-mono text-[10px] text-muted-foreground opacity-50">
                    {new Date(msg.timestamp).toLocaleTimeString([], { hour12: false, hour: '2-digit', minute:'2-digit', second:'2-digit' })}
                  </span>
                  <Badge variant="outline" className="h-4 text-[10px] px-1 gap-1 font-normal bg-background/50 flex items-center">
                    {msg.source === 'LLM' && <Bot className="w-3 h-3" />}
                    {msg.source === 'RAG' && <Database className="w-3 h-3" />}
                    {msg.source === 'Engine' && <Terminal className="w-3 h-3" />}
                    {msg.source === 'Monitor' && <Activity className="w-3 h-3" />}
                    <span>{msg.source}</span>
                  </Badge>
                </div>
                <p className="text-sm text-foreground/90 leading-snug">
                  {msg.text}
                </p>
              </div>
            </div>
          ))}
          
          {messages.length === 0 && (
            <div className="text-xs text-muted-foreground italic pt-4">
              Waiting for execution...
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
