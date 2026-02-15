import { useState, useEffect, useRef } from 'react';
import { Terminal as TerminalIcon } from 'lucide-react';

interface ApiNarrationMessage {
  id?: string;
  number?: number;
  timestamp: string;
  source: string;
  message: string;
  type?: string;
}

interface Message {
  id: string;
  seq: number;
  timestamp: string;
  source: 'Engine' | 'RAG' | 'LLM' | 'Monitor' | 'Ingest';
  text: string;
  type?: 'info' | 'warn' | 'success' | 'error';
}

const getMessageColor = (type?: string, text?: string) => {
  if (text?.includes('[WARN]') || type === 'warn') {
    return 'text-yellow-400';
  }
  if (text?.includes('[SUCCESS]') || type === 'success') {
    return 'text-green-400';
  }
  if (text?.includes('[ERROR]') || type === 'error') {
    return 'text-red-400';
  }
  if (text?.includes('[INFO]')) {
    return 'text-blue-400';
  }
  return 'text-green-300/80';
};

interface NarrationPanelProps {
  runId?: string;
}

export function NarrationPanel({ runId }: NarrationPanelProps) {
  const [messages, setMessages] = useState<Message[]>([]);
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    console.log('[NarrationPanel] runId changed:', runId);
    setMessages([]);
    if (!runId) return;

    const fetchMessages = async () => {
      try {
        const response = await fetch(`/api/dcl/narration/${runId}`);
        if (!response.ok) {
          console.warn(`Narration fetch failed: ${response.status}`);
          return;
        }
        const data = await response.json();
        const apiMessages = data.messages || [];
        console.log('[NarrationPanel] Fetched', apiMessages.length, 'messages for runId', runId);
        const mappedMessages = apiMessages.map((m: ApiNarrationMessage, idx: number) => ({
          id: m.id || `msg-${idx}`,
          seq: m.number || idx + 1,
          timestamp: m.timestamp,
          source: m.source,
          text: m.message,
          type: m.type
        }));
        setMessages(mappedMessages.reverse());
      } catch (error) {
        console.error('Error fetching narration:', error);
      }
    };

    fetchMessages();
    const interval = setInterval(fetchMessages, 500);

    return () => clearInterval(interval);
  }, [runId]);

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = 0;
    }
  }, [messages]);

  return (
    <div className="h-full flex flex-col bg-black/90">
      <div className="px-3 py-2 border-b border-green-500/30 bg-black flex items-center gap-2">
        <TerminalIcon className="w-4 h-4 text-green-400" />
        <span className="font-mono text-xs text-green-400 uppercase tracking-wider">System Log</span>
        <span className="text-[10px] text-green-400/50 font-mono ml-auto">
          {messages.length} entries
        </span>
      </div>
      <div ref={scrollRef} className="flex-1 overflow-y-auto p-2 font-mono text-[11px] leading-tight">
        {messages.map((msg) => (
          <div key={msg.id} className="py-0.5 flex gap-2">
            <span className="text-green-500/50 w-20 shrink-0">
              {new Date(msg.timestamp).toLocaleTimeString('en-US', { hour12: false, hour: '2-digit', minute:'2-digit', second:'2-digit' })} PST
            </span>
            <span className={`${getMessageColor(msg.type, msg.text)} break-all`}>
              {msg.text}
            </span>
          </div>
        ))}
        
        {messages.length === 0 && (
          <div className="text-green-500/30 py-2">
            <span className="animate-pulse">_</span> Awaiting stream...
          </div>
        )}
      </div>
    </div>
  );
}
