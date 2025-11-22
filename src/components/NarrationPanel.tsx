import React, { useEffect, useState } from 'react';
import './NarrationPanel.css';

interface Message {
  number: number;
  timestamp: string;
  source: string;
  message: string;
}

interface NarrationPanelProps {
  runId: string;
}

const NarrationPanel: React.FC<NarrationPanelProps> = ({ runId }) => {
  const [messages, setMessages] = useState<Message[]>([]);

  useEffect(() => {
    if (!runId) return;

    const fetchMessages = async () => {
      try {
        const response = await fetch(`/api/dcl/narration/${runId}`);
        const data = await response.json();
        setMessages(data.messages || []);
      } catch (error) {
        console.error('Error fetching narration:', error);
      }
    };

    fetchMessages();
    const interval = setInterval(fetchMessages, 2000);

    return () => clearInterval(interval);
  }, [runId]);

  return (
    <div className="narration-panel">
      <h3>Narration</h3>
      <div className="messages">
        {messages.length === 0 ? (
          <p className="no-messages">Run the engine to see narration messages</p>
        ) : (
          messages.slice().reverse().map((msg) => (
            <div key={msg.number} className="message">
              <div className="message-header">
                <span className="message-number">#{msg.number}</span>
                <span className="message-source">{msg.source}</span>
                <span className="message-time">
                  {new Date(msg.timestamp).toLocaleTimeString()}
                </span>
              </div>
              <div className="message-body">{msg.message}</div>
            </div>
          ))
        )}
      </div>
    </div>
  );
};

export default NarrationPanel;
