import React, { useEffect, useState } from 'react';
import './MonitorPanel.css';

interface MonitorPanelProps {
  runId: string;
}

const MonitorPanel: React.FC<MonitorPanelProps> = ({ runId }) => {
  const [monitorData, setMonitorData] = useState<any>(null);

  useEffect(() => {
    if (!runId) return;

    const fetchMonitorData = async () => {
      try {
        const response = await fetch(`/api/dcl/monitor/${runId}`);
        const data = await response.json();
        setMonitorData(data.monitor_data);
      } catch (error) {
        console.error('Error fetching monitor data:', error);
      }
    };

    fetchMonitorData();
  }, [runId]);

  return (
    <div className="monitor-panel">
      <h2>Enterprise Monitor</h2>
      {!monitorData ? (
        <p className="placeholder-text">Run the engine to see monitor data</p>
      ) : (
        <div className="monitor-content">
          <div className="monitor-section">
            <h3>Sources</h3>
            <p>Data sources loaded and processing stats</p>
          </div>
          <div className="monitor-section">
            <h3>Ontology</h3>
            <p>Unified ontology mappings and conflicts</p>
          </div>
          <div className="monitor-section">
            <h3>Status</h3>
            <p className="status-ok">{monitorData.message}</p>
          </div>
        </div>
      )}
    </div>
  );
};

export default MonitorPanel;
