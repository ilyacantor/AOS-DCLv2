import React, { useState } from 'react';
import './ControlPanel.css';

interface ControlPanelProps {
  onRun: (mode: string, runMode: string, personas: string[]) => void;
  isLoading: boolean;
}

const ControlPanel: React.FC<ControlPanelProps> = ({ onRun, isLoading }) => {
  const [mode, setMode] = useState<'Demo' | 'Farm'>('Demo');
  const [runMode, setRunMode] = useState<'Dev' | 'Prod'>('Dev');
  const [personas, setPersonas] = useState<string[]>(['CFO', 'CRO', 'COO', 'CTO']);

  const togglePersona = (persona: string) => {
    setPersonas(prev =>
      prev.includes(persona) ? prev.filter(p => p !== persona) : [...prev, persona]
    );
  };

  const handleRun = () => {
    onRun(mode, runMode, personas);
  };

  return (
    <div className="control-panel">
      <div className="control-group">
        <label>Data Mode:</label>
        <div className="toggle-group">
          <button
            className={`toggle-btn ${mode === 'Demo' ? 'active' : ''}`}
            onClick={() => setMode('Demo')}
          >
            Demo
          </button>
          <button
            className={`toggle-btn ${mode === 'Farm' ? 'active' : ''}`}
            onClick={() => setMode('Farm')}
          >
            Farm
          </button>
        </div>
      </div>

      <div className="control-group">
        <label>Run Mode:</label>
        <div className="toggle-group">
          <button
            className={`toggle-btn ${runMode === 'Dev' ? 'active' : ''}`}
            onClick={() => setRunMode('Dev')}
          >
            Dev
          </button>
          <button
            className={`toggle-btn ${runMode === 'Prod' ? 'active' : ''}`}
            onClick={() => setRunMode('Prod')}
          >
            Prod
          </button>
        </div>
      </div>

      <div className="control-group">
        <label>Personas:</label>
        <div className="checkbox-group">
          {['CFO', 'CRO', 'COO', 'CTO'].map(persona => (
            <label key={persona} className="checkbox-label">
              <input
                type="checkbox"
                checked={personas.includes(persona)}
                onChange={() => togglePersona(persona)}
              />
              <span>{persona}</span>
            </label>
          ))}
        </div>
      </div>

      <button className="run-btn" onClick={handleRun} disabled={isLoading}>
        {isLoading ? 'Running...' : 'Run'}
      </button>
    </div>
  );
};

export default ControlPanel;
