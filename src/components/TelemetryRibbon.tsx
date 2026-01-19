import { useState, useEffect } from 'react';
import { Activity, Shield, Ban, Wrench, CheckCircle, Gauge } from 'lucide-react';

interface TelemetryMetrics {
  total_processed: number;
  toxic_blocked: number;
  drift_detected: number;
  repaired_success: number;
  repair_failed: number;
  verified_count: number;
  verified_failed: number;
  tps: number;
  quality_score: number;
  repair_rate: number;
  uptime_seconds: number;
}

interface TelemetryData {
  ts: number;
  metrics: TelemetryMetrics;
}

export function TelemetryRibbon() {
  const [telemetry, setTelemetry] = useState<TelemetryData | null>(null);
  const [isActive, setIsActive] = useState(false);

  useEffect(() => {
    const fetchTelemetry = async () => {
      try {
        const response = await fetch('/api/ingest/telemetry');
        const data = await response.json();
        setTelemetry(data);
        setIsActive(data.ts > 0 && (Date.now() - data.ts) < 5000);
      } catch (error) {
        setIsActive(false);
      }
    };

    fetchTelemetry();
    const interval = setInterval(fetchTelemetry, 500);
    return () => clearInterval(interval);
  }, []);

  const metrics = telemetry?.metrics;

  return (
    <div className="bg-black/80 border-b border-green-500/30 px-4 py-2">
      <div className="flex items-center justify-between gap-4">
        <div className="flex items-center gap-6">
          <div className="flex items-center gap-2">
            <div className={`w-2 h-2 rounded-full ${isActive ? 'bg-green-500 animate-pulse' : 'bg-gray-500'}`} />
            <span className="text-xs font-mono text-green-400 uppercase tracking-wider">
              Industrial Mode
            </span>
          </div>

          <div className="flex items-center gap-2 bg-green-500/10 px-3 py-1 rounded border border-green-500/30">
            <Gauge className="w-4 h-4 text-green-400" />
            <span className="text-lg font-mono font-bold text-green-400">
              {metrics?.tps?.toFixed(0) || 0}
            </span>
            <span className="text-xs text-green-400/70">rec/sec</span>
          </div>
        </div>

        <div className="flex items-center gap-4">
          <div className="flex items-center gap-1.5 px-2 py-1 bg-blue-500/10 rounded border border-blue-500/30">
            <Activity className="w-3.5 h-3.5 text-blue-400" />
            <span className="text-sm font-mono text-blue-400">
              {(metrics?.total_processed || 0).toLocaleString()}
            </span>
            <span className="text-[10px] text-blue-400/70 uppercase">Processed</span>
          </div>

          <div className="flex items-center gap-1.5 px-2 py-1 bg-red-500/10 rounded border border-red-500/30">
            <Ban className="w-3.5 h-3.5 text-red-400" />
            <span className="text-sm font-mono text-red-400">
              {(metrics?.toxic_blocked || 0).toLocaleString()}
            </span>
            <span className="text-[10px] text-red-400/70 uppercase">Blocked</span>
          </div>

          <div className="flex items-center gap-1.5 px-2 py-1 bg-yellow-500/10 rounded border border-yellow-500/30">
            <Wrench className="w-3.5 h-3.5 text-yellow-400" />
            <span className="text-sm font-mono text-yellow-400">
              {(metrics?.repaired_success || 0).toLocaleString()}
            </span>
            <span className="text-[10px] text-yellow-400/70 uppercase">Healed</span>
          </div>

          <div className="flex items-center gap-1.5 px-2 py-1 bg-emerald-500/10 rounded border border-emerald-500/30">
            <CheckCircle className="w-3.5 h-3.5 text-emerald-400" />
            <span className="text-sm font-mono text-emerald-400">
              {(metrics?.verified_count || 0).toLocaleString()}
            </span>
            <span className="text-[10px] text-emerald-400/70 uppercase">Verified</span>
          </div>

          <div className="flex items-center gap-1.5 px-2 py-1 bg-green-500/20 rounded border border-green-500/50">
            <Shield className="w-3.5 h-3.5 text-green-400" />
            <span className="text-sm font-mono font-bold text-green-400">
              {metrics?.quality_score?.toFixed(0) || 100}%
            </span>
            <span className="text-[10px] text-green-400/70 uppercase">Quality</span>
          </div>
        </div>
      </div>
    </div>
  );
}
