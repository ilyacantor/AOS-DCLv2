import { useState, useEffect } from 'react';
import Database from 'lucide-react/dist/esm/icons/database';
import Server from 'lucide-react/dist/esm/icons/server';
import Layers from 'lucide-react/dist/esm/icons/layers';
import AlertTriangle from 'lucide-react/dist/esm/icons/alert-triangle';
import Clock from 'lucide-react/dist/esm/icons/clock';
import ChevronDown from 'lucide-react/dist/esm/icons/chevron-down';
import ChevronRight from 'lucide-react/dist/esm/icons/chevron-right';
import Activity from 'lucide-react/dist/esm/icons/activity';
import Zap from 'lucide-react/dist/esm/icons/zap';
import type { RunMetrics } from '../types';

interface BatchSummary {
  batch_id: string;
  snapshot_name: string;
  tenant_id: string;
  run_count: number;
  total_rows: number;
  unique_sources: number;
  source_list: string[];
  first_run_id: string;
  latest_run_id: string;
  first_received_at: string;
  latest_received_at: string;
  drift_count: number;
  aam_meta?: {
    pipes?: number;
    fabrics?: string[];
    fabric_details?: string[];
    source_names?: string[];
    sors?: number;
    sor_vendors?: string[];
    [key: string]: unknown;
  };
}

interface SnapshotPanelProps {
  currentSnapshotName?: string;
  runMetrics?: RunMetrics;
  aodRunId?: string;
  onSnapshotSelect?: (snapshotName: string) => void;
}

export function SnapshotPanel({ currentSnapshotName, runMetrics, aodRunId, onSnapshotSelect }: SnapshotPanelProps) {
  const [batches, setBatches] = useState<BatchSummary[]>([]);
  const [selectedSnapshot, setSelectedSnapshot] = useState<string | null>(null);
  const [dropdownOpen, setDropdownOpen] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [expandedStats, setExpandedStats] = useState<Record<string, boolean>>({});

  useEffect(() => {
    const fetchBatches = async () => {
      try {
        const res = await fetch('/api/dcl/ingest/batches');
        if (!res.ok) {
          const errBody = await res.json().catch(() => null);
          throw new Error(
            `Failed to fetch snapshots: ${errBody?.detail || `HTTP ${res.status}`}`
          );
        }
        const data = await res.json();
        const list: BatchSummary[] = data.batches || [];
        setBatches(list);
        if (list.length > 0 && !selectedSnapshot) {
          const match = currentSnapshotName
            ? list.find((b) => b.snapshot_name === currentSnapshotName)
            : null;
          const name = match ? match.snapshot_name : list[0].snapshot_name;
          setSelectedSnapshot(name);
          onSnapshotSelect?.(name);
        }
      } catch (err) {
        const msg = err instanceof Error ? err.message : 'Unknown error fetching snapshots';
        console.error('[SnapshotPanel]', msg);
        setError(msg);
      }
    };
    fetchBatches();
  }, [currentSnapshotName]);

  const handleSelectSnapshot = (name: string) => {
    setSelectedSnapshot(name);
    setDropdownOpen(false);
    onSnapshotSelect?.(name);
  };

  const toggleStat = (key: string) => {
    setExpandedStats((prev) => ({ ...prev, [key]: !prev[key] }));
  };

  // Aggregate all batches for the selected snapshot
  const snapshotBatches = batches.filter((b) => b.snapshot_name === selectedSnapshot);
  const profile = snapshotBatches.length > 0 ? aggregateProfile(snapshotBatches) : null;

  const uniqueSnapshots = [...new Set(batches.map((b) => b.snapshot_name))];

  // Fabric vendor names (Workato, Snowflake, etc. parsed from "iPaaS:Workato")
  const fabricVendors = profile?.aamMeta?.fabric_details
    ? profile.aamMeta.fabric_details.map((fd) => {
        const colonIdx = fd.indexOf(':');
        return colonIdx >= 0 ? fd.substring(colonIdx + 1) : fd;
      })
    : [];
  const fabricCount = fabricVendors.length || (profile?.aamMeta?.fabrics?.length ?? 0);

  // Pipe vendor names (the source_names from aam_meta)
  const pipeVendorNames = profile?.aamMeta?.source_names ?? [];

  // SOR vendor names (salesforce, sap, etc.)
  const sorVendors = profile?.aamMeta?.sor_vendors ?? [];
  const sorCount = profile?.aamMeta?.sors ?? sorVendors.length;

  return (
    <div className="h-full flex flex-col">
      {/* Snapshot Selector */}
      <div className="px-3 py-2 border-b shrink-0">
        <div className="relative">
          <button
            onClick={() => setDropdownOpen(!dropdownOpen)}
            className="w-full flex items-center justify-between gap-2 px-3 py-1.5 rounded-md border bg-background text-sm hover:bg-accent transition-colors"
          >
            <div className="flex items-center gap-2 min-w-0">
              <Database className="w-3.5 h-3.5 text-muted-foreground shrink-0" />
              <span className="truncate font-medium">
                {selectedSnapshot || 'No snapshots'}
              </span>
            </div>
            <ChevronDown className={`w-3.5 h-3.5 text-muted-foreground transition-transform ${dropdownOpen ? 'rotate-180' : ''}`} />
          </button>
          {dropdownOpen && uniqueSnapshots.length > 0 && (
            <div className="absolute z-20 mt-1 w-full rounded-md border bg-popover shadow-lg max-h-48 overflow-y-auto">
              {uniqueSnapshots.map((name) => (
                <button
                  key={name}
                  onClick={() => handleSelectSnapshot(name)}
                  className={`w-full text-left px-3 py-1.5 text-sm hover:bg-accent transition-colors ${
                    name === selectedSnapshot ? 'bg-accent font-medium' : ''
                  }`}
                >
                  {name}
                </button>
              ))}
            </div>
          )}
        </div>
      </div>

      {/* Snapshot Profile */}
      <div className="flex-1 overflow-y-auto p-3 space-y-3">
        {error && (
          <div className="p-3 rounded-lg border border-destructive/30 bg-destructive/5">
            <p className="text-xs text-destructive">{error}</p>
          </div>
        )}

        {!profile && !error && (
          <div className="flex items-center justify-center h-32 text-muted-foreground text-sm">
            No snapshot data available
          </div>
        )}

        {profile && (
          <>
            {/* Identity */}
            <ProfileSection title="Identity">
              <ProfileRow label="Snapshot" value={profile.snapshotName} />
              <ProfileRow
                label={profile.tenantId === 'aam' ? 'Source' : 'Tenant'}
                value={profile.tenantId}
              />
              {aodRunId && (
                <ProfileRow label="AOD ID" value={aodRunId} />
              )}
            </ProfileSection>

            {/* Scale — drillable */}
            <ProfileSection title="Scale">
              <div className="grid grid-cols-2 gap-2">
                <DrillableStatCard
                  icon={<Layers className="w-3.5 h-3.5" />}
                  label="Fabrics"
                  value={fabricCount || profile.uniqueSources}
                  expanded={!!expandedStats['fabrics']}
                  onToggle={() => toggleStat('fabrics')}
                >
                  <div className="flex flex-wrap gap-1 mt-1.5">
                    {fabricVendors.map((vendor) => (
                      <span key={vendor} className="px-1.5 py-0.5 text-[10px] rounded bg-blue-500/10 text-blue-400 border border-blue-500/20">
                        {vendor}
                      </span>
                    ))}
                  </div>
                </DrillableStatCard>

                <DrillableStatCard
                  icon={<Server className="w-3.5 h-3.5" />}
                  label="Pipes"
                  value={profile.aamMeta?.pipes ?? profile.runCount}
                  expanded={!!expandedStats['pipes']}
                  onToggle={() => toggleStat('pipes')}
                >
                  <div className="flex flex-wrap gap-1 mt-1.5">
                    {pipeVendorNames.length > 0
                      ? pipeVendorNames.map((name) => (
                          <span key={name} className="px-1.5 py-0.5 text-[10px] rounded bg-muted text-muted-foreground">
                            {name}
                          </span>
                        ))
                      : (
                          <span className="text-[10px] text-muted-foreground">
                            {profile.runCount} batch{profile.runCount !== 1 ? 'es' : ''} ingested
                          </span>
                        )
                    }
                  </div>
                </DrillableStatCard>

                {sorCount > 0 && (
                  <DrillableStatCard
                    icon={<Database className="w-3.5 h-3.5" />}
                    label="SORs"
                    value={sorCount}
                    expanded={!!expandedStats['sors']}
                    onToggle={() => toggleStat('sors')}
                  >
                    <div className="flex flex-wrap gap-1 mt-1.5">
                      {sorVendors.map((vendor) => (
                        <span key={vendor} className="px-1.5 py-0.5 text-[10px] rounded bg-green-500/10 text-green-400 border border-green-500/20">
                          {vendor}
                        </span>
                      ))}
                    </div>
                  </DrillableStatCard>
                )}

                {profile.totalRows > 0 && (
                  <DrillableStatCard
                    icon={<Database className="w-3.5 h-3.5" />}
                    label="Rows"
                    value={formatNumber(profile.totalRows)}
                    expanded={!!expandedStats['rows']}
                    onToggle={() => toggleStat('rows')}
                  >
                    <div className="text-[10px] text-muted-foreground mt-1.5">
                      {profile.totalRows.toLocaleString()} total rows
                    </div>
                  </DrillableStatCard>
                )}

                {profile.driftCount > 0 && (
                  <StatCard
                    icon={<AlertTriangle className="w-3.5 h-3.5 text-yellow-500" />}
                    label="Drift"
                    value={profile.driftCount}
                    warn
                  />
                )}
              </div>
            </ProfileSection>

            {/* Run Metrics */}
            {runMetrics && (
              <ProfileSection title="Run Metrics">
                <div className="grid grid-cols-2 gap-2">
                  <MiniStat icon={<Zap className="w-3 h-3" />} label="LLM Calls" value={runMetrics.llmCalls} />
                  <MiniStat icon={<Database className="w-3 h-3" />} label="RAG Reads" value={runMetrics.ragReads} />
                  <MiniStat icon={<Database className="w-3 h-3" />} label="RAG Writes" value={runMetrics.ragWrites} />
                  <MiniStat icon={<Activity className="w-3 h-3" />} label="Mappings" value={runMetrics.totalMappings} />
                </div>
                <div className="flex items-center gap-2 text-xs mt-1">
                  <Clock className="w-3 h-3 text-muted-foreground" />
                  <span className="text-muted-foreground">Processing:</span>
                  <span className="font-mono">{(runMetrics.processingMs / 1000).toFixed(1)}s</span>
                </div>
              </ProfileSection>
            )}

            {/* Timeline */}
            <ProfileSection title="Timeline">
              <div className="space-y-1.5">
                <div className="flex items-center gap-2 text-xs">
                  <Clock className="w-3 h-3 text-muted-foreground" />
                  <span className="text-muted-foreground">First:</span>
                  <span className="font-mono">{formatTimestamp(profile.firstReceivedAt)}</span>
                </div>
                <div className="flex items-center gap-2 text-xs">
                  <Clock className="w-3 h-3 text-muted-foreground" />
                  <span className="text-muted-foreground">Latest:</span>
                  <span className="font-mono">{formatTimestamp(profile.latestReceivedAt)}</span>
                </div>
              </div>
            </ProfileSection>
          </>
        )}
      </div>
    </div>
  );
}

/* ── helpers ── */

interface SnapshotProfile {
  snapshotName: string;
  tenantId: string;
  runCount: number;
  totalRows: number;
  uniqueSources: number;
  sourceList: string[];
  firstReceivedAt: string;
  latestReceivedAt: string;
  driftCount: number;
  aamMeta: BatchSummary['aam_meta'] | null;
}

function aggregateProfile(batches: BatchSummary[]): SnapshotProfile {
  const allSources = new Set<string>();
  batches.forEach((b) => b.source_list.forEach((s) => allSources.add(s)));

  const sorted = [...batches].sort(
    (a, b) => new Date(a.first_received_at).getTime() - new Date(b.first_received_at).getTime()
  );

  const aamBatch = batches.find((b) => b.aam_meta);

  return {
    snapshotName: batches[0].snapshot_name,
    tenantId: batches[0].tenant_id,
    runCount: batches.reduce((sum, b) => sum + b.run_count, 0),
    totalRows: batches.reduce((sum, b) => sum + b.total_rows, 0),
    uniqueSources: allSources.size,
    sourceList: [...allSources].sort(),
    firstReceivedAt: sorted[0].first_received_at,
    latestReceivedAt: sorted[sorted.length - 1].latest_received_at,
    driftCount: batches.reduce((sum, b) => sum + b.drift_count, 0),
    aamMeta: aamBatch?.aam_meta ?? null,
  };
}

function formatNumber(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return String(n);
}

function formatTimestamp(iso: string): string {
  try {
    const d = new Date(iso);
    return d.toLocaleString('en-US', {
      month: 'short', day: 'numeric',
      hour: '2-digit', minute: '2-digit', second: '2-digit',
      hour12: false,
    });
  } catch {
    return iso;
  }
}

/* ── sub-components ── */

function ProfileSection({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="space-y-1.5">
      <h4 className="text-[10px] uppercase tracking-wider text-muted-foreground font-semibold">{title}</h4>
      {children}
    </div>
  );
}

function ProfileRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-center justify-between text-xs">
      <span className="text-muted-foreground">{label}</span>
      <span className="font-medium font-mono">{value}</span>
    </div>
  );
}

function StatCard({
  icon, label, value, warn,
}: { icon: React.ReactNode; label: string; value: string | number; warn?: boolean }) {
  return (
    <div className={`flex items-center gap-2 px-2.5 py-2 rounded-lg border ${
      warn ? 'border-yellow-500/30 bg-yellow-500/5' : 'bg-card/50'
    }`}>
      <div className="text-muted-foreground">{icon}</div>
      <div>
        <div className={`text-sm font-semibold ${warn ? 'text-yellow-500' : ''}`}>{value}</div>
        <div className="text-[10px] text-muted-foreground">{label}</div>
      </div>
    </div>
  );
}

function DrillableStatCard({
  icon, label, value, warn, expanded, onToggle, children,
}: {
  icon: React.ReactNode;
  label: string;
  value: string | number;
  warn?: boolean;
  expanded: boolean;
  onToggle: () => void;
  children: React.ReactNode;
}) {
  return (
    <div
      className={`px-2.5 py-2 rounded-lg border cursor-pointer transition-colors hover:bg-accent/50 ${
        warn ? 'border-yellow-500/30 bg-yellow-500/5' : 'bg-card/50'
      } ${expanded ? 'col-span-2' : ''}`}
      onClick={onToggle}
    >
      <div className="flex items-center gap-2">
        <div className="text-muted-foreground">{icon}</div>
        <div className="flex-1">
          <div className={`text-sm font-semibold ${warn ? 'text-yellow-500' : ''}`}>{value}</div>
          <div className="text-[10px] text-muted-foreground">{label}</div>
        </div>
        <ChevronRight className={`w-3 h-3 text-muted-foreground transition-transform ${expanded ? 'rotate-90' : ''}`} />
      </div>
      {expanded && children}
    </div>
  );
}

function MiniStat({
  icon, label, value, warn,
}: { icon?: React.ReactNode; label: string; value: number; warn?: boolean }) {
  return (
    <div className={`flex items-center gap-1.5 px-2 py-1.5 rounded border text-xs ${
      warn ? 'border-yellow-500/30 bg-yellow-500/5' : 'bg-card/50'
    }`}>
      {icon && <div className="text-muted-foreground">{icon}</div>}
      <span className={`font-semibold ${warn ? 'text-yellow-500' : ''}`}>{value}</span>
      <span className="text-muted-foreground">{label}</span>
    </div>
  );
}
