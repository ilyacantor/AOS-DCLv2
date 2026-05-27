import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

/**
 * Snapshot-grained selector with follow-latest / pin semantics.
 *
 * Model (identical to NLQ Ask and NLQ Dashboard):
 *  - `*` = the latest snapshot = the one with the max run_timestamp in the
 *    list. Computed client-side. The `is_current` field is NOT used to find
 *    it — that field tracks tenant_runs.current_run_id, not the newest run.
 *  - Default mode is FOLLOW-LATEST: the surface shows `*`. The list is
 *    polled every ~12s; when a newer snapshot appears, `*` advances and the
 *    surface auto-updates.
 *  - Manually selecting a non-`*` snapshot PINS to that snapshot (no more
 *    auto-advance). Selecting the `*` snapshot again clears the pin and
 *    re-engages follow-latest.
 *  - The `*` snapshot is marked with a literal `*` in the dropdown.
 */

const POLL_INTERVAL_MS = 12_000;

export interface Snapshot {
  dcl_ingest_id: string;
  snapshot_name: string | null;
  entity_id: string | null;
  run_timestamp: string;
  total_rows: number;
  is_current: boolean;
}

/** Snapshot state shared by all 5 monitoring tabs. Built once at app level. */
export interface SnapshotState {
  snapshots: Snapshot[];
  /** dcl_ingest_id of the latest snapshot (max run_timestamp), or null. */
  latestId: string | null;
  /** The snapshot currently driving the tabs (pinned, or `*` when following). */
  selectedSnapshot: Snapshot | null;
  /** entity_id of the selected snapshot — this is what the 5 tabs consume. */
  selectedEntityId: string;
  /** True when following `*`; false when pinned to an explicit snapshot. */
  isFollowingLatest: boolean;
  /** Operator picked a snapshot from the dropdown. */
  onSelect: (dclIngestId: string) => void;
  loading: boolean;
  error: string | null;
}

/** Latest = max run_timestamp. ISO 8601 strings sort lexically. */
function findLatest(snapshots: Snapshot[]): Snapshot | null {
  if (snapshots.length === 0) return null;
  return snapshots.reduce((latest, s) =>
    (s.run_timestamp || '') > (latest.run_timestamp || '') ? s : latest,
  );
}

function formatRelativeTime(timestamp: string): string {
  const date = new Date(timestamp);
  if (isNaN(date.getTime())) return timestamp;
  const diffMs = Date.now() - date.getTime();
  const diffMin = Math.floor(diffMs / 60000);
  if (diffMin < 1) return 'just now';
  if (diffMin < 60) return `${diffMin}m ago`;
  const diffHrs = Math.floor(diffMin / 60);
  if (diffHrs < 24) return `${diffHrs}h ago`;
  const diffDays = Math.floor(diffHrs / 24);
  return `${diffDays}d ago`;
}

/**
 * App-level hook. Fetches GET /api/dcl/snapshots, polls every ~12s, and
 * implements follow-latest / pin. Returns SnapshotState for the tabs.
 */
export function useSnapshots(): SnapshotState {
  const [snapshots, setSnapshots] = useState<Snapshot[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  // The dcl_ingest_id the operator pinned. null = follow-latest.
  const [pinnedId, setPinnedId] = useState<string | null>(null);

  const fetchSnapshots = useCallback(async (isInitial: boolean) => {
    if (isInitial) setLoading(true);
    try {
      const res = await fetch('/api/dcl/snapshots');
      if (!res.ok) {
        let detail = `Snapshots endpoint returned HTTP ${res.status}`;
        try {
          const body = await res.json();
          if (body.detail) detail = body.detail;
        } catch { /* non-JSON response */ }
        throw new Error(detail);
      }
      const data = await res.json();
      const list: Snapshot[] = data.snapshots || [];
      setSnapshots(list);
      setError(null);
    } catch (e) {
      const msg = e instanceof Error ? e.message : 'Failed to load snapshots';
      // On a poll failure keep the last good list; only surface on initial load.
      if (isInitial) setError(msg);
    } finally {
      if (isInitial) setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchSnapshots(true);
    const timer = setInterval(() => fetchSnapshots(false), POLL_INTERVAL_MS);
    return () => clearInterval(timer);
  }, [fetchSnapshots]);

  const latest = useMemo(() => findLatest(snapshots), [snapshots]);
  const latestId = latest ? latest.dcl_ingest_id : null;

  // Resolve the pinned id against the current list. If the pinned snapshot
  // is no longer present, fall back to follow-latest rather than dead state.
  const pinnedSnapshot = useMemo(
    () => (pinnedId ? snapshots.find((s) => s.dcl_ingest_id === pinnedId) ?? null : null),
    [pinnedId, snapshots],
  );
  const isFollowingLatest = pinnedSnapshot === null;
  const selectedSnapshot = isFollowingLatest ? latest : pinnedSnapshot;

  const onSelect = useCallback(
    (dclIngestId: string) => {
      // Selecting the latest snapshot clears the pin → re-engage follow-latest.
      setPinnedId((prev) => {
        const next = dclIngestId === latestId ? null : dclIngestId;
        return next === prev ? prev : next;
      });
    },
    [latestId],
  );

  return {
    snapshots,
    latestId,
    selectedSnapshot,
    selectedEntityId: selectedSnapshot?.entity_id ?? '',
    isFollowingLatest,
    onSelect,
    loading,
    error,
  };
}

/**
 * Snapshot dropdown. Renders snapshot_name + relative time + total_rows,
 * marks the latest snapshot with `*`. Pure presentational — all state
 * lives in the SnapshotState passed from App.
 */
export function SnapshotSelector({ snapshot }: { snapshot: SnapshotState }) {
  const { snapshots, latestId, selectedSnapshot, isFollowingLatest, onSelect, loading, error } = snapshot;
  // Re-render relative timestamps on the poll cadence so "5m ago" stays fresh.
  const [, forceTick] = useState(0);
  const tickRef = useRef<ReturnType<typeof setInterval> | null>(null);
  useEffect(() => {
    tickRef.current = setInterval(() => forceTick((n) => n + 1), POLL_INTERVAL_MS);
    return () => { if (tickRef.current) clearInterval(tickRef.current); };
  }, []);

  if (error) {
    return (
      <div className="flex items-center gap-2 px-3 py-1.5 rounded border border-destructive/40 bg-destructive/5 text-xs text-destructive">
        Snapshots unavailable: {error}
      </div>
    );
  }

  return (
    <div className="flex items-center gap-1.5">
      <span className="text-xs text-muted-foreground">Snapshot:</span>
      <select
        id="snapshot-selector"
        value={selectedSnapshot?.dcl_ingest_id ?? ''}
        onChange={(e) => onSelect(e.target.value)}
        disabled={loading || snapshots.length === 0}
        className="px-2.5 py-1 text-sm font-bold rounded border border-primary/30 bg-primary/5 text-primary disabled:opacity-50"
      >
        {snapshots.length === 0 && <option value="">No snapshots</option>}
        {snapshots.map((s) => {
          const isLatest = s.dcl_ingest_id === latestId;
          const name = s.snapshot_name || s.dcl_ingest_id.slice(0, 12);
          return (
            <option key={s.dcl_ingest_id} value={s.dcl_ingest_id}>
              {isLatest ? '* ' : ''}{name} — {formatRelativeTime(s.run_timestamp)} — {s.total_rows.toLocaleString()} triples
            </option>
          );
        })}
      </select>
      <span
        className="text-xs text-muted-foreground"
        data-role="snapshot-follow-state"
        title={isFollowingLatest
          ? 'Following the latest snapshot — auto-advances when a newer one is ingested'
          : 'Pinned to a specific snapshot — will not auto-advance'}
      >
        {isFollowingLatest ? 'following latest' : 'pinned'}
      </span>
    </div>
  );
}
