import { useEffect, useState } from 'react';

export interface EntityInfo {
  entity_id: string;
  display_name: string;
  triple_count: number;
  latest_ingest: string;
  is_most_recent: boolean;
}

interface EntitySelectorProps {
  entities: EntityInfo[];
  selectedEntityId: string;
  onEntityChange: (id: string) => void;
  loading?: boolean;
  error?: string | null;
}

export function EntitySelector({ entities, selectedEntityId, onEntityChange, loading, error }: EntitySelectorProps) {
  if (error) {
    return (
      <div className="flex items-center gap-2 px-3 py-1.5 rounded border border-destructive/40 bg-destructive/5 text-xs text-destructive">
        Entities unavailable: {error}
      </div>
    );
  }

  return (
    <div className="flex items-center gap-1.5">
      <span className="text-xs text-muted-foreground">Entity:</span>
      <select
        value={selectedEntityId}
        onChange={(e) => onEntityChange(e.target.value)}
        disabled={loading || entities.length === 0}
        className="px-2.5 py-1 text-sm font-bold rounded border border-primary/30 bg-primary/5 text-primary disabled:opacity-50"
      >
        <option value="">All Entities</option>
        {entities.map((e) => (
          <option key={e.entity_id} value={e.entity_id}>
            {e.display_name}{e.is_most_recent ? ' *' : ''}
          </option>
        ))}
      </select>
    </div>
  );
}

/** Hook to fetch entities from GET /api/dcl/entities. Called once at app level. */
export function useEntities() {
  const [entities, setEntities] = useState<EntityInfo[]>([]);
  const [selectedEntityId, setSelectedEntityId] = useState('');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchEntities = async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch('/api/dcl/entities');
      if (!res.ok) throw new Error(`Entities endpoint returned HTTP ${res.status}`);
      const data = await res.json();
      const list: EntityInfo[] = data.entities || [];
      setEntities(list);
      // Auto-select the most recent entity
      const mostRecent = list.find((e) => e.is_most_recent);
      if (mostRecent) {
        setSelectedEntityId((prev) => prev || mostRecent.entity_id);
      }
    } catch (e) {
      const msg = e instanceof Error ? e.message : 'Failed to load entities';
      setError(msg);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { fetchEntities(); }, []);

  return { entities, selectedEntityId, setSelectedEntityId, loading, error };
}
