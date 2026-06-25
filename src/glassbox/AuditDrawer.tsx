import { useTraceStore } from './traceStore'
import './glassbox.css'

// Audit-grade drawer: the simulated raw Postgres row behind any node (conflict
// source or traversal hop). In RAILS MODE these rows come from the captured
// gallery fixture; live, they are the actual store rows DCL resolved over.
export function AuditDrawer() {
  const selectedNodeId = useTraceStore((s) => s.selectedNodeId)
  const nodes = useTraceStore((s) => s.nodes)
  const travNodes = useTraceStore((s) => s.travNodes)
  const selectNode = useTraceStore((s) => s.selectNode)

  const conflict = nodes.find((n) => n.id === selectedNodeId)
  const trav = travNodes.find((n) => n.id === selectedNodeId)
  const node = conflict ?? trav
  if (!node) return null

  const row: Record<string, any> | undefined = (node as any).raw_row
  const label = conflict ? conflict.source : (trav as any).label
  const dropped = !!(conflict && conflict.dropped)

  return (
    <div className="gb-drawer" role="dialog" aria-label="Raw Postgres row" data-testid="audit-drawer">
      <div className="gb-drawer__head">
        <span>raw row · <b>{label}</b></span>
        <button className="gb-drawer__close" aria-label="close" onClick={() => selectNode(null)}>✕</button>
      </div>

      {dropped && <div className="gb-drawer__dropped">Pruned before compute — {conflict!.dropReason}</div>}
      {trav?.discovered && <div className="gb-drawer__found">Discovered relationship — this hop is the link no single system held.</div>}

      {row ? (
        <>
          <dl className="gb-drawer__badges">
            {row.source_system && <div><dt>source_system</dt><dd data-testid="drawer-source">{row.source_system}</dd></div>}
            {row.updated_at && <div><dt>updated_at</dt><dd data-testid="drawer-updated">{row.updated_at}</dd></div>}
            {row.bitemporal_id && <div><dt>bitemporal_id</dt><dd data-testid="drawer-bitemporal">{row.bitemporal_id}</dd></div>}
          </dl>
          <pre className="gb-drawer__json">{JSON.stringify(row, null, 2)}</pre>
        </>
      ) : (
        <div className="gb-drawer__empty">No raw row on this node (it is the query root, not a stored record).</div>
      )}
    </div>
  )
}
