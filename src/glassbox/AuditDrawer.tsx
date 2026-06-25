import { useTraceStore } from './traceStore'
import './glassbox.css'

// Audit-grade drawer: the simulated raw Postgres row behind a node. In RAILS
// MODE these rows come from the captured trace fixture; live, they are the
// actual store rows DCL resolved over.
export function AuditDrawer() {
  const selectedNodeId = useTraceStore((s) => s.selectedNodeId)
  const nodes = useTraceStore((s) => s.nodes)
  const selectNode = useTraceStore((s) => s.selectNode)
  const node = nodes.find((n) => n.id === selectedNodeId)
  if (!node) return null
  const row = node.raw_row

  return (
    <div className="gb-drawer" role="dialog" aria-label="Raw Postgres row" data-testid="audit-drawer">
      <div className="gb-drawer__head">
        <span>raw row · <b>{node.source}</b></span>
        <button className="gb-drawer__close" aria-label="close" onClick={() => selectNode(null)}>✕</button>
      </div>
      {node.dropped && (
        <div className="gb-drawer__dropped">Pruned before compute — {node.dropReason}</div>
      )}
      <dl className="gb-drawer__badges">
        <div><dt>source_system</dt><dd data-testid="drawer-source">{row.source_system}</dd></div>
        <div><dt>updated_at</dt><dd data-testid="drawer-updated">{row.updated_at}</dd></div>
        <div><dt>bitemporal_id</dt><dd data-testid="drawer-bitemporal">{row.bitemporal_id}</dd></div>
      </dl>
      <pre className="gb-drawer__json">{JSON.stringify(row, null, 2)}</pre>
    </div>
  )
}
