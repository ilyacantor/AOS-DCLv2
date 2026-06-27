import { useTraceStore } from './traceStore'
import './glassbox.css'

// Audit-grade drawer: the raw record behind a beat. In RAILS MODE these come
// from the captured story fixture; live, they are the actual rows DCL resolved.
export function AuditDrawer() {
  const selectedBeatId = useTraceStore((s) => s.selectedBeatId)
  const beats = useTraceStore((s) => s.beats)
  const selectBeat = useTraceStore((s) => s.selectBeat)

  const beat = beats.find((b) => b.id === selectedBeatId)
  if (!beat) return null
  const row = beat.record

  return (
    <div className="gb-drawer" role="dialog" aria-label="Raw record" data-testid="audit-drawer">
      <div className="gb-drawer__head">
        <span>raw record</span>
        <button className="gb-drawer__close" aria-label="close" onClick={() => selectBeat(null)}>✕</button>
      </div>

      {beat.link && <div className="gb-drawer__found">This is the link no single system held.</div>}

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
        <div className="gb-drawer__empty">No raw record on this step.</div>
      )}
    </div>
  )
}
