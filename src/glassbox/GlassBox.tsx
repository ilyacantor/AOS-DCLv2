import { Panel, PanelGroup, PanelResizeHandle } from 'react-resizable-panels'
import { ReactFlowProvider } from '@xyflow/react'
import { ChatPane } from './ChatPane'
import { XRayCanvas } from './XRayCanvas'
import { AuditDrawer } from './AuditDrawer'
import './glassbox.css'

// Glass Box — the commercial "MRI machine" demo. Left: query + deterministic
// answer. Right: a live X-ray of the engine resolving a lateral source
// conflict (workday_main vs shadow_crm) before any LLM sees the data.
export default function GlassBox() {
  return (
    <div className="gb-root">
      <header className="gb-topbar">
        <div className="gb-brand">AOS · <span>Glass Box</span></div>
        <div
          className="gb-replay"
          data-testid="replay-tag"
          title="This canvas replays a captured, verified lab trace — it is not a live computation while contextOS is being extracted."
        >
          captured lab trace · replay
        </div>
      </header>
      <PanelGroup direction="horizontal" className="gb-panels">
        <Panel defaultSize={38} minSize={26}>
          <ChatPane />
        </Panel>
        <PanelResizeHandle className="gb-resize" />
        <Panel defaultSize={62} minSize={40}>
          <div className="gb-canvas-wrap">
            <ReactFlowProvider>
              <XRayCanvas />
            </ReactFlowProvider>
            <AuditDrawer />
          </div>
        </Panel>
      </PanelGroup>
    </div>
  )
}
