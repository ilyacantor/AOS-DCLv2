import { Panel, PanelGroup, PanelResizeHandle } from 'react-resizable-panels'
import { ChatPane } from './ChatPane'
import { StoryStage } from './StoryStage'
import { AuditDrawer } from './AuditDrawer'
import './glassbox.css'

// Glass Box — the commercial demo. Left: the question gallery. Right: a
// presenter-paced, plain-English walk-through of how the engine finds the
// answer (one step per click), with the raw record one click away.
export default function GlassBox() {
  return (
    <div className="gb-root">
      <header className="gb-topbar">
        <div className="gb-brand">AOS · <span>Glass Box</span></div>
        <div
          className="gb-replay"
          data-testid="replay-tag"
          title="This walk-through replays a captured, verified lab trace — not a live computation while contextOS is being extracted."
        >
          captured lab trace · replay
        </div>
      </header>
      <PanelGroup direction="horizontal" className="gb-panels">
        <Panel defaultSize={40} minSize={28}>
          <ChatPane />
        </Panel>
        <PanelResizeHandle className="gb-resize" />
        <Panel defaultSize={60} minSize={40}>
          <div className="gb-canvas-wrap">
            <StoryStage />
            <AuditDrawer />
          </div>
        </Panel>
      </PanelGroup>
    </div>
  )
}
