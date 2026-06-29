import '@xyflow/react/dist/style.css'
import { ReactFlowProvider } from '@xyflow/react'
import { Activity } from 'lucide-react'
import { Canvas } from './Canvas'
import { ChatPane } from './ChatPane'
import { GalleryTraceSource } from './trace/galleryTrace'
import { useExecutionTrace } from './trace/useExecutionTrace'

// Glass Box — the commercial demo. Left: the question gallery + answer. Right: a
// live React-Flow execution canvas that routes the query across candidate source
// systems and excises the decoy before answering. Rails Mode: the trace is the
// /api/demo replay. The swap point is the TraceSource — a live contextOS engine
// emitting the same graph replaces GalleryTraceSource with no UI change.
const source = new GalleryTraceSource()

export default function GlassBox() {
  const { nodes, edges, messages, loading, running, submit } = useExecutionTrace(source)

  return (
    <div className="flex h-full w-full overflow-hidden bg-slate-950 text-slate-100">
      <ChatPane messages={messages} loading={loading} running={running} onSelect={submit} />

      <main className="relative flex-1">
        <div className="pointer-events-none absolute left-4 top-4 z-10 flex items-center gap-1.5 rounded-md border border-slate-800 bg-slate-900/70 px-2.5 py-1 text-[11px] font-medium text-slate-400 backdrop-blur-sm">
          <Activity size={12} className={running ? 'text-sky-400' : 'text-slate-500'} />
          Execution Canvas
          <span className="text-slate-600">·</span>
          <span className="text-amber-400/80">Rails Mode</span>
        </div>
        <ReactFlowProvider>
          <Canvas nodes={nodes} edges={edges} />
        </ReactFlowProvider>
      </main>
    </div>
  )
}
