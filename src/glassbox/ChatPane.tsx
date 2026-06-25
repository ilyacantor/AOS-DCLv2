import { useEffect, useRef, useState } from 'react'
import { useTraceStore } from './traceStore'
import { startTraceStream } from './useTraceStream'

interface GalleryItem {
  id: string
  vertical: string
  capability: 'conflict' | 'traversal'
  question: string
  entity_id: string
}

const VERTICAL_ORDER = ['General', 'BFSI', 'Healthcare']

export function ChatPane() {
  const status = useTraceStore((s) => s.status)
  const stageLog = useTraceStore((s) => s.stageLog)
  const answer = useTraceStore((s) => s.answer)
  const resultLabel = useTraceStore((s) => s.resultLabel)
  const error = useTraceStore((s) => s.error)
  const activeId = useTraceStore((s) => s.questionId)

  const [items, setItems] = useState<GalleryItem[]>([])
  const [loadError, setLoadError] = useState<string | null>(null)
  const esRef = useRef<EventSource | null>(null)

  const running = status === 'INTAKE' || status === 'TRAVERSE' || status === 'RETRIEVE' || status === 'PRUNE' || status === 'COMPUTE'

  useEffect(() => {
    // No silent fallback: surface a load failure rather than show an empty gallery.
    fetch('/api/demo/questions')
      .then((r) => {
        if (!r.ok) throw new Error(`questions endpoint returned ${r.status}`)
        return r.json()
      })
      .then((d) => setItems(d.questions ?? []))
      .catch((e) => setLoadError(`Could not load the question gallery: ${String(e)}`))
    return () => esRef.current?.close()
  }, [])

  const ask = (id: string) => {
    esRef.current?.close()
    esRef.current = startTraceStream(id)
  }

  const groups = VERTICAL_ORDER
    .map((v) => ({ vertical: v, qs: items.filter((q) => q.vertical === v) }))
    .filter((g) => g.qs.length > 0)

  return (
    <div className="gb-chat">
      <div className="gb-chat__head">
        <h1>Ask the graph</h1>
        <p>Pick a question. Watch the engine find the relationships and assemble the answer before any LLM sees the data.</p>
      </div>

      {loadError && <div className="gb-error" data-testid="gallery-error">{loadError}</div>}

      <div className="gb-gallery" data-testid="question-gallery">
        {groups.map((g) => (
          <div key={g.vertical} className="gb-gallery__group">
            <div className="gb-gallery__label">{g.vertical}</div>
            {g.qs.map((q) => (
              <button
                key={q.id}
                data-testid={`q-${q.id}`}
                className={`gb-qcard ${activeId === q.id ? 'gb-qcard--active' : ''}`}
                onClick={() => ask(q.id)}
                disabled={running}
              >
                <span className={`gb-qcard__cap gb-qcard__cap--${q.capability}`}>
                  {q.capability === 'traversal' ? 'traversal' : 'conflict'}
                </span>
                <span className="gb-qcard__q">{q.question}</span>
              </button>
            ))}
          </div>
        ))}
      </div>

      {stageLog.length > 0 && (
        <div className="gb-chat__log" data-testid="stage-log">
          {stageLog.map((s, i) => (
            <div key={i} className={`gb-log gb-log--${s.stage.toLowerCase()}`}>
              <span className="gb-log__stage">{s.stage}</span>
              <span className="gb-log__msg">{s.message}</span>
            </div>
          ))}
        </div>
      )}

      {error && <div className="gb-error" data-testid="trace-error">{error}</div>}

      {answer && (
        <div className="gb-answer" data-testid="trace-answer">
          <div className="gb-answer__big" data-testid="result-value">{resultLabel}</div>
          <div className="gb-answer__text">{answer}</div>
        </div>
      )}
    </div>
  )
}
