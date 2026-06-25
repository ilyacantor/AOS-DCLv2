import { useEffect, useRef, useState } from 'react'
import { useTraceStore } from './traceStore'
import { startTraceStream } from './useTraceStream'

const QUERY = 'What is the run-rate for the Engineering team?'

export function ChatPane() {
  const status = useTraceStore((s) => s.status)
  const stageLog = useTraceStore((s) => s.stageLog)
  const answer = useTraceStore((s) => s.answer)
  const resultLabel = useTraceStore((s) => s.resultLabel)
  const error = useTraceStore((s) => s.error)
  const [query, setQuery] = useState(QUERY)
  const esRef = useRef<EventSource | null>(null)

  const running = status === 'INTAKE' || status === 'RETRIEVE' || status === 'PRUNE' || status === 'COMPUTE'

  const run = () => {
    esRef.current?.close()
    esRef.current = startTraceStream()
  }

  useEffect(() => () => esRef.current?.close(), [])

  return (
    <div className="gb-chat">
      <div className="gb-chat__head">
        <h1>Engineering run-rate</h1>
        <p>Ask, and watch the engine resolve conflicting sources before the LLM ever sees them.</p>
      </div>

      <div className="gb-chat__query">
        <input
          aria-label="query"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          spellCheck={false}
        />
        <button data-testid="run-trace" onClick={run} disabled={running}>
          {running ? 'Tracing…' : 'Ask'}
        </button>
      </div>

      <div className="gb-chat__log" data-testid="stage-log">
        {stageLog.map((s, i) => (
          <div key={i} className={`gb-log gb-log--${s.stage.toLowerCase()}`}>
            <span className="gb-log__stage">{s.stage}</span>
            <span className="gb-log__msg">{s.message}</span>
          </div>
        ))}
      </div>

      {error && (
        <div className="gb-error" data-testid="trace-error">{error}</div>
      )}

      {answer && (
        <div className="gb-answer" data-testid="trace-answer">
          <div className="gb-answer__big" data-testid="result-value">{resultLabel}</div>
          <div className="gb-answer__text">{answer}</div>
        </div>
      )}
    </div>
  )
}
