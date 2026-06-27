import { useEffect } from 'react'
import { useTraceStore } from './traceStore'
import './glassbox.css'

// The presenter-paced narrative: plain-English beats revealed one click at a
// time, the 'aha' beat emphasized, a plain answer at the end. → / space / Enter
// or a click advances; per-beat 'view record' opens the audit drawer.
export function StoryStage() {
  const status = useTraceStore((s) => s.status)
  const questionText = useTraceStore((s) => s.questionText)
  const beats = useTraceStore((s) => s.beats)
  const revealed = useTraceStore((s) => s.revealed)
  const answer = useTraceStore((s) => s.answer)
  const answerShown = useTraceStore((s) => s.answerShown)
  const error = useTraceStore((s) => s.error)
  const reveal = useTraceStore((s) => s.reveal)
  const selectBeat = useTraceStore((s) => s.selectBeat)

  const moreToReveal = status === 'revealing' && (revealed < beats.length || !answerShown)

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (moreToReveal && (e.key === 'ArrowRight' || e.key === ' ' || e.key === 'Enter')) {
        e.preventDefault()
        reveal()
      }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [moreToReveal, reveal])

  if (status === 'idle') return <div className="gb-stage gb-stage--idle">Pick a question to watch the engine work →</div>
  if (status === 'loading') return <div className="gb-stage gb-stage--idle">Loading…</div>
  if (status === 'error') return <div className="gb-stage"><div className="gb-error" data-testid="trace-error">{error}</div></div>

  return (
    <div className="gb-stage" onClick={() => moreToReveal && reveal()}>
      <div className="gb-story__q">{questionText}</div>
      <ol className="gb-story__beats">
        {beats.slice(0, revealed).map((b, i) => (
          <li key={b.id} className={`gb-beat ${b.link ? 'gb-beat--link' : ''}`} data-testid={`beat-${b.id}`}>
            <span className="gb-beat__n">{b.link ? '⚡' : i + 1}</span>
            <span className="gb-beat__t">{b.text}</span>
            {b.record && (
              <button
                className="gb-beat__rec"
                data-testid={`record-${b.id}`}
                onClick={(e) => { e.stopPropagation(); selectBeat(b.id) }}
              >
                view record
              </button>
            )}
          </li>
        ))}
      </ol>

      {answerShown && answer && (
        <div className="gb-story__answer" data-testid="trace-answer">
          <div className="gb-story__headline" data-testid="answer-headline">{answer.headline}</div>
          <div className="gb-story__sub">{answer.sub}</div>
        </div>
      )}

      {moreToReveal && (
        <button className="gb-reveal" data-testid="reveal-next" onClick={(e) => { e.stopPropagation(); reveal() }}>
          {revealed < beats.length ? 'Reveal next ▸' : 'Show the answer ▸'}
        </button>
      )}
    </div>
  )
}
