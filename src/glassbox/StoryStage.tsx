import { useEffect } from 'react'
import { useTraceStore } from './traceStore'
import './glassbox.css'

// Presenter-paced reveal, rendered as a stack of cards: each click reveals the
// next card, the 'aha' card is the accented climax, a bold answer card lands at
// the end. Middle ground between the (rejected) node graph and bare text.
// → / space / Enter or a click advances; per-card 'view record' opens the drawer.
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

  if (status === 'idle') return <div className="gb-stage gb-stage--idle">Pick a question to walk through how the engine finds the answer →</div>
  if (status === 'loading') return <div className="gb-stage gb-stage--idle">Loading…</div>
  if (status === 'error') return <div className="gb-stage"><div className="gb-error" data-testid="trace-error">{error}</div></div>

  return (
    <div className="gb-stage" onClick={() => moreToReveal && reveal()}>
      <div className="gb-story__q">{questionText}</div>

      <div className="gb-cards">
        {beats.slice(0, revealed).map((b, i) => {
          const src = b.source ?? b.record?.source_system
          return (
          <div className="gb-cardwrap" key={b.id}>
            {i > 0 && <div className="gb-cardconn" />}
            <div className={`gb-card ${b.link ? 'gb-card--link' : ''}`} data-testid={`beat-${b.id}`}>
              <div className="gb-card__badge">{b.link ? '⚡' : i + 1}</div>
              <div className="gb-card__body">
                {b.link && <div className="gb-card__tag">the link no system had</div>}
                <div className="gb-card__text">{b.text}</div>
                {src && <span className="gb-card__src">{src}</span>}
              </div>
              {b.record && (
                <button
                  className="gb-card__rec"
                  data-testid={`record-${b.id}`}
                  onClick={(e) => { e.stopPropagation(); selectBeat(b.id) }}
                >
                  view record
                </button>
              )}
            </div>
          </div>
          )
        })}
      </div>

      {answerShown && answer && (
        <div className="gb-answercard" data-testid="trace-answer">
          <div className="gb-answercard__eyebrow">Answer</div>
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
