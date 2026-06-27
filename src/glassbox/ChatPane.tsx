import { useEffect, useState } from 'react'
import { useTraceStore } from './traceStore'
import { fetchStory } from './storyApi'

interface GalleryItem {
  id: string
  category: string
  capability: 'conflict' | 'traversal'
  question: string
  entity_id: string
}

const CATEGORY_ORDER = ['Grow Revenue', 'See the Real Risk', 'Stop the Leakage', 'Operate with Confidence']

export function ChatPane() {
  const activeId = useTraceStore((s) => s.questionId)
  const [items, setItems] = useState<GalleryItem[]>([])
  const [loadError, setLoadError] = useState<string | null>(null)

  useEffect(() => {
    // No silent fallback: surface a load failure rather than show an empty gallery.
    fetch('/api/demo/questions')
      .then((r) => { if (!r.ok) throw new Error(`questions endpoint returned ${r.status}`); return r.json() })
      .then((d) => setItems(d.questions ?? []))
      .catch((e) => setLoadError(`Could not load the question gallery: ${String(e)}`))
  }, [])

  const groups = CATEGORY_ORDER
    .map((c) => ({ category: c, qs: items.filter((q) => q.category === c) }))
    .filter((g) => g.qs.length > 0)

  return (
    <div className="gb-chat">
      <div className="gb-chat__head">
        <h1>Ask the graph</h1>
        <p>Pick a question. Walk through how the engine finds the answer — one step per click.</p>
      </div>

      {loadError && <div className="gb-error" data-testid="gallery-error">{loadError}</div>}

      <div className="gb-gallery" data-testid="question-gallery">
        {groups.map((g) => (
          <div key={g.category} className="gb-gallery__group">
            <div className="gb-gallery__label">{g.category}</div>
            {g.qs.map((q) => (
              <button
                key={q.id}
                data-testid={`q-${q.id}`}
                className={`gb-qcard ${activeId === q.id ? 'gb-qcard--active' : ''}`}
                onClick={() => fetchStory(q.id)}
              >
                <span className={`gb-qcard__cap gb-qcard__cap--${q.capability}`}>{q.capability}</span>
                <span className="gb-qcard__q">{q.question}</span>
              </button>
            ))}
          </div>
        ))}
      </div>
    </div>
  )
}
