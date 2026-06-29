import type { TraceSource } from './TraceSource'
import type { TraceEvent } from './types'

// Adapter: the /api/demo replay backend serves each question's authored `graph`
// (intake -> source-system candidates -> link/verify -> reducer). This turns
// that topology into the animated trace the React-Flow Glass Box renders. A
// live contextOS engine emitting the same graph plugs in with no UI change.

export interface GalleryItem {
  id: string
  question: string
  entity_id: string
  capability?: string
}

interface GraphCandidate {
  id: string
  label: string
  detail?: string
  badge?: string
  verdict: 'verified' | 'excised'
  excisedDetail?: string
}

interface QuestionGraph {
  parse?: string
  candidates: GraphCandidate[]
  link: { label: string; detail?: string }
  answer: { headline: string; sub: string }
}

interface Step {
  at: number
  events: TraceEvent[]
}

const COL = 320
const ROW = { intake: 0, candidate: 150, link: 310, reducer: 470 }

/** Fetch the preselected gallery for the picker. No silent fallback (A1). */
export async function fetchQuestions(): Promise<GalleryItem[]> {
  const r = await fetch('/api/demo/questions')
  if (!r.ok) throw new Error(`questions endpoint returned ${r.status}`)
  const data = await r.json()
  if (!data?.questions?.length) throw new Error('gallery returned no questions')
  return data.questions
}

function buildTimeline(question: { id: string; question: string; entity_id: string; graph?: QuestionGraph }): Step[] {
  const g = question.graph
  if (!g?.candidates?.length || !g.link || !g.answer) {
    throw new Error(`question '${question.id}' has no graph topology to animate`)
  }

  const n = g.candidates.length
  const centerX = ((n - 1) * COL) / 2
  const INTAKE = 'intake'
  const LINK = 'verify'
  const REDUCER = 'reducer'

  const resolve: TraceEvent[] = []
  for (const c of g.candidates) {
    if (c.verdict === 'excised') {
      resolve.push({ kind: 'node.patch', id: c.id, patch: { state: 'excised', detail: c.excisedDetail ?? 'Excised.' } })
      resolve.push({ kind: 'edge.sever', id: `e-intake-${c.id}` })
      resolve.push({ kind: 'edge.sever', id: `e-${c.id}-verify` })
    } else {
      resolve.push({ kind: 'node.patch', id: c.id, patch: { state: 'verified' } })
    }
  }
  resolve.push({ kind: 'node.patch', id: LINK, patch: { state: 'verified' } })

  return [
    {
      at: 0,
      events: [
        { kind: 'reset' },
        { kind: 'chat.message', message: { id: `u-${question.id}`, role: 'user', text: question.question } },
        { kind: 'chat.loading', value: true },
      ],
    },
    {
      at: 500,
      events: [
        {
          kind: 'node.add',
          node: {
            id: INTAKE,
            position: { x: centerX, y: ROW.intake },
            data: { label: 'Intake Parser', state: 'verified', icon: 'parser', detail: g.parse ?? `entity=${question.entity_id}` },
          },
        },
      ],
    },
    {
      at: 1500,
      events: [
        ...g.candidates.map<TraceEvent>((c, i) => ({
          kind: 'node.add',
          node: {
            id: c.id,
            position: { x: i * COL, y: ROW.candidate },
            data: { label: c.label, state: 'processing', icon: 'database', detail: c.detail, badge: c.badge },
          },
        })),
        ...g.candidates.map<TraceEvent>((c) => ({
          kind: 'edge.add',
          edge: { id: `e-intake-${c.id}`, source: INTAKE, target: c.id },
        })),
      ],
    },
    {
      at: 3000,
      events: [
        {
          kind: 'node.add',
          node: {
            id: LINK,
            position: { x: centerX, y: ROW.link },
            data: { label: g.link.label, state: 'processing', icon: 'shield', detail: g.link.detail ?? 'Arbitrating…' },
          },
        },
        ...g.candidates.map<TraceEvent>((c) => ({
          kind: 'edge.add',
          edge: { id: `e-${c.id}-verify`, source: c.id, target: LINK },
        })),
      ],
    },
    { at: 4500, events: resolve },
    {
      at: 5700,
      events: [
        {
          kind: 'node.add',
          node: {
            id: REDUCER,
            position: { x: centerX, y: ROW.reducer },
            data: { label: 'Compute Reducer', state: 'verified', icon: 'reducer', detail: `Push-down complete. Payload: ${g.answer.headline}` },
          },
        },
        { kind: 'edge.add', edge: { id: 'e-verify-reducer', source: LINK, target: REDUCER } },
      ],
    },
    {
      at: 7000,
      events: [
        { kind: 'chat.message', message: { id: `a-${question.id}`, role: 'assistant', text: `${g.answer.headline}\n\n${g.answer.sub}` } },
        { kind: 'chat.loading', value: false },
        { kind: 'done' },
      ],
    },
  ]
}

export class GalleryTraceSource implements TraceSource {
  run(questionId: string, emit: (event: TraceEvent) => void): () => void {
    const timers: ReturnType<typeof setTimeout>[] = []
    let cancelled = false

    fetch(`/api/demo/trace?q=${encodeURIComponent(questionId)}`)
      .then((r) => {
        if (!r.ok) throw new Error(`trace endpoint returned ${r.status}`)
        return r.json()
      })
      .then((question) => {
        if (cancelled) return
        for (const step of buildTimeline(question)) {
          timers.push(
            setTimeout(() => {
              for (const event of step.events) emit(event)
            }, step.at),
          )
        }
      })
      .catch((e) => {
        if (cancelled) return
        // No silent fallback (A1): surface a readable error, resolve nothing.
        emit({
          kind: 'chat.message',
          message: { id: `err-${questionId}`, role: 'assistant', tone: 'error', text: `Couldn't load this trace (${String(e)}). Nothing is shown as resolved.` },
        })
        emit({ kind: 'chat.loading', value: false })
        emit({ kind: 'done' })
      })

    return () => {
      cancelled = true
      for (const t of timers) clearTimeout(t)
    }
  }
}
