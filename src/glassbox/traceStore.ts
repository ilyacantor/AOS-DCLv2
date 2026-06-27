import { create } from 'zustand'

// Glass Box trace state — a presenter-paced STORY. Beats are revealed one click
// at a time; the 'aha' beat is flagged link. Models exactly what the live
// contextOS engine will return, so the wiring survives the engine swap.

export type Status = 'idle' | 'loading' | 'revealing' | 'done' | 'error'
export interface Beat { id: string; text: string; link?: boolean; record?: Record<string, any> }
export interface Answer { headline: string; sub: string }

interface TraceState {
  status: Status
  questionId: string | null
  questionText: string | null
  entityId: string | null
  beats: Beat[]
  revealed: number
  answer: Answer | null
  answerShown: boolean
  selectedBeatId: string | null
  error: string | null
  setLoading: (id: string) => void
  loadStory: (q: any) => void
  reveal: () => void
  selectBeat: (id: string | null) => void
  setError: (msg: string) => void
  reset: () => void
}

const base = {
  status: 'idle' as Status,
  questionId: null as string | null,
  questionText: null as string | null,
  entityId: null as string | null,
  beats: [] as Beat[],
  revealed: 0,
  answer: null as Answer | null,
  answerShown: false,
  selectedBeatId: null as string | null,
  error: null as string | null,
}

export const useTraceStore = create<TraceState>((set) => ({
  ...base,
  setLoading: (id) => set({ ...base, status: 'loading', questionId: id }),
  loadStory: (q) =>
    set({
      ...base,
      status: 'revealing',
      questionId: q.id,
      questionText: q.question,
      entityId: q.entity_id,
      beats: q.story?.beats ?? [],
      answer: q.story?.answer ?? null,
      revealed: 1, // show the first beat immediately; clicks reveal the rest
    }),
  reveal: () =>
    set((s) => {
      if (s.status !== 'revealing') return s
      if (s.revealed < s.beats.length) return { revealed: s.revealed + 1 }
      if (!s.answerShown) return { answerShown: true, status: 'done' }
      return s
    }),
  selectBeat: (id) => set({ selectedBeatId: id }),
  setError: (msg) => set({ status: 'error', error: msg }),
  reset: () => set({ ...base }),
}))
