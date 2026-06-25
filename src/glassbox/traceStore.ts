import { create } from 'zustand'

// Glass Box trace state. Driven by the SSE replay stream (useTraceStream.ts).
// The engine is mocked in RAILS MODE; this store models exactly what the live
// contextOS stream will emit, so the wiring survives the engine swap.

export type Stage =
  | 'IDLE' | 'INTAKE' | 'RETRIEVE' | 'PRUNE' | 'COMPUTE' | 'DONE' | 'ERROR'

export interface RawRow {
  bitemporal_id: string
  concept: string
  entity_id: string
  tenant_id: string
  source_system: string
  value_usd: number
  status: string
  valid_from: string
  updated_at: string
}

export interface TraceNode {
  id: string
  label: string
  source: string
  value: number
  value_label: string
  status: string
  authority: string
  raw_row: RawRow
  dropped: boolean
  dropReason: string | null
}

export interface StageLog {
  stage: Stage
  message: string
}

interface TraceState {
  status: Stage
  stageLog: StageLog[]
  plan: string[]
  hops: number | null
  nodes: TraceNode[]
  survivedId: string | null
  operator: string | null
  resultLabel: string | null
  answer: string | null
  error: string | null
  selectedNodeId: string | null
  tenantId: string | null
  entityId: string | null
  reset: () => void
  setError: (msg: string) => void
  selectNode: (id: string | null) => void
  applyEvent: (stage: string, data: Record<string, any>) => void
}

const initial = {
  status: 'IDLE' as Stage,
  stageLog: [] as StageLog[],
  plan: [] as string[],
  hops: null as number | null,
  nodes: [] as TraceNode[],
  survivedId: null as string | null,
  operator: null as string | null,
  resultLabel: null as string | null,
  answer: null as string | null,
  error: null as string | null,
  selectedNodeId: null as string | null,
  tenantId: null as string | null,
  entityId: null as string | null,
}

export const useTraceStore = create<TraceState>((set) => ({
  ...initial,
  reset: () => set({ ...initial }),
  setError: (msg) => set({ error: msg, status: 'ERROR' }),
  selectNode: (id) => set({ selectedNodeId: id }),
  applyEvent: (stage, data) =>
    set((s) => {
      const stageLog = [...s.stageLog, { stage: stage as Stage, message: data.message ?? stage }]
      const base = {
        stageLog,
        status: stage as Stage,
        tenantId: data.tenant_id ?? s.tenantId,
        entityId: data.entity_id ?? s.entityId,
      }
      switch (stage) {
        case 'INTAKE':
          return { ...base, plan: data.plan ?? [], hops: data.hops ?? null }
        case 'RETRIEVE':
          return {
            ...base,
            nodes: (data.nodes ?? []).map((n: any) => ({ ...n, dropped: false, dropReason: null })),
          }
        case 'PRUNE': {
          const droppedMap = new Map<string, string>(
            (data.dropped ?? []).map((d: any) => [d.id, d.reason]),
          )
          return {
            ...base,
            survivedId: (data.survived ?? [])[0] ?? s.survivedId,
            nodes: s.nodes.map((n) =>
              droppedMap.has(n.id)
                ? { ...n, dropped: true, dropReason: droppedMap.get(n.id) ?? null }
                : n,
            ),
          }
        }
        case 'COMPUTE':
          return { ...base, operator: data.operator ?? null, resultLabel: data.result_label ?? null }
        case 'DONE':
          return { ...base, answer: data.answer ?? null, resultLabel: data.result_label ?? s.resultLabel }
        default:
          return base
      }
    }),
}))
