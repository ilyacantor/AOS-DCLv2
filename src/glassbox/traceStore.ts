import { create } from 'zustand'

// Glass Box trace state. Driven by the SSE replay stream (useTraceStream.ts).
// Two capabilities: 'conflict' (source-authority prune) and 'traversal'
// (relationship discovery — a path grown hop by hop, the hard edge flagged
// discovered). The store models exactly what the live contextOS stream will
// emit so the wiring survives the engine swap.

export type Stage =
  | 'IDLE' | 'INTAKE' | 'RETRIEVE' | 'PRUNE' | 'TRAVERSE' | 'COMPUTE' | 'DONE' | 'ERROR'
export type Capability = 'conflict' | 'traversal' | null

export type RawRow = Record<string, any>

export interface ConflictNode {
  id: string; label: string; source: string; value: number; value_label: string
  status: string; authority: string; raw_row: RawRow; dropped: boolean; dropReason: string | null
}

export interface TravNode {
  id: string; label: string; source: string; sublabel?: string; value_label?: string
  discovered?: boolean; raw_row?: RawRow
}
export interface TravEdge { id: string; from: string; to: string; label: string; discovered?: boolean }

export interface StageLog { stage: Stage; message: string }

interface TraceState {
  status: Stage
  capability: Capability
  questionId: string | null
  stageLog: StageLog[]
  plan: string[]
  // conflict
  nodes: ConflictNode[]
  survivedId: string | null
  // traversal
  travNodes: TravNode[]
  travEdges: TravEdge[]
  // shared
  operator: string | null
  resultLabel: string | null
  answer: string | null
  error: string | null
  selectedNodeId: string | null
  tenantId: string | null
  entityId: string | null
  reset: () => void
  setQuestion: (id: string) => void
  setError: (msg: string) => void
  selectNode: (id: string | null) => void
  applyEvent: (stage: string, data: Record<string, any>) => void
}

const initial = {
  status: 'IDLE' as Stage,
  capability: null as Capability,
  questionId: null as string | null,
  stageLog: [] as StageLog[],
  plan: [] as string[],
  nodes: [] as ConflictNode[],
  survivedId: null as string | null,
  travNodes: [] as TravNode[],
  travEdges: [] as TravEdge[],
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
  setQuestion: (id) => set({ ...initial, questionId: id }),
  setError: (msg) => set({ error: msg, status: 'ERROR' }),
  selectNode: (id) => set({ selectedNodeId: id }),
  applyEvent: (stage, data) =>
    set((s) => {
      const stageLog = [...s.stageLog, { stage: stage as Stage, message: data.message ?? stage }]
      const base = {
        stageLog,
        status: stage as Stage,
        capability: (data.capability ?? s.capability) as Capability,
        tenantId: data.tenant_id ?? s.tenantId,
        entityId: data.entity_id ?? s.entityId,
      }
      switch (stage) {
        case 'INTAKE':
          return {
            ...base,
            plan: data.plan ?? [],
            // traversal questions seed the path with a root node
            travNodes: data.root ? [{ ...data.root }] : s.travNodes,
            travEdges: data.root ? [] : s.travEdges,
          }
        case 'TRAVERSE': {
          const node: TravNode = { ...data.node, discovered: !!data.discovered }
          const e = data.edge ?? {}
          const edge: TravEdge = {
            id: `e-${e.from}-${e.to}`, from: e.from, to: e.to, label: e.label ?? '', discovered: !!data.discovered,
          }
          return { ...base, travNodes: [...s.travNodes, node], travEdges: [...s.travEdges, edge] }
        }
        case 'RETRIEVE':
          return {
            ...base,
            nodes: (data.nodes ?? []).map((n: any) => ({ ...n, dropped: false, dropReason: null })),
          }
        case 'PRUNE': {
          const droppedMap = new Map<string, string>((data.dropped ?? []).map((d: any) => [d.id, d.reason]))
          return {
            ...base,
            survivedId: (data.survived ?? [])[0] ?? s.survivedId,
            nodes: s.nodes.map((n) =>
              droppedMap.has(n.id) ? { ...n, dropped: true, dropReason: droppedMap.get(n.id) ?? null } : n,
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
