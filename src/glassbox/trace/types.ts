// The trace protocol. Both the gallery adapter and a future live-engine stream
// emit these events; the reducer hook (useExecutionTrace) projects them into
// React Flow + chat state. One contract decouples the timeline from the UI.

/** The four execution states a contextOS engine node can be in. */
export type NodeState = 'pending' | 'processing' | 'verified' | 'excised'

/** Serializable icon key — resolved to a lucide icon in the component layer. */
export type IconKey = 'parser' | 'database' | 'shield' | 'reducer' | 'action'

/** Which system a step runs in — denoted on the node so the agent↔contextOS
 *  boundary is explicit. contextOS serves context; the agent acts; HITL gates. */
export type StepSystem = 'contextOS' | 'agent' | 'hitl'

export interface EngineNodeData {
  label: string
  state: NodeState
  icon: IconKey
  detail?: string
  badge?: string
  /** Denotes where the step takes place (agent vs contextOS vs human gate). */
  system?: StepSystem
  [key: string]: unknown
}

export interface NodeSpec {
  id: string
  position: { x: number; y: number }
  data: EngineNodeData
}

export interface EdgeSpec {
  id: string
  source: string
  target: string
}

export interface ChatMessage {
  id: string
  role: 'user' | 'assistant'
  text: string
  /** 'error' renders a readable failure surface (no silent blank). */
  tone?: 'error'
  /** Source of the request — e.g. "FinOps Agent", "Head of Engineering". */
  author?: string
  /** True when the author is an agent (vs a human persona) — drives the icon. */
  authorIsAgent?: boolean
}

export type TraceEvent =
  | { kind: 'reset' }
  | { kind: 'chat.message'; message: ChatMessage }
  | { kind: 'chat.loading'; value: boolean }
  | { kind: 'node.add'; node: NodeSpec }
  | { kind: 'node.patch'; id: string; patch: Partial<EngineNodeData> }
  | { kind: 'edge.add'; edge: EdgeSpec }
  | { kind: 'edge.sever'; id: string }
  | { kind: 'done' }
