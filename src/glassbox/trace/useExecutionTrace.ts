import { useCallback, useEffect, useRef, useState } from 'react'
import { MarkerType, type Edge, type Node } from '@xyflow/react'
import type { TraceSource } from './TraceSource'
import type { ChatMessage, EdgeSpec, NodeSpec, TraceEvent } from './types'

const ACTIVE_STROKE = '#38bdf8' // sky-400 — an in-flight / trusted path
const SEVERED_STROKE = '#ef4444' // red-500 — a severed decoy path

function toRFNode(spec: NodeSpec): Node {
  return { id: spec.id, type: 'engine', position: spec.position, data: spec.data }
}

function toRFEdge(spec: EdgeSpec): Edge {
  return {
    id: spec.id,
    source: spec.source,
    target: spec.target,
    type: 'smoothstep',
    animated: true,
    style: { stroke: ACTIVE_STROKE, strokeWidth: 2 },
    markerEnd: { type: MarkerType.ArrowClosed, color: ACTIVE_STROKE },
    data: { severed: false },
  }
}

/** Turn an edge red + dashed and halt its animation to prove the path is cut. */
function severEdge(edge: Edge): Edge {
  return {
    ...edge,
    animated: false,
    style: { stroke: SEVERED_STROKE, strokeWidth: 2, strokeDasharray: '6 5' },
    markerEnd: { type: MarkerType.ArrowClosed, color: SEVERED_STROKE },
    data: { ...edge.data, severed: true },
  }
}

export interface ExecutionTrace {
  nodes: Node[]
  edges: Edge[]
  messages: ChatMessage[]
  loading: boolean
  running: boolean
  submit: (questionId: string) => void
}

/**
 * Subscribes to a TraceSource and reduces its events into render-ready state.
 * Pure projection — it knows nothing about timing or transport, so swapping the
 * gallery replay for a live-engine source is a one-line change in GlassBox.
 */
export function useExecutionTrace(source: TraceSource): ExecutionTrace {
  const [nodes, setNodes] = useState<Node[]>([])
  const [edges, setEdges] = useState<Edge[]>([])
  const [messages, setMessages] = useState<ChatMessage[]>([])
  const [loading, setLoading] = useState(false)
  const [running, setRunning] = useState(false)
  const cancelRef = useRef<(() => void) | null>(null)

  const reduce = useCallback((event: TraceEvent) => {
    switch (event.kind) {
      case 'reset':
        setNodes([])
        setEdges([])
        break
      case 'chat.message':
        setMessages((prev) => [...prev, event.message])
        break
      case 'chat.loading':
        setLoading(event.value)
        break
      case 'node.add':
        setNodes((prev) => [...prev, toRFNode(event.node)])
        break
      case 'node.patch':
        setNodes((prev) =>
          prev.map((n) =>
            n.id === event.id ? { ...n, data: { ...n.data, ...event.patch } } : n,
          ),
        )
        break
      case 'edge.add':
        setEdges((prev) => [...prev, toRFEdge(event.edge)])
        break
      case 'edge.sever':
        setEdges((prev) => prev.map((e) => (e.id === event.id ? severEdge(e) : e)))
        break
      case 'done':
        setRunning(false)
        break
    }
  }, [])

  const submit = useCallback(
    (questionId: string) => {
      cancelRef.current?.()
      setRunning(true)
      cancelRef.current = source.run(questionId, reduce)
    },
    [source, reduce],
  )

  useEffect(() => () => cancelRef.current?.(), [])

  return { nodes, edges, messages, loading, running, submit }
}
