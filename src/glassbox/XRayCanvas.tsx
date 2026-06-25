import { useEffect } from 'react'
import {
  ReactFlow,
  Background,
  Controls,
  Handle,
  Position,
  MarkerType,
  useNodesState,
  useEdgesState,
  useReactFlow,
  type Node,
  type Edge,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { useTraceStore, type TraceNode } from './traceStore'
import './glassbox.css'

// ---- custom node renderers --------------------------------------------------

function IntakeNode({ data }: { data: any }) {
  return (
    <div className="gb-node gb-node--intake">
      <Handle type="source" position={Position.Bottom} className="gb-handle" />
      <div className="gb-node__title">INTAKE · routing plan</div>
      <div className="gb-node__plan">
        {(data.plan ?? []).map((p: string, i: number) => (
          <span key={p}>
            {i > 0 && <span className="gb-arrow"> → </span>}
            {p}
          </span>
        ))}
      </div>
      <div className="gb-node__sub">{data.entityId ?? ''}</div>
    </div>
  )
}

function SourceNode({ data }: { data: { node: TraceNode } }) {
  const n = data.node
  const cls = [
    'gb-node',
    'gb-node--source',
    n.dropped ? 'gb-node--dead' : '',
    n.authority === 'system_of_record' ? 'gb-node--sor' : 'gb-node--shadow',
  ].join(' ')
  return (
    <div className={cls} data-node-id={n.id} data-dropped={n.dropped ? 'true' : 'false'}>
      <Handle type="target" position={Position.Top} className="gb-handle" />
      <div className="gb-node__title">{n.source}</div>
      <div className="gb-node__value">{n.value_label}</div>
      <div className="gb-node__status">status: {n.status}</div>
      {n.dropped && <div className="gb-node__drop" data-testid={`drop-${n.id}`}>✕ {n.dropReason}</div>}
      <div className="gb-node__hint">click → raw row</div>
      <Handle type="source" position={Position.Bottom} className="gb-handle" />
    </div>
  )
}

function OperatorNode({ data }: { data: any }) {
  return (
    <div className="gb-node gb-node--operator" data-testid="operator-block">
      <Handle type="target" position={Position.Top} className="gb-handle" />
      <div className="gb-node__op">Σ {data.operator ?? 'SUM'}(run_rate)</div>
      <div className="gb-node__value gb-node__value--result" data-testid="operator-result">
        {data.resultLabel ?? '—'}
      </div>
      <div className="gb-node__sub">deterministic operator</div>
      <Handle type="source" position={Position.Bottom} className="gb-handle" />
    </div>
  )
}

function AnswerNode({ data }: { data: any }) {
  return (
    <div className="gb-node gb-node--answer">
      <Handle type="target" position={Position.Top} className="gb-handle" />
      <div className="gb-node__value gb-node__value--result">{data.resultLabel ?? ''}</div>
      <div className="gb-node__sub">served to the LLM</div>
    </div>
  )
}

const nodeTypes = { intake: IntakeNode, source: SourceNode, operator: OperatorNode, answer: AnswerNode }

// ---- derive the DAG from trace state ---------------------------------------

const POS = {
  intake: { x: 300, y: 16 },
  left: { x: 96, y: 196 },
  right: { x: 496, y: 196 },
  operator: { x: 300, y: 392 },
  answer: { x: 330, y: 560 },
}

function buildGraph(s: ReturnType<typeof useTraceStore.getState>): { nodes: Node[]; edges: Edge[] } {
  const nodes: Node[] = []
  const edges: Edge[] = []
  if (s.status === 'IDLE') return { nodes, edges }

  nodes.push({ id: 'intake', type: 'intake', position: POS.intake, data: { plan: s.plan, entityId: s.entityId } })

  s.nodes.forEach((n, i) => {
    nodes.push({ id: n.id, type: 'source', position: i === 0 ? POS.left : POS.right, data: { node: n } })
    // Sever the edge into a pruned node — the decoy is cut from the graph.
    if (!n.dropped) {
      edges.push({
        id: `e-intake-${n.id}`,
        source: 'intake',
        target: n.id,
        animated: s.status === 'RETRIEVE',
        style: { stroke: '#3f3f46', strokeWidth: 1.5 },
      })
    }
  })

  const operatorPresent = s.operator != null || s.status === 'COMPUTE' || s.status === 'DONE'
  const survivor = s.nodes.find((n) => n.id === s.survivedId) ?? s.nodes.find((n) => !n.dropped)

  if (operatorPresent) {
    nodes.push({ id: 'operator', type: 'operator', position: POS.operator, data: { operator: s.operator, resultLabel: s.resultLabel } })
    if (survivor && !survivor.dropped) {
      edges.push({
        id: `e-${survivor.id}-op`,
        source: survivor.id,
        target: 'operator',
        animated: true,
        style: { stroke: '#34d399', strokeWidth: 2.5 },
        markerEnd: { type: MarkerType.ArrowClosed, color: '#34d399' },
      })
    }
  }

  if (s.answer) {
    nodes.push({ id: 'answer', type: 'answer', position: POS.answer, data: { resultLabel: s.resultLabel } })
    edges.push({ id: 'e-op-answer', source: 'operator', target: 'answer', animated: true, style: { stroke: '#a78bfa', strokeWidth: 2 } })
  }

  return { nodes, edges }
}

export function XRayCanvas() {
  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([])
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([])
  const selectNode = useTraceStore((s) => s.selectNode)
  const rf = useReactFlow()

  // Re-derive the graph whenever the trace advances.
  const status = useTraceStore((s) => s.status)
  const traceNodes = useTraceStore((s) => s.nodes)
  const operator = useTraceStore((s) => s.operator)
  const answer = useTraceStore((s) => s.answer)

  useEffect(() => {
    const { nodes: n, edges: e } = buildGraph(useTraceStore.getState())
    setNodes(n)
    setEdges(e)
    const id = requestAnimationFrame(() => rf.fitView({ duration: 450, padding: 0.22 }))
    return () => cancelAnimationFrame(id)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [status, traceNodes, operator, answer])

  return (
    <div className="gb-canvas">
      {status === 'IDLE' && (
        <div className="gb-idle">Ask the question to X-ray the 3-hop trace →</div>
      )}
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onNodeClick={(_, node) => {
          if (node.type === 'source') selectNode(node.id)
        }}
        colorMode="dark"
        fitView
        fitViewOptions={{ padding: 0.22 }}
        nodesDraggable={false}
        nodesConnectable={false}
        proOptions={{ hideAttribution: false }}
      >
        <Background color="#1f1f23" gap={22} />
        <Controls showInteractive={false} />
      </ReactFlow>
    </div>
  )
}
