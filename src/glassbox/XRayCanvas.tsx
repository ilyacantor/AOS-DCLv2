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
import { useTraceStore, type TravNode } from './traceStore'
import './glassbox.css'

// ===== conflict-capability nodes ============================================

function IntakeNode({ data }: { data: any }) {
  return (
    <div className="gb-node gb-node--intake">
      <Handle type="source" position={Position.Bottom} className="gb-handle" />
      <div className="gb-node__title">INTAKE · routing plan</div>
      <div className="gb-node__plan">
        {(data.plan ?? []).map((p: string, i: number) => (
          <span key={p}>{i > 0 && <span className="gb-arrow"> → </span>}{p}</span>
        ))}
      </div>
      <div className="gb-node__sub">{data.entityId ?? ''}</div>
    </div>
  )
}

function SourceNode({ data }: { data: { node: any } }) {
  const n = data.node
  const cls = ['gb-node', 'gb-node--source', n.dropped ? 'gb-node--dead' : '',
    n.authority === 'system_of_record' ? 'gb-node--sor' : 'gb-node--shadow'].join(' ')
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

// ===== traversal-capability nodes ===========================================

function TravNodeView({ data }: { data: { node: TravNode; isRoot?: boolean } }) {
  const n = data.node
  const cls = ['gb-node', 'gb-tnode', data.isRoot ? 'gb-tnode--root' : '',
    n.discovered ? 'gb-tnode--found' : ''].join(' ')
  return (
    <div className={cls} data-node-id={n.id} data-discovered={n.discovered ? 'true' : 'false'}>
      <Handle type="target" position={Position.Top} className="gb-handle" />
      {n.discovered && <div className="gb-tnode__ribbon" data-testid="discovered-ribbon">relationship found</div>}
      <div className="gb-node__title">{n.label}</div>
      {n.value_label && <div className="gb-node__value gb-tnode__value">{n.value_label}</div>}
      {n.sublabel && <div className="gb-tnode__sub">{n.sublabel}</div>}
      <div className="gb-node__status">{n.source}</div>
      {n.raw_row && <div className="gb-node__hint">click → raw row</div>}
      <Handle type="source" position={Position.Bottom} className="gb-handle" />
    </div>
  )
}

function OperatorNode({ data }: { data: any }) {
  return (
    <div className="gb-node gb-node--operator" data-testid="operator-block">
      <Handle type="target" position={Position.Top} className="gb-handle" />
      <div className="gb-node__op">{data.operator ?? 'SUM'}</div>
      <div className="gb-node__value gb-node__value--result" data-testid="operator-result">{data.resultLabel ?? '—'}</div>
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

const nodeTypes = { intake: IntakeNode, source: SourceNode, operator: OperatorNode, answer: AnswerNode, trav: TravNodeView }

// ===== graph builders =======================================================

type S = ReturnType<typeof useTraceStore.getState>

function buildConflict(s: S): { nodes: Node[]; edges: Edge[] } {
  const nodes: Node[] = [{ id: 'intake', type: 'intake', position: { x: 300, y: 16 }, data: { plan: s.plan, entityId: s.entityId } }]
  const edges: Edge[] = []
  s.nodes.forEach((n, i) => {
    nodes.push({ id: n.id, type: 'source', position: i === 0 ? { x: 96, y: 196 } : { x: 496, y: 196 }, data: { node: n } })
    if (!n.dropped) edges.push({ id: `e-intake-${n.id}`, source: 'intake', target: n.id, animated: s.status === 'RETRIEVE', style: { stroke: '#3f3f46', strokeWidth: 1.5 } })
  })
  const operatorPresent = s.operator != null || s.status === 'COMPUTE' || s.status === 'DONE'
  const survivor = s.nodes.find((n) => n.id === s.survivedId) ?? s.nodes.find((n) => !n.dropped)
  if (operatorPresent) {
    nodes.push({ id: 'operator', type: 'operator', position: { x: 300, y: 392 }, data: { operator: `Σ ${s.operator}(run_rate)`, resultLabel: s.resultLabel } })
    if (survivor && !survivor.dropped) edges.push({ id: `e-${survivor.id}-op`, source: survivor.id, target: 'operator', animated: true, style: { stroke: '#34d399', strokeWidth: 2.5 }, markerEnd: { type: MarkerType.ArrowClosed, color: '#34d399' } })
  }
  if (s.answer) {
    nodes.push({ id: 'answer', type: 'answer', position: { x: 330, y: 560 }, data: { resultLabel: s.resultLabel } })
    edges.push({ id: 'e-op-answer', source: 'operator', target: 'answer', animated: true, style: { stroke: '#a78bfa', strokeWidth: 2 } })
  }
  return { nodes, edges }
}

function buildTraversal(s: S): { nodes: Node[]; edges: Edge[] } {
  const nodes: Node[] = []
  const edges: Edge[] = []
  // Cascade the path so each discovered hop steps down-and-right.
  s.travNodes.forEach((n, i) => {
    nodes.push({ id: n.id, type: 'trav', position: { x: 40 + i * 196, y: 24 + i * 104 }, data: { node: n, isRoot: i === 0 } })
  })
  s.travEdges.forEach((e) => {
    edges.push({
      id: e.id, source: e.from, target: e.to, label: e.label, animated: true,
      style: e.discovered ? { stroke: '#a78bfa', strokeWidth: 3 } : { stroke: '#52525b', strokeWidth: 1.5 },
      labelStyle: { fill: e.discovered ? '#c4b5fd' : '#71717a', fontSize: 10 },
      labelBgStyle: { fill: '#0a0a0d' },
      markerEnd: { type: MarkerType.ArrowClosed, color: e.discovered ? '#a78bfa' : '#52525b' },
    })
  })
  const operatorPresent = s.operator != null || s.status === 'COMPUTE' || s.status === 'DONE'
  if (operatorPresent && s.travNodes.length) {
    const last = s.travNodes[s.travNodes.length - 1]
    const i = s.travNodes.length
    nodes.push({ id: 'operator', type: 'operator', position: { x: 40 + i * 196, y: 24 + i * 104 }, data: { operator: s.operator, resultLabel: s.resultLabel } })
    edges.push({ id: 'e-last-op', source: last.id, target: 'operator', animated: true, style: { stroke: '#34d399', strokeWidth: 2.5 }, markerEnd: { type: MarkerType.ArrowClosed, color: '#34d399' } })
  }
  return { nodes, edges }
}

export function XRayCanvas() {
  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([])
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([])
  const selectNode = useTraceStore((s) => s.selectNode)
  const rf = useReactFlow()

  const status = useTraceStore((s) => s.status)
  const capability = useTraceStore((s) => s.capability)
  const conflictNodes = useTraceStore((s) => s.nodes)
  const travNodes = useTraceStore((s) => s.travNodes)
  const operator = useTraceStore((s) => s.operator)
  const answer = useTraceStore((s) => s.answer)

  useEffect(() => {
    const s = useTraceStore.getState()
    const { nodes: n, edges: e } = s.capability === 'traversal' ? buildTraversal(s) : buildConflict(s)
    setNodes(n)
    setEdges(e)
    const id = requestAnimationFrame(() => rf.fitView({ duration: 450, padding: 0.2 }))
    return () => cancelAnimationFrame(id)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [status, capability, conflictNodes, travNodes, operator, answer])

  return (
    <div className="gb-canvas">
      {status === 'IDLE' && <div className="gb-idle">Pick a question to X-ray how the engine finds the answer →</div>}
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onNodeClick={(_, node) => {
          if (node.type === 'source' || node.type === 'trav') selectNode(node.id)
        }}
        colorMode="dark"
        fitView
        fitViewOptions={{ padding: 0.2 }}
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
