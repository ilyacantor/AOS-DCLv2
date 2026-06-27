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
import Dagre from '@dagrejs/dagre'
import { useTraceStore, type TravNode } from './traceStore'
import './glassbox.css'

// ===== node renderers (Handles top/bottom so a TB layout connects cleanly) ==

function IntakeNode({ data }: { data: any }) {
  return (
    <div className="gb-node gb-node--intake">
      <Handle type="source" position={Position.Bottom} className="gb-handle" />
      <div className="gb-node__title">INTAKE · {data.entityId ?? 'query'}</div>
      <div className="gb-node__plan">
        {(data.plan ?? []).map((p: string, i: number) => (
          <span key={p}>{i > 0 && <span className="gb-arrow"> → </span>}{p}</span>
        ))}
      </div>
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
      <Handle type="source" position={Position.Bottom} className="gb-handle" />
    </div>
  )
}

function OperatorNode({ data }: { data: any }) {
  return (
    <div className="gb-node gb-node--operator" data-testid="operator-block">
      <Handle type="target" position={Position.Top} className="gb-handle" />
      <div className="gb-node__eyebrow">ANSWER · {data.operator ?? 'SUM'}</div>
      <div className="gb-node__value gb-node__value--result" data-testid="operator-result">{data.resultLabel ?? '—'}</div>
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

// ===== layout (dagre, top-down) =============================================

const DIM: Record<string, { w: number; h: number }> = {
  intake: { w: 190, h: 92 }, source: { w: 168, h: 134 }, trav: { w: 204, h: 112 },
  operator: { w: 214, h: 96 }, answer: { w: 150, h: 80 },
}
const dimOf = (t?: string) => DIM[t ?? ''] ?? { w: 190, h: 100 }

function layoutTB(nodes: Node[], edges: Edge[]): Node[] {
  const g = new Dagre.graphlib.Graph().setDefaultEdgeLabel(() => ({}))
  g.setGraph({ rankdir: 'TB', ranksep: 64, nodesep: 46, marginx: 24, marginy: 24 })
  nodes.forEach((n) => { const d = dimOf(n.type); g.setNode(n.id, { width: d.w, height: d.h }) })
  edges.forEach((e) => g.setEdge(e.source, e.target))
  Dagre.layout(g)
  return nodes.map((n) => {
    const d = dimOf(n.type)
    const p = g.node(n.id)
    return { ...n, position: { x: p.x - d.w / 2, y: p.y - d.h / 2 } }
  })
}

// A labeled edge — the relationship name IS the meaning, so make it readable.
function relEdge(e: { id: string; from: string; to: string; label: string; discovered?: boolean }): Edge {
  const disc = !!e.discovered
  return {
    id: e.id, source: e.from, target: e.to, animated: true, label: e.label,
    labelStyle: { fill: disc ? '#d8b4fe' : '#a1a1aa', fontSize: 11, fontWeight: disc ? 700 : 500 },
    labelBgStyle: { fill: '#0a0a0d', fillOpacity: 0.92 },
    labelBgPadding: [6, 3], labelBgBorderRadius: 4,
    style: disc ? { stroke: '#a78bfa', strokeWidth: 3 } : { stroke: '#52525b', strokeWidth: 1.6 },
    markerEnd: { type: MarkerType.ArrowClosed, color: disc ? '#a78bfa' : '#52525b' },
  }
}

type S = ReturnType<typeof useTraceStore.getState>
const at = (): Node['position'] => ({ x: 0, y: 0 }) // placeholder; dagre assigns real positions

function buildConflict(s: S): { nodes: Node[]; edges: Edge[] } {
  const nodes: Node[] = [{ id: 'intake', type: 'intake', position: at(), data: { plan: s.plan, entityId: s.entityId } }]
  const edges: Edge[] = []
  s.nodes.forEach((n) => {
    nodes.push({ id: n.id, type: 'source', position: at(), data: { node: n } })
    edges.push(
      n.dropped
        ? { id: `e-intake-${n.id}`, source: 'intake', target: n.id, label: 'pruned',
            labelStyle: { fill: '#f87171', fontSize: 11, fontWeight: 700 }, labelBgStyle: { fill: '#0a0a0d', fillOpacity: 0.92 }, labelBgPadding: [6, 3], labelBgBorderRadius: 4,
            style: { stroke: '#7f1d1d', strokeWidth: 1.5, strokeDasharray: '5 4', opacity: 0.65 } }
        : { id: `e-intake-${n.id}`, source: 'intake', target: n.id, animated: s.status === 'RETRIEVE', style: { stroke: '#3f3f46', strokeWidth: 1.6 } },
    )
  })
  const operatorPresent = s.operator != null || s.status === 'COMPUTE' || s.status === 'DONE'
  const survivor = s.nodes.find((n) => n.id === s.survivedId) ?? s.nodes.find((n) => !n.dropped)
  if (operatorPresent) {
    nodes.push({ id: 'operator', type: 'operator', position: at(), data: { operator: `${s.operator}(run_rate)`, resultLabel: s.resultLabel } })
    if (survivor && !survivor.dropped) edges.push({ id: `e-${survivor.id}-op`, source: survivor.id, target: 'operator', animated: true, style: { stroke: '#34d399', strokeWidth: 2.5 }, markerEnd: { type: MarkerType.ArrowClosed, color: '#34d399' } })
  }
  return { nodes, edges }
}

function buildTraversal(s: S): { nodes: Node[]; edges: Edge[] } {
  const nodes: Node[] = []
  const edges: Edge[] = []
  s.travNodes.forEach((n, i) => nodes.push({ id: n.id, type: 'trav', position: at(), data: { node: n, isRoot: i === 0 } }))
  s.travEdges.forEach((e) => edges.push(relEdge(e)))
  const operatorPresent = s.operator != null || s.status === 'COMPUTE' || s.status === 'DONE'
  if (operatorPresent && s.travNodes.length) {
    const last = s.travNodes[s.travNodes.length - 1]
    nodes.push({ id: 'operator', type: 'operator', position: at(), data: { operator: s.operator, resultLabel: s.resultLabel } })
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
    const built =
      s.status === 'IDLE'
        ? { nodes: [] as Node[], edges: [] as Edge[] }
        : s.capability === 'traversal'
          ? buildTraversal(s)
          : buildConflict(s)
    setNodes(layoutTB(built.nodes, built.edges))
    setEdges(built.edges)
    const id = requestAnimationFrame(() => rf.fitView({ duration: 450, padding: 0.18, maxZoom: 1.15 }))
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
        onNodeClick={(_, node) => { if (node.type === 'source' || node.type === 'trav') selectNode(node.id) }}
        colorMode="dark"
        fitView
        fitViewOptions={{ padding: 0.18, maxZoom: 1.15 }}
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
