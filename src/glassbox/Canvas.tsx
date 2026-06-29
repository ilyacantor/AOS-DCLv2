import { useEffect } from 'react'
import {
  Background,
  BackgroundVariant,
  ReactFlow,
  useReactFlow,
  type Edge,
  type Node,
  type NodeTypes,
} from '@xyflow/react'
import { EngineNode } from './EngineNode'

const nodeTypes: NodeTypes = { engine: EngineNode }

/**
 * The live execution canvas. Re-fits the view whenever the graph grows so the
 * scene stays centered as nodes stream in.
 */
export function Canvas({ nodes, edges }: { nodes: Node[]; edges: Edge[] }) {
  const { fitView } = useReactFlow()

  useEffect(() => {
    if (nodes.length === 0) return
    const id = setTimeout(() => {
      void fitView({ duration: 600, padding: 0.28, maxZoom: 1.15 })
    }, 80)
    return () => clearTimeout(id)
  }, [nodes.length, fitView])

  return (
    <ReactFlow
      nodes={nodes}
      edges={edges}
      nodeTypes={nodeTypes}
      fitView
      proOptions={{ hideAttribution: true }}
      nodesDraggable={false}
      nodesConnectable={false}
      elementsSelectable={false}
      panOnDrag
      zoomOnScroll
      minZoom={0.3}
      maxZoom={1.5}
      className="bg-slate-950"
    >
      <Background variant={BackgroundVariant.Dots} gap={26} size={1} color="#1e293b" />
    </ReactFlow>
  )
}
