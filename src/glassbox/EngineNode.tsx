import { Handle, Position, type Node, type NodeProps } from '@xyflow/react'
import { Loader2 } from 'lucide-react'
import type { EngineNodeData } from './trace/types'
import { NODE_THEME, badgeTone } from './theme'
import { ICONS } from './icons'

export type EngineNodeType = Node<EngineNodeData, 'engine'>

/**
 * A contextOS execution node. Styling is fully derived from `data.state`, so a
 * single `node.patch` event drives the pending → processing → verified/excised
 * transition with no imperative DOM work.
 */
export function EngineNode({ data }: NodeProps<EngineNodeType>) {
  const tone = NODE_THEME[data.state]
  const Icon = ICONS[data.icon]
  const struck = data.state === 'excised'

  return (
    <div
      className={`w-72 rounded-xl border ${tone.card} ${tone.glow} backdrop-blur-sm transition-all duration-500`}
    >
      <Handle type="target" position={Position.Top} className="!h-2 !w-2 !border-slate-700 !bg-slate-500" />

      <div className="flex items-start gap-2 px-3 pb-1.5 pt-2.5">
        <span className={`mt-0.5 shrink-0 ${tone.icon}`}>
          {data.state === 'processing' ? (
            <Loader2 size={16} className="animate-spin" />
          ) : (
            <Icon size={16} />
          )}
        </span>
        <span
          className={`min-w-0 text-[13px] font-semibold leading-tight tracking-tight ${tone.text} ${
            struck ? 'line-through decoration-red-400/60' : ''
          }`}
        >
          {data.label}
        </span>
        {data.badge && (
          <span
            className={`ml-auto mt-px shrink-0 rounded px-1.5 py-0.5 font-mono text-[10px] font-medium ${badgeTone(
              data.badge,
            )}`}
          >
            {data.badge}
          </span>
        )}
      </div>

      {data.detail && (
        <div className="px-3 pb-2.5">
          <p
            className={`font-mono text-[11px] leading-snug ${tone.detail} ${
              struck ? 'line-through decoration-red-400/50' : ''
            }`}
          >
            {data.detail}
          </p>
        </div>
      )}

      <Handle type="source" position={Position.Bottom} className="!h-2 !w-2 !border-slate-700 !bg-slate-500" />
    </div>
  )
}
