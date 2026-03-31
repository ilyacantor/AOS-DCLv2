/**
 * Graph v2 node label — renders a node bar with a label pill.
 * Rightmost column labels positioned to the LEFT to avoid canvas clipping.
 */

import { memo } from 'react';
import type { LayoutNodeV2 } from './types';

interface NodeLabelProps {
  node: LayoutNodeV2;
  color: string;
  textColor: string;
  maxColumn: number;
  onMouseEnter: (event: React.MouseEvent, node: LayoutNodeV2) => void;
  onMouseLeave: () => void;
}

const LABEL_OFFSET_X = 26;
const FONT_SIZE = 11;
const MAX_CHARS = 22;

export const NodeLabel = memo(function NodeLabel({
  node,
  color,
  textColor,
  maxColumn,
  onMouseEnter,
  onMouseLeave,
}: NodeLabelProps) {
  const nw = node.x1 - node.x0;
  const nh = node.y1 - node.y0;
  const cy = node.y0 + nh / 2;

  const display = node.label.length > MAX_CHARS
    ? `${node.label.slice(0, MAX_CHARS - 1)}\u2026`
    : node.label;

  const pillW = Math.min(display.length * 6.5 + 16, 160);
  const isRight = node.column === maxColumn;
  const pillX = isRight ? node.x0 - pillW - 8 : node.x0 + LABEL_OFFSET_X;
  const textX = pillX + 8;

  return (
    <g
      className="cursor-pointer"
      data-layer={node.column}
      data-node-id={node.id}
      data-status={node.status}
      onMouseEnter={e => onMouseEnter(e, node)}
      onMouseLeave={onMouseLeave}
    >
      {/* Node bar */}
      <rect
        x={node.x0}
        y={node.y0}
        width={nw}
        height={nh}
        rx={nw / 2}
        fill="#0c1222"
        stroke={color}
        strokeWidth={1.5}
        style={{ filter: `drop-shadow(0 0 6px ${color}40)` }}
      />

      {/* Inner accent line */}
      <line
        x1={node.x0 + nw / 2}
        y1={node.y0 + 4}
        x2={node.x0 + nw / 2}
        y2={node.y1 - 4}
        stroke="rgba(255,255,255,0.15)"
        strokeWidth={2}
        strokeLinecap="round"
      />

      {/* Label pill */}
      <rect
        x={pillX}
        y={cy - 10}
        width={pillW}
        height={20}
        rx={10}
        fill="rgba(8, 13, 24, 0.92)"
        stroke={`${color}50`}
        strokeWidth={1}
      />

      {/* Label text */}
      <text
        x={textX}
        y={cy}
        dy="0.35em"
        fill={textColor}
        fontSize={FONT_SIZE}
        fontWeight={500}
        fontFamily="ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace"
        className="select-none pointer-events-none"
      >
        {display}
      </text>
    </g>
  );
});
