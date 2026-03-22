/**
 * SE-mode node label — renders a node bar with a label pill.
 * L3 (rightmost column) labels are positioned to the LEFT of the node
 * to avoid clipping at the canvas edge.
 */

import { memo } from 'react';
import type { SankeyNodeProps } from './types';
import { SE_CONFIG } from './constants';

export const SankeyNodeLabel = memo(function SankeyNodeLabel({
  node,
  color,
  textColor,
  onMouseEnter,
  onMouseLeave,
}: SankeyNodeProps) {
  const { label: lc } = SE_CONFIG;
  const nw = node.x1 - node.x0;
  const nh = node.y1 - node.y0;
  const cy = node.y0 + nh / 2;

  const maxChars = 22;
  const display = node.label.length > maxChars
    ? `${node.label.slice(0, maxChars - 1)}\u2026`
    : node.label;

  const pillW = Math.min(display.length * 6.5 + 16, 160);
  const isRight = node.layer === 3; // rightmost column: label goes left
  const pillX = isRight ? node.x0 - pillW - 8 : node.x0 + lc.offsetX;
  const textX = pillX + 8;

  return (
    <g
      className="sankey-node cursor-pointer"
      data-layer={node.layer}
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
        fontSize={lc.fontSize}
        fontWeight={lc.fontWeight}
        fontFamily="ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace"
        className="select-none pointer-events-none"
      >
        {display}
      </text>
    </g>
  );
});
