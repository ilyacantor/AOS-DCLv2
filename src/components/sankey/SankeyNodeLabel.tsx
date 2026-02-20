/**
 * SankeyNodeLabel Component
 * Renders a node with its label using pure SVG (no foreignObject)
 */

import { memo } from 'react';
import type { SankeyNodeProps } from './types';
import { SANKEY_CONFIG } from './constants';
import { truncateLabel } from './utils';

export const SankeyNodeLabel = memo(function SankeyNodeLabel({
  node,
  color,
  textColor,
}: SankeyNodeProps) {
  const { label: labelConfig } = SANKEY_CONFIG;

  const nodeHeight = Math.max((node.y1 || 0) - (node.y0 || 0), SANKEY_CONFIG.node.minHeight);
  const nodeWidth = (node.x1 || 0) - (node.x0 || 0);
  const centerY = (node.y0 || 0) + nodeHeight / 2;
  const centerX = (node.x0 || 0) + nodeWidth / 2;

  // Truncate long labels
  const displayLabel = truncateLabel(node.label.replace('BLL ', ''), 20);
  const isBLL = node.kind === 'bll';

  return (
    <g className="sankey-node">
      {/* Node circle/pill */}
      <rect
        x={node.x0}
        y={node.y0}
        width={nodeWidth}
        height={nodeHeight}
        rx={nodeWidth / 2}
        ry={nodeWidth / 2}
        fill="#0f172a"
        stroke={color}
        strokeWidth={1.5}
        style={{
          filter: `drop-shadow(0 0 6px ${color}40)`,
        }}
      />

      {/* Inner line decoration */}
      <line
        x1={centerX}
        y1={(node.y0 || 0) + 4}
        x2={centerX}
        y2={(node.y1 || 0) - 4}
        stroke="rgba(255,255,255,0.2)"
        strokeWidth={2}
        strokeLinecap="round"
      />

      {/* Label background pill */}
      <rect
        x={(node.x0 || 0) + labelConfig.offsetX}
        y={centerY - 10}
        width={Math.min(displayLabel.length * 6.5 + 16, labelConfig.maxWidth)}
        height={20}
        rx={10}
        ry={10}
        fill={isBLL ? '#0f172a' : 'rgba(15, 23, 42, 0.9)'}
        stroke={isBLL ? color : `${color}40`}
        strokeWidth={1}
        style={{
          filter: isBLL ? `drop-shadow(0 0 8px ${color}60)` : undefined,
        }}
      />

      {/* Label text */}
      <text
        x={(node.x0 || 0) + labelConfig.offsetX + 8}
        y={centerY}
        dy="0.35em"
        fill={textColor}
        fontSize={labelConfig.fontSize}
        fontWeight={labelConfig.fontWeight}
        fontFamily="ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace"
        className="select-none"
      >
        {displayLabel}
      </text>
    </g>
  );
});
