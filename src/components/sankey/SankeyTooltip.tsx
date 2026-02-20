/**
 * SankeyTooltip Component
 * Displays hover information for links in the Sankey diagram
 */

import { memo } from 'react';
import type { SankeyTooltipProps } from './types';
import { formatConfidence } from './utils';

export const SankeyTooltip = memo(function SankeyTooltip({ tooltip }: SankeyTooltipProps) {
  if (!tooltip.visible || !tooltip.content) return null;

  const { sourceLabel, targetLabel, confidence, mappingInfo } = tooltip.content;
  const formattedConfidence = formatConfidence(confidence);

  return (
    <div
      className="absolute pointer-events-none z-50 px-3 py-2 bg-[#1e293b]/95 border border-white/20 rounded-lg shadow-xl backdrop-blur-sm"
      style={{
        left: tooltip.x,
        top: tooltip.y,
        transform: 'translate(-50%, -100%)',
      }}
    >
      <div className="text-xs font-medium text-white/90 space-y-1">
        <div className="flex items-center gap-2">
          <span>{sourceLabel}</span>
          <span className="text-white/50">→</span>
          <span>{targetLabel}</span>
        </div>
        {mappingInfo && (
          <div className="text-white/70 text-[10px]">{mappingInfo}</div>
        )}
        {formattedConfidence && (
          <div className="text-white/70 text-[10px]">
            Confidence: {formattedConfidence}
          </div>
        )}
      </div>
      {/* Tooltip arrow */}
      <div className="absolute left-1/2 -bottom-1 -translate-x-1/2 w-2 h-2 bg-[#1e293b]/95 border-b border-r border-white/20 rotate-45" />
    </div>
  );
});
