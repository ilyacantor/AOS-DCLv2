/**
 * SE-mode hover tooltip for nodes and links.
 */

import { memo } from 'react';
import type { SankeyTooltipProps } from './types';

export const SankeyTooltip = memo(function SankeyTooltip({ tooltip }: SankeyTooltipProps) {
  if (!tooltip.visible || !tooltip.content) return null;

  const { title, detail } = tooltip.content;

  return (
    <div
      className="absolute pointer-events-none z-50 px-3 py-2 bg-[#1e293b]/95 border border-white/20 rounded-lg shadow-xl backdrop-blur-sm"
      style={{
        left: tooltip.x,
        top: tooltip.y,
        transform: 'translate(-50%, -100%)',
        maxWidth: 320,
      }}
    >
      <div className="text-xs space-y-1">
        <div className="font-medium text-white/90">{title}</div>
        {detail && (
          <div className="text-[10px] text-white/60 font-mono leading-relaxed">{detail}</div>
        )}
      </div>
      <div className="absolute left-1/2 -bottom-1 -translate-x-1/2 w-2 h-2 bg-[#1e293b]/95 border-b border-r border-white/20 rotate-45" />
    </div>
  );
});
