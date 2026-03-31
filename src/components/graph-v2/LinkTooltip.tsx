/**
 * Graph v2 hover tooltip for nodes and links.
 * Shows real data — triple counts, source/target names from the backend.
 */

import { memo } from 'react';

export interface TooltipState {
  visible: boolean;
  x: number;
  y: number;
  title: string;
  detail?: string;
}

interface LinkTooltipProps {
  tooltip: TooltipState;
}

export const LinkTooltip = memo(function LinkTooltip({ tooltip }: LinkTooltipProps) {
  if (!tooltip.visible) return null;

  return (
    <div
      className="absolute pointer-events-none z-50 px-3 py-2 bg-[#1e293b]/95 border border-white/20 rounded-lg shadow-xl backdrop-blur-sm"
      style={{
        left: tooltip.x,
        top: tooltip.y,
        transform: 'translate(-50%, -100%)',
        maxWidth: 360,
      }}
    >
      <div className="text-xs space-y-1">
        <div className="font-medium text-white/90">{tooltip.title}</div>
        {tooltip.detail && (
          <div className="text-[10px] text-white/60 font-mono leading-relaxed">{tooltip.detail}</div>
        )}
      </div>
      <div className="absolute left-1/2 -bottom-1 -translate-x-1/2 w-2 h-2 bg-[#1e293b]/95 border-b border-r border-white/20 rotate-45" />
    </div>
  );
});
