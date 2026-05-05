/**
 * SE-mode Sankey Graph — 4-layer data flow through DCL.
 *
 * L0 Sources → L1 DCL Processing → L2 Ontology Concepts → L3 Personas
 *
 * Uses d3-sankey (sankeyLinkHorizontal) for link paths.
 * Column x-positions forced explicitly; nodeSort: null (order preserved).
 */

import { useRef, useMemo, useCallback, useState } from 'react';
import { useResizeObserver } from '../../hooks/useResizeObserver';
import { SE_CONFIG, LAYER_LABELS, BG_COLOR } from './constants';
import { computeSELayout, extractLinkValues, getNodeColor, getNodeTextColor, getLinkGradientId } from './utils';
import { SankeyNodeLabel } from './SankeyNodeLabel';
import { SankeyTooltip } from './SankeyTooltip';
import type { SankeyGraphProps, TooltipState, LayoutNode, LayoutLink } from './types';

export function SankeyGraph({ data }: SankeyGraphProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const containerRectRef = useRef<DOMRect | null>(null);

  const size = useResizeObserver(containerRef, {
    debounceMs: SE_CONFIG.performance.resizeDebounceMs,
    initialDelay: SE_CONFIG.performance.initialRenderDelayMs,
  });

  const [tooltip, setTooltip] = useState<TooltipState>({
    visible: false,
    x: 0,
    y: 0,
    content: null,
  });
  const [hoveredLinkId, setHoveredLinkId] = useState<string | null>(null);

  const linkValues = useMemo(() => extractLinkValues(data), [data]);

  const layout = useMemo(() => {
    if (size.width === 0 || size.height === 0) return null;
    return computeSELayout(size.width, size.height, linkValues);
  }, [size.width, size.height, linkValues]);

  const updateRect = useCallback(() => {
    if (containerRef.current) {
      containerRectRef.current = containerRef.current.getBoundingClientRect();
    }
  }, []);

  const showTooltip = useCallback(
    (event: React.MouseEvent, title: string, detail?: string, type: 'node' | 'link' = 'link') => {
      if (!containerRectRef.current) updateRect();
      const rect = containerRectRef.current;
      if (!rect) return;
      setTooltip({
        visible: true,
        x: event.clientX - rect.left,
        y: event.clientY - rect.top + SE_CONFIG.tooltip.offsetY,
        content: { title, detail, type },
      });
    },
    [updateRect],
  );

  const hideTooltip = useCallback(() => {
    setTooltip(prev => ({ ...prev, visible: false }));
    setHoveredLinkId(null);
  }, []);

  const handleLinkEnter = useCallback(
    (event: React.MouseEvent, link: LayoutLink) => {
      setHoveredLinkId(link.id);
      showTooltip(event, `${link.source.label} \u2192 ${link.target.label}`, link.hoverContent, 'link');
    },
    [showTooltip],
  );

  const handleLinkMove = useCallback(
    (event: React.MouseEvent) => {
      if (!containerRectRef.current) updateRect();
      const rect = containerRectRef.current;
      if (!rect) return;
      setTooltip(prev => ({
        ...prev,
        x: event.clientX - rect.left,
        y: event.clientY - rect.top + SE_CONFIG.tooltip.offsetY,
      }));
    },
    [updateRect],
  );

  const handleNodeEnter = useCallback(
    (event: React.MouseEvent, node: LayoutNode) => {
      if (node.hoverContent) {
        showTooltip(event, node.label, node.hoverContent, 'node');
      }
    },
    [showTooltip],
  );

  // Loading state — show spinner until container is measured
  if (!layout || layout.nodes.length === 0) {
    return (
      <div
        ref={containerRef}
        className="w-full h-full overflow-hidden relative select-none"
        style={{ backgroundColor: BG_COLOR }}
      >
        <div className="w-full h-full flex items-center justify-center">
          <div className="w-5 h-5 border-2 border-slate-600 border-t-slate-400 rounded-full animate-spin" />
        </div>
      </div>
    );
  }

  return (
    <div
      ref={containerRef}
      className="w-full h-full overflow-hidden relative select-none"
      style={{ backgroundColor: BG_COLOR }}
    >
      <svg
        width={size.width}
        height={size.height}
        className="overflow-visible"
        role="img"
        aria-label="SE mode data flow through DCL"
      >
        <defs>
          {/* Gradients for horizontal links (skip internal) */}
          {layout.links
            .filter(l => l.type !== 'internal')
            .map(link => {
              const srcColor = getNodeColor(link.source);
              const tgtColor = getNodeColor(link.target);
              const gid = getLinkGradientId(link);
              return (
                <linearGradient
                  key={gid}
                  id={gid}
                  gradientUnits="userSpaceOnUse"
                  x1={link.source.x1}
                  x2={link.target.x0}
                >
                  <stop offset="0%" stopColor={srcColor} stopOpacity="0.8" />
                  <stop offset="100%" stopColor={tgtColor} stopOpacity="0.8" />
                </linearGradient>
              );
            })}

          {/* Glow filter for nodes */}
          <filter id="node-glow">
            <feGaussianBlur stdDeviation="2" result="blur" />
            <feMerge>
              <feMergeNode in="blur" />
              <feMergeNode in="SourceGraphic" />
            </feMerge>
          </filter>
        </defs>

        {/* Layer labels */}
        {LAYER_LABELS.map((label, idx) => (
          <text
            key={label}
            x={layout.columnXs[idx] + SE_CONFIG.node.width / 2}
            y={SE_CONFIG.margin.top - 16}
            textAnchor="middle"
            fill="rgba(148, 163, 184, 0.7)"
            fontSize={10}
            fontFamily="ui-monospace, SFMono-Regular, Menlo, monospace"
            fontWeight={500}
            letterSpacing="0.05em"
          >
            {label}
          </text>
        ))}

        {/* Links */}
        <g aria-label="Data flow connections">
          {layout.links.map(link => {
            const isHovered = hoveredLinkId === link.id;
            const isInternal = link.type === 'internal';
            // Value-weighted opacity: high-flow visible at rest, low-flow recedes; floor 0.15 keeps thin links visible.
            const wRange = SE_CONFIG.link.maxStrokeWidth - SE_CONFIG.link.minStrokeWidth;
            const norm = wRange > 0
              ? Math.max(0, Math.min(1, (link.width - SE_CONFIG.link.minStrokeWidth) / wRange))
              : 0;
            const baseOpacity = isInternal
              ? SE_CONFIG.link.internalRestOpacity
              : Math.min(0.6, 0.15 + 0.45 * norm);
            const opacity = isHovered ? SE_CONFIG.link.hoverOpacity : baseOpacity;

            return (
              <path
                key={link.id}
                d={link.path}
                stroke={
                  isInternal
                    ? getNodeColor(link.source)
                    : `url(#${getLinkGradientId(link)})`
                }
                strokeWidth={link.width}
                fill="none"
                opacity={opacity}
                className="cursor-pointer"
                style={{
                  pointerEvents: 'stroke',
                  transition: `opacity ${SE_CONFIG.link.transitionMs}ms`,
                }}
                onMouseEnter={e => handleLinkEnter(e, link)}
                onMouseMove={handleLinkMove}
                onMouseLeave={hideTooltip}
                aria-label={`${link.source.label} to ${link.target.label}`}
              />
            );
          })}
        </g>

        {/* Nodes */}
        <g aria-label="Data entities">
          {layout.nodes.map(node => (
            <SankeyNodeLabel
              key={node.id}
              node={node}
              color={getNodeColor(node)}
              textColor={getNodeTextColor(node)}
              onMouseEnter={handleNodeEnter}
              onMouseLeave={hideTooltip}
            />
          ))}
        </g>
      </svg>

      <SankeyTooltip tooltip={tooltip} />

      {/* Tenant provenance — readable entity names, no UUIDs */}
      {data?.meta?.snapshotName && (
        <span className="absolute bottom-2 right-3 text-[10px] text-slate-500 font-mono pointer-events-none text-right">
          {data.meta.snapshotName}
        </span>
      )}
    </div>
  );
}
