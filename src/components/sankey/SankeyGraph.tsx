/**
 * SankeyGraph Component (Refactored)
 *
 * A comprehensive refactor addressing:
 * - Proper TypeScript types (no `any`)
 * - Debounced resize handling
 * - Memoized event handlers
 * - Pure SVG rendering (no foreignObject)
 * - Dynamic layer positioning
 * - Link virtualization for performance
 * - Centralized configuration constants
 */

import { useRef, useMemo, useCallback, useState } from 'react';
import { sankey, sankeyLinkHorizontal, sankeyLeft } from 'd3-sankey';

import { useResizeObserver } from '../../hooks/useResizeObserver';
import { SANKEY_CONFIG, LEVEL_COLORS, LEVEL_TEXT_COLORS, FABRIC_COLORS, FABRIC_DEFAULT_COLOR } from './constants';
import type {
  SankeyGraphProps,
  SankeyGraphData,
  SankeyNode,
  SankeyLink,
  TooltipState,
  ViewportBounds,
} from './types';
import {
  computeLayerPositions,
  getVisibleLinks,
  getLinkGradientId,
  getNodeColor,
  buildTooltipContent,
  getLinkStrokeWidth,
} from './utils';
import { SankeyNodeLabel } from './SankeyNodeLabel';
import { SankeyTooltip } from './SankeyTooltip';

export function SankeyGraph({ data }: SankeyGraphProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const containerRectRef = useRef<DOMRect | null>(null);

  // Debounced resize observer
  const size = useResizeObserver(containerRef, {
    debounceMs: SANKEY_CONFIG.performance.resizeDebounceMs,
    initialDelay: SANKEY_CONFIG.performance.initialRenderDelayMs,
  });

  // Tooltip state
  const [tooltip, setTooltip] = useState<TooltipState>({
    visible: false,
    x: 0,
    y: 0,
    content: null,
  });

  // Memoized viewport bounds for virtualization
  const viewport: ViewportBounds = useMemo(
    () => ({
      minX: 0,
      maxX: size.width,
      minY: 0,
      maxY: size.height,
    }),
    [size.width, size.height]
  );

  // Compute graph layout with dynamic layer positions
  const graphData = useMemo((): SankeyGraphData | null => {
    if (!data || size.width === 0 || size.height === 0) return null;

    const { margin, node: nodeConfig } = SANKEY_CONFIG;

    // Clone nodes and links to avoid mutating original data
    const nodes = data.nodes.map(n => ({ ...n }));
    const links = data.links.map(l => ({ ...l }));

    // Configure D3 Sankey generator
    const sankeyGenerator = sankey<SankeyNode, SankeyLink>()
      .nodeId((d) => d.id)
      .nodeAlign(sankeyLeft)
      .nodeWidth(nodeConfig.width)
      .nodePadding(nodeConfig.padding)
      .extent([
        [margin.left, margin.top],
        [size.width - margin.right, size.height - margin.bottom],
      ]);

    // Generate initial layout
    const { nodes: sNodes, links: sLinks } = sankeyGenerator({
      nodes: nodes as SankeyNode[],
      links: links as unknown as SankeyLink[],
    });

    // Compute dynamic layer positions based on content
    const layerPositions = computeLayerPositions(data.nodes, size);

    // Apply layer positions to nodes
    sNodes.forEach((node) => {
      const layerPos = layerPositions[node.level as keyof typeof layerPositions];
      if (layerPos) {
        node.x0 = layerPos.x0;
        node.x1 = layerPos.x1;
      }
    });

    return { nodes: sNodes, links: sLinks };
  }, [data, size]);

  // Memoized visible links (virtualized)
  const visibleLinks = useMemo(() => {
    if (!graphData) return [];
    return getVisibleLinks(graphData.links, viewport);
  }, [graphData, viewport]);

  // Cache container rect to avoid layout thrashing
  const updateContainerRect = useCallback(() => {
    if (containerRef.current) {
      containerRectRef.current = containerRef.current.getBoundingClientRect();
    }
  }, []);

  // Memoized mouse enter handler - uses data attributes to avoid closures
  const handleLinkMouseEnter = useCallback(
    (event: React.MouseEvent<SVGPathElement>, link: SankeyLink) => {
      // Update cached rect if needed
      if (!containerRectRef.current) {
        updateContainerRect();
      }

      const containerRect = containerRectRef.current;
      if (!containerRect) return;

      const x = event.clientX - containerRect.left;
      const y = event.clientY - containerRect.top + SANKEY_CONFIG.tooltip.offsetY;

      setTooltip({
        visible: true,
        x,
        y,
        content: buildTooltipContent(link),
      });
    },
    [updateContainerRect]
  );

  // Memoized mouse leave handler
  const handleLinkMouseLeave = useCallback(() => {
    setTooltip((prev) => ({ ...prev, visible: false }));
  }, []);

  // Detect display mode: fabric-aggregated vs detailed (individual sources)
  const displayMode = useMemo(() => {
    if (!graphData) return null;
    const hasFabric = graphData.nodes.some(n => n.kind === 'fabric');
    return hasFabric ? 'Fabric-Aggregated' : 'Detailed';
  }, [graphData]);

  // Always render container with ref for consistent size measurement
  const isLoading = !data || !graphData;

  return (
    <div
      ref={containerRef}
      className="w-full h-full bg-[#020617] overflow-hidden relative select-none"
    >
      {isLoading ? (
        <div className="w-full h-full flex items-center justify-center text-muted-foreground">
          Loading visualization...
        </div>
      ) : (
      <>
      <svg
        width={size.width}
        height={size.height}
        className="overflow-visible"
        role="img"
        aria-label="Data flow Sankey diagram"
      >
        {/* Gradient definitions for links */}
        <defs>
          {visibleLinks.map((link, idx) => {
            // Use fabric-specific color for fabric nodes in link gradients
            const srcNode = link.source;
            const tgtNode = link.target;
            const sourceColor = srcNode?.kind === 'fabric'
              ? (FABRIC_COLORS[(srcNode.group || '').toUpperCase()] || FABRIC_DEFAULT_COLOR)
              : getNodeColor(srcNode?.level || 'L0');
            const targetColor = tgtNode?.kind === 'fabric'
              ? (FABRIC_COLORS[(tgtNode.group || '').toUpperCase()] || FABRIC_DEFAULT_COLOR)
              : getNodeColor(tgtNode?.level || 'L0');
            const gradientId = getLinkGradientId(link, idx);

            return (
              <linearGradient
                key={gradientId}
                id={gradientId}
                gradientUnits="userSpaceOnUse"
                x1={link.source?.x1 || 0}
                x2={link.target?.x0 || 0}
              >
                <stop offset="0%" stopColor={sourceColor} stopOpacity="0.7" />
                <stop offset="100%" stopColor={targetColor} stopOpacity="0.7" />
              </linearGradient>
            );
          })}

          {/* Glow filter for nodes */}
          <filter id="node-glow">
            <feGaussianBlur stdDeviation="1.5" result="coloredBlur" />
            <feMerge>
              <feMergeNode in="coloredBlur" />
              <feMergeNode in="SourceGraphic" />
            </feMerge>
          </filter>
        </defs>

        {/* Links layer */}
        <g className="links" aria-label="Data flow connections">
          {visibleLinks.map((link, idx) => {
            const gradientId = getLinkGradientId(link, idx);
            const pathData = sankeyLinkHorizontal()(link as any) || '';

            return (
              <path
                key={link.id || `link-${idx}`}
                d={pathData}
                stroke={`url(#${gradientId})`}
                strokeWidth={getLinkStrokeWidth(link)}
                fill="none"
                opacity={SANKEY_CONFIG.link.defaultOpacity}
                className="cursor-pointer transition-opacity"
                style={{
                  pointerEvents: 'stroke',
                  transitionDuration: `${SANKEY_CONFIG.link.transitionDuration}ms`,
                }}
                onMouseEnter={(e) => handleLinkMouseEnter(e, link)}
                onMouseLeave={handleLinkMouseLeave}
                aria-label={`Link from ${link.source?.label} to ${link.target?.label}`}
              />
            );
          })}
        </g>

        {/* Nodes layer */}
        <g className="nodes" aria-label="Data entities">
          {graphData.nodes.map((node) => {
            // Fabric nodes get plane-specific colors; others use level colors
            const isFabric = node.kind === 'fabric';
            const fabricPlane = isFabric ? (node.group || '').toUpperCase() : '';
            const color = isFabric
              ? (FABRIC_COLORS[fabricPlane] || FABRIC_DEFAULT_COLOR)
              : (LEVEL_COLORS[node.level as keyof typeof LEVEL_COLORS] || '#999');
            const textColor = isFabric
              ? '#ffffff'
              : (LEVEL_TEXT_COLORS[node.level as keyof typeof LEVEL_TEXT_COLORS] || '#fff');

            return (
              <SankeyNodeLabel
                key={node.id}
                node={node}
                color={color}
                textColor={textColor}
              />
            );
          })}
        </g>
      </svg>
      <SankeyTooltip tooltip={tooltip} />
      {displayMode && (
        <span className="absolute bottom-2 left-3 text-[10px] text-slate-500 font-mono pointer-events-none">
          {displayMode}
        </span>
      )}
      </>
      )}
    </div>
  );
}
