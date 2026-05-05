/**
 * Graph v2 — Data-driven Sankey visualization.
 *
 * Renders nodes and links directly from GraphSnapshot.
 * No hardcoded topology. No static node lists. No lossy bridge.
 */

import { useRef, useMemo, useCallback, useState } from 'react';
import { useResizeObserver } from '../../hooks/useResizeObserver';
import { computeDataDrivenLayout } from './layout';
import { NodeLabel } from './NodeLabel';
import { LinkTooltip, type TooltipState } from './LinkTooltip';
import { DEFAULT_CONFIG } from './types';
import type { GraphSnapshot, GraphNode, PersonaId } from '../../types';
import type { LayoutNodeV2, LayoutLinkV2 } from './types';

const BG_COLOR = '#060a14';
const TOOLTIP_OFFSET_Y = -12;
const DEBOUNCE_MS = 150;
const INITIAL_DELAY_MS = 50;

/** Node color by kind — no hardcoded domain names. */
function getNodeColor(node: LayoutNodeV2): string {
  switch (node.kind) {
    case 'pipe': return '#3730a3';
    case 'source': return node.status === 'stub' ? '#334155' : '#1e3a8a';
    case 'ontology': return '#0e7490';
    case 'bll': return '#a5b4fc';
    default: return '#475569';
  }
}

function getNodeTextColor(node: LayoutNodeV2): string {
  switch (node.kind) {
    case 'pipe': return '#ddd6fe';
    case 'source': return node.status === 'stub' ? '#94a3b8' : '#c7d2fe';
    case 'ontology': return '#cffafe';
    case 'bll': return '#1e1b4b';
    default: return '#e2e8f0';
  }
}

/** Build hover detail for a node from its metrics and connected links. */
function buildNodeDetail(node: LayoutNodeV2): string {
  const parts: string[] = [];
  const tc = node.metrics?.triple_count;
  if (typeof tc === 'number') {
    parts.push(`${tc.toLocaleString()} triples`);
  }
  if (node.status === 'stub') {
    parts.push('registered, no data');
  }
  const vendor = node.metrics?.vendor;
  if (typeof vendor === 'string' && vendor) {
    parts.push(vendor);
  }
  return parts.join(' \u00b7 ');
}

interface DataDrivenSankeyProps {
  data: GraphSnapshot;
  selectedPersonas?: PersonaId[];
}

export function DataDrivenSankey({ data, selectedPersonas }: DataDrivenSankeyProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const containerRectRef = useRef<DOMRect | null>(null);

  const size = useResizeObserver(containerRef, {
    debounceMs: DEBOUNCE_MS,
    initialDelay: INITIAL_DELAY_MS,
  });

  const [tooltip, setTooltip] = useState<TooltipState>({
    visible: false,
    x: 0,
    y: 0,
    title: '',
  });
  const [hoveredLinkId, setHoveredLinkId] = useState<string | null>(null);

  const { nodes: filteredNodes, links: filteredLinks } = useMemo(() => {
    const allBll = data.nodes.filter(n => n.kind === 'bll');
    const allowedBll = new Set(
      (selectedPersonas && selectedPersonas.length < allBll.length
        ? selectedPersonas
        : []
      ).map(p => `bll_${p.toLowerCase()}`),
    );
    if (allowedBll.size === 0) return { nodes: data.nodes, links: data.links };

    const lid = (v: string | GraphNode): string => typeof v === 'string' ? v : v.id;
    const keepNodes = new Set(allowedBll);
    const links = data.links.filter(l => {
      const tid = lid(l.target);
      if (!keepNodes.has(tid) && data.nodes.find(n => n.id === tid)?.kind === 'bll') return false;
      return true;
    });
    for (const l of links) { keepNodes.add(lid(l.source)); keepNodes.add(lid(l.target)); }
    const nodes = data.nodes.filter(n => keepNodes.has(n.id));
    const nodeSet = new Set(nodes.map(n => n.id));
    return { nodes, links: links.filter(l => nodeSet.has(lid(l.source)) && nodeSet.has(lid(l.target))) };
  }, [data.nodes, data.links, selectedPersonas]);

  const layout = useMemo(() => {
    if (size.width === 0 || size.height === 0) return null;
    return computeDataDrivenLayout(filteredNodes, filteredLinks, size.width, size.height, DEFAULT_CONFIG);
  }, [filteredNodes, filteredLinks, size.width, size.height]);

  const maxColumn = useMemo(() => {
    if (!layout) return 0;
    return Math.max(0, ...layout.nodes.map(n => n.column));
  }, [layout]);

  const updateRect = useCallback(() => {
    if (containerRef.current) {
      containerRectRef.current = containerRef.current.getBoundingClientRect();
    }
  }, []);

  const showTooltip = useCallback(
    (event: React.MouseEvent, title: string, detail?: string) => {
      if (!containerRectRef.current) updateRect();
      const rect = containerRectRef.current;
      if (!rect) return;
      setTooltip({
        visible: true,
        x: event.clientX - rect.left,
        y: event.clientY - rect.top + TOOLTIP_OFFSET_Y,
        title,
        detail,
      });
    },
    [updateRect],
  );

  const hideTooltip = useCallback(() => {
    setTooltip(prev => ({ ...prev, visible: false }));
    setHoveredLinkId(null);
  }, []);

  const handleLinkEnter = useCallback(
    (event: React.MouseEvent, link: LayoutLinkV2) => {
      setHoveredLinkId(link.id);
      const title = `${link.source.label} \u2192 ${link.target.label}`;
      const detail = link.value > 0
        ? `${link.value.toLocaleString()} triples`
        : 'No data (registered source)';
      const extra = link.infoSummary ? `\n${link.infoSummary}` : '';
      showTooltip(event, title, detail + extra);
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
        y: event.clientY - rect.top + TOOLTIP_OFFSET_Y,
      }));
    },
    [updateRect],
  );

  const handleNodeEnter = useCallback(
    (event: React.MouseEvent, node: LayoutNodeV2) => {
      showTooltip(event, node.label, buildNodeDetail(node));
    },
    [showTooltip],
  );

  // Loading spinner until container is measured
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

  // Build gradient defs for each link
  const gradients = layout.links.map(link => {
    const srcColor = getNodeColor(link.source);
    const tgtColor = getNodeColor(link.target);
    const gid = `v2-grad-${link.id}`;
    return { gid, link, srcColor, tgtColor };
  });

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
        aria-label="Data-driven graph of DCL triple flow"
      >
        <defs>
          {gradients.map(({ gid, link, srcColor, tgtColor }) => (
            <linearGradient
              key={gid}
              id={gid}
              gradientUnits="userSpaceOnUse"
              x1={link.source.x1}
              x2={link.target.x0}
            >
              <stop offset="0%" stopColor={srcColor} stopOpacity="1" />
              <stop offset="100%" stopColor={tgtColor} stopOpacity="1" />
            </linearGradient>
          ))}

          <filter id="v2-node-glow">
            <feGaussianBlur stdDeviation="2" result="blur" />
            <feMerge>
              <feMergeNode in="blur" />
              <feMergeNode in="SourceGraphic" />
            </feMerge>
          </filter>
        </defs>

        {/* Column level labels */}
        {layout.levelLabels.map((label, idx) => (
          <text
            key={label}
            x={layout.columnXs[idx] + DEFAULT_CONFIG.node.width / 2}
            y={DEFAULT_CONFIG.margin.top - 16}
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
            const isZero = link.value === 0;
            const baseOpacity = isZero ? 0.25 : 0.5;
            const opacity = isHovered ? 0.85 : baseOpacity;

            return (
              <path
                key={link.id}
                d={link.path}
                stroke={`url(#v2-grad-${link.id})`}
                strokeWidth={link.width}
                strokeDasharray={isZero ? '4 3' : undefined}
                fill="none"
                opacity={opacity}
                className="cursor-pointer"
                style={{
                  pointerEvents: 'stroke',
                  transition: 'opacity 200ms',
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
            <NodeLabel
              key={node.id}
              node={node}
              color={getNodeColor(node)}
              textColor={getNodeTextColor(node)}
              maxColumn={maxColumn}
              onMouseEnter={handleNodeEnter}
              onMouseLeave={hideTooltip}
            />
          ))}
        </g>
      </svg>

      <LinkTooltip tooltip={tooltip} />

      {/* Snapshot provenance label */}
      {data.meta?.snapshotName && (
        <span className="absolute bottom-2 right-3 text-[10px] text-slate-500 font-mono pointer-events-none text-right">
          {data.meta.snapshotName}
        </span>
      )}
    </div>
  );
}
