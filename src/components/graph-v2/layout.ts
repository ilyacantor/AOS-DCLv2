/**
 * Data-driven Sankey layout computation.
 *
 * Reads GraphSnapshot nodes/links directly — no static topology.
 * Column assignment by level field. Link widths from real triple counts.
 */

import { sankeyLinkHorizontal } from 'd3-sankey';
import { scaleWidth } from './scale';
import type { GraphNode, GraphLink } from '../../types';
import type { LayoutNodeV2, LayoutLinkV2, DataDrivenLayout, LayoutConfig } from './types';

const linkPath = sankeyLinkHorizontal();

/** Parse level string to numeric column index. 'L0' -> 0, 'L1' -> 1, etc. */
function levelToColumn(level: string): number {
  const match = level.match(/^L(\d+)$/);
  return match ? parseInt(match[1], 10) : 0;
}

/** Resolve a link endpoint to a node ID string. */
function nodeId(ref: string | { id: string }): string {
  return typeof ref === 'string' ? ref : ref.id;
}

export function computeDataDrivenLayout(
  nodes: GraphNode[],
  links: GraphLink[],
  width: number,
  height: number,
  config: LayoutConfig,
): DataDrivenLayout {
  const { margin, node: nc } = config;
  const usableW = width - margin.left - margin.right;
  const usableH = height - margin.top - margin.bottom;

  if (nodes.length === 0 || usableW <= 0 || usableH <= 0) {
    return { nodes: [], links: [], columnXs: [], levelLabels: [] };
  }

  // ── Group nodes by column ──
  const columnMap = new Map<number, GraphNode[]>();
  for (const n of nodes) {
    const col = levelToColumn(n.level);
    if (!columnMap.has(col)) columnMap.set(col, []);
    columnMap.get(col)!.push(n);
  }

  const columns = Array.from(columnMap.keys()).sort((a, b) => a - b);
  const numCols = columns.length;
  if (numCols === 0) {
    return { nodes: [], links: [], columnXs: [], levelLabels: [] };
  }

  // ── Column x-positions ──
  const colSpan = numCols > 1 ? (usableW - nc.width) / (numCols - 1) : 0;
  const columnXs = columns.map((_, i) => margin.left + i * colSpan);

  // Level labels derived from data
  const levelLabels = columns.map(col => `L${col}`);

  // ── Node positions ──
  const nodeMap = new Map<string, LayoutNodeV2>();

  columns.forEach((col, colIdx) => {
    const defs = columnMap.get(col)!;
    const count = defs.length;
    const gap = nc.padding;
    const rawH = (usableH - (count - 1) * gap) / count;
    const nh = Math.min(rawH, config.maxNodeHeight);
    const totalH = count * nh + (count - 1) * gap;
    const startY = margin.top + (usableH - totalH) / 2;

    defs.forEach((def, idx) => {
      nodeMap.set(def.id, {
        ...def,
        column: col,
        x0: columnXs[colIdx],
        x1: columnXs[colIdx] + nc.width,
        y0: startY + idx * (nh + gap),
        y1: startY + idx * (nh + gap) + nh,
      });
    });
  });

  // ── Collect link values for width scaling ──
  const linkValues: number[] = [];
  for (const l of links) {
    if (l.value > 0) linkValues.push(l.value);
  }
  const minVal = linkValues.length > 0 ? Math.min(...linkValues) : 0;
  const maxVal = linkValues.length > 0 ? Math.max(...linkValues) : 0;

  // ── Build per-node link lists for stacking ──
  const outgoing = new Map<string, GraphLink[]>();
  const incoming = new Map<string, GraphLink[]>();

  for (const l of links) {
    const srcId = nodeId(l.source);
    const tgtId = nodeId(l.target);
    if (!nodeMap.has(srcId) || !nodeMap.has(tgtId)) continue;
    if (!outgoing.has(srcId)) outgoing.set(srcId, []);
    outgoing.get(srcId)!.push(l);
    if (!incoming.has(tgtId)) incoming.set(tgtId, []);
    incoming.get(tgtId)!.push(l);
  }

  // Sort by connected node y position to minimize crossings
  for (const [, list] of outgoing) {
    list.sort((a, b) => nodeMap.get(nodeId(a.target))!.y0 - nodeMap.get(nodeId(b.target))!.y0);
  }
  for (const [, list] of incoming) {
    list.sort((a, b) => nodeMap.get(nodeId(a.source))!.y0 - nodeMap.get(nodeId(b.source))!.y0);
  }

  // ── Compute widths ──
  const { minStrokeWidth, maxStrokeWidth } = config.link;
  const widthMap = new Map<string, number>();
  for (const l of links) {
    const w = l.value > 0 && linkValues.length > 0
      ? scaleWidth(l.value, minVal, maxVal, minStrokeWidth, maxStrokeWidth)
      : minStrokeWidth;
    widthMap.set(l.id, w);
  }

  // ── Width-aware stacking offsets per node face ──
  const LINK_GAP = 1;
  const PAD = 5;

  function computeStackOffsets(linkDefs: GraphLink[], nId: string): Map<string, number> {
    const node = nodeMap.get(nId)!;
    const nodeH = node.y1 - node.y0 - 2 * PAD;
    const offsets = new Map<string, number>();

    const totalWidth = linkDefs.reduce((sum, l) => sum + (widthMap.get(l.id) ?? minStrokeWidth), 0);
    const totalGap = Math.max(0, linkDefs.length - 1) * LINK_GAP;
    let stackH = totalWidth + totalGap;

    const scale = stackH > nodeH ? nodeH / stackH : 1;
    stackH = Math.min(stackH, nodeH);

    let cursor = node.y0 + PAD + (nodeH - stackH) / 2;

    for (const l of linkDefs) {
      const w = (widthMap.get(l.id) ?? minStrokeWidth) * scale;
      offsets.set(l.id, cursor + w / 2);
      cursor += w + LINK_GAP * scale;
    }
    return offsets;
  }

  const srcOffsets = new Map<string, Map<string, number>>();
  const tgtOffsets = new Map<string, Map<string, number>>();

  for (const [nId, list] of outgoing) {
    srcOffsets.set(nId, computeStackOffsets(list, nId));
  }
  for (const [nId, list] of incoming) {
    tgtOffsets.set(nId, computeStackOffsets(list, nId));
  }

  // ── Build layout links ──
  const layoutLinks: LayoutLinkV2[] = [];

  for (const l of links) {
    const srcId = nodeId(l.source);
    const tgtId = nodeId(l.target);
    const src = nodeMap.get(srcId);
    const tgt = nodeMap.get(tgtId);
    if (!src || !tgt) continue;

    const linkWidth = widthMap.get(l.id) ?? minStrokeWidth;
    const y0 = srcOffsets.get(srcId)?.get(l.id) ?? (src.y0 + src.y1) / 2;
    const y1 = tgtOffsets.get(tgtId)?.get(l.id) ?? (tgt.y0 + tgt.y1) / 2;

    const pathStr = (linkPath as (d: unknown) => string | null)({
      source: { x1: src.x1 },
      target: { x0: tgt.x0 },
      y0,
      y1,
    }) ?? '';

    layoutLinks.push({
      id: l.id,
      source: src,
      target: tgt,
      value: l.value,
      y0,
      y1,
      width: linkWidth,
      path: pathStr,
      infoSummary: l.infoSummary ?? `${src.label} \u2192 ${tgt.label}: ${l.value.toLocaleString()} triples`,
    });
  }

  return {
    nodes: Array.from(nodeMap.values()),
    links: layoutLinks,
    columnXs,
    levelLabels,
  };
}
