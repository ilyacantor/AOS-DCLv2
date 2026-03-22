/**
 * SE-mode Sankey layout computation.
 *
 * Positions are computed manually for full control over the 4-column layout.
 * d3-sankey's sankeyLinkHorizontal generates the bezier link paths.
 */

import { sankeyLinkHorizontal } from 'd3-sankey';
import { SE_NODES, SE_LINKS, type SELinkDef } from './data';
import { SE_CONFIG, NODE_COLORS, DOMAIN_COLORS, NODE_TEXT_COLORS } from './constants';
import type { LayoutNode, LayoutLink, SELayout } from './types';

/* d3-sankey link path generator — reads source.x1, target.x0, y0, y1 */
const linkPath = sankeyLinkHorizontal();

/** Compute full SE-mode layout for the given canvas dimensions. */
export function computeSELayout(width: number, height: number): SELayout {
  const { margin, node: nc } = SE_CONFIG;
  const usableW = width - margin.left - margin.right;
  const usableH = height - margin.top - margin.bottom;

  if (usableW <= 0 || usableH <= 0) {
    return { nodes: [], links: [], columnXs: [] };
  }

  // ---- columns: 4 evenly spaced ----
  const colSpan = (usableW - nc.width) / 3;
  const columnXs = [0, 1, 2, 3].map(i => margin.left + i * colSpan);

  // ---- node positions ----
  const layers: (typeof SE_NODES[number])[][] = [[], [], [], []];
  SE_NODES.forEach(n => layers[n.layer].push(n));

  const MAX_NODE_H = 70;
  const nodeMap = new Map<string, LayoutNode>();

  layers.forEach((defs, col) => {
    const count = defs.length;
    const gap = nc.padding;
    const rawH = (usableH - (count - 1) * gap) / count;
    const nh = Math.min(rawH, MAX_NODE_H);
    const totalH = count * nh + (count - 1) * gap;
    const startY = margin.top + (usableH - totalH) / 2;

    defs.forEach((def, idx) => {
      nodeMap.set(def.id, {
        ...def,
        x0: columnXs[col],
        x1: columnXs[col] + nc.width,
        y0: startY + idx * (nh + gap),
        y1: startY + idx * (nh + gap) + nh,
      });
    });
  });

  // ---- per-node link lists (excluding internal) ----
  const outgoing = new Map<string, SELinkDef[]>();
  const incoming = new Map<string, SELinkDef[]>();

  for (const l of SE_LINKS) {
    if (l.type === 'internal') continue;
    if (!outgoing.has(l.source)) outgoing.set(l.source, []);
    outgoing.get(l.source)!.push(l);
    if (!incoming.has(l.target)) incoming.set(l.target, []);
    incoming.get(l.target)!.push(l);
  }

  // sort by connected node y position so links don't cross unnecessarily
  for (const [, list] of outgoing) {
    list.sort((a, b) => nodeMap.get(a.target)!.y0 - nodeMap.get(b.target)!.y0);
  }
  for (const [, list] of incoming) {
    list.sort((a, b) => nodeMap.get(a.source)!.y0 - nodeMap.get(b.source)!.y0);
  }

  // ---- link positions + paths ----
  const layoutLinks: LayoutLink[] = [];
  const PAD = 5; // inset from node top/bottom edge for attachment points

  for (const def of SE_LINKS) {
    const src = nodeMap.get(def.source)!;
    const tgt = nodeMap.get(def.target)!;
    const id = `${def.source}-${def.target}`;

    if (def.type === 'internal') {
      // Right-side arc connecting the two L1 nodes
      const rx = src.x1;
      const sy = src.y1;
      const ty = tgt.y0;
      const midY = (sy + ty) / 2;
      const bulge = 35;

      layoutLinks.push({
        id,
        source: src,
        target: tgt,
        type: def.type,
        hoverContent: def.hoverContent,
        y0: sy,
        y1: ty,
        width: SE_CONFIG.link.internalStrokeWidth,
        path: `M ${rx} ${sy} C ${rx + bulge} ${midY - (ty - sy) * 0.1}, ${rx + bulge} ${midY + (ty - sy) * 0.1}, ${rx} ${ty}`,
      });
      continue;
    }

    // Horizontal link: compute attachment y within source and target nodes
    const outList = outgoing.get(def.source) || [];
    const inList = incoming.get(def.target) || [];
    const oi = outList.indexOf(def);
    const ii = inList.indexOf(def);

    const srcH = src.y1 - src.y0 - 2 * PAD;
    const tgtH = tgt.y1 - tgt.y0 - 2 * PAD;

    const y0 = src.y0 + PAD + (oi + 0.5) * srcH / outList.length;
    const y1 = tgt.y0 + PAD + (ii + 0.5) * tgtH / inList.length;

    // sankeyLinkHorizontal reads source.x1, target.x0, y0, y1
    const pathStr = (linkPath as any)({ source: { x1: src.x1 }, target: { x0: tgt.x0 }, y0, y1 }) || '';

    layoutLinks.push({
      id,
      source: src,
      target: tgt,
      type: def.type,
      hoverContent: def.hoverContent,
      y0,
      y1,
      width: SE_CONFIG.link.strokeWidth,
      path: pathStr,
    });
  }

  return { nodes: Array.from(nodeMap.values()), links: layoutLinks, columnXs };
}

/** Resolve node fill color based on layer and domain. */
export function getNodeColor(node: { layer: number; domain?: string }): string {
  if (node.layer === 0) return NODE_COLORS.source;
  if (node.layer === 1) return NODE_COLORS.dcl;
  if (node.layer === 3) return NODE_COLORS.persona;
  return DOMAIN_COLORS[node.domain || 'financial'] || DOMAIN_COLORS.financial;
}

/** Resolve label text color for readability on dark background. */
export function getNodeTextColor(node: { layer: number; domain?: string }): string {
  if (node.layer === 0) return NODE_TEXT_COLORS.source;
  if (node.layer === 1) return NODE_TEXT_COLORS.dcl;
  if (node.layer === 3) return NODE_TEXT_COLORS.persona;
  return NODE_TEXT_COLORS[node.domain || 'financial'] || NODE_TEXT_COLORS.financial;
}

/** Gradient ID for a layout link. */
export function getLinkGradientId(link: LayoutLink): string {
  return `se-grad-${link.id}`;
}
