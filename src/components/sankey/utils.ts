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
import type { GraphSnapshot } from '../../types';

/* d3-sankey link path generator \u2014 reads source.x1, target.x0, y0, y1 */
const linkPath = sankeyLinkHorizontal();

/** Extract the string ID from a GraphLink source/target (may be string or object). */
function linkNodeId(ref: string | { id: string }): string {
  return typeof ref === 'string' ? ref : ref.id;
}

/**
 * Extract real triple-count values from a GraphSnapshot and map them
 * to SE layout link IDs.
 *
 * Backend link patterns:
 *   source_{system} \u2192 ontology_{domain}  \u2192 maps to SE "semantic_map-{domain}"
 *   ontology_{domain} \u2192 bll_{persona}    \u2192 maps to SE "{domain}-{persona}"
 *   pipe_farm \u2192 source_{system}          \u2192 aggregated for L0\u2192L1
 *
 * Returns a Map<seLinkId, tripleCount> for links that have data.
 */
export function extractLinkValues(data: GraphSnapshot | null): Map<string, number> {
  const values = new Map<string, number>();
  if (!data?.links?.length) return values;

  // Per-domain totals for L1\u2192L2 (semantic_map \u2192 concept)
  const domainTotals = new Map<string, number>();
  // Per-(domain, persona) for L2\u2192L3
  // Total ingest volume for L0\u2192L1
  let totalIngest = 0;

  for (const link of data.links) {
    const srcId = linkNodeId(link.source);
    const tgtId = linkNodeId(link.target);
    const val = link.value;

    if (srcId.startsWith('source_') && tgtId.startsWith('ontology_')) {
      // source\u2192domain: aggregate by domain for L1\u2192L2
      const domain = tgtId.replace('ontology_', '');
      domainTotals.set(domain, (domainTotals.get(domain) || 0) + val);
    } else if (srcId.startsWith('ontology_') && tgtId.startsWith('bll_')) {
      // domain\u2192persona: maps to L2\u2192L3
      const domain = srcId.replace('ontology_', '');
      const persona = tgtId.replace('bll_', '');
      values.set(`${domain}-${persona}`, val);
    } else if (srcId.startsWith('pipe_') && tgtId.startsWith('source_')) {
      // pipe\u2192source: accumulate total ingest
      totalIngest += val;
    }
  }

  // Write domain totals as semantic_map\u2192concept links
  for (const [domain, total] of domainTotals) {
    values.set(`semantic_map-${domain}`, total);
  }

  // Distribute total ingest uniformly across L0\u2192L1 links
  if (totalIngest > 0) {
    const ingestLinks = SE_LINKS.filter(l => l.type === 'ingest');
    const perSource = totalIngest / ingestLinks.length;
    for (const l of ingestLinks) {
      values.set(`${l.source}-${l.target}`, perSource);
    }
  }

  return values;
}

/** Scale a triple-count value to a stroke width between min and max bounds. */
function scaleWidth(value: number, minVal: number, maxVal: number): number {
  const { minStrokeWidth, maxStrokeWidth } = SE_CONFIG.link;
  const range = maxVal - minVal || 1;
  return minStrokeWidth + ((value - minVal) / range) * (maxStrokeWidth - minStrokeWidth);
}

/** Compute full SE-mode layout for the given canvas dimensions. */
export function computeSELayout(
  width: number,
  height: number,
  linkValues: Map<string, number>,
): SELayout {
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

  // ---- compute proportional link widths from real data ----
  const hasData = linkValues.size > 0;
  const widthMap = new Map<string, number>();

  if (hasData) {
    // Collect all values that map to SE links
    const mappedValues: number[] = [];
    for (const def of SE_LINKS) {
      if (def.type === 'internal') continue;
      const key = `${def.source}-${def.target}`;
      const val = linkValues.get(key);
      if (val != null) mappedValues.push(val);
    }

    if (mappedValues.length > 0) {
      const minVal = Math.min(...mappedValues);
      const maxVal = Math.max(...mappedValues);

      for (const def of SE_LINKS) {
        if (def.type === 'internal') continue;
        const key = `${def.source}-${def.target}`;
        const val = linkValues.get(key);
        widthMap.set(key, val != null ? scaleWidth(val, minVal, maxVal) : SE_CONFIG.link.strokeWidth);
      }
    }
  }

  // Fallback: no data yet, uniform widths
  if (widthMap.size === 0) {
    for (const def of SE_LINKS) {
      if (def.type === 'internal') continue;
      widthMap.set(`${def.source}-${def.target}`, SE_CONFIG.link.strokeWidth);
    }
  }

  // ---- width-aware stacking offsets per node face ----
  const LINK_GAP = 1;
  const PAD = 5;

  function computeStackOffsets(linkDefs: SELinkDef[], nodeId: string): Map<string, number> {
    const node = nodeMap.get(nodeId)!;
    const nodeH = node.y1 - node.y0 - 2 * PAD;
    const offsets = new Map<string, number>();

    const totalWidth = linkDefs.reduce((sum, d) => {
      return sum + widthMap.get(`${d.source}-${d.target}`)!;
    }, 0);
    const totalGap = Math.max(0, linkDefs.length - 1) * LINK_GAP;
    let stackH = totalWidth + totalGap;

    // If stack overflows node, compress proportionally
    const scale = stackH > nodeH ? nodeH / stackH : 1;
    stackH = Math.min(stackH, nodeH);

    let cursor = node.y0 + PAD + (nodeH - stackH) / 2;

    for (const def of linkDefs) {
      const key = `${def.source}-${def.target}`;
      const w = widthMap.get(key)! * scale;
      offsets.set(key, cursor + w / 2);
      cursor += w + LINK_GAP * scale;
    }
    return offsets;
  }

  const srcOffsets = new Map<string, Map<string, number>>();
  const tgtOffsets = new Map<string, Map<string, number>>();

  for (const [nodeId, list] of outgoing) {
    srcOffsets.set(nodeId, computeStackOffsets(list, nodeId));
  }
  for (const [nodeId, list] of incoming) {
    tgtOffsets.set(nodeId, computeStackOffsets(list, nodeId));
  }

  // ---- link positions + paths ----
  const layoutLinks: LayoutLink[] = [];

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

    const linkWidth = widthMap.get(id)!;
    const y0 = srcOffsets.get(def.source)!.get(id)!;
    const y1 = tgtOffsets.get(def.target)!.get(id)!;

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
      width: linkWidth,
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
