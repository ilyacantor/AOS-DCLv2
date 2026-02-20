/**
 * Sankey Graph Utility Functions
 * Helper functions for layout, virtualization, and data transformation
 */

import { SANKEY_CONFIG, LEVEL_COLORS } from './constants';
import type {
  SankeyLink,
  ContainerSize,
  ViewportBounds,
  ComputedLayerPositions,
  TooltipContent,
} from './types';
import type { GraphNode } from '../../types';

/**
 * Compute dynamic layer positions based on available width and content
 */
export function computeLayerPositions(
  nodes: GraphNode[],
  size: ContainerSize
): ComputedLayerPositions {
  const { margin, node: nodeConfig, label } = SANKEY_CONFIG;

  // Calculate max label width per layer (estimate based on label length)
  const estimateLabelWidth = (n: GraphNode) =>
    Math.min(n.label.length * 6.5, label.maxWidth);

  // Safe max that handles empty arrays
  const safeMax = (arr: number[], fallback: number) =>
    arr.length > 0 ? Math.max(...arr) : fallback;

  const maxLabelWidths = {
    L0: safeMax(nodes.filter(n => n.level === 'L0').map(estimateLabelWidth), 40),
    L1: safeMax(nodes.filter(n => n.level === 'L1').map(estimateLabelWidth), 60),
    L2: safeMax(nodes.filter(n => n.level === 'L2').map(estimateLabelWidth), 80),
    L3: safeMax(nodes.filter(n => n.level === 'L3').map(estimateLabelWidth), 60),
  };

  // Minimum spacing between layers to prevent overlap
  const minSpacing = nodeConfig.width + label.offsetX + 20;

  // Calculate positions ensuring no overlap
  const L0_x = margin.left;
  const L3_x = size.width - margin.right - nodeConfig.width - maxLabelWidths.L3;

  // Distribute L1 and L2 in the remaining space
  const middleSpace = L3_x - L0_x - nodeConfig.width - maxLabelWidths.L0 - minSpacing;

  // Weight L2 slightly more since ontology labels tend to be longer
  const L1_x = L0_x + nodeConfig.width + maxLabelWidths.L0 + minSpacing;
  const L2_x = L1_x + Math.max(middleSpace * 0.45, minSpacing + maxLabelWidths.L1);

  return {
    L0: { x0: L0_x, x1: L0_x + nodeConfig.width },
    L1: { x0: L1_x, x1: L1_x + nodeConfig.width },
    L2: { x0: L2_x, x1: L2_x + nodeConfig.width },
    L3: { x0: L3_x, x1: L3_x + nodeConfig.width },
  };
}

/**
 * Check if a link is within the visible viewport (for virtualization)
 */
export function isLinkVisible(
  link: SankeyLink,
  viewport: ViewportBounds,
  padding: number = 50
): boolean {
  const linkMinX = Math.min(link.source.x1, link.target.x0);
  const linkMaxX = Math.max(link.source.x1, link.target.x0);
  const linkMinY = Math.min(link.y0, link.y1) - link.width / 2;
  const linkMaxY = Math.max(link.y0, link.y1) + link.width / 2;

  return (
    linkMaxX >= viewport.minX - padding &&
    linkMinX <= viewport.maxX + padding &&
    linkMaxY >= viewport.minY - padding &&
    linkMinY <= viewport.maxY + padding
  );
}

/**
 * Filter links for virtualization - only render visible links
 */
export function getVisibleLinks(
  links: SankeyLink[],
  viewport: ViewportBounds
): SankeyLink[] {
  const { virtualizationThreshold } = SANKEY_CONFIG.performance;

  // If under threshold, render all links
  if (links.length <= virtualizationThreshold) {
    return links;
  }

  return links.filter(link => isLinkVisible(link, viewport));
}

/**
 * Generate a unique gradient ID for a link
 */
export function getLinkGradientId(link: SankeyLink, index: number): string {
  const sourceId = link.source?.id ?? 'unknown';
  const targetId = link.target?.id ?? 'unknown';
  return `gradient-${link.id || `${sourceId}-${targetId}-${index}`}`;
}

/**
 * Get the color for a node based on its level
 */
export function getNodeColor(level: string): string {
  return LEVEL_COLORS[level as keyof typeof LEVEL_COLORS] || '#999';
}

/**
 * Build structured tooltip content from a link
 */
export function buildTooltipContent(link: SankeyLink): TooltipContent {
  const sourceLabel = link.source?.label || link.source?.id || 'Unknown';
  const targetLabel = link.target?.label || link.target?.id || 'Unknown';
  const confidence = link.confidence;
  const mappingInfo = link.infoSummary;

  return {
    sourceLabel,
    targetLabel,
    confidence,
    mappingInfo,
  };
}

/**
 * Truncate a label to fit within maxWidth (approximate)
 */
export function truncateLabel(label: string, maxChars: number = 18): string {
  if (label.length <= maxChars) return label;
  return `${label.slice(0, maxChars - 1)}…`;
}

/**
 * Calculate the stroke width for a link, ensuring minimum visibility
 */
export function getLinkStrokeWidth(link: SankeyLink): number {
  return Math.max(SANKEY_CONFIG.link.minStrokeWidth, link.width || 1);
}

/**
 * Format confidence value for display
 */
export function formatConfidence(confidence: string | number | undefined): string | null {
  if (confidence === undefined || confidence === null) return null;
  if (typeof confidence === 'number') {
    return `${Math.round(confidence * 100)}%`;
  }
  if (confidence === 'high' || confidence === '') return null;
  return String(confidence);
}
