/**
 * Graph v2 — data-driven Sankey type definitions.
 * No hardcoded topology. All types derive from GraphSnapshot data.
 */

import type { GraphNode } from '../../types';

/** Node with computed layout positions. */
export interface LayoutNodeV2 extends GraphNode {
  /** Numeric column index derived from level ('L0' -> 0, etc.) */
  column: number;
  x0: number;
  x1: number;
  y0: number;
  y1: number;
}

/** Link with computed layout positions and SVG path. */
export interface LayoutLinkV2 {
  id: string;
  source: LayoutNodeV2;
  target: LayoutNodeV2;
  value: number;
  y0: number;
  y1: number;
  width: number;
  path: string;
  infoSummary: string;
}

/** Complete computed layout. */
export interface DataDrivenLayout {
  nodes: LayoutNodeV2[];
  links: LayoutLinkV2[];
  columnXs: number[];
  levelLabels: string[];
}

/** Layout configuration. */
export interface LayoutConfig {
  margin: { top: number; right: number; bottom: number; left: number };
  node: { width: number; padding: number };
  link: { minStrokeWidth: number; maxStrokeWidth: number };
  maxNodeHeight: number;
}

export const DEFAULT_CONFIG: LayoutConfig = {
  margin: { top: 52, right: 50, bottom: 20, left: 50 },
  node: { width: 18, padding: 14 },
  link: { minStrokeWidth: 1.5, maxStrokeWidth: 6 },
  maxNodeHeight: 70,
};
