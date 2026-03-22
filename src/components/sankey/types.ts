/**
 * SE-mode Sankey type definitions.
 */

import type { PersonaId } from '../../types';
import type { SENodeDef, SELinkType } from './data';
import type { LayerLevel } from './constants';

/** Node with computed layout positions */
export interface LayoutNode extends SENodeDef {
  x0: number;
  x1: number;
  y0: number;
  y1: number;
}

/** Link with computed layout positions and SVG path */
export interface LayoutLink {
  id: string;
  source: LayoutNode;
  target: LayoutNode;
  type: SELinkType;
  hoverContent: string;
  y0: number;
  y1: number;
  width: number;
  path: string;
}

/** Complete computed layout */
export interface SELayout {
  nodes: LayoutNode[];
  links: LayoutLink[];
  columnXs: number[];
}

/** Tooltip display state */
export interface TooltipState {
  visible: boolean;
  x: number;
  y: number;
  content: {
    title: string;
    detail?: string;
    type: 'node' | 'link';
  } | null;
}

/** Props for the main SankeyGraph component */
export interface SankeyGraphProps {
  data: import('../../types').GraphSnapshot | null;
  selectedPersonas: PersonaId[];
}

/** Props for SankeyNodeLabel */
export interface SankeyNodeProps {
  node: LayoutNode;
  color: string;
  textColor: string;
  onMouseEnter: (event: React.MouseEvent, node: LayoutNode) => void;
  onMouseLeave: () => void;
}

/** Props for SankeyTooltip */
export interface SankeyTooltipProps {
  tooltip: TooltipState;
}

// Backward-compat aliases
export type SankeyNode = LayoutNode;
export type SankeyLink = LayoutLink;
export type { LayerLevel };
export interface SankeyGraphData {
  nodes: LayoutNode[];
  links: LayoutLink[];
}
export interface TooltipContent {
  sourceLabel: string;
  targetLabel: string;
  confidence?: string | number;
  mappingInfo?: string;
}
export interface ContainerSize {
  width: number;
  height: number;
}
export interface ViewportBounds {
  minX: number;
  maxX: number;
  minY: number;
  maxY: number;
}
export interface SankeyLinkProps {
  link: LayoutLink;
  index: number;
  onMouseEnter: (event: React.MouseEvent<SVGPathElement>, link: LayoutLink) => void;
  onMouseLeave: () => void;
}
export interface LayerLayout {
  level: LayerLevel;
  nodeCount: number;
  maxLabelWidth: number;
  x0: number;
  x1: number;
}
export interface ComputedLayerPositions {
  L0: { x0: number; x1: number };
  L1: { x0: number; x1: number };
  L2: { x0: number; x1: number };
  L3: { x0: number; x1: number };
}
