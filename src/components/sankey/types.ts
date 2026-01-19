/**
 * Sankey Graph Type Definitions
 * Properly typed interfaces for the Sankey visualization
 */

import type { GraphNode, PersonaId } from '../../types';
import type { LayerLevel } from './constants';

/**
 * Node after D3 Sankey processing with computed positions
 */
export interface SankeyNode extends GraphNode {
  x0: number;
  x1: number;
  y0: number;
  y1: number;
  sourceLinks: SankeyLink[];
  targetLinks: SankeyLink[];
  index: number;
  depth: number;
  height: number;
  layer: number;
  value: number;
}

/**
 * Link after D3 Sankey processing with resolved source/target
 */
export interface SankeyLink {
  id: string;
  source: SankeyNode;
  target: SankeyNode;
  value: number;
  width: number;
  y0: number;
  y1: number;
  index: number;
  confidence?: number | string;
  flowType?: string;
  flow_type?: string;
  infoSummary?: string;
  info_summary?: string;
}

/**
 * Processed graph data ready for rendering
 */
export interface SankeyGraphData {
  nodes: SankeyNode[];
  links: SankeyLink[];
}

/**
 * Container size for responsive rendering
 */
export interface ContainerSize {
  width: number;
  height: number;
}

/**
 * Tooltip state for hover interactions
 */
export interface TooltipState {
  visible: boolean;
  x: number;
  y: number;
  content: TooltipContent | null;
}

/**
 * Structured tooltip content (replaces string concatenation)
 */
export interface TooltipContent {
  sourceLabel: string;
  targetLabel: string;
  confidence?: string | number;
  mappingInfo?: string;
}

/**
 * Viewport bounds for virtualization
 */
export interface ViewportBounds {
  minX: number;
  maxX: number;
  minY: number;
  maxY: number;
}

/**
 * Props for the main SankeyGraph component
 */
export interface SankeyGraphProps {
  data: import('../../types').GraphSnapshot | null;
  selectedPersonas: PersonaId[];
}

/**
 * Props for the SankeyLink subcomponent
 */
export interface SankeyLinkProps {
  link: SankeyLink;
  index: number;
  onMouseEnter: (event: React.MouseEvent<SVGPathElement>, link: SankeyLink) => void;
  onMouseLeave: () => void;
}

/**
 * Props for the SankeyNode subcomponent
 */
export interface SankeyNodeProps {
  node: SankeyNode;
  color: string;
  textColor: string;
}

/**
 * Props for the SankeyTooltip subcomponent
 */
export interface SankeyTooltipProps {
  tooltip: TooltipState;
}

/**
 * Layer layout information for dynamic positioning
 */
export interface LayerLayout {
  level: LayerLevel;
  nodeCount: number;
  maxLabelWidth: number;
  x0: number;
  x1: number;
}

/**
 * Result of computing dynamic layer positions
 */
export interface ComputedLayerPositions {
  L0: { x0: number; x1: number };
  L1: { x0: number; x1: number };
  L2: { x0: number; x1: number };
  L3: { x0: number; x1: number };
}
