/**
 * SE-mode Sankey configuration constants.
 */

export const SE_CONFIG = {
  node: { width: 18, padding: 14 },
  margin: { top: 52, right: 50, bottom: 20, left: 50 },
  link: {
    strokeWidth: 2.5,
    internalStrokeWidth: 1.5,
    restOpacity: 0.25,
    hoverOpacity: 0.65,
    internalRestOpacity: 0.15,
    transitionMs: 200,
  },
  label: {
    offsetX: 26,
    fontSize: 11,
    fontWeight: 500,
  },
  tooltip: { offsetY: -12 },
  performance: {
    resizeDebounceMs: 150,
    initialRenderDelayMs: 50,
  },
} as const;

export const LAYER_LABELS = [
  'L0 \u00b7 Sources',
  'L1 \u00b7 DCL',
  'L2 \u00b7 Concepts',
  'L3 \u00b7 Personas',
] as const;

export const NODE_COLORS: Record<string, string> = {
  source: '#14b8a6',
  dcl: '#8b5cf6',
  persona: '#3b82f6',
};

export const DOMAIN_COLORS: Record<string, string> = {
  financial: '#f59e0b',
  hr: '#14b8a6',
  crm: '#3b82f6',
  ops: '#6b7280',
};

export const NODE_TEXT_COLORS: Record<string, string> = {
  source: '#ccfbf1',
  dcl: '#ede9fe',
  financial: '#fef3c7',
  hr: '#ccfbf1',
  crm: '#dbeafe',
  ops: '#e5e7eb',
  persona: '#dbeafe',
};

export const BG_COLOR = '#080d18';

// Backward-compat aliases (index.ts re-exports everything)
export type LayerLevel = 'L0' | 'L1' | 'L2' | 'L3';

export const LEVEL_COLORS = {
  L0: NODE_COLORS.source,
  L1: NODE_COLORS.dcl,
  L2: DOMAIN_COLORS.financial,
  L3: NODE_COLORS.persona,
} as const;

export const LEVEL_TEXT_COLORS = {
  L0: NODE_TEXT_COLORS.source,
  L1: NODE_TEXT_COLORS.dcl,
  L2: NODE_TEXT_COLORS.financial,
  L3: NODE_TEXT_COLORS.persona,
} as const;

export const SANKEY_CONFIG = {
  node: SE_CONFIG.node,
  margin: SE_CONFIG.margin,
  layerPositions: { L0: 0, L1: 0.33, L2: 0.66, L3: 1.0 },
  label: SE_CONFIG.label,
  link: {
    minStrokeWidth: 1,
    defaultOpacity: SE_CONFIG.link.restOpacity,
    hoverOpacity: SE_CONFIG.link.hoverOpacity,
    transitionDuration: SE_CONFIG.link.transitionMs,
  },
  tooltip: SE_CONFIG.tooltip,
  performance: SE_CONFIG.performance,
} as const;

export const FABRIC_COLORS: Record<string, string> = {};
export const FABRIC_DEFAULT_COLOR = '#14b8a6';
