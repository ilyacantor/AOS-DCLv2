/**
 * Sankey Graph Configuration Constants
 * Centralized configuration for the Sankey visualization
 */

export const SANKEY_CONFIG = {
  // Node dimensions
  node: {
    width: 20,
    padding: 30,
    minHeight: 24,
  },

  // Graph margins
  margin: {
    top: 20,
    right: 60,
    bottom: 20,
    left: 20,
  },

  // Layer X positions as percentages of available width
  // These are base positions that will be adjusted dynamically
  layerPositions: {
    L0: 0.02,   // Pipeline node at 2%
    L1: 0.20,   // Sources at 20%
    L2: 0.55,   // Ontology at 55%
    L3: 0.85,   // BLL/Personas at 85%
  },

  // Label configuration
  label: {
    offsetX: 28,           // Offset from node edge
    maxWidth: 120,         // Max label width before truncation
    fontSize: 10,
    fontWeight: 500,
  },

  // Link styling
  link: {
    minStrokeWidth: 1,
    defaultOpacity: 0.6,
    hoverOpacity: 1,
    transitionDuration: 150,
  },

  // Tooltip configuration
  tooltip: {
    offsetY: -10,
  },

  // Performance settings
  performance: {
    resizeDebounceMs: 150,
    initialRenderDelayMs: 50,
    virtualizationThreshold: 100, // Links beyond this count get virtualized
  },
} as const;

// Color scheme for each layer level
export const LEVEL_COLORS = {
  L0: '#10b981', // Emerald - Pipeline
  L1: '#10b981', // Emerald - Sources
  L2: '#06b6d4', // Cyan - Ontology
  L3: '#8b5cf6', // Violet - BLL/Personas
} as const;

// Text colors corresponding to each level (for readability on dark bg)
export const LEVEL_TEXT_COLORS = {
  L0: '#d1fae5', // Emerald light
  L1: '#d1fae5', // Emerald light
  L2: '#cffafe', // Cyan light
  L3: '#ede9fe', // Violet light
} as const;

// Colors for fabric plane nodes (AAM mode aggregation)
export const FABRIC_COLORS: Record<string, string> = {
  IPAAS: '#f59e0b',          // Amber
  API_GATEWAY: '#ec4899',    // Pink
  EVENT_BUS: '#8b5cf6',      // Violet
  DATA_WAREHOUSE: '#06b6d4', // Cyan
  UNMAPPED: '#6b7280',       // Gray
} as const;

export const FABRIC_DEFAULT_COLOR = '#10b981'; // Emerald fallback

export type LayerLevel = keyof typeof LEVEL_COLORS;
