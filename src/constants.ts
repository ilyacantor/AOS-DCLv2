/**
 * Centralized constants for the DCL frontend.
 *
 * Dashboard and Monitor intentionally use different confidence thresholds:
 * - Dashboard (CONFIDENCE): stricter grading for business stakeholders
 * - Monitor (MONITOR_CONFIDENCE): more permissive view for technical debugging
 */

export const CONFIDENCE = {
  HIGH: 0.85,
  MEDIUM: 0.60,
} as const;

export const MONITOR_CONFIDENCE = {
  HIGH: 0.80,
  MEDIUM: 0.50,
} as const;
