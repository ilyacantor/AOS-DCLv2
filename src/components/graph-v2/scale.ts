/**
 * Pure scaling function for link widths.
 * No dependencies on constants — all parameters are explicit.
 */

/** Linear interpolation of a value into [minWidth, maxWidth]. */
export function scaleWidth(
  value: number,
  minVal: number,
  maxVal: number,
  minWidth: number,
  maxWidth: number,
): number {
  const range = maxVal - minVal || 1;
  return minWidth + ((value - minVal) / range) * (maxWidth - minWidth);
}
