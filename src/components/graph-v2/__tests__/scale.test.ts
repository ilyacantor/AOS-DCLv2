import { describe, it, expect } from 'vitest';
import { scaleWidth } from '../scale';

describe('scaleWidth', () => {
  it('returns minWidth for the minimum value', () => {
    expect(scaleWidth(0, 0, 100, 1.5, 6)).toBe(1.5);
  });

  it('returns maxWidth for the maximum value', () => {
    expect(scaleWidth(100, 0, 100, 1.5, 6)).toBe(6);
  });

  it('returns midpoint for the middle value', () => {
    expect(scaleWidth(50, 0, 100, 1.5, 6)).toBe(3.75);
  });

  it('handles degenerate range (all same values)', () => {
    // When minVal === maxVal, range is forced to 1
    // scaleWidth(50, 50, 50) => 1.5 + ((50-50)/1) * 4.5 = 1.5
    expect(scaleWidth(50, 50, 50, 1.5, 6)).toBe(1.5);
  });

  it('handles all zeros', () => {
    expect(scaleWidth(0, 0, 0, 1.5, 6)).toBe(1.5);
  });

  it('handles single link (value equals both min and max)', () => {
    expect(scaleWidth(42, 42, 42, 1, 6)).toBe(1);
  });

  it('interpolates correctly at quarter marks', () => {
    expect(scaleWidth(25, 0, 100, 0, 10)).toBeCloseTo(2.5);
    expect(scaleWidth(75, 0, 100, 0, 10)).toBeCloseTo(7.5);
  });

  it('works with non-zero min values', () => {
    // value=150 in range [100, 200] => 50% => minW + 0.5 * (maxW - minW)
    expect(scaleWidth(150, 100, 200, 2, 8)).toBe(5);
  });
});
