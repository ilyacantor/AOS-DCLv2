import { describe, it, expect } from 'vitest';
import { computeDataDrivenLayout } from '../layout';
import type { LayoutConfig } from '../types';
import type { GraphNode, GraphLink } from '../../../types';

const config: LayoutConfig = {
  margin: { top: 50, right: 50, bottom: 20, left: 50 },
  node: { width: 18, padding: 14 },
  link: { minStrokeWidth: 1.5, maxStrokeWidth: 6 },
  maxNodeHeight: 70,
};

function makeNode(id: string, level: 'L0' | 'L1' | 'L2' | 'L3', kind: GraphNode['kind'] = 'source'): GraphNode {
  return { id, label: id, level, kind };
}

function makeLink(id: string, source: string, target: string, value: number): GraphLink {
  return { id, source, target, value };
}

describe('computeDataDrivenLayout', () => {
  it('returns empty layout for empty input', () => {
    const result = computeDataDrivenLayout([], [], 800, 600, config);
    expect(result.nodes).toHaveLength(0);
    expect(result.links).toHaveLength(0);
    expect(result.columnXs).toHaveLength(0);
    expect(result.levelLabels).toHaveLength(0);
  });

  it('returns empty layout for zero dimensions', () => {
    const nodes = [makeNode('a', 'L0')];
    const result = computeDataDrivenLayout(nodes, [], 0, 0, config);
    expect(result.nodes).toHaveLength(0);
  });

  it('groups nodes by level into correct columns', () => {
    const nodes = [
      makeNode('s1', 'L0', 'pipe'),
      makeNode('s2', 'L1'),
      makeNode('s3', 'L2', 'ontology'),
      makeNode('s4', 'L3', 'bll'),
    ];
    const result = computeDataDrivenLayout(nodes, [], 800, 600, config);
    expect(result.nodes).toHaveLength(4);

    const byId = new Map(result.nodes.map(n => [n.id, n]));
    expect(byId.get('s1')!.column).toBe(0);
    expect(byId.get('s2')!.column).toBe(1);
    expect(byId.get('s3')!.column).toBe(2);
    expect(byId.get('s4')!.column).toBe(3);

    // Nodes in same column have same x0
    const x0s = result.nodes.map(n => n.x0);
    const uniqueX = new Set(x0s);
    expect(uniqueX.size).toBe(4);
  });

  it('assigns link widths within bounds', () => {
    const nodes = [makeNode('a', 'L0'), makeNode('b', 'L1')];
    const links = [
      makeLink('l1', 'a', 'b', 100),
      makeLink('l2', 'a', 'b', 500),
      makeLink('l3', 'a', 'b', 1000),
    ];
    const result = computeDataDrivenLayout(nodes, links, 800, 600, config);

    for (const link of result.links) {
      expect(link.width).toBeGreaterThanOrEqual(config.link.minStrokeWidth);
      expect(link.width).toBeLessThanOrEqual(config.link.maxStrokeWidth);
    }

    // Min value gets minStrokeWidth, max gets maxStrokeWidth
    const byId = new Map(result.links.map(l => [l.id, l]));
    expect(byId.get('l1')!.width).toBe(config.link.minStrokeWidth);
    expect(byId.get('l3')!.width).toBe(config.link.maxStrokeWidth);
  });

  it('assigns minStrokeWidth to zero-value links', () => {
    const nodes = [makeNode('a', 'L0'), makeNode('b', 'L1')];
    const links = [
      makeLink('l1', 'a', 'b', 0),
      makeLink('l2', 'a', 'b', 100),
    ];
    const result = computeDataDrivenLayout(nodes, links, 800, 600, config);
    const byId = new Map(result.links.map(l => [l.id, l]));
    expect(byId.get('l1')!.width).toBe(config.link.minStrokeWidth);
  });

  it('generates valid SVG paths for links', () => {
    const nodes = [makeNode('a', 'L0'), makeNode('b', 'L1')];
    const links = [makeLink('l1', 'a', 'b', 50)];
    const result = computeDataDrivenLayout(nodes, links, 800, 600, config);
    expect(result.links).toHaveLength(1);
    expect(result.links[0].path).toMatch(/^M/); // SVG path starts with M
  });

  it('derives level labels from data', () => {
    const nodes = [makeNode('a', 'L0'), makeNode('b', 'L2')];
    const result = computeDataDrivenLayout(nodes, [], 800, 600, config);
    expect(result.levelLabels).toEqual(['L0', 'L2']);
  });

  it('skips links with missing source/target nodes', () => {
    const nodes = [makeNode('a', 'L0')];
    const links = [makeLink('l1', 'a', 'nonexistent', 50)];
    const result = computeDataDrivenLayout(nodes, links, 800, 600, config);
    expect(result.links).toHaveLength(0);
  });
});
