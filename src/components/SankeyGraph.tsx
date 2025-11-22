import React, { useEffect, useRef } from 'react';
import { sankey, sankeyLinkHorizontal } from 'd3-sankey';
import * as d3 from 'd3';
import './SankeyGraph.css';

interface SankeyGraphProps {
  data: {
    nodes: any[];
    links: any[];
    meta: any;
  };
}

const SankeyGraph: React.FC<SankeyGraphProps> = ({ data }) => {
  const svgRef = useRef<SVGSVGElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!data || !svgRef.current || !containerRef.current) return;

    const container = containerRef.current;
    const width = container.clientWidth;
    const height = container.clientHeight;

    d3.select(svgRef.current).selectAll('*').remove();

    const svg = d3.select(svgRef.current)
      .attr('width', width)
      .attr('height', height);

    const sankeyData = {
      nodes: data.nodes.map((n: any, i: number) => ({
        ...n,
        node: i,
        name: n.label
      })),
      links: data.links.map((l: any) => ({
        ...l,
        source: data.nodes.findIndex((n: any) => n.id === l.source),
        target: data.nodes.findIndex((n: any) => n.id === l.target)
      }))
    };

    const sankeyGenerator = sankey()
      .nodeWidth(15)
      .nodePadding(20)
      .extent([[1, 1], [width - 1, height - 5]]);

    const graph = sankeyGenerator(sankeyData as any);

    const colorScale = d3.scaleOrdinal()
      .domain(['L0', 'L1', 'L2', 'L3'])
      .range(['#22d3ee', '#4ade80', '#60a5fa', '#c084fc']);

    svg.append('g')
      .selectAll('path')
      .data(graph.links)
      .join('path')
      .attr('d', sankeyLinkHorizontal())
      .attr('stroke', '#aaa')
      .attr('stroke-width', (d: any) => Math.max(1, d.width))
      .attr('fill', 'none')
      .attr('opacity', 0.4)
      .append('title')
      .text((d: any) => d.info_summary || `${d.value}`);

    svg.append('g')
      .selectAll('rect')
      .data(graph.nodes)
      .join('rect')
      .attr('x', (d: any) => d.x0)
      .attr('y', (d: any) => d.y0)
      .attr('width', (d: any) => d.x1 - d.x0)
      .attr('height', (d: any) => d.y1 - d.y0)
      .attr('fill', (d: any) => colorScale(d.level) as string)
      .attr('stroke', '#333')
      .attr('stroke-width', 1)
      .append('title')
      .text((d: any) => `${d.label} (${d.level})`);

    svg.append('g')
      .selectAll('text')
      .data(graph.nodes)
      .join('text')
      .attr('x', (d: any) => d.x0 < width / 2 ? d.x1 + 6 : d.x0 - 6)
      .attr('y', (d: any) => (d.y1 + d.y0) / 2)
      .attr('dy', '0.35em')
      .attr('text-anchor', (d: any) => d.x0 < width / 2 ? 'start' : 'end')
      .attr('font-size', '12px')
      .attr('fill', '#333')
      .text((d: any) => d.label);

  }, [data]);

  return (
    <div ref={containerRef} className="sankey-container">
      <svg ref={svgRef}></svg>
    </div>
  );
};

export default SankeyGraph;
