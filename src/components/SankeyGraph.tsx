import React, { useEffect, useRef, useMemo, useState } from 'react';
import * as d3 from 'd3';
import { sankey, sankeyLinkHorizontal, sankeyLeft } from 'd3-sankey';
import { GraphSnapshot, PersonaId } from '../types';

interface SankeyGraphProps {
  data: GraphSnapshot | null;
  selectedPersonas: PersonaId[];
}

const LEVEL_COLORS = {
  L0: '#10b981',
  L1: '#10b981',
  L2: '#06b6d4',
  L3: '#8b5cf6',
};

export function SankeyGraph({ data, selectedPersonas }: SankeyGraphProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [size, setSize] = useState({ width: 800, height: 600 });

  useEffect(() => {
    const handleResize = () => {
      if (containerRef.current) {
        const { width, height } = containerRef.current.getBoundingClientRect();
        setSize({ width, height });
      }
    };
    
    window.addEventListener('resize', handleResize);
    handleResize();
    
    return () => window.removeEventListener('resize', handleResize);
  }, []);

  const graphData = useMemo(() => {
    if (!data || size.width === 0 || size.height === 0) return null;

    const nodes = data.nodes.map(n => ({ ...n }));
    const links = data.links.map(l => ({ ...l }));

    const sankeyGenerator = sankey<any, any>()
      .nodeId((d: any) => d.id)
      .nodeAlign(sankeyLeft)
      .nodeWidth(20)
      .nodePadding(30)
      .extent([[20, 20], [size.width - 20, size.height - 20]]);

    const { nodes: sNodes, links: sLinks } = sankeyGenerator({
      nodes,
      links
    });
    
    return { nodes: sNodes, links: sLinks };
  }, [data, size]);

  if (!data || !graphData) return <div className="w-full h-full flex items-center justify-center text-muted-foreground">Loading visualization...</div>;

  return (
    <div ref={containerRef} className="w-full h-full bg-[#020617] overflow-hidden relative select-none">
      <svg width={size.width} height={size.height} className="overflow-visible">
        <defs>
          {graphData.links.map((link: any, idx: number) => {
             const sourceColor = LEVEL_COLORS[(link.source.level || 'L0') as keyof typeof LEVEL_COLORS] || '#999';
             const targetColor = LEVEL_COLORS[(link.target.level || 'L0') as keyof typeof LEVEL_COLORS] || '#999';
             const gradientId = `gradient-${link.id || `${link.source.id}-${link.target.id}-${idx}`}`;
             return (
               <linearGradient key={gradientId} id={gradientId} gradientUnits="userSpaceOnUse" x1={link.source.x1 || 0} x2={link.target.x0 || 0}>
                 <stop offset="0%" stopColor={sourceColor} stopOpacity="1" />
                 <stop offset="100%" stopColor={targetColor} stopOpacity="1" />
               </linearGradient>
             );
          })}
          
          <filter id="glow">
            <feGaussianBlur stdDeviation="4" result="coloredBlur"/>
            <feMerge>
              <feMergeNode in="coloredBlur"/>
              <feMergeNode in="SourceGraphic"/>
            </feMerge>
          </filter>
        </defs>

        <g className="links" style={{ mixBlendMode: 'screen' }}>
          {graphData.links.map((link: any, idx: number) => {
            const isHighlighted = 
               (link.target.kind === 'bll' && selectedPersonas.includes(link.target.personaId)) ||
               (selectedPersonas.length === 0);
            
            const gradientId = `gradient-${link.id || `${link.source.id}-${link.target.id}-${idx}`}`;

            return (
              <path
                key={link.id || `link-${idx}`}
                d={sankeyLinkHorizontal()(link) || ''}
                stroke={`url(#${gradientId})`}
                strokeWidth={Math.max(8, (link.width || 1) * 1.5)}
                fill="none"
                filter="url(#glow)"
                className={`transition-all duration-500 ease-in-out
                  ${isHighlighted ? 'opacity-100' : 'opacity-20 blur-[1px]'}
                `}
              />
            );
          })}
        </g>

        <g className="nodes">
          {graphData.nodes.map((node: any) => {
            const isHighlighted = 
              node.kind !== 'bll' ||
              selectedPersonas.length === 0 ||
              (node.personaId && selectedPersonas.includes(node.personaId));

            const color = LEVEL_COLORS[node.level as keyof typeof LEVEL_COLORS] || '#999';
            
            return (
              <foreignObject
                key={node.id}
                x={node.x0 || 0}
                y={node.y0 || 0}
                width={Math.max((node.x1 || 0) - (node.x0 || 0), 140)}
                height={Math.max((node.y1 || 0) - (node.y0 || 0), 24)} 
                className="overflow-visible"
              >
                <div 
                  className={`
                    h-full min-w-[20px] w-[20px] rounded-full border transition-all duration-300
                    flex items-center relative group
                    ${isHighlighted 
                      ? 'opacity-100 scale-100 shadow-[0_0_15px_-3px_rgba(0,0,0,0.3)]' 
                      : 'opacity-40 scale-95 grayscale'}
                  `}
                  style={{
                    borderColor: color,
                    backgroundColor: '#0f172a',
                    boxShadow: isHighlighted ? `0 0 10px ${color}40` : 'none'
                  }}
                >
                  <div className="absolute left-1/2 top-1 bottom-1 -translate-x-1/2 w-0.5 bg-white/20 rounded-full" />
                </div>

                <div 
                  className={`
                    absolute top-1/2 -translate-y-1/2 whitespace-nowrap px-2 py-1 rounded-md
                    bg-[#0f172a]/90 border backdrop-blur-sm text-[10px] font-medium text-white
                    transition-all duration-300 z-10 pointer-events-none
                    ${isHighlighted ? 'opacity-100 translate-x-0' : 'opacity-0 -translate-x-2'}
                  `}
                  style={{
                    left: 24,
                    borderColor: `${color}40`,
                    color: color === '#10b981' ? '#d1fae5' : color === '#06b6d4' ? '#cffafe' : '#ede9fe'
                  }}
                >
                  {node.label}
                </div>
              </foreignObject>
            );
          })}
        </g>
      </svg>
    </div>
  );
}
