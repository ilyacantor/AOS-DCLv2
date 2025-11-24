import { useEffect, useRef, useMemo, useState } from 'react';
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

interface TooltipState {
  visible: boolean;
  x: number;
  y: number;
  content: string;
}

export function SankeyGraph({ data }: SankeyGraphProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [size, setSize] = useState({ width: 800, height: 600 });
  const [tooltip, setTooltip] = useState<TooltipState>({
    visible: false,
    x: 0,
    y: 0,
    content: ''
  });

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
      .extent([[20, 20], [size.width - 60, size.height - 20]]);

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
                 <stop offset="0%" stopColor={sourceColor} stopOpacity="0.7" />
                 <stop offset="100%" stopColor={targetColor} stopOpacity="0.7" />
               </linearGradient>
             );
          })}
          
          <filter id="glow">
            <feGaussianBlur stdDeviation="1.5" result="coloredBlur"/>
            <feMerge>
              <feMergeNode in="coloredBlur"/>
              <feMergeNode in="SourceGraphic"/>
            </feMerge>
          </filter>
        </defs>

        <g className="links">
          {graphData.links.map((link: any, idx: number) => {
            const gradientId = `gradient-${link.id || `${link.source.id}-${link.target.id}-${idx}`}`;
            
            const handleMouseEnter = (event: React.MouseEvent<SVGPathElement>) => {
              const containerRect = containerRef.current?.getBoundingClientRect();
              
              if (containerRect) {
                const x = event.clientX - containerRect.left;
                const y = event.clientY - containerRect.top;
                
                const sourceLabel = link.source.label || link.source.id;
                const targetLabel = link.target.label || link.target.id;
                const confidence = link.confidence || '';
                const mappingInfo = link.info_summary || '';
                
                let content = `${sourceLabel} → ${targetLabel}`;
                if (mappingInfo) {
                  content += `\n${mappingInfo}`;
                }
                if (confidence && confidence !== 'high' && confidence !== '') {
                  content += `\nConfidence: ${confidence}`;
                }
                
                setTooltip({
                  visible: true,
                  x,
                  y: y - 10,
                  content
                });
              }
            };
            
            const handleMouseLeave = () => {
              setTooltip(prev => ({ ...prev, visible: false }));
            };

            return (
              <path
                key={link.id || `link-${idx}`}
                d={sankeyLinkHorizontal()(link) || ''}
                stroke={`url(#${gradientId})`}
                strokeWidth={Math.max(1, link.width || 1)}
                fill="none"
                opacity="0.6"
                className="cursor-pointer hover:opacity-100 transition-opacity"
                onMouseEnter={handleMouseEnter}
                onMouseLeave={handleMouseLeave}
                style={{ pointerEvents: 'stroke' }}
              />
            );
          })}
        </g>

        <g className="nodes">
          {graphData.nodes.map((node: any) => {
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
                  className="h-full min-w-[20px] w-[20px] rounded-full border flex items-center relative group"
                  style={{
                    borderColor: color,
                    backgroundColor: '#0f172a',
                    boxShadow: `0 0 10px ${color}40`
                  }}
                >
                  <div className="absolute left-1/2 top-1 bottom-1 -translate-x-1/2 w-0.5 bg-white/20 rounded-full" />
                </div>

                <div 
                  className={`
                    absolute top-1/2 -translate-y-1/2 whitespace-nowrap px-3 py-1.5 rounded-full
                    border backdrop-blur-sm text-[10px] font-medium z-10 pointer-events-none
                    ${node.kind === 'bll' 
                      ? 'bg-[#0f172a] shadow-lg' 
                      : 'bg-[#0f172a]/90'
                    }
                  `}
                  style={{
                    left: 24,
                    borderColor: node.kind === 'bll' ? color : `${color}40`,
                    color: color === '#10b981' ? '#d1fae5' : color === '#06b6d4' ? '#cffafe' : '#ede9fe',
                    boxShadow: node.kind === 'bll' ? `0 0 12px ${color}60` : undefined
                  }}
                >
                  {node.label.replace('BLL ', '')}
                </div>
              </foreignObject>
            );
          })}
        </g>
      </svg>
      
      {tooltip.visible && (
        <div
          className="absolute pointer-events-none z-50 px-3 py-2 bg-[#1e293b]/95 border border-white/20 rounded-lg shadow-xl backdrop-blur-sm"
          style={{
            left: tooltip.x,
            top: tooltip.y,
            transform: 'translate(-50%, -100%)',
          }}
        >
          <div className="text-xs font-medium text-white/90 whitespace-pre-line">
            {tooltip.content}
          </div>
          <div className="absolute left-1/2 -bottom-1 -translate-x-1/2 w-2 h-2 bg-[#1e293b]/95 border-b border-r border-white/20 rotate-45" />
        </div>
      )}
    </div>
  );
}
