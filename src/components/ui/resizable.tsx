import React from 'react';

interface ResizablePanelGroupProps {
  direction: 'horizontal' | 'vertical';
  children: React.ReactNode;
  className?: string;
}

export function ResizablePanelGroup({ direction, children, className = '' }: ResizablePanelGroupProps) {
  return (
    <div className={`flex ${direction === 'horizontal' ? 'flex-row' : 'flex-col'} h-full w-full ${className}`}>
      {children}
    </div>
  );
}

interface ResizablePanelProps {
  defaultSize?: number;
  minSize?: number;
  children: React.ReactNode;
  className?: string;
}

export function ResizablePanel({ defaultSize = 50, children, className = '' }: ResizablePanelProps) {
  return (
    <div className={`h-full ${className}`} style={{ flex: defaultSize }}>
      {children}
    </div>
  );
}

interface ResizableHandleProps {
  className?: string;
}

export function ResizableHandle({ className = '' }: ResizableHandleProps) {
  return <div className={`shrink-0 ${className}`} />;
}
