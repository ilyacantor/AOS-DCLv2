import React from 'react';

interface BadgeProps {
  children: React.ReactNode;
  variant?: 'outline' | 'secondary' | 'default' | 'destructive';
  className?: string;
}

export function Badge({ children, variant = 'default', className = '' }: BadgeProps) {
  const variants = {
    default: 'bg-primary text-primary-foreground',
    outline: 'border border-border text-foreground',
    secondary: 'bg-secondary text-secondary-foreground',
    destructive: 'bg-red-500/20 border border-red-500/30 text-red-200',
  };
  
  return (
    <span className={`inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-semibold ${variants[variant]} ${className}`}>
      {children}
    </span>
  );
}
