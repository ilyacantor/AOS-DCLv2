import type { NodeState } from './trace/types'

interface NodeTone {
  card: string
  glow: string
  icon: string
  text: string
  detail: string
}

/** Visual tokens per execution state. Excised dims + strikes via the component. */
export const NODE_THEME: Record<NodeState, NodeTone> = {
  pending: {
    card: 'border-slate-700/70 bg-slate-900/70',
    glow: '',
    icon: 'text-slate-500',
    text: 'text-slate-400',
    detail: 'text-slate-600',
  },
  processing: {
    card: 'border-sky-500/60 bg-sky-950/40',
    glow: 'shadow-[0_0_24px_-6px_rgba(56,189,248,0.75)] animate-pulse',
    icon: 'text-sky-300',
    text: 'text-sky-50',
    detail: 'text-sky-300/80',
  },
  verified: {
    card: 'border-emerald-500/60 bg-emerald-950/40',
    glow: 'shadow-[0_0_24px_-8px_rgba(16,185,129,0.65)]',
    icon: 'text-emerald-300',
    text: 'text-emerald-50',
    detail: 'text-emerald-300/80',
  },
  excised: {
    card: 'border-red-500/50 bg-red-950/30 opacity-50',
    glow: '',
    icon: 'text-red-400',
    text: 'text-red-300',
    detail: 'text-red-400/70',
  },
}

/** Tone for the source badge — 0.0/shadow reads red, SoR/1.0 reads green. */
export function badgeTone(badge: string): string {
  if (/0\.0\s*$/.test(badge) || /shadow/i.test(badge)) return 'bg-red-500/15 text-red-300 ring-1 ring-red-500/30'
  if (/1\.0\s*$/.test(badge) || /\bSoR\b/.test(badge)) return 'bg-emerald-500/15 text-emerald-300 ring-1 ring-emerald-500/30'
  return 'bg-slate-700/40 text-slate-300 ring-1 ring-slate-600/40'
}
