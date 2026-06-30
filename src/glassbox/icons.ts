import { Database, ShieldCheck, Sigma, Workflow, Zap, type LucideIcon } from 'lucide-react'
import type { IconKey } from './trace/types'

/** Maps a serializable icon key from the trace to a concrete lucide icon. */
export const ICONS: Record<IconKey, LucideIcon> = {
  parser: Workflow,
  database: Database,
  shield: ShieldCheck,
  reducer: Sigma,
  action: Zap,
}
