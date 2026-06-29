import type { TraceEvent } from './types'

/**
 * The swap point. A TraceSource turns a selected question into a stream of
 * TraceEvents and returns a cancel function. `GalleryTraceSource` implements
 * this over the /api/demo replay today; a live-engine source will implement the
 * same interface — the hook and UI never change.
 */
export interface TraceSource {
  run(questionId: string, emit: (event: TraceEvent) => void): () => void
}
