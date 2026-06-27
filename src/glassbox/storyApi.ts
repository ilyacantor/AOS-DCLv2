import { useTraceStore } from './traceStore'

/**
 * Fetch one question's full story and load it for presenter-paced reveal.
 * No silent fallback (A1): a failed fetch surfaces a readable error — the UI
 * never sits blank pretending it resolved. When the live contextOS engine
 * replaces the replay endpoint, this is unchanged.
 */
export async function fetchStory(id: string): Promise<void> {
  useTraceStore.getState().setLoading(id)
  try {
    const r = await fetch(`/api/demo/trace?q=${encodeURIComponent(id)}`)
    if (!r.ok) throw new Error(`trace endpoint returned ${r.status}`)
    const q = await r.json()
    if (!q?.story?.beats?.length) throw new Error('trace had no story beats')
    useTraceStore.getState().loadStory(q)
  } catch (e) {
    useTraceStore.getState().setError(
      `Couldn't load this trace (${String(e)}). Nothing is shown as resolved.`,
    )
  }
}
