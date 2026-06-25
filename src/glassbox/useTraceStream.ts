import { useTraceStore } from './traceStore'

const STAGES = ['INTAKE', 'TRAVERSE', 'RETRIEVE', 'PRUNE', 'COMPUTE', 'DONE'] as const

/**
 * Open the Glass Box SSE replay stream for a gallery question and drive the
 * trace store. Pass the question id (omit for the default first question).
 *
 * No silent fallback (A1): a stream error before DONE surfaces a readable
 * error — the UI must never sit blank pretending it resolved. When the live
 * contextOS stream replaces the replay endpoint, this hook is unchanged.
 */
export function startTraceStream(questionId?: string): EventSource {
  if (questionId) useTraceStore.getState().setQuestion(questionId)
  else useTraceStore.getState().reset()

  const url = questionId
    ? `/api/demo/stream-trace?q=${encodeURIComponent(questionId)}`
    : '/api/demo/stream-trace'
  const es = new EventSource(url)
  let done = false

  for (const stage of STAGES) {
    es.addEventListener(stage, (ev) => {
      try {
        const data = JSON.parse((ev as MessageEvent).data)
        useTraceStore.getState().applyEvent(stage, data)
      } catch (err) {
        useTraceStore.getState().setError(`Glass Box trace frame (${stage}) was not valid JSON: ${String(err)}`)
        es.close()
        return
      }
      if (stage === 'DONE') {
        done = true
        es.close()
      }
    })
  }

  es.onerror = () => {
    if (done) return
    useTraceStore.getState().setError(
      'Trace stream failed before completing — no connection to /api/demo/stream-trace. ' +
        'The replay did not reach COMPUTE, so nothing is shown as resolved.',
    )
    es.close()
  }

  return es
}
