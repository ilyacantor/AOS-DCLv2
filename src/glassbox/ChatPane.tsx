import { useEffect, useState } from 'react'
import { Bot, Boxes, Loader2, UserRound } from 'lucide-react'
import type { ChatMessage } from './trace/types'
import { fetchQuestions, type GalleryItem } from './trace/galleryTrace'

interface ChatPaneProps {
  messages: ChatMessage[]
  loading: boolean
  running: boolean
  onSelect: (questionId: string) => void
}

// Curated demo: the five preselected questions are the gallery (flat — no
// category buckets). Picking one auto-runs its trace on the canvas and lands the
// answer here. The source system shows up as a node on the canvas, not a tag.
export function ChatPane({ messages, loading, running, onSelect }: ChatPaneProps) {
  const [items, setItems] = useState<GalleryItem[]>([])
  const [loadError, setLoadError] = useState<string | null>(null)

  useEffect(() => {
    // No silent fallback: surface a load failure rather than show an empty gallery.
    fetchQuestions()
      .then(setItems)
      .catch((e) => setLoadError(`Could not load the question gallery: ${String(e)}`))
  }, [])

  return (
    <aside className="flex h-full w-[34%] min-w-[360px] max-w-[460px] flex-col border-r border-slate-800 bg-slate-900/40">
      <header className="border-b border-slate-800 px-5 py-4">
        <div className="flex items-center gap-2">
          <Boxes size={18} className="text-emerald-400" />
          <h1 className="text-sm font-semibold tracking-tight text-slate-100">
            contextOS <span className="text-slate-500">·</span> Glass Box
          </h1>
          <span className="ml-auto rounded-full bg-amber-500/10 px-2 py-0.5 text-[10px] font-medium uppercase tracking-wider text-amber-400 ring-1 ring-amber-500/30">
            Rails Mode
          </span>
        </div>
        <p className="mt-1.5 text-xs leading-relaxed text-slate-500">
          Deterministic semantic routing &amp; trust arbitration over the resolved base.
        </p>
      </header>

      <div className="flex-1 space-y-4 overflow-y-auto px-4 py-4">
        <div className="space-y-2" data-testid="question-gallery">
          <p className="px-1 text-[10px] font-medium uppercase tracking-wider text-slate-500">Demo questions</p>
          {loadError && (
            <div data-testid="gallery-error" className="rounded-md border border-red-500/30 bg-red-950/30 px-3 py-2 text-xs text-red-300">
              {loadError}
            </div>
          )}
          {items.map((q) => (
            <button
              key={q.id}
              data-testid={`q-${q.id}`}
              disabled={running}
              onClick={() => onSelect(q.id)}
              className="block w-full rounded-lg border border-slate-700/80 bg-slate-800/40 px-3 py-2 text-left transition hover:border-emerald-500/40 hover:bg-slate-800/70 disabled:cursor-not-allowed disabled:opacity-50"
            >
              {q.asker && (
                <span className="mb-1 flex items-center gap-1 text-[10px] font-medium uppercase tracking-wider text-slate-400">
                  {q.askerKind === 'agent' ? (
                    <Bot size={11} className="text-emerald-400" />
                  ) : (
                    <UserRound size={11} className="text-sky-400" />
                  )}
                  {q.asker}
                </span>
              )}
              <span className="block text-sm leading-snug text-slate-100">{q.question}</span>
              <span className="mt-0.5 block text-[11px] text-slate-500">{q.entity_id}</span>
            </button>
          ))}
        </div>

        {(messages.length > 0 || loading) && (
          <div data-testid="chat-messages" className="space-y-3 border-t border-slate-800/70 pt-3">
            {messages.map((m) => (
              <Bubble key={m.id} message={m} />
            ))}
            {loading && <Thinking />}
          </div>
        )}
      </div>
    </aside>
  )
}

function Bubble({ message }: { message: ChatMessage }) {
  const isUser = message.role === 'user'
  const isError = message.tone === 'error'
  return (
    <div className={`flex flex-col ${isUser ? 'items-end' : 'items-start'}`}>
      {message.author && (
        <span className="mb-1 flex items-center gap-1 px-1 text-[10px] font-medium uppercase tracking-wider text-slate-400">
          {message.authorIsAgent ? (
            <Bot size={11} className="text-emerald-400" />
          ) : (
            <UserRound size={11} className="text-sky-400" />
          )}
          {message.author}
        </span>
      )}
      <div
        data-testid={isError ? 'trace-error' : undefined}
        className={`max-w-[88%] whitespace-pre-line rounded-2xl px-3.5 py-2.5 text-sm leading-relaxed ${
          isUser
            ? 'rounded-br-sm bg-slate-700/70 text-slate-100'
            : isError
              ? 'rounded-bl-sm border border-red-500/30 bg-red-950/30 text-red-200'
              : 'rounded-bl-sm border border-emerald-500/20 bg-emerald-950/30 text-emerald-50'
        }`}
      >
        {message.text}
      </div>
    </div>
  )
}

function Thinking() {
  return (
    <div className="flex items-center gap-2 px-1 text-xs text-sky-300/80">
      <Loader2 size={13} className="animate-spin" />
      <span>Routing &amp; arbitrating trust…</span>
    </div>
  )
}
