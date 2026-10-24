import { useEffect, useState, type MouseEvent } from 'react'
import { messageId, slimMessageForUi } from '../lib/messageText'
import type { KafkaMessageView } from '../types/kafdesk'

/** Swaps a size-capped message for its full value on demand (one broker read). */
export function useFullMessage(m: KafkaMessageView | null) {
  const [full, setFull] = useState<KafkaMessageView | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const id = m ? messageId(m) : null

  useEffect(() => {
    setFull(null)
    setLoading(false)
    setError(null)
  }, [id])

  const shown = full && id && messageId(full) === id ? full : m

  async function load() {
    if (!m || loading || !window.kafdesk?.fetchMessage) return
    setLoading(true)
    setError(null)
    try {
      const msg = await window.kafdesk.fetchMessage(m.topic, m.partition, m.offset)
      if (messageId(msg) === messageId(m)) setFull(slimMessageForUi(msg))
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setLoading(false)
    }
  }

  return { shown, loading, error, load }
}

function formatSize(bytes: number): string {
  if (bytes >= 1024 * 1024) return `${(bytes / 1024 / 1024).toFixed(1)} MB`
  return `${Math.round(bytes / 1024)} KB`
}

export default function TruncatedValueNotice({
  bytes,
  text,
  action,
  loading,
  error,
  onLoad,
}: {
  bytes: number
  /** `{size}` placeholder → original payload size */
  text: string
  action: string
  loading: boolean
  error: string | null
  onLoad: () => void
}) {
  return (
    <div className="msg-truncated" role="status">
      <span>{text.replace('{size}', formatSize(bytes))}</span>
      <button
        type="button"
        className="msg-action"
        disabled={loading}
        onClick={(e: MouseEvent) => {
          e.stopPropagation()
          onLoad()
        }}
      >
        {loading ? '…' : action}
      </button>
      {error ? <span className="msg-truncated-error">{error}</span> : null}
    </div>
  )
}
