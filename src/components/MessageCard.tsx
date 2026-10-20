import { useMemo, useState } from 'react'
import type { KafkaMessageView } from '../../electron/types'
import { formatBadgeLabel, formatMessageDateTime } from '../lib/format'
import { messageId } from '../lib/messageText'

const COLLAPSE_CHARS = 480

export default function MessageCard({
  message: m,
  keyLabel = 'key',
  selectedForDiff = false,
  diffFull = false,
  onToggleDiff,
  onRepublish,
  labels,
}: {
  message: KafkaMessageView
  keyLabel?: string
  selectedForDiff?: boolean
  diffFull?: boolean
  onToggleDiff?: (m: KafkaMessageView) => void
  onRepublish?: (m: KafkaMessageView) => void
  labels?: {
    republish: string
    diffSelect: string
    diffSelected: string
    expand: string
    collapse: string
  }
}) {
  const headerEntries = Object.entries(m.headers || {})
  const when = formatMessageDateTime(m.timestamp)
  const body = useMemo(
    () => (typeof m.value === 'string' ? m.value : JSON.stringify(m.value, null, 2)),
    [m.value],
  )
  const long = body.length > COLLAPSE_CHARS
  const [expanded, setExpanded] = useState(!long)

  return (
    <article className={`msg ${selectedForDiff ? 'msg-diff-selected' : ''}`} data-msgid={messageId(m)}>
      <div className="msg-head">
        <span className="msg-meta">
          <span className="msg-chip">p{m.partition}</span>
          <span className="msg-chip msg-offset">
            offset <strong>{m.offset}</strong>
            {when ? <span className="msg-datetime"> · {when}</span> : null}
          </span>
          {m.key != null && (
            <span className="msg-key" title={String(m.key)}>
              <em>{keyLabel}</em>
              <strong>{m.key}</strong>
            </span>
          )}
        </span>
        <span className="msg-head-right">
          <span className={`badge ${m.format}`} title={`Detected type: ${m.format}`}>
            {formatBadgeLabel(m.format, m.schemaId)}
          </span>
          {long && labels && (
            <button
              type="button"
              className="msg-fold"
              aria-expanded={expanded}
              onClick={() => setExpanded((v) => !v)}
            >
              {expanded ? labels.collapse : labels.expand}
            </button>
          )}
        </span>
      </div>
      {(onRepublish || onToggleDiff) && labels && (
        <div className="msg-actions">
          {onRepublish && (
            <button type="button" className="msg-action" onClick={() => onRepublish(m)}>
              {labels.republish}
            </button>
          )}
          {onToggleDiff && (
            <button
              type="button"
              className={`msg-action ${selectedForDiff ? 'active' : ''}`}
              disabled={!selectedForDiff && diffFull}
              onClick={() => onToggleDiff(m)}
            >
              {selectedForDiff ? labels.diffSelected : labels.diffSelect}
            </button>
          )}
        </div>
      )}
      {headerEntries.length > 0 && (
        <div className="msg-headers">
          {headerEntries.map(([k, v]) => (
            <span key={k} className="chip">
              {k}={v}
            </span>
          ))}
        </div>
      )}
      {expanded ? (
        <pre>{body}</pre>
      ) : (
        <button type="button" className="msg-preview" onClick={() => setExpanded(true)}>
          <pre>{body.slice(0, COLLAPSE_CHARS)}…</pre>
          <span className="msg-preview-more">{labels?.expand ?? 'Expand'}</span>
        </button>
      )}
    </article>
  )
}
