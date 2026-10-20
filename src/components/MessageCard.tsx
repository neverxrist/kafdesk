import { useMemo, useState, type MouseEvent } from 'react'
import type { KafkaMessageView } from '../../electron/types'
import { formatBadgeLabel, formatMessageDateTime } from '../lib/format'
import { highlightMessageBody } from '../lib/jsonHighlight'
import { messageId } from '../lib/messageText'
import type { MessageViewMode } from '../lib/messageView'

const COLLAPSE_CHARS = 280

function CopyIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 16 16" aria-hidden="true">
      <rect
        x="5.5"
        y="5.5"
        width="8"
        height="9"
        rx="1.5"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.4"
      />
      <path
        d="M10.5 5.5V3.8A1.3 1.3 0 0 0 9.2 2.5H3.8A1.3 1.3 0 0 0 2.5 3.8v7.4A1.3 1.3 0 0 0 3.8 12.5H5.5"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.4"
        strokeLinecap="round"
      />
    </svg>
  )
}

function CheckIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 16 16" aria-hidden="true">
      <path
        d="M3.5 8.5 6.5 11.5 12.5 4.5"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.6"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  )
}

export default function MessageCard({
  message: m,
  keyLabel = 'key',
  viewMode = 'full',
  selectedForDiff = false,
  diffFull = false,
  onToggleDiff,
  onRepublish,
  labels,
}: {
  message: KafkaMessageView
  keyLabel?: string
  viewMode?: MessageViewMode
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
    copyValue: string
    copied: string
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
  const [copied, setCopied] = useState(false)
  const shown = expanded || !long ? body : `${body.slice(0, COLLAPSE_CHARS)}…`
  const highlighted = useMemo(() => highlightMessageBody(shown), [shown])

  function fallbackCopy(text: string): boolean {
    try {
      const ta = document.createElement('textarea')
      ta.value = text
      ta.setAttribute('readonly', '')
      ta.style.position = 'fixed'
      ta.style.left = '-9999px'
      document.body.appendChild(ta)
      ta.select()
      const ok = document.execCommand('copy')
      document.body.removeChild(ta)
      return ok
    } catch {
      return false
    }
  }

  function copyBody(e: MouseEvent) {
    e.stopPropagation()
    e.preventDefault()
    if (navigator.clipboard?.writeText) {
      void navigator.clipboard.writeText(body).catch(() => {
        fallbackCopy(body)
      })
    } else {
      fallbackCopy(body)
    }
    setCopied(true)
    window.setTimeout(() => setCopied(false), 1200)
  }

  if (viewMode === 'compact') {
    return (
      <article
        className={`msg msg-compact ${selectedForDiff ? 'msg-diff-selected' : ''}`}
        data-msgid={messageId(m)}
      >
        <div className="msg-compact-row">
          {m.key != null ? (
            <span className="msg-key" title={String(m.key)}>
              <em>{keyLabel}</em>
              <strong>{m.key}</strong>
            </span>
          ) : (
            <span className="msg-key muted">—</span>
          )}
          <span className="msg-head-right">
            <span className="msg-chip">p{m.partition}</span>
            <span className="msg-chip msg-offset">
              offset <strong>{m.offset}</strong>
            </span>
            <span className="msg-datetime">{when ?? ''}</span>
            <span className={`badge ${m.format}`} title={`Detected type: ${m.format}`}>
              {formatBadgeLabel(m.format, m.schemaId)}
            </span>
          </span>
        </div>
      </article>
    )
  }

  const showActions = Boolean(labels && (onRepublish || onToggleDiff || long))

  return (
    <article className={`msg ${selectedForDiff ? 'msg-diff-selected' : ''}`} data-msgid={messageId(m)}>
      <div className="msg-head">
        <span className="msg-meta">
          {m.key != null ? (
            <span className="msg-key" title={String(m.key)}>
              <em>{keyLabel}</em>
              <strong>{m.key}</strong>
            </span>
          ) : (
            <span className="msg-key muted">—</span>
          )}
        </span>
        <span className="msg-head-right">
          <span className="msg-chip">p{m.partition}</span>
          <span className="msg-chip msg-offset">
            offset <strong>{m.offset}</strong>
          </span>
          <span className="msg-datetime">{when ?? ''}</span>
          <span className={`badge ${m.format}`} title={`Detected type: ${m.format}`}>
            {formatBadgeLabel(m.format, m.schemaId)}
          </span>
        </span>
      </div>
      {showActions && labels && (
        <div className="msg-actions">
          {onRepublish && labels.republish && (
            <button type="button" className="msg-action" onClick={() => onRepublish(m)}>
              {labels.republish}
            </button>
          )}
          {onToggleDiff && labels.diffSelect && (
            <button
              type="button"
              className={`msg-action ${selectedForDiff ? 'active' : ''}`}
              disabled={!selectedForDiff && diffFull}
              onClick={() => onToggleDiff(m)}
            >
              {selectedForDiff ? labels.diffSelected : labels.diffSelect}
            </button>
          )}
          {long && (
            <button
              type="button"
              className="msg-action"
              aria-expanded={expanded}
              onClick={() => setExpanded((v) => !v)}
            >
              {expanded ? labels.collapse : labels.expand}
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
      <div className={`msg-body-wrap${expanded || !long ? '' : ' is-collapsed'}`}>
        {labels?.copyValue && (
          <button
            type="button"
            className={`msg-copy${copied ? ' is-copied' : ''}`}
            title={copied ? labels.copied : labels.copyValue}
            aria-label={copied ? labels.copied : labels.copyValue}
            onClick={(e) => void copyBody(e)}
          >
            {copied ? <CheckIcon /> : <CopyIcon />}
          </button>
        )}
        {expanded || !long ? (
          <pre className="msg-body">{highlighted}</pre>
        ) : (
          <button type="button" className="msg-preview" onClick={() => setExpanded(true)}>
            <pre className="msg-body">{highlighted}</pre>
          </button>
        )}
      </div>
    </article>
  )
}
