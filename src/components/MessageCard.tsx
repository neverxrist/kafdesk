import { useEffect, useMemo, useState, type MouseEvent } from 'react'
import type { KafkaMessageView } from '../types/kafdesk'
import { formatBadgeLabel, formatMessageDateTime } from '../lib/format'
import { highlightMessageBody } from '../lib/jsonHighlight'
import { messageId } from '../lib/messageText'
import type { MessageViewMode } from '../lib/messageView'

/** Collapsed preview ≈ 2 lines; shorter bodies stay fully open. */
const PREVIEW_MAX_LINES = 2
const PREVIEW_CHARS_PER_LINE = 96

function bodyNeedsCollapse(body: string): boolean {
  const lines = body.split('\n')
  if (lines.length > PREVIEW_MAX_LINES) return true
  const longest = lines.reduce((n, line) => Math.max(n, line.length), 0)
  return longest > PREVIEW_CHARS_PER_LINE * PREVIEW_MAX_LINES
}

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

function ExpandIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 16 16" fill="none" aria-hidden="true">
      <path
        d="M4 6.2 8 10.2 12 6.2"
        stroke="currentColor"
        strokeWidth="1.6"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  )
}

function CollapseIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 16 16" fill="none" aria-hidden="true">
      <path
        d="M4 9.8 8 5.8 12 9.8"
        stroke="currentColor"
        strokeWidth="1.6"
        strokeLinecap="round"
        strokeLinejoin="round"
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
    headers?: string
  }
}) {
  const headerEntries = Object.entries(m.headers || {})
  const when = formatMessageDateTime(m.timestamp)
  const id = messageId(m)
  const compact = viewMode === 'compact'
  const body = useMemo(
    () => (typeof m.value === 'string' ? m.value : JSON.stringify(m.value, null, 2)),
    [m.value],
  )
  const needsCollapse = useMemo(() => bodyNeedsCollapse(body), [body])
  /** Full: collapse only when taller than ~2 lines; Compact: hidden until open. */
  const [expanded, setExpanded] = useState(() => !compact && !needsCollapse)
  const [copied, setCopied] = useState(false)
  const highlighted = useMemo(() => highlightMessageBody(body), [body])
  const toggleLabel = expanded
    ? labels?.collapse ?? 'Collapse'
    : labels?.expand ?? 'Expand'
  const showFullBody = compact ? expanded : expanded || !needsCollapse

  useEffect(() => {
    setExpanded(!compact && !needsCollapse)
  }, [id, compact, needsCollapse])

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

  function toggleExpanded() {
    setExpanded((v) => !v)
  }

  const headersBlock =
    headerEntries.length > 0 ? (
      <div className="msg-headers" aria-label={labels?.headers ?? 'Headers'}>
        <span className="msg-headers-label">{labels?.headers ?? 'Headers'}</span>
        <dl className="msg-headers-list">
          {headerEntries.map(([k, v]) => (
            <div key={k} className="msg-header-row">
              <dt title={k}>{k}</dt>
              <dd title={v}>{v}</dd>
            </div>
          ))}
        </dl>
      </div>
    ) : null

  const metaRight = (
    <span className="msg-head-right">
      <span className="msg-chip">p{m.partition}</span>
      <span className="msg-chip msg-offset">
        offset <strong>{m.offset}</strong>
      </span>
      {headerEntries.length > 0 && (
        <span className="msg-chip msg-headers-chip" title={labels?.headers ?? 'Headers'}>
          hdr <strong>{headerEntries.length}</strong>
        </span>
      )}
      <span className="msg-datetime">{when ?? ''}</span>
      <span className={`badge ${m.format}`} title={`Detected type: ${m.format}`}>
        {formatBadgeLabel(m.format, m.schemaId)}
      </span>
    </span>
  )

  const keyBlock =
    m.key != null ? (
      <span className="msg-key" title={String(m.key)}>
        <em>{keyLabel}</em>
        <strong>{m.key}</strong>
      </span>
    ) : (
      <span className="msg-key muted">—</span>
    )

  if (compact) {
    return (
      <article
        className={`msg msg-compact${expanded ? ' is-open' : ''}${selectedForDiff ? ' msg-diff-selected' : ''}`}
        data-msgid={id}
      >
        <button
          type="button"
          className="msg-compact-row"
          aria-expanded={expanded}
          title={toggleLabel}
          onClick={toggleExpanded}
        >
          <span className="msg-compact-chevron" aria-hidden="true">
            {expanded ? '▾' : '▸'}
          </span>
          {keyBlock}
          {metaRight}
        </button>
        {expanded && (
          <>
            {headersBlock}
            <div className="msg-body-wrap msg-compact-body">
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
              <pre className="msg-body">{highlighted}</pre>
            </div>
          </>
        )}
      </article>
    )
  }

  return (
    <article
      className={`msg${expanded ? ' is-open' : ''}${selectedForDiff ? ' msg-diff-selected' : ''}`}
      data-msgid={id}
    >
      <div className="msg-head">
        <span className="msg-meta">{keyBlock}</span>
        {metaRight}
      </div>
      {labels && (onRepublish || onToggleDiff || needsCollapse) && (
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
          {needsCollapse && (
            <button
              type="button"
              className="msg-action msg-action-icon"
              aria-expanded={expanded}
              title={toggleLabel}
              aria-label={toggleLabel}
              onClick={toggleExpanded}
            >
              {expanded ? <CollapseIcon /> : <ExpandIcon />}
            </button>
          )}
        </div>
      )}
      {headersBlock}
      <div className={`msg-body-wrap${showFullBody ? '' : ' is-collapsed'}`}>
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
        {showFullBody ? (
          <pre className="msg-body">{highlighted}</pre>
        ) : (
          <button
            type="button"
            className="msg-preview"
            aria-expanded={false}
            title={toggleLabel}
            onClick={toggleExpanded}
          >
            <pre className="msg-body">{highlighted}</pre>
          </button>
        )}
      </div>
    </article>
  )
}
