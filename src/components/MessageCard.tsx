import { useEffect, useMemo, useState, type MouseEvent } from 'react'
import type { KafkaMessageView } from '../types/kafdesk'
import { formatBadgeLabel, formatMessageDateTime } from '../lib/format'
import { highlightMessageBody } from '../lib/jsonHighlight'
import { messageBodyTextFiltered } from '../lib/jsonSearchFilter'
import { messageBodyText, messageId } from '../lib/messageText'
import type { MessageViewMode } from '../lib/messageView'
import { searchHighlightQueries, searchHitsNeedles, type SearchClause } from '../lib/searchScope'
import { highlightSearchMatches } from '../lib/searchHighlight'
import TruncatedValueNotice, { useFullMessage } from './TruncatedValueNotice'

/** Collapsed preview ≈ 2 lines; shorter bodies stay fully open. */
const PREVIEW_MAX_LINES = 2
const PREVIEW_CHARS_PER_LINE = 96
const PREVIEW_MAX_CHARS = 400

function bodyNeedsCollapse(body: string): boolean {
  const lines = body.split('\n')
  if (lines.length > PREVIEW_MAX_LINES) return true
  const longest = lines.reduce((n, line) => Math.max(n, line.length), 0)
  return longest > PREVIEW_CHARS_PER_LINE * PREVIEW_MAX_LINES
}

/** Cheap sample for collapse heuristic — no pretty-print. */
function collapseSample(m: KafkaMessageView): string {
  if (typeof m.value === 'string') return m.value
  if (m.rawPreview) return m.rawPreview
  try {
    return JSON.stringify(m.value)
  } catch {
    return String(m.value)
  }
}

/** Plain truncated preview for collapsed Full cards (no highlight spans). */
function truncatePreview(text: string): string {
  const lines = text.split('\n')
  const head = lines.slice(0, PREVIEW_MAX_LINES).join('\n')
  const clipped =
    head.length > PREVIEW_MAX_CHARS ? `${head.slice(0, PREVIEW_MAX_CHARS)}…` : head
  const more = lines.length > PREVIEW_MAX_LINES || text.length > clipped.length
  if (!more) return clipped
  return clipped.endsWith('…') ? clipped : `${clipped}…`
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
  message,
  keyLabel = 'key',
  viewMode = 'full',
  selectedForDiff = false,
  diffSlot,
  diffFull = false,
  inspected = false,
  searchClauses = [],
  searchHitsOn = [false, false],
  onToggleDiff,
  onRepublish,
  onInspect,
  labels,
}: {
  message: KafkaMessageView
  keyLabel?: string
  viewMode?: MessageViewMode
  selectedForDiff?: boolean
  /** 1 = left / first pick, 2 = right / compare-with */
  diffSlot?: 1 | 2
  diffFull?: boolean
  inspected?: boolean
  /** Live AND filter clauses — highlight. */
  searchClauses?: SearchClause[]
  /** Per-row Hits toggles (same order as searchClauses). */
  searchHitsOn?: [boolean, boolean]
  onToggleDiff?: (m: KafkaMessageView) => void
  onRepublish?: (m: KafkaMessageView) => void
  onInspect?: (m: KafkaMessageView) => void
  labels?: {
    republish: string
    diffSelect: string
    diffSelected: string
    expand: string
    collapse: string
    copyValue: string
    copied: string
    headers?: string
    inspect?: string
    /** `{size}` → original payload size */
    truncated?: string
    loadFull?: string
  }
}) {
  const full = useFullMessage(message)
  const m = full.shown ?? message
  const headerEntries = Object.entries(m.headers || {})
  const when = formatMessageDateTime(m.timestamp)
  const id = messageId(m)
  const compact = viewMode === 'compact'

  const needsCollapse = useMemo(
    () => bodyNeedsCollapse(collapseSample(m)),
    [m.value, m.rawPreview],
  )
  /** Full: collapse only when taller than ~2 lines; Compact: hidden until open. */
  const [expanded, setExpanded] = useState(() => !compact && !needsCollapse)
  const [copied, setCopied] = useState(false)
  const toggleLabel = expanded
    ? labels?.collapse ?? 'Collapse'
    : labels?.expand ?? 'Expand'
  const showFullBody = compact ? expanded : expanded || !needsCollapse

  const { keyQueries, valueQueries } = searchHighlightQueries(searchClauses)
  const hitsQueries = useMemo(
    () => searchHitsNeedles(searchClauses, searchHitsOn),
    [searchClauses, searchHitsOn],
  )
  const matchesOnly = hitsQueries.length > 0

  // Pretty body + highlight only when the full body is on screen (RAM).
  const fullBody = useMemo(
    () => (showFullBody ? messageBodyTextFiltered(m, hitsQueries, matchesOnly) : null),
    [m.value, m.rawPreview, showFullBody, hitsQueries, matchesOnly],
  )

  const highlighted = useMemo(
    () => (fullBody != null ? highlightMessageBody(fullBody, valueQueries) : null),
    [fullBody, valueQueries],
  )
  // Collapsed Full preview: plain truncate from cheap sample (no token spans).
  const previewPlain = useMemo(() => {
    if (showFullBody || compact) return null
    const plain = truncatePreview(collapseSample(m))
    return valueQueries.length ? highlightSearchMatches(plain, valueQueries) : plain
  }, [m.value, m.rawPreview, showFullBody, compact, valueQueries])

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
    const text = fullBody ?? messageBodyText(m)
    if (navigator.clipboard?.writeText) {
      void navigator.clipboard.writeText(text).catch(() => {
        fallbackCopy(text)
      })
    } else {
      fallbackCopy(text)
    }
    setCopied(true)
    window.setTimeout(() => setCopied(false), 1200)
  }

  function toggleExpanded() {
    setExpanded((v) => !v)
  }

  const headersBlock =
    headerEntries.length > 0 ? (
      <details className="msg-headers">
        <summary className="msg-headers-label">
          {labels?.headers ?? 'Headers'}
          <span className="msg-headers-count">{headerEntries.length}</span>
        </summary>
        <dl className="msg-headers-list">
          {headerEntries.map(([k, v]) => (
            <div key={k} className="msg-header-row">
              <dt title={k}>{k}</dt>
              <dd title={v}>{v}</dd>
            </div>
          ))}
        </dl>
      </details>
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
      <span
        className="msg-key allow-select"
        title={String(m.key)}
        onMouseDown={(e) => e.stopPropagation()}
        onClick={(e) => e.stopPropagation()}
      >
        <em>{keyLabel}</em>
        <strong>
          {keyQueries.length ? highlightSearchMatches(String(m.key), keyQueries) : m.key}
        </strong>
      </span>
    ) : (
      <span className="msg-key muted">—</span>
    )

  const showActions =
    Boolean(labels) &&
    (Boolean(onRepublish && labels?.republish) ||
      Boolean(onToggleDiff && labels?.diffSelect) ||
      Boolean(onInspect && labels?.inspect) ||
      (!compact && needsCollapse))

  const actionsBlock = showActions ? (
    <div className="msg-actions">
      {onInspect && labels?.inspect && (
        <button
          type="button"
          className={`msg-action${inspected ? ' active' : ''}`}
          onClick={() => onInspect(m)}
        >
          {labels.inspect}
        </button>
      )}
      {onRepublish && labels?.republish && (
        <button type="button" className="msg-action" onClick={() => onRepublish(m)}>
          {labels.republish}
        </button>
      )}
      {onToggleDiff && labels?.diffSelect && (
        <button
          type="button"
          className={`msg-action ${selectedForDiff ? 'active' : ''}`}
          disabled={!selectedForDiff && diffFull}
          onClick={() => onToggleDiff(m)}
        >
          {selectedForDiff ? labels.diffSelected : labels.diffSelect}
        </button>
      )}
      {!compact && needsCollapse && (
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
  ) : null

  const truncatedNotice =
    m.truncatedBytes && labels?.truncated && labels.loadFull ? (
      <TruncatedValueNotice
        bytes={m.truncatedBytes}
        text={labels.truncated}
        action={labels.loadFull}
        loading={full.loading}
        error={full.error}
        onLoad={() => void full.load()}
      />
    ) : null

  const diffClass = diffSlot
    ? ` msg-diff-selected msg-diff-slot-${diffSlot}`
    : selectedForDiff
      ? ' msg-diff-selected'
      : ''

  if (compact) {
    const openRow = () => {
      const sel = window.getSelection()
      // Don't open dock after a text selection (e.g. copying the key).
      if (sel && !sel.isCollapsed) return
      if (onInspect) onInspect(m)
      else toggleExpanded()
    }
    return (
      <article
        className={`msg msg-compact${expanded ? ' is-open' : ''}${diffClass}${inspected ? ' msg-inspected' : ''}`}
        data-msgid={id}
      >
        <div
          className="msg-compact-row"
          role={onInspect ? 'button' : undefined}
          tabIndex={onInspect ? 0 : undefined}
          title={labels?.inspect ?? toggleLabel}
          onClick={openRow}
          onKeyDown={
            onInspect
              ? (e) => {
                  if (e.key === 'Enter' || e.key === ' ') {
                    e.preventDefault()
                    openRow()
                  }
                }
              : undefined
          }
        >
          <button
            type="button"
            className="msg-compact-chevron"
            aria-expanded={expanded}
            title={toggleLabel}
            aria-label={toggleLabel}
            onClick={(e) => {
              e.stopPropagation()
              toggleExpanded()
            }}
          >
            {expanded ? '▾' : '▸'}
          </button>
          {keyBlock}
          <span className="msg-compact-meta">{metaRight}</span>
        </div>
        {expanded && (
          <>
            {actionsBlock}
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
              {truncatedNotice}
              <pre className="msg-body">{highlighted}</pre>
            </div>
          </>
        )}
      </article>
    )
  }

  return (
    <article
      className={`msg${expanded ? ' is-open' : ''}${diffClass}${inspected ? ' msg-inspected' : ''}`}
      data-msgid={id}
    >
      <div
        className={`msg-head${onInspect ? ' msg-head-inspectable' : ''}`}
        onClick={onInspect ? () => onInspect(m) : undefined}
        onKeyDown={
          onInspect
            ? (e) => {
                if (e.key === 'Enter' || e.key === ' ') {
                  e.preventDefault()
                  onInspect(m)
                }
              }
            : undefined
        }
        role={onInspect ? 'button' : undefined}
        tabIndex={onInspect ? 0 : undefined}
        title={onInspect && labels?.inspect ? labels.inspect : undefined}
      >
        <span className="msg-meta">{keyBlock}</span>
        {metaRight}
      </div>
      {actionsBlock}
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
        {truncatedNotice}
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
            <pre className="msg-body">{previewPlain}</pre>
          </button>
        )}
      </div>
    </article>
  )
}
