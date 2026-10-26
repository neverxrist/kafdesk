import { useEffect, useMemo, useRef, useState, type PointerEvent as ReactPointerEvent } from 'react'
import type { KafkaMessageView } from '../types/kafdesk'
import { formatBadgeLabel, formatMessageDateTime } from '../lib/format'
import { highlightMessageBody } from '../lib/jsonHighlight'
import { messageBodyTextFiltered } from '../lib/jsonSearchFilter'
import { searchHighlightQueries, searchHitsNeedles, type SearchClause } from '../lib/searchScope'
import { highlightSearchMatches } from '../lib/searchHighlight'
import { messageId } from '../lib/messageText'
import MessageDiffPanel from './MessageDiffPanel'
import TruncatedValueNotice, { useFullMessage } from './TruncatedValueNotice'
import type { useLocale } from '../hooks/useLocale'

type Tr = ReturnType<typeof useLocale>['tr']

const DOCK_HEIGHT_KEY = 'kafdesk.msgDockHeight'
const DEFAULT_H = 280
const MIN_H = 160
const MAX_H_RATIO = 0.72

function loadDockHeight(): number {
  try {
    const n = Number(localStorage.getItem(DOCK_HEIGHT_KEY))
    if (Number.isFinite(n) && n >= MIN_H) return n
  } catch {
    /* ignore */
  }
  return DEFAULT_H
}

function persistDockHeight(h: number) {
  try {
    localStorage.setItem(DOCK_HEIGHT_KEY, String(Math.round(h)))
  } catch {
    /* ignore */
  }
}

export default function MessageInspectorDock({
  tr,
  messages,
  activeId,
  showDiff,
  onShowDiffChange,
  diffMessages,
  onToggleDiff,
  onClearDiff,
  onSelect,
  onClose,
  onRepublish,
  searchClauses = [],
  searchHitsOn = [false, false],
}: {
  tr: Tr
  messages: KafkaMessageView[]
  activeId: string | null
  showDiff: boolean
  onShowDiffChange: (on: boolean) => void
  diffMessages: KafkaMessageView[]
  onToggleDiff: (m: KafkaMessageView) => void
  onClearDiff: () => void
  onSelect: (id: string) => void
  onClose: () => void
  onRepublish?: (m: KafkaMessageView) => void
  searchClauses?: SearchClause[]
  /** Per-row Hits toggles (same order as searchClauses). */
  searchHitsOn?: [boolean, boolean]
}) {
  const [height, setHeight] = useState(loadDockHeight)
  const [diffOnlyChanges, setDiffOnlyChanges] = useState(false)
  const dragRef = useRef<{ startY: number; startH: number } | null>(null)
  const index = activeId ? messages.findIndex((m) => messageId(m) === activeId) : -1
  const full = useFullMessage(index >= 0 ? messages[index] : null)
  const active = full.shown
  const diffReady = showDiff && diffMessages.length === 2

  useEffect(() => {
    if (!diffReady) setDiffOnlyChanges(false)
  }, [diffReady])

  const { keyQueries, valueQueries } = searchHighlightQueries(searchClauses)
  const hitsQueries = useMemo(
    () => searchHitsNeedles(searchClauses, searchHitsOn),
    [searchClauses, searchHitsOn],
  )
  const matchesOnly = hitsQueries.length > 0

  const bodyHtml = useMemo(
    () =>
      active && !diffReady
        ? highlightMessageBody(
            messageBodyTextFiltered(active, hitsQueries, matchesOnly),
            valueQueries,
          )
        : null,
    [active, diffReady, hitsQueries, valueQueries, matchesOnly],
  )

  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      const t = e.target as HTMLElement | null
      if (t && (t.tagName === 'INPUT' || t.tagName === 'TEXTAREA' || t.tagName === 'SELECT' || t.isContentEditable)) {
        return
      }
      if (e.key === 'Escape') {
        e.preventDefault()
        onClose()
        return
      }
      if (diffReady) return
      if (e.key === 'ArrowLeft' || e.key === 'ArrowUp') {
        e.preventDefault()
        if (index > 0) onSelect(messageId(messages[index - 1]))
      } else if (e.key === 'ArrowRight' || e.key === 'ArrowDown') {
        e.preventDefault()
        if (index >= 0 && index < messages.length - 1) onSelect(messageId(messages[index + 1]))
      }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [diffReady, index, messages, onClose, onSelect])

  function onResizePointerDown(e: ReactPointerEvent<HTMLDivElement>) {
    e.preventDefault()
    dragRef.current = { startY: e.clientY, startH: height }
    e.currentTarget.setPointerCapture(e.pointerId)
  }

  function onResizePointerMove(e: ReactPointerEvent<HTMLDivElement>) {
    const drag = dragRef.current
    if (!drag) return
    const maxH = Math.floor(window.innerHeight * MAX_H_RATIO)
    const next = Math.min(maxH, Math.max(MIN_H, drag.startH + (drag.startY - e.clientY)))
    setHeight(next)
  }

  function onResizePointerUp(e: ReactPointerEvent<HTMLDivElement>) {
    if (!dragRef.current) return
    dragRef.current = null
    try {
      e.currentTarget.releasePointerCapture(e.pointerId)
    } catch {
      /* ignore */
    }
    setHeight((h) => {
      persistDockHeight(h)
      return h
    })
  }

  const headers = active ? Object.entries(active.headers || {}) : []

  return (
    <aside className="msg-dock" style={{ height }} aria-label={tr('msgDockTitle')}>
      <div
        className="msg-dock-resize"
        onPointerDown={onResizePointerDown}
        onPointerMove={onResizePointerMove}
        onPointerUp={onResizePointerUp}
        onPointerCancel={onResizePointerUp}
        role="separator"
        aria-orientation="horizontal"
        title={tr('msgDockResize')}
      />
      <div className="msg-dock-toolbar">
        <div className="msg-dock-toolbar-left">
          <strong className="msg-dock-title">
            {diffReady ? tr('diffTitle') : tr('msgDockTitle')}
          </strong>
          {!diffReady && active && (
            <span className="msg-dock-pos">
              {tr('msgDockPos', { i: index + 1, n: messages.length })}
            </span>
          )}
          <label className="msg-dock-diff-toggle" title={tr('msgDockDiffTitle')}>
            <input
              type="checkbox"
              checked={showDiff}
              onChange={(e) => onShowDiffChange(e.target.checked)}
            />
            {tr('msgDockDiff')}
          </label>
        </div>
        <div className="msg-dock-toolbar-right">
          {!diffReady && (
            <>
              <button
                type="button"
                className="msg-dock-nav"
                disabled={index <= 0}
                onClick={() => index > 0 && onSelect(messageId(messages[index - 1]))}
                title={tr('msgDockPrev')}
                aria-label={tr('msgDockPrev')}
              >
                ‹
              </button>
              <button
                type="button"
                className="msg-dock-nav"
                disabled={index < 0 || index >= messages.length - 1}
                onClick={() =>
                  index >= 0 &&
                  index < messages.length - 1 &&
                  onSelect(messageId(messages[index + 1]))
                }
                title={tr('msgDockNext')}
                aria-label={tr('msgDockNext')}
              >
                ›
              </button>
            </>
          )}
          {active && onRepublish && !diffReady && (
            <button type="button" className="msg-dock-action" onClick={() => onRepublish(active)}>
              {tr('republish')}
            </button>
          )}
          {diffReady && (
            <>
              <button
                type="button"
                className={diffOnlyChanges ? 'msg-dock-action msg-diff-only active' : 'msg-dock-action msg-diff-only'}
                aria-pressed={diffOnlyChanges}
                onClick={() => setDiffOnlyChanges((v) => !v)}
              >
                {tr('diffOnlyChanges')}
              </button>
              <button type="button" className="msg-dock-action" onClick={onClearDiff}>
                {tr('diffClear')}
              </button>
            </>
          )}
          <button
            type="button"
            className="msg-dock-close"
            onClick={onClose}
            title={tr('msgDockClose')}
            aria-label={tr('msgDockClose')}
          >
            ×
          </button>
        </div>
      </div>

      <div className="msg-dock-body drag-scroll">
        {diffReady ? (
          <MessageDiffPanel
            tr={tr}
            left={diffMessages[0]}
            right={diffMessages[1]}
            onClear={onClearDiff}
            onRemove={onToggleDiff}
            embedded
            onlyChanges={diffOnlyChanges}
            onOnlyChangesChange={setDiffOnlyChanges}
          />
        ) : showDiff && diffMessages.length === 1 ? (
          <div className="msg-dock-hint">
            <span>{tr('diffPickSecond')}</span>
            <button type="button" onClick={() => onToggleDiff(diffMessages[0])}>
              {tr('diffRemove')}
            </button>
          </div>
        ) : showDiff && diffMessages.length === 0 ? (
          <div className="msg-dock-hint">{tr('msgDockDiffEmpty')}</div>
        ) : active ? (
          <div className="msg-dock-detail">
            <div className="msg-dock-meta">
              <span className="msg-chip">p{active.partition}</span>
              <span className="msg-chip msg-offset">
                offset <strong>{active.offset}</strong>
              </span>
              <span className="msg-datetime">{formatMessageDateTime(active.timestamp) ?? ''}</span>
              <span className={`badge ${active.format}`}>
                {formatBadgeLabel(active.format, active.schemaId)}
              </span>
            </div>
            {active.key != null && active.key !== '' && (
              <div className="msg-dock-key" title={String(active.key)}>
                <em>{tr('msgKey')}</em>
                <strong>
                  {keyQueries.length
                    ? highlightSearchMatches(String(active.key), keyQueries)
                    : String(active.key)}
                </strong>
              </div>
            )}
            {headers.length > 0 && (
              <details className="msg-headers">
                <summary className="msg-headers-label">
                  {tr('msgHeaders')}
                  <span className="msg-headers-count">{headers.length}</span>
                </summary>
                <dl className="msg-headers-list">
                  {headers.map(([k, v]) => (
                    <div key={k} className="msg-header-row">
                      <dt title={k}>{k}</dt>
                      <dd title={v}>{v}</dd>
                    </div>
                  ))}
                </dl>
              </details>
            )}
            {active.truncatedBytes ? (
              <TruncatedValueNotice
                bytes={active.truncatedBytes}
                text={tr('msgTruncated')}
                action={tr('msgLoadFull')}
                loading={full.loading}
                error={full.error}
                onLoad={() => void full.load()}
              />
            ) : null}
            <pre className="msg-body msg-dock-json">{bodyHtml}</pre>
          </div>
        ) : (
          <div className="msg-dock-hint">{tr('msgDockEmpty')}</div>
        )}
      </div>
    </aside>
  )
}
