import { useEffect, useRef, useState, type MouseEvent as ReactMouseEvent, type PointerEvent as ReactPointerEvent } from 'react'
import EmptyState from './EmptyState'
import TopicSchemaPanel from './TopicSchemaPanel'
import type { TopicInfo } from '../types/kafdesk'
import type { useLocale } from '../hooks/useLocale'
import { isDlqTopic, isInternalTopic } from '../lib/pinnedTopics'
import {
  clampTopicsWidth,
  loadOnlyPinnedTopics,
  loadTopicFilters,
  loadTopicsOpen,
  loadTopicsWidth,
  persistOnlyPinnedTopics,
  persistTopicFilters,
  persistTopicsOpen,
  persistTopicsWidth,
  type SavedTopicFilter,
} from '../lib/topicFilters'

type Tr = ReturnType<typeof useLocale>['tr']

type CtxMenu = { topic: string; x: number; y: number }
type SchemaPopover = { topic: string; x: number; y: number }

function clampPopoverPos(x: number, y: number, w: number, h: number) {
  const pad = 8
  const maxX = Math.max(pad, window.innerWidth - w - pad)
  const maxY = Math.max(pad, window.innerHeight - h - pad)
  return {
    x: Math.min(Math.max(pad, x), maxX),
    y: Math.min(Math.max(pad, y), maxY),
  }
}

export default function TopicsSidebar({
  tr,
  connected,
  topics,
  topicQuery,
  setTopicQuery,
  selectedTopic,
  onSelectTopic,
  pinnedTopics,
  onTogglePin,
  subjects,
  hasRegistry,
  onOpenSubjectInSchemas,
}: {
  tr: Tr
  connected: boolean
  topics: TopicInfo[]
  topicQuery: string
  setTopicQuery: (v: string) => void
  selectedTopic: string | null
  onSelectTopic: (name: string) => void
  pinnedTopics: string[]
  onTogglePin: (name: string) => void
  subjects: string[]
  hasRegistry: boolean
  onOpenSubjectInSchemas?: (subject: string) => void
}) {
  const [topicsOpen, setTopicsOpen] = useState(() => loadTopicsOpen())
  const [topicsWidth, setTopicsWidth] = useState(() => loadTopicsWidth())
  const [resizing, setResizing] = useState(false)
  const resizeStart = useRef<{ x: number; w: number } | null>(null)
  const [dlqOpen, setDlqOpen] = useState(false)
  const [internalOpen, setInternalOpen] = useState(false)
  const [savedFilters, setSavedFilters] = useState<SavedTopicFilter[]>(() => loadTopicFilters())
  const [activeFilterIds, setActiveFilterIds] = useState<string[]>([])
  const [onlyPinned, setOnlyPinned] = useState(() => loadOnlyPinnedTopics())
  const [ctxMenu, setCtxMenu] = useState<CtxMenu | null>(null)
  const [schemaPopover, setSchemaPopover] = useState<SchemaPopover | null>(null)
  const ctxMenuRef = useRef<HTMLDivElement | null>(null)
  const schemaPopoverRef = useRef<HTMLDivElement | null>(null)

  function setTopicsPanelOpen(open: boolean) {
    setTopicsOpen(open)
    persistTopicsOpen(open)
  }

  useEffect(() => {
    if (!resizing) return
    function onMove(e: PointerEvent) {
      const start = resizeStart.current
      if (!start) return
      const next = clampTopicsWidth(start.w + (e.clientX - start.x))
      setTopicsWidth(next)
    }
    function onUp() {
      resizeStart.current = null
      setResizing(false)
      document.body.classList.remove('topics-resizing')
      setTopicsWidth((w) => {
        persistTopicsWidth(w)
        return w
      })
    }
    window.addEventListener('pointermove', onMove)
    window.addEventListener('pointerup', onUp)
    window.addEventListener('pointercancel', onUp)
    return () => {
      window.removeEventListener('pointermove', onMove)
      window.removeEventListener('pointerup', onUp)
      window.removeEventListener('pointercancel', onUp)
    }
  }, [resizing])

  function onResizePointerDown(e: ReactPointerEvent<HTMLDivElement>) {
    e.preventDefault()
    resizeStart.current = { x: e.clientX, w: topicsWidth }
    setResizing(true)
    document.body.classList.add('topics-resizing')
  }

  useEffect(() => {
    if (!ctxMenu && !schemaPopover) return
    function onKey(e: KeyboardEvent) {
      if (e.key === 'Escape') {
        setCtxMenu(null)
        setSchemaPopover(null)
      }
    }
    function onPointerDown(e: PointerEvent) {
      const t = e.target as Node
      if (ctxMenuRef.current?.contains(t)) return
      if (schemaPopoverRef.current?.contains(t)) return
      setCtxMenu(null)
      setSchemaPopover(null)
    }
    window.addEventListener('keydown', onKey)
    window.addEventListener('pointerdown', onPointerDown)
    return () => {
      window.removeEventListener('keydown', onKey)
      window.removeEventListener('pointerdown', onPointerDown)
    }
  }, [ctxMenu, schemaPopover])

  function onTopicContextMenu(e: ReactMouseEvent, topic: string) {
    e.preventDefault()
    e.stopPropagation()
    setSchemaPopover(null)
    setCtxMenu({ topic, x: e.clientX, y: e.clientY })
  }

  function openTopicSchema() {
    if (!ctxMenu) return
    const approxW = 420
    const approxH = 320
    const pos = clampPopoverPos(ctxMenu.x, ctxMenu.y, approxW, approxH)
    setSchemaPopover({ topic: ctxMenu.topic, ...pos })
    setCtxMenu(null)
  }

  const chipQueries = savedFilters
    .filter((f) => activeFilterIds.includes(f.id))
    .map((f) => f.query.trim().toLowerCase())
    .filter(Boolean)
  const typedQuery = topicQuery.trim().toLowerCase()
  const filteredTopics = topics.filter((t) => {
    const name = t.name.toLowerCase()
    const chipOk = chipQueries.length === 0 || chipQueries.some((q) => name.includes(q))
    const typedOk = !typedQuery || name.includes(typedQuery)
    return chipOk && typedOk
  })

  const pinnedSet = new Set(pinnedTopics)
  const pinned = filteredTopics.filter((t) => pinnedSet.has(t.name))
  const dlq = onlyPinned
    ? []
    : filteredTopics.filter(
        (t) => !pinnedSet.has(t.name) && isDlqTopic(t.name) && !isInternalTopic(t.name),
      )
  const internal = onlyPinned
    ? []
    : filteredTopics.filter((t) => !pinnedSet.has(t.name) && isInternalTopic(t.name))
  const rest = onlyPinned
    ? []
    : filteredTopics.filter(
        (t) =>
          !pinnedSet.has(t.name) && !isDlqTopic(t.name) && !isInternalTopic(t.name),
      )
  const visibleCount = onlyPinned ? pinned.length : filteredTopics.length

  function setOnlyPinnedPref(on: boolean) {
    setOnlyPinned(on)
    persistOnlyPinnedTopics(on)
  }

  // Can't keep "only pinned" on with nothing starred
  useEffect(() => {
    if (onlyPinned && pinnedTopics.length === 0) setOnlyPinnedPref(false)
  }, [pinnedTopics.length, onlyPinned]) // eslint-disable-line react-hooks/exhaustive-deps

  // Open the fold that contains the selected topic
  useEffect(() => {
    if (!selectedTopic || pinnedSet.has(selectedTopic)) return
    if (isDlqTopic(selectedTopic) && !isInternalTopic(selectedTopic)) setDlqOpen(true)
    if (isInternalTopic(selectedTopic)) setInternalOpen(true)
  }, [selectedTopic]) // eslint-disable-line react-hooks/exhaustive-deps

  function saveCurrentFilter() {
    const query = topicQuery.trim()
    if (!query) return
    const existing = savedFilters.find((f) => f.query.toLowerCase() === query.toLowerCase())
    if (existing) {
      setActiveFilterIds((ids) => (ids.includes(existing.id) ? ids : [...ids, existing.id]))
      setTopicQuery('')
      return
    }
    const id = crypto.randomUUID()
    const next: SavedTopicFilter[] = [
      {
        id,
        name: query,
        query,
        createdAt: Date.now(),
      },
      ...savedFilters,
    ].slice(0, 20)
    setSavedFilters(next)
    persistTopicFilters(next)
    setActiveFilterIds((ids) => [...ids, id])
    setTopicQuery('')
  }

  function toggleFilter(f: SavedTopicFilter) {
    setActiveFilterIds((ids) =>
      ids.includes(f.id) ? ids.filter((id) => id !== f.id) : [...ids, f.id],
    )
  }

  function deleteFilter(id: string) {
    const next = savedFilters.filter((f) => f.id !== id)
    setSavedFilters(next)
    persistTopicFilters(next)
    setActiveFilterIds((ids) => ids.filter((x) => x !== id))
  }

  function renderTopic(t: TopicInfo) {
    const isPinned = pinnedSet.has(t.name)
    const active = selectedTopic === t.name
    return (
      <div key={t.name} className={`topic-row ${active ? 'active' : ''}`}>
        <button
          type="button"
          className="topic-pin"
          title={isPinned ? tr('unpinTopic') : tr('pinTopic')}
          aria-label={`${isPinned ? tr('unpinTopic') : tr('pinTopic')}: ${t.name}`}
          onClick={(e) => {
            e.stopPropagation()
            onTogglePin(t.name)
          }}
        >
          {isPinned ? '★' : '☆'}
        </button>
        <button
          type="button"
          className="topic-item"
          title={`${t.name} · ${tr('topicSchemaCtxHint')}`}
          onClick={() => onSelectTopic(t.name)}
          onContextMenu={(e) => onTopicContextMenu(e, t.name)}
        >
          <span className="topic-name">{t.name}</span>
          <span className="topic-stats" aria-label={tr('partitionsCount', { n: t.partitions.length })}>
            {t.approxMessages != null && (
              <span className="topic-stat">{tr('topicMsgsShort', { n: t.approxMessages })}</span>
            )}
            <span className="topic-stat">p{t.partitions.length}</span>
          </span>
        </button>
      </div>
    )
  }

  const canSaveFilter = Boolean(topicQuery.trim())

  if (!topicsOpen) {
    return (
      <section className="topics topics-collapsed" aria-label={tr('topics', { n: visibleCount })}>
        <button
          type="button"
          className="topics-expand-btn"
          title={tr('expandTopics')}
          aria-label={tr('expandTopics')}
          onClick={() => setTopicsPanelOpen(true)}
        >
          <span className="topics-expand-chevron">⟩</span>
          <span className="topics-expand-label">{tr('topicsShort')}</span>
        </button>
      </section>
    )
  }

  return (
    <section
      className={`topics drag-scroll${resizing ? ' is-resizing' : ''}`}
      style={{ width: topicsWidth }}
    >
      <div
        className="topics-resize"
        role="separator"
        aria-orientation="vertical"
        aria-valuenow={topicsWidth}
        title={tr('resizeTopics')}
        onPointerDown={onResizePointerDown}
      />
      <div className="topics-head">
        <label className="topics-head-label">{tr('topics', { n: visibleCount })}</label>
      </div>
      <div className="topic-filter-row">
        <input
          value={topicQuery}
          onChange={(e) => setTopicQuery(e.target.value)}
          placeholder={tr('filterTopics')}
          disabled={!connected}
          aria-label={tr('filterTopics')}
          autoCapitalize="none"
          autoCorrect="off"
          autoComplete="off"
          spellCheck={false}
          lang="en"
        />
        <button
          type="button"
          className="topic-filter-save"
          title={tr('saveTopicFilterTitle')}
          disabled={!connected || !canSaveFilter}
          onClick={saveCurrentFilter}
        >
          {tr('saveTopicFilter')}
        </button>
        <button
          type="button"
          className="topics-collapse-btn"
          title={tr('collapseTopics')}
          aria-label={tr('collapseTopics')}
          onClick={() => setTopicsPanelOpen(false)}
        >
          ⟨
        </button>
      </div>
      {savedFilters.length > 0 && (
        <div className="topic-filter-chips" aria-label={tr('savedTopicFilters')}>
          {savedFilters.map((f) => (
            <span
              key={f.id}
              className={`topic-filter-chip${activeFilterIds.includes(f.id) ? ' active' : ''}`}
            >
              <button
                type="button"
                className="topic-filter-chip-apply"
                aria-pressed={activeFilterIds.includes(f.id)}
                title={f.query}
                onClick={() => toggleFilter(f)}
              >
                {f.name}
              </button>
              <button
                type="button"
                className="topic-filter-chip-del"
                title={tr('deleteTopicFilter')}
                aria-label={tr('deleteTopicFilter')}
                onClick={() => deleteFilter(f.id)}
              >
                ×
              </button>
            </span>
          ))}
        </div>
      )}
      {!connected && (
        <EmptyState kind="topics" title={tr('emptyTopicsTitle')} hint={tr('emptyTopicsHint')} />
      )}

      {connected && onlyPinned && pinned.length === 0 && pinnedTopics.length > 0 && (
        <EmptyState kind="topics" title={tr('emptyTopicsTitle')} hint={tr('onlyPinnedEmpty')} />
      )}

      {connected &&
        !onlyPinned &&
        pinned.length === 0 &&
        rest.length === 0 &&
        dlq.length === 0 &&
        internal.length === 0 && (
          <EmptyState kind="topics" title={tr('emptyTopicsTitle')} hint={tr('emptyTopicsNone')} />
        )}

      {(pinned.length > 0 || (onlyPinned && pinnedTopics.length > 0)) && (
        <div className="topic-section">
          <div className="topic-section-head">
            <span>{tr('pinnedTopics')}</span>
            {pinnedTopics.length > 0 && (
              <label className="topic-only-pinned">
                <input
                  type="checkbox"
                  checked={onlyPinned}
                  disabled={!connected}
                  onChange={(e) => setOnlyPinnedPref(e.target.checked)}
                />
                {tr('onlyPinnedTopics')}
              </label>
            )}
          </div>
          {pinned.map(renderTopic)}
        </div>
      )}

      {rest.length > 0 && (
        <div className="topic-section">
          {(pinned.length > 0 || dlq.length > 0 || internal.length > 0) && (
            <div className="topic-section-head">{tr('allTopics')}</div>
          )}
          {rest.map(renderTopic)}
        </div>
      )}

      {dlq.length > 0 && (
        <div className="topic-section">
          <button
            type="button"
            className="topic-fold-toggle"
            aria-expanded={dlqOpen}
            onClick={() => setDlqOpen((v) => !v)}
          >
            <span>{tr('dlqTopics')}</span>
            <span className="topic-fold-meta">
              {tr('topicsFoldCount', { n: dlq.length })}
              <span className="topic-fold-chevron">{dlqOpen ? '▴' : '▾'}</span>
            </span>
          </button>
          {dlqOpen && dlq.map(renderTopic)}
        </div>
      )}

      {internal.length > 0 && (
        <div className="topic-section">
          <button
            type="button"
            className="topic-fold-toggle"
            aria-expanded={internalOpen}
            onClick={() => setInternalOpen((v) => !v)}
          >
            <span>{tr('internalTopics')}</span>
            <span className="topic-fold-meta">
              {tr('topicsFoldCount', { n: internal.length })}
              <span className="topic-fold-chevron">{internalOpen ? '▴' : '▾'}</span>
            </span>
          </button>
          {internalOpen && internal.map(renderTopic)}
        </div>
      )}

      {ctxMenu && (
        <div
          ref={ctxMenuRef}
          className="topic-ctx-menu"
          style={{ left: ctxMenu.x, top: ctxMenu.y }}
          role="menu"
        >
          <button
            type="button"
            role="menuitem"
            className="topic-ctx-item"
            onClick={openTopicSchema}
          >
            {tr('topicSchema')}
          </button>
        </div>
      )}

      {schemaPopover && (
        <div
          ref={schemaPopoverRef}
          className="topic-schema-popover"
          style={{ left: schemaPopover.x, top: schemaPopover.y }}
          role="dialog"
          aria-label={tr('topicSchema')}
        >
          <div className="topic-schema-popover-head">
            <strong title={schemaPopover.topic}>{schemaPopover.topic}</strong>
            <button
              type="button"
              className="topic-schema-popover-close"
              aria-label={tr('close')}
              onClick={() => setSchemaPopover(null)}
            >
              ×
            </button>
          </div>
          <TopicSchemaPanel
            tr={tr}
            topic={schemaPopover.topic}
            subjects={subjects}
            hasRegistry={hasRegistry}
            onOpenInSchemas={
              onOpenSubjectInSchemas
                ? (subject) => {
                    setSchemaPopover(null)
                    onOpenSubjectInSchemas(subject)
                  }
                : undefined
            }
          />
        </div>
      )}
    </section>
  )
}
