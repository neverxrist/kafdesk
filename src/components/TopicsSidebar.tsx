import { useEffect, useState } from 'react'
import type { TopicInfo } from '../../electron/types'
import type { useLocale } from '../hooks/useLocale'
import { isDlqTopic, isInternalTopic } from '../lib/pinnedTopics'
import {
  loadTopicFilters,
  persistTopicFilters,
  type SavedTopicFilter,
} from '../lib/topicFilters'

type Tr = ReturnType<typeof useLocale>['tr']

export default function TopicsSidebar({
  tr,
  connected,
  filteredTopics,
  topicQuery,
  setTopicQuery,
  selectedTopic,
  onSelectTopic,
  pinnedTopics,
  onTogglePin,
}: {
  tr: Tr
  connected: boolean
  filteredTopics: TopicInfo[]
  topicQuery: string
  setTopicQuery: (v: string) => void
  selectedTopic: string | null
  onSelectTopic: (name: string) => void
  pinnedTopics: string[]
  onTogglePin: (name: string) => void
}) {
  const pinnedSet = new Set(pinnedTopics)
  const pinned = filteredTopics.filter((t) => pinnedSet.has(t.name))
  const dlq = filteredTopics.filter(
    (t) => !pinnedSet.has(t.name) && isDlqTopic(t.name) && !isInternalTopic(t.name),
  )
  const internal = filteredTopics.filter(
    (t) => !pinnedSet.has(t.name) && isInternalTopic(t.name),
  )
  const rest = filteredTopics.filter(
    (t) =>
      !pinnedSet.has(t.name) && !isDlqTopic(t.name) && !isInternalTopic(t.name),
  )

  const [dlqOpen, setDlqOpen] = useState(false)
  const [internalOpen, setInternalOpen] = useState(false)
  const [savedFilters, setSavedFilters] = useState<SavedTopicFilter[]>(() => loadTopicFilters())

  // Open the fold that contains the selected topic
  useEffect(() => {
    if (!selectedTopic || pinnedSet.has(selectedTopic)) return
    if (isDlqTopic(selectedTopic) && !isInternalTopic(selectedTopic)) setDlqOpen(true)
    if (isInternalTopic(selectedTopic)) setInternalOpen(true)
  }, [selectedTopic]) // eslint-disable-line react-hooks/exhaustive-deps

  function saveCurrentFilter() {
    const query = topicQuery.trim()
    if (!query) return
    const next: SavedTopicFilter[] = [
      {
        id: crypto.randomUUID(),
        name: query,
        query,
        createdAt: Date.now(),
      },
      ...savedFilters.filter((f) => f.query.toLowerCase() !== query.toLowerCase()),
    ].slice(0, 20)
    setSavedFilters(next)
    persistTopicFilters(next)
  }

  function applyFilter(f: SavedTopicFilter) {
    setTopicQuery(f.query)
  }

  function deleteFilter(id: string) {
    const next = savedFilters.filter((f) => f.id !== id)
    setSavedFilters(next)
    persistTopicFilters(next)
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
          title={t.name}
          onClick={() => onSelectTopic(t.name)}
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

  return (
    <section className="topics">
      <div className="field">
        <label>{tr('topics', { n: filteredTopics.length })}</label>
        <div className="topic-filter-row">
          <input
            value={topicQuery}
            onChange={(e) => setTopicQuery(e.target.value)}
            placeholder={tr('filterTopics')}
            disabled={!connected}
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
        </div>
        {savedFilters.length > 0 && (
          <div className="topic-filter-chips" aria-label={tr('savedTopicFilters')}>
            {savedFilters.map((f) => (
              <span
                key={f.id}
                className={`topic-filter-chip${topicQuery === f.query ? ' active' : ''}`}
              >
                <button type="button" className="topic-filter-chip-apply" onClick={() => applyFilter(f)}>
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
      </div>
      {!connected && <div className="empty">{tr('connectToSeeTopics')}</div>}

      {pinned.length > 0 && (
        <div className="topic-section">
          <div className="topic-section-head">{tr('pinnedTopics')}</div>
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
    </section>
  )
}
