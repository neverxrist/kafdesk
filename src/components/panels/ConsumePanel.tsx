import { useEffect, useMemo, useRef, useState } from 'react'
import type { ConsumeStartFrom, KafkaMessageView, TopicInfo } from '../../types/kafdesk'
import EmptyState from '../EmptyState'
import MessageCard from '../MessageCard'
import MessageDiffPanel from '../MessageDiffPanel'
import type { useLocale } from '../../hooks/useLocale'
import type { SavedConsumeQuery } from '../../lib/savedQueries'
import { RefreshIcon } from '../ConnActionIcons'
import { messageId } from '../../lib/messageText'
import {
  loadBatchTimeSort,
  loadGroupByPartitions,
  loadMessageViewMode,
  persistBatchTimeSort,
  persistGroupByPartitions,
  persistMessageViewMode,
  type BatchTimeSort,
  type MessageViewMode,
} from '../../lib/messageView'

type Tr = ReturnType<typeof useLocale>['tr']

const FILTER_MAX = 255

export function ConsumeToolbar({
  tr,
  selectedTopic,
  topicDetails,
  search,
  setSearch,
  searchKey,
  setSearchKey,
  headerKey,
  setHeaderKey,
  headerValue,
  setHeaderValue,
  timeFrom,
  setTimeFrom,
  timeTo,
  setTimeTo,
  savedQueries,
  onSaveQuery,
  onApplyQuery,
  onDeleteQuery,
  partitionFilter,
  setPartitionFilter,
  startFrom,
  setStartFrom,
  startOffset,
  setStartOffset,
  limit,
  setLimit,
  busy,
  onLoadMessages,
}: {
  tr: Tr
  selectedTopic: string | null
  topicDetails: TopicInfo | null
  search: string
  setSearch: (v: string) => void
  searchKey: string
  setSearchKey: (v: string) => void
  headerKey: string
  setHeaderKey: (v: string) => void
  headerValue: string
  setHeaderValue: (v: string) => void
  timeFrom: string
  setTimeFrom: (v: string) => void
  timeTo: string
  setTimeTo: (v: string) => void
  savedQueries: SavedConsumeQuery[]
  onSaveQuery: (name: string) => void
  onApplyQuery: (id: string) => void
  onDeleteQuery: (id: string) => void
  partitionFilter: string
  setPartitionFilter: (v: string) => void
  startFrom: ConsumeStartFrom
  setStartFrom: (v: ConsumeStartFrom) => void
  startOffset: string
  setStartOffset: (v: string) => void
  limit: number
  setLimit: (v: number) => void
  busy: boolean
  onLoadMessages: () => void
}) {
  const [savedId, setSavedId] = useState('')
  const [queryName, setQueryName] = useState('')
  const [showAdvanced, setShowAdvanced] = useState(false)
  const [limitText, setLimitText] = useState(String(limit))

  const advancedActive =
    (headerKey.trim() ? 1 : 0) +
    (headerValue.trim() ? 1 : 0) +
    (timeFrom ? 1 : 0) +
    (timeTo ? 1 : 0)

  // Keep the raw text while typing (allow empty field); sync from outside
  // changes (saved queries, initial value) only when not focused editing.
  useEffect(() => {
    setLimitText(String(limit))
  }, [limit])

  function commitLimit(raw: string) {
    const n = Number(raw)
    if (!Number.isFinite(n)) return
    const clamped = Math.min(500, Math.max(1, Math.trunc(n)))
    setLimit(clamped)
    setLimitText(String(clamped))
  }

  return (
    <div className="toolbar toolbar-consume">
      <div className="toolbar-row toolbar-consume-main">
        <div className="toolbar-zone toolbar-zone-filter">
          <span className="toolbar-zone-label">{tr('filterTag')}</span>
          <div className="toolbar-zone-fields">
            <div className="field toolbar-filter-field">
              <label htmlFor="consume-search-value">{tr('searchValue')}</label>
              <input
                id="consume-search-value"
                value={search}
                maxLength={FILTER_MAX}
                onChange={(e) => setSearch(e.target.value.slice(0, FILTER_MAX))}
                placeholder={tr('substring')}
                disabled={!selectedTopic}
              />
            </div>
            <div className="field toolbar-filter-field">
              <label htmlFor="consume-search-key">{tr('searchKey')}</label>
              <input
                id="consume-search-key"
                value={searchKey}
                maxLength={FILTER_MAX}
                onChange={(e) => setSearchKey(e.target.value.slice(0, FILTER_MAX))}
                placeholder={tr('keyContains')}
                disabled={!selectedTopic}
              />
            </div>
          </div>
        </div>

        <div className="toolbar-zone toolbar-zone-seek" title={tr('seekHint')}>
          <span className="toolbar-zone-label">{tr('seekTag')}</span>
          <div className="toolbar-zone-fields">
            <div className="field toolbar-seek-field toolbar-seek-part">
              <label htmlFor="consume-partition">{tr('partition')}</label>
              <select
                id="consume-partition"
                value={partitionFilter}
                onChange={(e) => setPartitionFilter(e.target.value)}
                disabled={!topicDetails}
              >
                <option value="all">{tr('all')}</option>
                {topicDetails?.partitions.map((p) => (
                  <option key={p.partitionId} value={String(p.partitionId)}>
                    {p.partitionId}
                  </option>
                ))}
              </select>
            </div>
            <div className="field toolbar-seek-field toolbar-seek-from">
              <label htmlFor="consume-batch">{tr('startFrom')}</label>
              <select
                id="consume-batch"
                value={startFrom}
                onChange={(e) => setStartFrom(e.target.value as ConsumeStartFrom)}
                disabled={!selectedTopic}
              >
                <option value="beginning">{tr('batchFromStart')}</option>
                <option value="end">{tr('batchFromEnd')}</option>
                <option value="offset">{tr('offset')}</option>
              </select>
            </div>
            {startFrom === 'offset' && (
              <div className="field toolbar-seek-field toolbar-seek-offset">
                <label htmlFor="consume-offset">{tr('offset')}</label>
                <input
                  id="consume-offset"
                  value={startOffset}
                  onChange={(e) => setStartOffset(e.target.value)}
                  placeholder="0"
                  disabled={!selectedTopic}
                />
              </div>
            )}
            <div className="field toolbar-seek-field toolbar-seek-limit">
              <label htmlFor="consume-limit">{tr('limit')}</label>
              <input
                id="consume-limit"
                type="number"
                min={1}
                max={500}
                value={limitText}
                onChange={(e) => {
                  setLimitText(e.target.value)
                  const n = Number(e.target.value)
                  if (Number.isFinite(n) && e.target.value.trim() !== '') {
                    setLimit(Math.min(500, Math.max(1, Math.trunc(n))))
                  }
                }}
                onBlur={(e) => {
                  if (e.target.value.trim() === '') {
                    setLimitText(String(limit))
                  } else {
                    commitLimit(e.target.value)
                  }
                }}
                onKeyDown={(e) => {
                  if (e.key === 'Enter') (e.target as HTMLInputElement).blur()
                }}
                disabled={!selectedTopic}
              />
            </div>
          </div>
        </div>

        <div className="toolbar-actions">
          <button
            type="button"
            className={
              showAdvanced || advancedActive ? 'toolbar-more active' : 'toolbar-more'
            }
            onClick={() => setShowAdvanced((v) => !v)}
            aria-expanded={showAdvanced}
            title={
              advancedActive
                ? tr('moreFiltersActive', { n: advancedActive })
                : tr('moreFilters')
            }
          >
            {advancedActive ? (
              <span className="toolbar-more-count">{advancedActive}</span>
            ) : null}
            <MoreFiltersIcon />
          </button>
          <button
            type="button"
            className={`primary toolbar-refresh-btn${busy ? ' busy' : ''}`}
            disabled={!selectedTopic || busy}
            onClick={() => onLoadMessages()}
            title={busy ? tr('loadingMessages') : tr('refresh')}
            aria-label={busy ? tr('loadingMessages') : tr('refresh')}
          >
            <RefreshIcon />
          </button>
        </div>
      </div>

      {showAdvanced && (
        <div className="toolbar-advanced">
          <div className="toolbar-adv-section">
            <span className="toolbar-adv-caption">{tr('advHeadersTime')}</span>
            <div className="toolbar-adv-grid">
              <div className="field">
                <label htmlFor="consume-header-key">{tr('headerKey')}</label>
                <input
                  id="consume-header-key"
                  value={headerKey}
                  maxLength={FILTER_MAX}
                  onChange={(e) => setHeaderKey(e.target.value.slice(0, FILTER_MAX))}
                  placeholder={tr('headerKeyPh')}
                  disabled={!selectedTopic}
                />
              </div>
              <div className="field">
                <label htmlFor="consume-header-value">{tr('headerValue')}</label>
                <input
                  id="consume-header-value"
                  value={headerValue}
                  maxLength={FILTER_MAX}
                  onChange={(e) => setHeaderValue(e.target.value.slice(0, FILTER_MAX))}
                  placeholder={tr('headerValuePh')}
                  disabled={!selectedTopic}
                />
              </div>
              <div className="field">
                <label htmlFor="consume-time-from">{tr('timeFrom')}</label>
                <input
                  id="consume-time-from"
                  type="datetime-local"
                  value={timeFrom}
                  onChange={(e) => setTimeFrom(e.target.value)}
                  disabled={!selectedTopic}
                />
              </div>
              <div className="field toolbar-adv-time-to">
                <label htmlFor="consume-time-to">{tr('timeTo')}</label>
                <div className="toolbar-adv-time-wrap">
                  <input
                    id="consume-time-to"
                    type="datetime-local"
                    value={timeTo}
                    onChange={(e) => setTimeTo(e.target.value)}
                    disabled={!selectedTopic}
                  />
                  {(timeFrom || timeTo) && (
                    <button
                      type="button"
                      className="toolbar-clear-time"
                      onClick={() => {
                        setTimeFrom('')
                        setTimeTo('')
                      }}
                      title={tr('clear')}
                      aria-label={tr('clear')}
                    >
                      ×
                    </button>
                  )}
                </div>
              </div>
            </div>
          </div>

          <div className="toolbar-adv-section toolbar-adv-saved">
            <span className="toolbar-adv-caption">{tr('savedQueries')}</span>
            <div className="toolbar-adv-saved-rows">
              <div className="toolbar-adv-saved-row">
                <select
                  value={savedId}
                  onChange={(e) => setSavedId(e.target.value)}
                  aria-label={tr('savedQueries')}
                >
                  <option value="">
                    {savedQueries.length ? tr('selectQuery') : tr('noSavedQueries')}
                  </option>
                  {savedQueries.map((q) => (
                    <option key={q.id} value={q.id}>
                      {q.name}
                      {q.topic ? ` · ${q.topic}` : ''}
                    </option>
                  ))}
                </select>
                <button
                  type="button"
                  className="toolbar-adv-btn"
                  disabled={!savedId || busy}
                  onClick={() => onApplyQuery(savedId)}
                >
                  {tr('applyQuery')}
                </button>
                <button
                  type="button"
                  className="toolbar-adv-btn"
                  disabled={!savedId}
                  onClick={() => {
                    onDeleteQuery(savedId)
                    setSavedId('')
                  }}
                >
                  {tr('deleteQuery')}
                </button>
              </div>
              <div className="toolbar-adv-saved-row">
                <input
                  value={queryName}
                  onChange={(e) => setQueryName(e.target.value)}
                  placeholder={tr('queryNamePh')}
                  disabled={!selectedTopic}
                  aria-label={tr('queryNamePh')}
                />
                <button
                  type="button"
                  className="primary toolbar-adv-btn toolbar-adv-save"
                  disabled={!queryName.trim() || !selectedTopic}
                  onClick={() => {
                    onSaveQuery(queryName)
                    setQueryName('')
                  }}
                >
                  {tr('saveQuery')}
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

function compareByTime(a: KafkaMessageView, b: KafkaMessageView, newestFirst: boolean): number {
  const ta = Number(a.timestamp)
  const tb = Number(b.timestamp)
  if (ta !== tb) return newestFirst ? tb - ta : ta - tb
  if (a.partition !== b.partition) {
    return newestFirst ? b.partition - a.partition : a.partition - b.partition
  }
  const oa = Number(a.offset)
  const ob = Number(b.offset)
  return newestFirst ? ob - oa : oa - ob
}

function groupByPartition(
  messages: KafkaMessageView[],
  newestFirst: boolean,
): Array<{
  partition: number
  messages: KafkaMessageView[]
}> {
  const map = new Map<number, KafkaMessageView[]>()
  for (const m of messages) {
    const list = map.get(m.partition)
    if (list) list.push(m)
    else map.set(m.partition, [m])
  }
  return [...map.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([partition, msgs]) => ({
      partition,
      messages: [...msgs].sort((a, b) => compareByTime(a, b, newestFirst)),
    }))
}

export default function ConsumePanel({
  tr,
  topicDetails,
  topicGroupLags,
  messages,
  windowMessageCount = 0,
  selectedTopic,
  busy,
  startFrom = 'end',
  diffMessages,
  onToggleDiff,
  onClearDiff,
  onRepublish,
  onOpenConsumerGroup,
}: {
  tr: Tr
  topicDetails: TopicInfo | null
  topicGroupLags: Array<{ groupId: string; totalLag: number }> | null
  /** Already client-filtered view of the loaded window */
  messages: KafkaMessageView[]
  /** Raw broker window size (before search/key/headers/time) */
  windowMessageCount?: number
  selectedTopic: string | null
  busy?: boolean
  startFrom?: ConsumeStartFrom
  diffMessages: KafkaMessageView[]
  onToggleDiff: (m: KafkaMessageView) => void
  onClearDiff: () => void
  onRepublish: (m: KafkaMessageView) => void
  onOpenConsumerGroup?: (groupId: string) => void
}) {
  const [hiddenPartitions, setHiddenPartitions] = useState<Set<number>>(() => new Set())
  const [viewMode, setViewMode] = useState<MessageViewMode>(() => loadMessageViewMode())
  const [groupByPartitions, setGroupByPartitions] = useState(() => loadGroupByPartitions())
  const [timeSort, setTimeSort] = useState<BatchTimeSort>(
    () => loadBatchTimeSort() ?? (startFrom === 'end' ? 'newest' : 'oldest'),
  )
  const newestFirst = timeSort === 'newest'

  function setView(mode: MessageViewMode) {
    setViewMode(mode)
    persistMessageViewMode(mode)
  }

  function setGroupBy(on: boolean) {
    setGroupByPartitions(on)
    persistGroupByPartitions(on)
  }

  function setBatchSort(sort: BatchTimeSort) {
    setTimeSort(sort)
    persistBatchTimeSort(sort)
  }

  function toggleBatchSort() {
    setBatchSort(newestFirst ? 'oldest' : 'newest')
  }

  const byPartition = useMemo(
    () => groupByPartition(messages, newestFirst),
    [messages, newestFirst],
  )
  const visiblePartitions = useMemo(
    () => byPartition.filter((g) => !hiddenPartitions.has(g.partition)),
    [byPartition, hiddenPartitions],
  )
  const flatMessages = useMemo(
    () =>
      messages
        .filter((m) => !hiddenPartitions.has(m.partition))
        .sort((a, b) => compareByTime(a, b, newestFirst)),
    [messages, hiddenPartitions, newestFirst],
  )
  const diffIds = useMemo(() => new Set(diffMessages.map(messageId)), [diffMessages])
  const diffFull = diffMessages.length >= 2

  useEffect(() => {
    setHiddenPartitions(new Set())
  }, [selectedTopic])

  // When Seek/Batch direction changes, reset list order to match (flip still local).
  const prevStartFrom = useRef(startFrom)
  useEffect(() => {
    if (prevStartFrom.current === startFrom) return
    prevStartFrom.current = startFrom
    setBatchSort(startFrom === 'end' ? 'newest' : 'oldest')
  }, [startFrom])

  function togglePartition(id: number) {
    setHiddenPartitions((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }

  function renderCard(m: KafkaMessageView) {
    return (
      <MessageCard
        key={`${m.partition}-${m.offset}`}
        message={m}
        keyLabel={tr('msgKey')}
        viewMode={viewMode}
        selectedForDiff={diffIds.has(messageId(m))}
        diffFull={diffFull}
        onToggleDiff={viewMode === 'full' ? onToggleDiff : undefined}
        onRepublish={viewMode === 'full' ? onRepublish : undefined}
        labels={{
          republish: tr('republish'),
          diffSelect: tr('diffSelect'),
          diffSelected: tr('diffSelected'),
          expand: tr('expand'),
          collapse: tr('collapse'),
          copyValue: tr('copyValue'),
          copied: tr('copied'),
          headers: tr('msgHeaders'),
        }}
      />
    )
  }

  return (
    <section className="messages drag-scroll">
      {topicDetails && (
        <div className="panel messages-topic-panel">
          <div className="messages-panel-head">
            <div className="topic-head-text">
              <div className="topic-title-row">
                <h2>
                  {topicDetails.name}
                  {topicDetails.approxMessages != null && (
                    <span className="topic-meta">
                      {' '}
                      · {tr('msgsApprox', { n: topicDetails.approxMessages })}
                    </span>
                  )}
                </h2>
                <div className="partitions">
                  {topicDetails.partitions.map((p) => {
                    const hidden = hiddenPartitions.has(p.partitionId)
                    const offsets = tr('partitionOffsets', {
                      low: p.offsetLow ?? '?',
                      high: p.offsetHigh ?? '?',
                    })
                    return (
                      <button
                        key={p.partitionId}
                        type="button"
                        className={`chip partition-chip${hidden ? ' muted' : ' active'}`}
                        title={
                          hidden
                            ? tr('partitionFilterOff')
                            : `${tr('partitionFilterOn', { id: p.partitionId })} · ${offsets}`
                        }
                        aria-pressed={!hidden}
                        onClick={() => togglePartition(p.partitionId)}
                      >
                        <strong>p{p.partitionId}</strong>
                        <span className="partition-msgs">
                          ≈{p.approxMessages ?? '?'}
                        </span>
                        <span className="partition-offsets">{offsets}</span>
                      </button>
                    )
                  })}
                </div>
              </div>
              {topicGroupLags != null && (
                <div className="topic-lag-row" title={tr('topicConsumersLabel')}>
                  <span className="topic-lag-label">{tr('topicConsumersLabel')}</span>
                  {topicGroupLags.length === 0 ? (
                    <span className="topic-lag-empty">{tr('topicLagNone')}</span>
                  ) : (
                    topicGroupLags.map((g) => (
                      <button
                        key={g.groupId}
                        type="button"
                        className={`topic-lag-chip${g.totalLag > 0 ? ' behind' : ''}`}
                        title={`${g.groupId}: lag ${g.totalLag} · ${tr('topicConsumerOpen')}`}
                        onClick={() => onOpenConsumerGroup?.(g.groupId)}
                      >
                        <em>{g.groupId}</em>
                        <strong>{g.totalLag}</strong>
                      </button>
                    ))
                  )}
                </div>
              )}
            </div>
            <div className="messages-panel-controls">
              <label className="message-group-opt">
                <input
                  type="checkbox"
                  checked={groupByPartitions}
                  onChange={(e) => setGroupBy(e.target.checked)}
                />
                {tr('groupByPartitions')}
              </label>
              <button
                type="button"
                className="batch-sort-btn"
                onClick={toggleBatchSort}
                title={newestFirst ? tr('batchSortNewest') : tr('batchSortOldest')}
                aria-label={newestFirst ? tr('batchSortNewest') : tr('batchSortOldest')}
              >
                <BatchSortIcon newestFirst={newestFirst} />
              </button>
              <div className="message-view-toggle" role="group" aria-label={tr('messageView')}>
                <button
                  type="button"
                  className={viewMode === 'full' ? 'active' : undefined}
                  onClick={() => setView('full')}
                >
                  {tr('viewFull')}
                </button>
                <button
                  type="button"
                  className={viewMode === 'compact' ? 'active' : undefined}
                  onClick={() => setView('compact')}
                >
                  {tr('viewCompact')}
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {diffMessages.length === 2 && (
        <MessageDiffPanel
          tr={tr}
          left={diffMessages[0]}
          right={diffMessages[1]}
          onClear={onClearDiff}
        />
      )}
      {diffMessages.length === 1 && (
        <div className="msg-diff-hint panel">
          {tr('diffPickSecond')}
          <button type="button" onClick={onClearDiff}>
            {tr('diffClear')}
          </button>
        </div>
      )}

      {messages.length === 0 && (
        <EmptyState
          kind="events"
          title={tr('emptyEventsTitle')}
          hint={
            !selectedTopic
              ? tr('emptyEventsHint')
              : busy
                ? tr('loadingMessages')
                : windowMessageCount > 0
                  ? tr('noMessagesFilter')
                  : tr('noMessagesWindow')
          }
        />
      )}

      {groupByPartitions
        ? visiblePartitions.map(({ partition, messages: partMsgs }) => (
            <div key={partition} className="partition-group">
              <div className="partition-group-head">
                {tr('loadedInPartition', { id: partition, n: partMsgs.length })}
              </div>
              {partMsgs.map(renderCard)}
            </div>
          ))
        : flatMessages.map(renderCard)}
    </section>
  )
}

function MoreFiltersIcon() {
  return (
    <svg width="15" height="15" viewBox="0 0 16 16" aria-hidden="true">
      <path
        fill="currentColor"
        d="M2.4 3.2h11.2l-3.9 4.7v3.4L6.3 13.2V7.9L2.4 3.2z"
        opacity="0.9"
      />
    </svg>
  )
}

/** Clock + direction — batch timestamp order (local, no seek). */
function BatchSortIcon({ newestFirst }: { newestFirst: boolean }) {
  return (
    <svg width="16" height="16" viewBox="0 0 16 16" aria-hidden="true">
      <circle cx="7" cy="8" r="5.25" fill="none" stroke="currentColor" strokeWidth="1.35" />
      <path
        fill="none"
        stroke="currentColor"
        strokeWidth="1.35"
        strokeLinecap="round"
        d="M7 5.2v3.1l2.1 1.25"
      />
      {newestFirst ? (
        <path
          fill="currentColor"
          d="M13.15 3.1v3.55h-.95V4.55l-1.35 1.35-.65-.65L12.5 3.1h.65z"
        />
      ) : (
        <path
          fill="currentColor"
          d="M13.15 12.9V9.35h-.95v2.1l-1.35-1.35-.65.65 2.3 2.15h.65z"
        />
      )}
    </svg>
  )
}
