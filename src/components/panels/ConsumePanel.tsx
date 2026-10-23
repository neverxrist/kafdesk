import { useEffect, useMemo, useRef, useState } from 'react'
import type { ConsumeStartFrom, KafkaMessageView, TopicInfo } from '../../types/kafdesk'
import EmptyState from '../EmptyState'
import MessageCard from '../MessageCard'
import MessageDiffPanel from '../MessageDiffPanel'
import MessageInspectorDock from '../MessageInspectorDock'
import MsgFontControls from '../MsgFontControls'
import VirtualList from '../VirtualList'
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
import { DualMessageSearchFilters } from '../MessageSearchFilter'
import BatchExportControls from '../BatchExportControls'
import type { SearchClause, SearchScope } from '../../lib/searchScope'

type Tr = ReturnType<typeof useLocale>['tr']

const FILTER_MAX = 255

export function ConsumeToolbar({
  tr,
  selectedTopic,
  topicDetails,
  search,
  setSearch,
  searchScope,
  setSearchScope,
  search2,
  setSearch2,
  searchScope2,
  setSearchScope2,
  dualSearch,
  onOpenDualSearch,
  onCloseDualSearch,
  searchHitsOn,
  onSearchHitsSlot,
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
  searchScope: SearchScope
  setSearchScope: (v: SearchScope) => void
  search2: string
  setSearch2: (v: string) => void
  searchScope2: SearchScope
  setSearchScope2: (v: SearchScope) => void
  dualSearch: boolean
  onOpenDualSearch: () => void
  onCloseDualSearch: () => void
  searchHitsOn: [boolean, boolean]
  onSearchHitsSlot: (slot: 0 | 1, on: boolean) => void
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
    const clamped = Math.min(1000, Math.max(1, Math.trunc(n)))
    setLimit(clamped)
    setLimitText(String(clamped))
  }

  return (
    <div className="toolbar toolbar-consume">
      <div className="toolbar-row toolbar-consume-main">
        <div className="toolbar-zone toolbar-zone-filter">
          <span className="toolbar-zone-label">{tr('filterTag')}</span>
          <div className="toolbar-zone-fields">
            <DualMessageSearchFilters
              tr={tr}
              idPrefix="consume"
              search={search}
              searchScope={searchScope}
              setSearch={setSearch}
              setSearchScope={setSearchScope}
              search2={search2}
              searchScope2={searchScope2}
              setSearch2={setSearch2}
              setSearchScope2={setSearchScope2}
              dualSearch={dualSearch}
              onOpenDual={onOpenDualSearch}
              onCloseDual={onCloseDualSearch}
              hitsOn={searchHitsOn}
              onHitsOn={onSearchHitsSlot}
              disabled={!selectedTopic}
              maxLength={FILTER_MAX}
            />
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
                max={1000}
                value={limitText}
                onChange={(e) => {
                  setLimitText(e.target.value)
                  const n = Number(e.target.value)
                  if (Number.isFinite(n) && e.target.value.trim() !== '') {
                    setLimit(Math.min(1000, Math.max(1, Math.trunc(n))))
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
  searchClauses = [],
  searchHitsOn = [false, false],
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
  searchClauses?: SearchClause[]
  searchHitsOn?: [boolean, boolean]
}) {
  const [hiddenPartitions, setHiddenPartitions] = useState<Set<number>>(() => new Set())
  const [viewMode, setViewMode] = useState<MessageViewMode>(() => loadMessageViewMode())
  const [groupByPartitions, setGroupByPartitions] = useState(() => loadGroupByPartitions())
  const [timeSort, setTimeSort] = useState<BatchTimeSort>(
    () => loadBatchTimeSort() ?? (startFrom === 'end' ? 'newest' : 'oldest'),
  )
  const [dockOpen, setDockOpen] = useState(false)
  const [inspectId, setInspectId] = useState<string | null>(null)
  const [dockShowDiff, setDockShowDiff] = useState(() => {
    try {
      return localStorage.getItem('kafdesk.msgDockDiff') === '1'
    } catch {
      return false
    }
  })
  const newestFirst = timeSort === 'newest'

  function setView(mode: MessageViewMode) {
    setViewMode(mode)
    persistMessageViewMode(mode)
    // Bottom inspector is for compact / Offset Explorer style — hide in full.
    // Clear comparison on either switch so slot colors / inline / dock diff don't orphan.
    if (diffMessages.length > 0) onClearDiff()
    if (mode === 'full') {
      setDockOpen(false)
      setDockDiff(false)
    } else {
      setDockDiff(false)
    }
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
    setDockOpen(false)
    setInspectId(null)
  }, [selectedTopic])

  // When Seek/Batch direction changes, reset list order to match (flip still local).
  const prevStartFrom = useRef(startFrom)
  useEffect(() => {
    if (prevStartFrom.current === startFrom) return
    prevStartFrom.current = startFrom
    setBatchSort(startFrom === 'end' ? 'newest' : 'oldest')
  }, [startFrom])

  useEffect(() => {
    if (!inspectId) return
    if (!flatMessages.some((m) => messageId(m) === inspectId)) {
      setInspectId(flatMessages[0] ? messageId(flatMessages[0]) : null)
      if (flatMessages.length === 0) setDockOpen(false)
    }
  }, [flatMessages, inspectId])

  useEffect(() => {
    if (dockShowDiff && diffMessages.length === 2) setDockOpen(true)
  }, [dockShowDiff, diffMessages.length])

  const prevDiffLen = useRef(diffMessages.length)
  useEffect(() => {
    // Leaving diff mode (Clear / last selection removed) → drop the checkbox.
    if (prevDiffLen.current > 0 && diffMessages.length === 0 && dockShowDiff) {
      setDockDiff(false)
    }
    prevDiffLen.current = diffMessages.length
  }, [diffMessages.length, dockShowDiff])

  function openInspect(m: KafkaMessageView) {
    if (dockShowDiff) {
      // Diff-in-panel: click messages to pick them (same toggle as Diff button).
      onToggleDiff(m)
      if (viewMode === 'full') return
      setInspectId(messageId(m))
      setDockOpen(true)
      return
    }
    if (viewMode === 'full') return
    const id = messageId(m)
    if (dockOpen && inspectId === id) {
      setDockOpen(false)
      setInspectId(null)
      return
    }
    setInspectId(id)
    setDockOpen(true)
  }

  function setDockDiff(on: boolean) {
    setDockShowDiff(on)
    try {
      localStorage.setItem('kafdesk.msgDockDiff', on ? '1' : '0')
    } catch {
      /* ignore */
    }
    if (!on) return
    if (viewMode !== 'full') setDockOpen(true)
    // Already viewing a message → treat it as the first diff pick.
    if (diffMessages.length === 0 && inspectId) {
      const current = flatMessages.find((m) => messageId(m) === inspectId)
      if (current) onToggleDiff(current)
    }
  }

  function togglePartition(id: number) {
    setHiddenPartitions((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }

  function renderCard(m: KafkaMessageView) {
    const id = messageId(m)
    const canInspect = viewMode === 'compact' || dockShowDiff
    const di = diffMessages.findIndex((x) => messageId(x) === id)
    const diffSlot = di === 0 ? 1 : di === 1 ? 2 : undefined
    return (
      <MessageCard
        key={`${m.partition}-${m.offset}`}
        message={m}
        keyLabel={tr('msgKey')}
        viewMode={viewMode}
        selectedForDiff={diffIds.has(id)}
        diffSlot={diffSlot}
        diffFull={diffFull}
        inspected={dockOpen && inspectId === id}
        searchClauses={searchClauses}
        searchHitsOn={searchHitsOn}
        onToggleDiff={dockShowDiff ? undefined : onToggleDiff}
        onRepublish={onRepublish}
        onInspect={canInspect ? openInspect : undefined}
        labels={{
          republish: tr('republish'),
          diffSelect: tr('diffSelect'),
          diffSelected: tr('diffSelected'),
          expand: tr('expand'),
          collapse: tr('collapse'),
          copyValue: tr('copyValue'),
          copied: tr('copied'),
          headers: tr('msgHeaders'),
          inspect: tr('msgDockOpen'),
        }}
      />
    )
  }

  // Compact: diff only in the bottom dock. Full: classic inline panel.
  const showInlineDiff = viewMode === 'full'
  // Virt only in Compact — Full cards vary a lot in height; absolute windowing overlapped text.
  const useVirt = viewMode === 'compact' && !groupByPartitions

  return (
    <section className={`messages${dockOpen ? ' messages-with-dock' : ''}`}>
      {topicDetails && (
        <div className="panel messages-topic-panel">
          <div className="messages-panel-head">
            <div className="messages-panel-toolbar">
              <div className="topic-toolbar-main">
                <h2 title={topicDetails.name}>{topicDetails.name}</h2>
                <div className="partitions" aria-label={tr('partitionsCount', { n: topicDetails.partitions.length })}>
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
                        <span className="partition-msgs">≈{p.approxMessages ?? '?'}</span>
                      </button>
                    )
                  })}
                  <button
                    type="button"
                    className={`batch-sort-btn group-by-partitions-btn${groupByPartitions ? ' active' : ''}`}
                    title={tr('groupByPartitions')}
                    aria-label={tr('groupByPartitions')}
                    aria-pressed={groupByPartitions}
                    onClick={() => setGroupBy(!groupByPartitions)}
                  >
                    <GroupByPartitionsIcon />
                  </button>
                </div>
                {(topicGroupLags == null || topicGroupLags.length > 0) && (
                  <div className="topic-lag-row" title={tr('topicConsumersLabel')}>
                    <span className="topic-lag-label">{tr('topicConsumersLabel')}</span>
                    {topicGroupLags == null ? (
                      <span className="topic-lag-empty">{tr('topicLagLoading')}</span>
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
                <button
                  type="button"
                  className="batch-sort-btn"
                  onClick={toggleBatchSort}
                  title={newestFirst ? tr('batchSortNewest') : tr('batchSortOldest')}
                  aria-label={newestFirst ? tr('batchSortNewest') : tr('batchSortOldest')}
                >
                  <BatchSortIcon newestFirst={newestFirst} />
                </button>
                <MsgFontControls tr={tr} />
                <BatchExportControls tr={tr} messages={flatMessages} topic={selectedTopic} />
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
        </div>
      )}

      {showInlineDiff && diffMessages.length === 2 && (
        <MessageDiffPanel
          tr={tr}
          left={diffMessages[0]}
          right={diffMessages[1]}
          onClear={onClearDiff}
          onRemove={onToggleDiff}
        />
      )}
      {showInlineDiff && diffMessages.length === 1 && (
        <div className="msg-diff-hint panel">
          <span>{tr('diffPickSecond')}</span>
          <button type="button" onClick={() => onToggleDiff(diffMessages[0])}>
            {tr('diffRemove')}
          </button>
        </div>
      )}

      {messages.length === 0 ? (
        <div className="messages-scroll drag-scroll">
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
        </div>
      ) : useVirt ? (
        <VirtualList
          items={flatMessages}
          estimateSize={48}
          gap={6}
          getKey={(m) => messageId(m)}
          className="messages-virt"
        >
          {(m) => renderCard(m)}
        </VirtualList>
      ) : (
        <div className="messages-scroll drag-scroll">
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
        </div>
      )}

      {dockOpen && (
        <MessageInspectorDock
          tr={tr}
          messages={flatMessages}
          activeId={inspectId}
          showDiff={dockShowDiff}
          onShowDiffChange={setDockDiff}
          diffMessages={diffMessages}
          onToggleDiff={onToggleDiff}
          onClearDiff={onClearDiff}
          onSelect={setInspectId}
          onClose={() => {
            setDockOpen(false)
            // Closing the dock dismisses comparison — Full must not reopen it inline.
            if (diffMessages.length > 0) onClearDiff()
            setDockDiff(false)
          }}
          onRepublish={onRepublish}
          searchClauses={searchClauses}
          searchHitsOn={searchHitsOn}
        />
      )}
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

/** Stacked sections — group the batch by partition. */
function GroupByPartitionsIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 16 16" aria-hidden="true">
      <path
        fill="none"
        stroke="currentColor"
        strokeWidth="1.35"
        strokeLinecap="round"
        d="M2.5 3.25h11M2.5 4.85h7.5M2.5 7.75h11M2.5 9.35h7.5M2.5 12.25h11M2.5 13.85h7.5"
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
