import { useEffect, useMemo, useState } from 'react'
import type { ConsumeStartFrom, KafkaMessageView, TopicInfo } from '../../../electron/types'
import MessageCard from '../MessageCard'
import MessageDiffPanel from '../MessageDiffPanel'
import type { useLocale } from '../../hooks/useLocale'
import type { SavedConsumeQuery } from '../../lib/savedQueries'
import { messageId } from '../../lib/messageText'

type Tr = ReturnType<typeof useLocale>['tr']

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
      <div className="toolbar-row">
        <div className="field">
          <label>{tr('searchValue')}</label>
          <input
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder={tr('substring')}
            disabled={!selectedTopic}
          />
        </div>
        <div className="field">
          <label>{tr('searchKey')}</label>
          <input
            value={searchKey}
            onChange={(e) => setSearchKey(e.target.value)}
            placeholder={tr('keyContains')}
            disabled={!selectedTopic}
          />
        </div>
        <div className="field" style={{ maxWidth: 120 }}>
          <label>{tr('partition')}</label>
          <select
            value={partitionFilter}
            onChange={(e) => setPartitionFilter(e.target.value)}
            disabled={!topicDetails}
          >
            <option value="all">{tr('all')}</option>
            {topicDetails?.partitions.map((p) => (
              <option key={p.partitionId} value={String(p.partitionId)}>
                p{p.partitionId}
                {p.approxMessages != null ? ` · ≈${p.approxMessages}` : ''}
              </option>
            ))}
          </select>
        </div>
        <div className="field" style={{ maxWidth: 140 }}>
          <label>{tr('startFrom')}</label>
          <select value={startFrom} onChange={(e) => setStartFrom(e.target.value as ConsumeStartFrom)}>
            <option value="beginning">{tr('beginning')}</option>
            <option value="end">{tr('end')}</option>
            <option value="offset">{tr('offset')}</option>
          </select>
        </div>
        {startFrom === 'offset' && (
          <div className="field" style={{ maxWidth: 120 }}>
            <label>{tr('offset')}</label>
            <input value={startOffset} onChange={(e) => setStartOffset(e.target.value)} placeholder="0" />
          </div>
        )}
        <div className="field" style={{ maxWidth: 90 }}>
          <label>{tr('limit')}</label>
          <input
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
          />
        </div>
        <button
          type="button"
          className={showAdvanced || advancedActive ? 'toolbar-more active' : 'toolbar-more'}
          onClick={() => setShowAdvanced((v) => !v)}
          aria-expanded={showAdvanced}
        >
          {advancedActive
            ? tr('moreFiltersActive', { n: advancedActive })
            : tr('moreFilters')}{' '}
          {showAdvanced ? '▴' : '▾'}
        </button>
        <button
          type="button"
          className="primary"
          disabled={!selectedTopic}
          onClick={() => onLoadMessages()}
          title={tr('refresh')}
        >
          {busy ? tr('loadingMessages') : tr('refresh')}
        </button>
      </div>

      {showAdvanced && (
      <div className="toolbar-row toolbar-row-filters">
        <div className="field">
          <label>{tr('headerKey')}</label>
          <input
            value={headerKey}
            onChange={(e) => setHeaderKey(e.target.value)}
            placeholder={tr('headerKeyPh')}
            disabled={!selectedTopic}
          />
        </div>
        <div className="field">
          <label>{tr('headerValue')}</label>
          <input
            value={headerValue}
            onChange={(e) => setHeaderValue(e.target.value)}
            placeholder={tr('headerValuePh')}
            disabled={!selectedTopic}
          />
        </div>
        <div className="field" style={{ minWidth: 190 }}>
          <label>{tr('timeFrom')}</label>
          <input
            type="datetime-local"
            value={timeFrom}
            onChange={(e) => setTimeFrom(e.target.value)}
            disabled={!selectedTopic}
          />
        </div>
        <div className="field" style={{ minWidth: 190 }}>
          <label>{tr('timeTo')}</label>
          <input
            type="datetime-local"
            value={timeTo}
            onChange={(e) => setTimeTo(e.target.value)}
            disabled={!selectedTopic}
          />
        </div>
        {(timeFrom || timeTo) && (
          <button
            type="button"
            onClick={() => {
              setTimeFrom('')
              setTimeTo('')
            }}
          >
            ×
          </button>
        )}
        <div className="field saved-queries-field">
          <label>{tr('savedQueries')}</label>
          <div className="saved-queries-row">
            <select value={savedId} onChange={(e) => setSavedId(e.target.value)}>
              <option value="">{savedQueries.length ? tr('selectQuery') : tr('noSavedQueries')}</option>
              {savedQueries.map((q) => (
                <option key={q.id} value={q.id}>
                  {q.name}
                  {q.topic ? ` · ${q.topic}` : ''}
                </option>
              ))}
            </select>
            <button
              type="button"
              disabled={!savedId || busy}
              onClick={() => onApplyQuery(savedId)}
            >
              {tr('applyQuery')}
            </button>
            <button
              type="button"
              disabled={!savedId}
              onClick={() => {
                onDeleteQuery(savedId)
                setSavedId('')
              }}
            >
              {tr('deleteQuery')}
            </button>
            <input
              value={queryName}
              onChange={(e) => setQueryName(e.target.value)}
              placeholder={tr('queryNamePh')}
              disabled={!selectedTopic}
            />
            <button
              type="button"
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
      )}
    </div>
  )
}

function groupByPartition(messages: KafkaMessageView[]): Array<{
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
      messages: [...msgs].sort((a, b) => Number(b.offset) - Number(a.offset)),
    }))
}

export default function ConsumePanel({
  tr,
  topicDetails,
  topicLag,
  messages,
  selectedTopic,
  busy,
  diffMessages,
  onToggleDiff,
  onClearDiff,
  onRepublish,
}: {
  tr: Tr
  topicDetails: TopicInfo | null
  topicLag: number | null
  messages: KafkaMessageView[]
  selectedTopic: string | null
  busy?: boolean
  diffMessages: KafkaMessageView[]
  onToggleDiff: (m: KafkaMessageView) => void
  onClearDiff: () => void
  onRepublish: (m: KafkaMessageView) => void
}) {
  const byPartition = useMemo(() => groupByPartition(messages), [messages])
  const diffIds = useMemo(() => new Set(diffMessages.map(messageId)), [diffMessages])
  const diffFull = diffMessages.length >= 2

  return (
    <section className="messages">
      {topicDetails && (
        <div className="panel" style={{ marginBottom: '0.9rem' }}>
          <h2>
            {topicDetails.name}
            {topicDetails.approxMessages != null && (
              <span className="topic-meta">
                {' '}
                · {tr('msgsApprox', { n: topicDetails.approxMessages })}
              </span>
            )}
            {topicLag != null && (
              <span className="topic-meta topic-lag">
                {' '}
                · {tr('topicLag', { n: topicLag })}
              </span>
            )}
          </h2>
          <div className="partitions">
            {topicDetails.partitions.map((p) => (
              <span key={p.partitionId} className="chip partition-chip">
                <strong>
                  {tr('partitionMsgs', {
                    id: p.partitionId,
                    n: p.approxMessages ?? '?',
                  })}
                </strong>
                <span className="partition-offsets">
                  {tr('partitionOffsets', {
                    low: p.offsetLow ?? '?',
                    high: p.offsetHigh ?? '?',
                  })}
                </span>
              </span>
            ))}
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
        <div className="empty">
          {!selectedTopic
            ? tr('selectTopic')
            : busy
              ? tr('loadingMessages')
              : tr('noMessagesWindow')}
        </div>
      )}

      {byPartition.map(({ partition, messages: partMsgs }) => (
        <div key={partition} className="partition-group">
          <div className="partition-group-head">
            {tr('loadedInPartition', { id: partition, n: partMsgs.length })}
          </div>
          {partMsgs.map((m) => (
            <MessageCard
              key={`${m.partition}-${m.offset}`}
              message={m}
              keyLabel={tr('msgKey')}
              selectedForDiff={diffIds.has(messageId(m))}
              diffFull={diffFull}
              onToggleDiff={onToggleDiff}
              onRepublish={onRepublish}
              labels={{
                republish: tr('republish'),
                diffSelect: tr('diffSelect'),
                diffSelected: tr('diffSelected'),
                expand: tr('expand'),
                collapse: tr('collapse'),
              }}
            />
          ))}
        </div>
      ))}
    </section>
  )
}
