import { useState } from 'react'
import type { KafkaMessageView, TopicInfo } from '../../types/kafdesk'
import EmptyState from '../EmptyState'
import MessageCard from '../MessageCard'
import MessageSearchFilter from '../MessageSearchFilter'
import MsgFontControls from '../MsgFontControls'
import type { useLocale } from '../../hooks/useLocale'
import type { SearchScope } from '../../lib/searchScope'
import { persistMessageViewMode, type MessageViewMode } from '../../lib/messageView'

type Tr = ReturnType<typeof useLocale>['tr']

export function TailToolbar({
  tr,
  selectedTopic,
  topicDetails,
  search,
  setSearch,
  searchScope,
  setSearchScope,
  partitionFilter,
  setPartitionFilter,
  tailing,
  busy,
  onStartTail,
  onStopTail,
}: {
  tr: Tr
  selectedTopic: string | null
  topicDetails: TopicInfo | null
  search: string
  setSearch: (v: string) => void
  searchScope: SearchScope
  setSearchScope: (v: SearchScope) => void
  partitionFilter: string
  setPartitionFilter: (v: string) => void
  tailing: boolean
  busy: boolean
  onStartTail: () => void
  onStopTail: () => void
}) {
  return (
    <div className="toolbar">
      <MessageSearchFilter
        tr={tr}
        id="tail-search"
        value={search}
        scope={searchScope}
        onChange={setSearch}
        onScopeChange={setSearchScope}
        disabled={!selectedTopic}
      />
      <div className="field toolbar-partition-field">
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
            </option>
          ))}
        </select>
      </div>
      {!tailing ? (
        <button className="primary" disabled={!selectedTopic || busy} onClick={onStartTail}>
          {tr('start')}
        </button>
      ) : (
        <button disabled={busy} onClick={onStopTail}>
          {tr('stop')}
        </button>
      )}
    </div>
  )
}

export default function TailPanel({
  tr,
  tailing,
  tailMessages,
  tailBufferCount,
  selectedTopic,
  search = '',
  searchScope = 'both',
}: {
  tr: Tr
  tailing: boolean
  tailMessages: KafkaMessageView[]
  /** Raw buffer size (before display filter). */
  tailBufferCount: number
  selectedTopic: string | null
  search?: string
  searchScope?: SearchScope
}) {
  const [viewMode, setViewMode] = useState<MessageViewMode>('compact')

  function setView(mode: MessageViewMode) {
    setViewMode(mode)
    persistMessageViewMode(mode)
  }

  const countLabel =
    tailBufferCount > 0
      ? tr('tailCount')
          .replace('{shown}', String(tailMessages.length))
          .replace('{buffer}', String(tailBufferCount))
      : null

  return (
    <section className="messages drag-scroll">
      <div className="panel" style={{ marginBottom: '0.9rem' }}>
        <div className="messages-panel-head">
          <div>
            <h2 style={{ margin: 0 }}>{tailing ? tr('tailRunning') : tr('tailStopped')}</h2>
            <div className="status">
              {countLabel ? (
                <span className="tail-buf-count" title={tr('tailCountTitle')}>
                  {countLabel}
                  <span className="tail-buf-sep"> · </span>
                </span>
              ) : null}
              {tr('tailHint')}
            </div>
          </div>
          <div className="messages-panel-controls">
            <MsgFontControls tr={tr} />
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
      {tailMessages.length === 0 && (
        <EmptyState
          kind="events"
          title={tr('emptyEventsTitle')}
          hint={
            !selectedTopic
              ? tr('emptyEventsHint')
              : tailBufferCount > 0
                ? tr('emptyEventsFilter')
                : tailing
                  ? tr('emptyEventsWaiting')
                  : tr('emptyEventsStartTail')
          }
        />
      )}
      {tailMessages.map((m, i) => (
        <MessageCard
          key={`${m.partition}-${m.offset}-${i}`}
          message={m}
          keyLabel={tr('msgKey')}
          viewMode={viewMode}
          searchQuery={search}
          searchScope={searchScope}
          labels={{
            republish: '',
            diffSelect: '',
            diffSelected: '',
            expand: tr('expand'),
            collapse: tr('collapse'),
            copyValue: tr('copyValue'),
            copied: tr('copied'),
          }}
        />
      ))}
    </section>
  )
}
