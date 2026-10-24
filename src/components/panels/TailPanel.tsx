import { useState } from 'react'
import type { KafkaMessageView, TopicInfo } from '../../types/kafdesk'
import EmptyState from '../EmptyState'
import MessageCard from '../MessageCard'
import { DualMessageSearchFilters } from '../MessageSearchFilter'
import BatchExportControls from '../BatchExportControls'
import MsgFontControls from '../MsgFontControls'
import type { useLocale } from '../../hooks/useLocale'
import type { SearchClause, SearchScope } from '../../lib/searchScope'
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
  search2,
  setSearch2,
  searchScope2,
  setSearchScope2,
  dualSearch,
  onOpenDualSearch,
  onCloseDualSearch,
  searchHitsOn,
  onSearchHitsSlot,
  partitionFilter,
  setPartitionFilter,
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
  partitionFilter: string
  setPartitionFilter: (v: string) => void
}) {
  return (
    <div className="toolbar toolbar-tail">
      <div className="toolbar-tail-filters">
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
        <DualMessageSearchFilters
          tr={tr}
          idPrefix="tail"
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
        />
      </div>
    </div>
  )
}

export default function TailPanel({
  tr,
  tailing,
  busy = false,
  tailMessages,
  tailBufferCount,
  tailDropped,
  selectedTopic,
  searchClauses = [],
  searchHitsOn = [false, false],
  onStartTail,
  onStopTail,
}: {
  tr: Tr
  tailing: boolean
  busy?: boolean
  tailMessages: KafkaMessageView[]
  /** Raw buffer size (before display filter). */
  tailBufferCount: number
  tailDropped: number
  selectedTopic: string | null
  searchClauses?: SearchClause[]
  searchHitsOn?: [boolean, boolean]
  onStartTail: () => void
  onStopTail: () => void
}) {
  const [viewMode, setViewMode] = useState<MessageViewMode>('compact')

  function setView(mode: MessageViewMode) {
    setViewMode(mode)
    persistMessageViewMode(mode)
  }

  const countLabel =
    tailBufferCount > 0
      ? tr('tailCount', { shown: tailMessages.length, buffer: tailBufferCount })
      : null

  return (
    <section className="messages drag-scroll">
      <div className="panel messages-tail-panel">
        <div className="messages-panel-head">
          <div className="messages-panel-toolbar">
            <div className="tail-panel-title">
              <button
                type="button"
                className={`tail-transport-btn${tailing ? ' stopping' : ' starting'}`}
                disabled={busy || (!tailing && !selectedTopic)}
                title={tailing ? tr('stop') : tr('start')}
                aria-label={tailing ? tr('stop') : tr('start')}
                onClick={() => (tailing ? onStopTail() : onStartTail())}
              >
                {tailing ? <StopIcon /> : <PlayIcon />}
              </button>
              <div>
                <h2>{tailing ? tr('tailRunning') : tr('tailStopped')}</h2>
                <div className="status">
                  {countLabel ? (
                    <span className="tail-buf-count" title={tr('tailCountTitle')}>
                      {countLabel}
                      <span className="tail-buf-sep"> · </span>
                    </span>
                  ) : null}
                  {tailDropped > 0 ? (
                    <span className="tail-buf-count tail-dropped" title={tr('tailDroppedTitle')}>
                      {tr('tailDropped', { n: tailDropped.toLocaleString() })}
                      <span className="tail-buf-sep"> · </span>
                    </span>
                  ) : null}
                  {tr('tailHint')}
                </div>
              </div>
            </div>
            <div className="messages-panel-controls">
              <MsgFontControls tr={tr} />
              <BatchExportControls tr={tr} messages={tailMessages} topic={selectedTopic} />
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
          searchClauses={searchClauses}
          searchHitsOn={searchHitsOn}
          labels={{
            republish: '',
            diffSelect: '',
            diffSelected: '',
            expand: tr('expand'),
            collapse: tr('collapse'),
            copyValue: tr('copyValue'),
            truncated: tr('msgTruncated'),
            loadFull: tr('msgLoadFull'),
            copied: tr('copied'),
          }}
        />
      ))}
    </section>
  )
}

function PlayIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 16 16" aria-hidden="true">
      <path fill="currentColor" d="M4.2 2.6v10.8L13.4 8 4.2 2.6z" />
    </svg>
  )
}

function StopIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 16 16" aria-hidden="true">
      <rect x="3.6" y="3.6" width="8.8" height="8.8" rx="1" fill="currentColor" />
    </svg>
  )
}
