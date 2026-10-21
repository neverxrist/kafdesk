import { useState } from 'react'
import type { KafkaMessageView, TopicInfo } from '../../types/kafdesk'
import MessageCard from '../MessageCard'
import type { useLocale } from '../../hooks/useLocale'
import {
  loadMessageViewMode,
  persistMessageViewMode,
  type MessageViewMode,
} from '../../lib/messageView'

type Tr = ReturnType<typeof useLocale>['tr']

export function TailToolbar({
  tr,
  selectedTopic,
  topicDetails,
  search,
  setSearch,
  searchKey,
  setSearchKey,
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
  searchKey: string
  setSearchKey: (v: string) => void
  partitionFilter: string
  setPartitionFilter: (v: string) => void
  tailing: boolean
  busy: boolean
  onStartTail: () => void
  onStopTail: () => void
}) {
  return (
    <div className="toolbar">
      <div className="field">
        <label>{tr('searchValue')}</label>
        <input
          value={search}
          maxLength={255}
          onChange={(e) => setSearch(e.target.value.slice(0, 255))}
          placeholder={tr('substring')}
          disabled={!selectedTopic}
        />
      </div>
      <div className="field">
        <label>{tr('searchKey')}</label>
        <input
          value={searchKey}
          maxLength={255}
          onChange={(e) => setSearchKey(e.target.value.slice(0, 255))}
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
  selectedTopic,
}: {
  tr: Tr
  tailing: boolean
  tailMessages: KafkaMessageView[]
  selectedTopic: string | null
}) {
  const [viewMode, setViewMode] = useState<MessageViewMode>(() => loadMessageViewMode())

  function setView(mode: MessageViewMode) {
    setViewMode(mode)
    persistMessageViewMode(mode)
  }

  return (
    <section className="messages">
      <div className="panel" style={{ marginBottom: '0.9rem' }}>
        <div className="messages-panel-head">
          <div>
            <h2 style={{ margin: 0 }}>{tailing ? tr('tailRunning') : tr('tailStopped')}</h2>
            <div className="status">{tr('tailHint')}</div>
          </div>
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
      {tailMessages.length === 0 && (
        <div className="empty">{selectedTopic ? tr('waitingMessages') : tr('selectTopic')}</div>
      )}
      {tailMessages.map((m, i) => (
        <MessageCard
          key={`${m.partition}-${m.offset}-${i}`}
          message={m}
          keyLabel={tr('msgKey')}
          viewMode={viewMode}
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
