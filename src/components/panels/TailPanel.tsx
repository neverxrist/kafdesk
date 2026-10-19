import type { KafkaMessageView, TopicInfo } from '../../../electron/types'
import MessageCard from '../MessageCard'
import type { useLocale } from '../../hooks/useLocale'

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
  return (
    <section className="messages">
      <div className="panel" style={{ marginBottom: '0.9rem' }}>
        <h2>{tailing ? tr('tailRunning') : tr('tailStopped')}</h2>
        <div className="status">{tr('tailHint')}</div>
      </div>
      {tailMessages.length === 0 && (
        <div className="empty">{selectedTopic ? tr('waitingMessages') : tr('selectTopic')}</div>
      )}
      {tailMessages.map((m, i) => (
        <MessageCard
          key={`${m.partition}-${m.offset}-${i}`}
          message={m}
          keyLabel={tr('msgKey')}
        />
      ))}
    </section>
  )
}
