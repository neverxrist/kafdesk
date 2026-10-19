import type { TopicInfo } from '../../electron/types'
import type { useLocale } from '../hooks/useLocale'

type Tr = ReturnType<typeof useLocale>['tr']

export default function TopicsSidebar({
  tr,
  connected,
  filteredTopics,
  topicQuery,
  setTopicQuery,
  selectedTopic,
  onSelectTopic,
}: {
  tr: Tr
  connected: boolean
  filteredTopics: TopicInfo[]
  topicQuery: string
  setTopicQuery: (v: string) => void
  selectedTopic: string | null
  onSelectTopic: (name: string) => void
}) {
  return (
    <section className="topics">
      <div className="field">
        <label>{tr('topics', { n: filteredTopics.length })}</label>
        <input
          value={topicQuery}
          onChange={(e) => setTopicQuery(e.target.value)}
          placeholder={tr('filterTopics')}
          disabled={!connected}
        />
      </div>
      {!connected && <div className="empty">{tr('connectToSeeTopics')}</div>}
      {filteredTopics.map((t) => (
        <button
          key={t.name}
          className={`topic-item ${selectedTopic === t.name ? 'active' : ''}`}
          onClick={() => onSelectTopic(t.name)}
        >
          {t.name}
          <small>{tr('partitionsCount', { n: t.partitions.length })}</small>
        </button>
      ))}
    </section>
  )
}
