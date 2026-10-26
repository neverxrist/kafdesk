/** Named lists of exact topic names, per connection. Not the pin list. */

const KEY = 'kafdesk.topicSets.v1'

export const TOPIC_SET_MAX = 20
export const TOPIC_SET_COUNT_MAX = 12

export interface TopicSet {
  id: string
  name: string
  topics: string[]
}

type Store = Record<string, TopicSet[]>

function read(): Store {
  try {
    const raw = localStorage.getItem(KEY)
    if (!raw) return {}
    const parsed = JSON.parse(raw) as Store
    return parsed && typeof parsed === 'object' ? parsed : {}
  } catch {
    return {}
  }
}

function write(store: Store) {
  try {
    localStorage.setItem(KEY, JSON.stringify(store))
  } catch {
    /* ignore */
  }
}

export function loadTopicSets(connectionId: string): TopicSet[] {
  const list = read()[connectionId]
  if (!Array.isArray(list)) return []
  return list
    .filter((s) => s && typeof s.name === 'string' && Array.isArray(s.topics))
    .map((s) => ({
      id: String(s.id),
      name: s.name.slice(0, 40),
      topics: s.topics.filter((t) => typeof t === 'string' && t.trim()).slice(0, TOPIC_SET_MAX),
    }))
}

export function persistTopicSets(connectionId: string, sets: TopicSet[]) {
  const store = read()
  store[connectionId] = sets.slice(0, TOPIC_SET_COUNT_MAX)
  write(store)
}

export function createTopicSet(sets: TopicSet[], name: string, topic?: string): TopicSet[] {
  const label = name.trim().slice(0, 40)
  if (!label || sets.length >= TOPIC_SET_COUNT_MAX) return sets
  const topics = topic && topic.trim() ? [topic.trim()] : []
  return [...sets, { id: `set-${Date.now().toString(36)}`, name: label, topics }]
}

export function addTopicToSet(sets: TopicSet[], setId: string, topic: string): TopicSet[] {
  const name = topic.trim()
  if (!name) return sets
  return sets.map((s) => {
    if (s.id !== setId || s.topics.includes(name) || s.topics.length >= TOPIC_SET_MAX) return s
    return { ...s, topics: [...s.topics, name] }
  })
}

export function removeTopicFromSet(sets: TopicSet[], setId: string, topic: string): TopicSet[] {
  return sets.map((s) =>
    s.id === setId ? { ...s, topics: s.topics.filter((t) => t !== topic) } : s,
  )
}

export function deleteTopicSet(sets: TopicSet[], setId: string): TopicSet[] {
  return sets.filter((s) => s.id !== setId)
}
