export const TOPIC_FILTERS_KEY = 'kafdesk.topicFilters.v1'
export const ONLY_PINNED_TOPICS_KEY = 'kafdesk.onlyPinnedTopics'

export interface SavedTopicFilter {
  id: string
  name: string
  query: string
  createdAt: number
}

export function loadOnlyPinnedTopics(): boolean {
  try {
    return localStorage.getItem(ONLY_PINNED_TOPICS_KEY) === '1'
  } catch {
    return false
  }
}

export function persistOnlyPinnedTopics(on: boolean): void {
  try {
    localStorage.setItem(ONLY_PINNED_TOPICS_KEY, on ? '1' : '0')
  } catch {
    /* ignore */
  }
}

export function loadTopicFilters(): SavedTopicFilter[] {
  try {
    const raw = localStorage.getItem(TOPIC_FILTERS_KEY)
    if (!raw) return []
    const parsed = JSON.parse(raw) as SavedTopicFilter[]
    return Array.isArray(parsed) ? parsed : []
  } catch {
    return []
  }
}

export function persistTopicFilters(list: SavedTopicFilter[]) {
  try {
    localStorage.setItem(TOPIC_FILTERS_KEY, JSON.stringify(list))
  } catch {
    /* ignore */
  }
}
