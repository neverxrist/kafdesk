export const TOPIC_FILTERS_KEY = 'kafdesk.topicFilters.v1'

export interface SavedTopicFilter {
  id: string
  name: string
  query: string
  createdAt: number
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
