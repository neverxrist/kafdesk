export const TOPIC_FILTERS_KEY = 'kafdesk.topicFilters.v1'
export const ONLY_PINNED_TOPICS_KEY = 'kafdesk.onlyPinnedTopics'
export const TOPICS_OPEN_KEY = 'kafdesk.topicsOpen'
export const TOPICS_WIDTH_KEY = 'kafdesk.topicsWidth'

export const TOPICS_WIDTH_DEFAULT = 280
export const TOPICS_WIDTH_MIN = 200
export const TOPICS_WIDTH_MAX = 480

export function loadTopicsOpen(): boolean {
  try {
    const v = localStorage.getItem(TOPICS_OPEN_KEY)
    if (v === '0') return false
    if (v === '1') return true
  } catch {
    /* ignore */
  }
  return true
}

export function persistTopicsOpen(open: boolean): void {
  try {
    localStorage.setItem(TOPICS_OPEN_KEY, open ? '1' : '0')
  } catch {
    /* ignore */
  }
}

export function clampTopicsWidth(px: number): number {
  return Math.min(TOPICS_WIDTH_MAX, Math.max(TOPICS_WIDTH_MIN, Math.round(px)))
}

export function loadTopicsWidth(): number {
  try {
    const n = Number(localStorage.getItem(TOPICS_WIDTH_KEY))
    if (Number.isFinite(n) && n > 0) return clampTopicsWidth(n)
  } catch {
    /* ignore */
  }
  return TOPICS_WIDTH_DEFAULT
}

export function persistTopicsWidth(px: number): void {
  try {
    localStorage.setItem(TOPICS_WIDTH_KEY, String(clampTopicsWidth(px)))
  } catch {
    /* ignore */
  }
}

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
