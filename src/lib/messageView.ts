export const MESSAGE_VIEW_KEY = 'kafdesk.messageView'
export const GROUP_BY_PARTITIONS_KEY = 'kafdesk.groupByPartitions'
export const BATCH_TIME_SORT_KEY = 'kafdesk.batchTimeSort'

export type MessageViewMode = 'full' | 'compact'
/** Client-side order of the loaded window (not a broker seek). */
export type BatchTimeSort = 'newest' | 'oldest'

export function loadBatchTimeSort(): BatchTimeSort | null {
  try {
    const v = localStorage.getItem(BATCH_TIME_SORT_KEY)
    if (v === 'newest' || v === 'oldest') return v
  } catch {
    /* ignore */
  }
  return null
}

export function persistBatchTimeSort(sort: BatchTimeSort): void {
  try {
    localStorage.setItem(BATCH_TIME_SORT_KEY, sort)
  } catch {
    /* ignore */
  }
}

export function loadMessageViewMode(): MessageViewMode {
  try {
    const v = localStorage.getItem(MESSAGE_VIEW_KEY)
    if (v === 'compact' || v === 'full') return v
  } catch {
    /* ignore */
  }
  return 'full'
}

export function persistMessageViewMode(mode: MessageViewMode): void {
  try {
    localStorage.setItem(MESSAGE_VIEW_KEY, mode)
  } catch {
    /* ignore */
  }
}

export function loadGroupByPartitions(): boolean {
  try {
    return localStorage.getItem(GROUP_BY_PARTITIONS_KEY) === '1'
  } catch {
    return false
  }
}

export function persistGroupByPartitions(on: boolean): void {
  try {
    localStorage.setItem(GROUP_BY_PARTITIONS_KEY, on ? '1' : '0')
  } catch {
    /* ignore */
  }
}
