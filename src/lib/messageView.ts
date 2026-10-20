export const MESSAGE_VIEW_KEY = 'kafdesk.messageView'
export const GROUP_BY_PARTITIONS_KEY = 'kafdesk.groupByPartitions'

export type MessageViewMode = 'full' | 'compact'

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
