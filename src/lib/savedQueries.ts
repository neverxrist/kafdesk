import type { ConsumeStartFrom } from '../types/kafdesk'

export const SAVED_QUERIES_KEY = 'kafdesk.savedQueries.v1'

export interface SavedConsumeQuery {
  id: string
  name: string
  createdAt: number
  topic: string | null
  search: string
  searchKey: string
  headerKey: string
  headerValue: string
  partitionFilter: string
  startFrom: ConsumeStartFrom
  startOffset: string
  limit: number
  timeFrom: string
  timeTo: string
}

export function loadSavedQueries(): SavedConsumeQuery[] {
  try {
    const raw = localStorage.getItem(SAVED_QUERIES_KEY)
    if (!raw) return []
    const parsed = JSON.parse(raw) as SavedConsumeQuery[]
    return Array.isArray(parsed) ? parsed : []
  } catch {
    return []
  }
}

export function persistSavedQueries(list: SavedConsumeQuery[]) {
  try {
    localStorage.setItem(SAVED_QUERIES_KEY, JSON.stringify(list))
  } catch {
    /* ignore */
  }
}

/** datetime-local string -> epoch ms (local time), undefined when empty/invalid */
export function timeInputToMs(value: string): number | undefined {
  if (!value.trim()) return undefined
  const ms = new Date(value).getTime()
  return Number.isFinite(ms) ? ms : undefined
}
