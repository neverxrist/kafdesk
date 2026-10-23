import type { ConsumeStartFrom } from '../types/kafdesk'
import type { SearchScope } from './searchScope'

export interface ConsumeFilterState {
  search: string
  searchKey: string
  /** Where the primary search box applies: key / value / both. */
  searchScope: SearchScope
  /** Second AND clause — UI-only, not persisted in saved queries. */
  search2: string
  searchScope2: SearchScope
  /** Second search row visible (max 2 filters). */
  dualSearch: boolean
  headerKey: string
  headerValue: string
  partitionFilter: string
  startFrom: ConsumeStartFrom
  startOffset: string
  limit: number
  timeFrom: string
  timeTo: string
}

export function defaultConsumeFilterState(): ConsumeFilterState {
  return {
    search: '',
    searchKey: '',
    searchScope: 'key',
    search2: '',
    searchScope2: 'value',
    dualSearch: false,
    headerKey: '',
    headerValue: '',
    partitionFilter: 'all',
    startFrom: 'end',
    startOffset: '0',
    limit: 30,
    timeFrom: '',
    timeTo: '',
  }
}

export function partitionsForFilter(filter: string): number[] | undefined {
  if (filter === 'all') return undefined
  const n = Number(filter)
  return Number.isFinite(n) ? [n] : undefined
}
