import type { ConsumeStartFrom } from '../types/kafdesk'
import type { SearchScope } from './searchScope'

export interface ConsumeFilterState {
  search: string
  searchKey: string
  /** Where the single search box applies: key / value / both. */
  searchScope: SearchScope
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
    searchScope: 'both',
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
