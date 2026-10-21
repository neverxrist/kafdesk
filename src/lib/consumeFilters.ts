import type { ConsumeStartFrom } from '../../electron/types'

export interface ConsumeFilterState {
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

export function defaultConsumeFilterState(): ConsumeFilterState {
  return {
    search: '',
    searchKey: '',
    headerKey: '',
    headerValue: '',
    partitionFilter: 'all',
    startFrom: 'end',
    startOffset: '0',
    limit: 25,
    timeFrom: '',
    timeTo: '',
  }
}

export function partitionsForFilter(filter: string): number[] | undefined {
  if (filter === 'all') return undefined
  const n = Number(filter)
  return Number.isFinite(n) ? [n] : undefined
}
