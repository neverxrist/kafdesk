import { useCallback, useMemo, useRef, useState } from 'react'
import type { ConsumeStartFrom, KafkaMessageView } from '../../electron/types'
import {
  defaultConsumeFilterState,
  partitionsForFilter,
  type ConsumeFilterState,
} from '../lib/consumeFilters'
import { filterMessages } from '../lib/messageFilter'

export type { ConsumeFilterState }

/**
 * Client filter state + broker window controls for Consume.
 * Broker reloads go through `brokerReloadRef` (set by parent after loadMessages exists).
 */
export function useConsumeFilters(messages: KafkaMessageView[]) {
  const initial = defaultConsumeFilterState()
  const [search, setSearch] = useState(initial.search)
  const [searchKey, setSearchKey] = useState(initial.searchKey)
  const [headerKey, setHeaderKey] = useState(initial.headerKey)
  const [headerValue, setHeaderValue] = useState(initial.headerValue)
  const [timeFrom, setTimeFrom] = useState(initial.timeFrom)
  const [timeTo, setTimeTo] = useState(initial.timeTo)
  const [limit, setLimit] = useState(initial.limit)
  const [startFrom, setStartFrom] = useState<ConsumeStartFrom>(initial.startFrom)
  const [startOffset, setStartOffset] = useState(initial.startOffset)
  const [partitionFilter, setPartitionFilter] = useState(initial.partitionFilter)

  const filterReloadTimer = useRef<ReturnType<typeof setTimeout> | null>(null)
  const filterStateRef = useRef<ConsumeFilterState>({
    search,
    searchKey,
    headerKey,
    headerValue,
    partitionFilter,
    startFrom,
    startOffset,
    limit,
    timeFrom,
    timeTo,
  })
  filterStateRef.current = {
    search,
    searchKey,
    headerKey,
    headerValue,
    partitionFilter,
    startFrom,
    startOffset,
    limit,
    timeFrom,
    timeTo,
  }

  /** Parent assigns: (filter) => void loadMessages(topic, filter) when topic/tab allow. */
  const brokerReloadRef = useRef<(filter: ConsumeFilterState) => void>(() => {})

  const filteredMessages = useMemo(
    () =>
      filterMessages(messages, {
        search,
        searchKey,
        headerKey,
        headerValue,
        timeFrom,
        timeTo,
      }),
    [messages, search, searchKey, headerKey, headerValue, timeFrom, timeTo],
  )

  function currentFilterState(): ConsumeFilterState {
    return { ...filterStateRef.current }
  }

  const cancelScheduledConsumeReload = useCallback(() => {
    if (filterReloadTimer.current) {
      clearTimeout(filterReloadTimer.current)
      filterReloadTimer.current = null
    }
  }, [])

  function applyFilterState(f: ConsumeFilterState) {
    cancelScheduledConsumeReload()
    filterStateRef.current = f
    setSearch(f.search)
    setSearchKey(f.searchKey)
    setHeaderKey(f.headerKey)
    setHeaderValue(f.headerValue)
    setPartitionFilter(f.partitionFilter)
    setStartFrom(f.startFrom)
    setStartOffset(f.startOffset)
    setLimit(f.limit)
    setTimeFrom(f.timeFrom)
    setTimeTo(f.timeTo)
  }

  function scheduleConsumeReload(patch: Partial<ConsumeFilterState>, delayMs = 0) {
    filterStateRef.current = { ...filterStateRef.current, ...patch }
    cancelScheduledConsumeReload()
    const run = () => {
      filterReloadTimer.current = null
      brokerReloadRef.current({ ...filterStateRef.current })
    }
    if (delayMs <= 0) run()
    else filterReloadTimer.current = setTimeout(run, delayMs)
  }

  function setSearchFilter(v: string) {
    setSearch(v)
    filterStateRef.current = { ...filterStateRef.current, search: v }
  }
  function setSearchKeyFilter(v: string) {
    setSearchKey(v)
    filterStateRef.current = { ...filterStateRef.current, searchKey: v }
  }
  function setHeaderKeyFilter(v: string) {
    setHeaderKey(v)
    filterStateRef.current = { ...filterStateRef.current, headerKey: v }
  }
  function setHeaderValueFilter(v: string) {
    setHeaderValue(v)
    filterStateRef.current = { ...filterStateRef.current, headerValue: v }
  }
  function setTimeFromFilter(v: string) {
    setTimeFrom(v)
    filterStateRef.current = { ...filterStateRef.current, timeFrom: v }
  }
  function setTimeToFilter(v: string) {
    setTimeTo(v)
    filterStateRef.current = { ...filterStateRef.current, timeTo: v }
  }
  function setPartitionFilterUI(v: string) {
    setPartitionFilter(v)
    scheduleConsumeReload({ partitionFilter: v })
  }
  function setStartFromFilter(v: ConsumeStartFrom) {
    setStartFrom(v)
    scheduleConsumeReload({ startFrom: v })
  }
  function setStartOffsetFilter(v: string) {
    setStartOffset(v)
    scheduleConsumeReload({ startOffset: v }, 350)
  }
  function setLimitFilter(v: number) {
    setLimit(v)
    scheduleConsumeReload({ limit: v }, 350)
  }

  /** Reset partition without broker reload (e.g. on topic switch). */
  function resetPartitionToAll() {
    setPartitionFilter('all')
    filterStateRef.current = { ...filterStateRef.current, partitionFilter: 'all' }
  }

  return {
    search,
    searchKey,
    headerKey,
    headerValue,
    timeFrom,
    timeTo,
    limit,
    startFrom,
    startOffset,
    partitionFilter,
    filteredMessages,
    filterStateRef,
    brokerReloadRef,
    currentFilterState,
    cancelScheduledConsumeReload,
    applyFilterState,
    scheduleConsumeReload,
    partitionsFor: partitionsForFilter,
    selectedPartitions: () => partitionsForFilter(partitionFilter),
    setSearchFilter,
    setSearchKeyFilter,
    setHeaderKeyFilter,
    setHeaderValueFilter,
    setTimeFromFilter,
    setTimeToFilter,
    setPartitionFilterUI,
    setStartFromFilter,
    setStartOffsetFilter,
    setLimitFilter,
    resetPartitionToAll,
  }
}
