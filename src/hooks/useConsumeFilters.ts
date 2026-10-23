import { useCallback, useMemo, useRef, useState } from 'react'
import type { ConsumeStartFrom, KafkaMessageView } from '../types/kafdesk'
import {
  defaultConsumeFilterState,
  partitionsForFilter,
  type ConsumeFilterState,
} from '../lib/consumeFilters'
import { clientSearchClauses, filterMessages } from '../lib/messageFilter'
import type { SearchScope } from '../lib/searchScope'
import { uiSearchFromStored } from '../lib/searchScope'

export type { ConsumeFilterState }

/** Independent Hits toggles for filter 1 and filter 2. */
export type SearchHitsOn = [boolean, boolean]

function normalizeFilter(f: ConsumeFilterState): ConsumeFilterState {
  const { text, scope } = uiSearchFromStored(f.search, f.searchKey, f.searchScope)
  return {
    ...defaultConsumeFilterState(),
    ...f,
    search: text,
    searchKey: '',
    searchScope: scope,
    search2: f.search2 ?? '',
    searchScope2: f.searchScope2 ?? 'both',
    dualSearch: Boolean(f.dualSearch),
  }
}

const HITS_ON_KEY = 'kafdesk.searchHitsOn'

function loadHitsOn(): SearchHitsOn {
  try {
    const v = localStorage.getItem(HITS_ON_KEY)
    if (v === '0,1') return [true, true]
    if (v === '0') return [true, false]
    if (v === '1') return [false, true]
    // migrate older keys
    const legacySlot = localStorage.getItem('kafdesk.searchHitsSlot')
    if (legacySlot === '0' || localStorage.getItem('kafdesk.searchMatchesOnly') === '1') {
      return [true, false]
    }
    if (legacySlot === '1') return [false, true]
    return [false, false]
  } catch {
    return [false, false]
  }
}

function persistHitsOn(flags: SearchHitsOn) {
  try {
    const v = flags[0] && flags[1] ? '0,1' : flags[0] ? '0' : flags[1] ? '1' : ''
    localStorage.setItem(HITS_ON_KEY, v)
  } catch {
    /* ignore */
  }
}

/**
 * Client filter state + broker window controls for Consume.
 * Broker reloads go through `brokerReloadRef` (set by parent after loadMessages exists).
 */
export function useConsumeFilters(messages: KafkaMessageView[]) {
  const initial = defaultConsumeFilterState()
  const [search, setSearch] = useState(initial.search)
  const [searchKey, setSearchKey] = useState(initial.searchKey)
  const [searchScope, setSearchScope] = useState<SearchScope>(initial.searchScope)
  const [search2, setSearch2] = useState(initial.search2)
  const [searchScope2, setSearchScope2] = useState<SearchScope>(initial.searchScope2)
  const [dualSearch, setDualSearch] = useState(initial.dualSearch)
  const [searchHitsOn, setSearchHitsOnState] = useState<SearchHitsOn>(loadHitsOn)
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
    searchScope,
    search2,
    searchScope2,
    dualSearch,
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
    searchScope,
    search2,
    searchScope2,
    dualSearch,
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
        searchScope,
        search2,
        searchScope2,
        dualSearch,
        headerKey,
        headerValue,
        timeFrom,
        timeTo,
      }),
    [
      messages,
      search,
      searchKey,
      searchScope,
      search2,
      searchScope2,
      dualSearch,
      headerKey,
      headerValue,
      timeFrom,
      timeTo,
    ],
  )

  const searchClauses = useMemo(
    () =>
      clientSearchClauses({
        search,
        searchKey,
        searchScope,
        search2,
        searchScope2,
        dualSearch,
      }),
    [search, searchKey, searchScope, search2, searchScope2, dualSearch],
  )

  function currentFilterState(): ConsumeFilterState {
    return normalizeFilter({ ...filterStateRef.current })
  }

  const cancelScheduledConsumeReload = useCallback(() => {
    if (filterReloadTimer.current) {
      clearTimeout(filterReloadTimer.current)
      filterReloadTimer.current = null
    }
  }, [])

  function applyFilterState(raw: ConsumeFilterState) {
    cancelScheduledConsumeReload()
    const f = normalizeFilter({
      ...defaultConsumeFilterState(),
      ...raw,
      // Saved queries never restore the second clause.
      search2: '',
      searchScope2: 'value',
      dualSearch: false,
      searchScope: raw.searchScope ?? 'key',
    })
    filterStateRef.current = f
    setSearch(f.search)
    setSearchKey(f.searchKey)
    setSearchScope(f.searchScope)
    setSearch2(f.search2)
    setSearchScope2(f.searchScope2)
    setDualSearch(false)
    setHeaderKey(f.headerKey)
    setHeaderValue(f.headerValue)
    setPartitionFilter(f.partitionFilter)
    setStartFrom(f.startFrom)
    setStartOffset(f.startOffset)
    setLimit(f.limit)
    setTimeFrom(f.timeFrom)
    setTimeTo(f.timeTo)
    // Drop second-row Hits when dual closes via saved query.
    setSearchHitsOnState((prev) => {
      const next: SearchHitsOn = [prev[0], false]
      persistHitsOn(next)
      return next
    })
  }

  function scheduleConsumeReload(patch: Partial<ConsumeFilterState>, delayMs = 0) {
    filterStateRef.current = { ...filterStateRef.current, ...patch }
    cancelScheduledConsumeReload()
    const run = () => {
      filterReloadTimer.current = null
      brokerReloadRef.current(normalizeFilter({ ...filterStateRef.current }))
    }
    if (delayMs <= 0) run()
    else filterReloadTimer.current = setTimeout(run, delayMs)
  }

  function setSearchFilter(v: string) {
    setSearch(v)
    filterStateRef.current = { ...filterStateRef.current, search: v, searchKey: '' }
    if (!v.trim()) {
      setSearchHitsOnState((prev) => {
        if (!prev[0]) return prev
        const next: SearchHitsOn = [false, prev[1]]
        persistHitsOn(next)
        return next
      })
    }
  }
  function setSearchScopeFilter(v: SearchScope) {
    setSearchScope(v)
    filterStateRef.current = { ...filterStateRef.current, searchScope: v }
    if (v === 'key') {
      setSearchHitsOnState((prev) => {
        if (!prev[0]) return prev
        const next: SearchHitsOn = [false, prev[1]]
        persistHitsOn(next)
        return next
      })
    }
  }
  function setSearch2Filter(v: string) {
    setSearch2(v)
    filterStateRef.current = { ...filterStateRef.current, search2: v }
    if (!v.trim()) {
      setSearchHitsOnState((prev) => {
        if (!prev[1]) return prev
        const next: SearchHitsOn = [prev[0], false]
        persistHitsOn(next)
        return next
      })
    }
  }
  function setSearchScope2Filter(v: SearchScope) {
    setSearchScope2(v)
    filterStateRef.current = { ...filterStateRef.current, searchScope2: v }
    if (v === 'key') {
      setSearchHitsOnState((prev) => {
        if (!prev[1]) return prev
        const next: SearchHitsOn = [prev[0], false]
        persistHitsOn(next)
        return next
      })
    }
  }
  function openDualSearch() {
    setDualSearch(true)
    setSearchScope2('value')
    filterStateRef.current = {
      ...filterStateRef.current,
      dualSearch: true,
      searchScope2: 'value',
    }
  }
  function closeDualSearch() {
    setDualSearch(false)
    setSearch2('')
    setSearchScope2('value')
    filterStateRef.current = {
      ...filterStateRef.current,
      dualSearch: false,
      search2: '',
      searchScope2: 'value',
    }
    setSearchHitsOnState((prev) => {
      const next: SearchHitsOn = [prev[0], false]
      persistHitsOn(next)
      return next
    })
  }
  /** Toggle Hits independently per filter row. */
  function setSearchHitsOnSlot(slot: 0 | 1, on: boolean) {
    if (slot === 1 && !dualSearch) return
    setSearchHitsOnState((prev) => {
      const next: SearchHitsOn = slot === 0 ? [on, prev[1]] : [prev[0], on]
      persistHitsOn(next)
      return next
    })
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
    searchScope,
    search2,
    searchScope2,
    dualSearch,
    searchHitsOn,
    searchClauses,
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
    setSearchScopeFilter,
    setSearch2Filter,
    setSearchScope2Filter,
    openDualSearch,
    closeDualSearch,
    setSearchHitsOnSlot,
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
