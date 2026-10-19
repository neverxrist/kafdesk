import { useEffect, useMemo, useRef, useState } from 'react'
import type {
  AuthSessionPublic,
  ClusterConfig,
  ConsumeStartFrom,
  ConsumerGroupInfo,
  GroupOffsetEntry,
  KafkaMessageView,
  SchemaSubjectInfo,
  SchemaRegistryStatus,
  TopicInfo,
} from '../../electron/types'
import { LOCALE_KEY, t } from '../i18n'
import {
  authMethodFromConfig,
  CONNECTIONS_KEY,
  defaultConnection,
  loadConnections,
  SIDEBAR_KEY,
  type AuthMethodUi,
} from '../lib/connections'
import {
  loadSavedQueries,
  persistSavedQueries,
  timeInputToMs,
  type SavedConsumeQuery,
} from '../lib/savedQueries'
import type { AppLogEntry } from './useAppLogs'

export type { SavedConsumeQuery }

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

export function useKafdeskApp(pushLog: (level: AppLogEntry['level'], msg: string) => void) {
  const initial = useMemo(() => loadConnections(), [])
  const [connections, setConnections] = useState(initial.connections)
  const [activeId, setActiveId] = useState(initial.activeId)
  const [sidebarOpen, setSidebarOpen] = useState(() => {
    const saved = localStorage.getItem(SIDEBAR_KEY)
    if (saved === '0') return false
    if (saved === '1') return true
    return true
  })

  const config = connections.find((c) => c.id === activeId) ?? connections[0]
  const [brokersText, setBrokersText] = useState(config?.brokers.join(',') ?? '')

  const [connected, setConnected] = useState(false)
  const [srStatus, setSrStatus] = useState<SchemaRegistryStatus>('none')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [pingInfo, setPingInfo] = useState<string | null>(null)

  const [topics, setTopics] = useState<TopicInfo[]>([])
  const [topicQuery, setTopicQuery] = useState('')
  const [selectedTopic, setSelectedTopic] = useState<string | null>(null)
  const [topicDetails, setTopicDetails] = useState<TopicInfo | null>(null)

  const [messages, setMessages] = useState<KafkaMessageView[]>([])
  const [loadingMessages, setLoadingMessages] = useState(false)
  const [search, setSearch] = useState('')
  const [searchKey, setSearchKey] = useState('')
  const [headerKey, setHeaderKey] = useState('')
  const [headerValue, setHeaderValue] = useState('')
  const [timeFrom, setTimeFrom] = useState('')
  const [timeTo, setTimeTo] = useState('')
  const [limit, setLimit] = useState(25)
  const [startFrom, setStartFrom] = useState<ConsumeStartFrom>('end')
  const [startOffset, setStartOffset] = useState('0')
  const [partitionFilter, setPartitionFilter] = useState<string>('all')
  const [savedQueries, setSavedQueries] = useState<SavedConsumeQuery[]>(() => loadSavedQueries())

  const [tab, setTab] = useState<'consume' | 'produce' | 'groups' | 'tail' | 'logs' | 'settings'>(
    'consume',
  )
  const [pendingDeleteId, setPendingDeleteId] = useState<string | null>(null)
  const [produceFormat, setProduceFormat] = useState<'avro' | 'json' | 'string'>('json')
  const [produceKey, setProduceKey] = useState('')
  const [produceValue, setProduceValue] = useState('{\n  "hello": "world"\n}')
  const [subjects, setSubjects] = useState<string[]>([])
  const [subject, setSubject] = useState('')
  const [schemaInfo, setSchemaInfo] = useState<SchemaSubjectInfo | null>(null)
  const [produceResult, setProduceResult] = useState<string | null>(null)

  const [groups, setGroups] = useState<ConsumerGroupInfo[]>([])
  const [groupQuery, setGroupQuery] = useState('')
  const [selectedGroup, setSelectedGroup] = useState<string | null>(null)
  const [groupOffsets, setGroupOffsets] = useState<GroupOffsetEntry[]>([])
  const [movePartition, setMovePartition] = useState('0')
  const [moveOffset, setMoveOffset] = useState('0')
  const [lagSummary, setLagSummary] = useState<import('../../electron/types').LagSummaryResult | null>(null)
  const [lagAutoRefresh, setLagAutoRefresh] = useState(true)
  const [updateStatus, setUpdateStatus] = useState<import('../../electron/types').UpdateStatus>({
    state: 'idle',
  })
  const [updateAuto, setUpdateAuto] = useState(true)

  const [tailing, setTailing] = useState(false)
  const [tailMessages, setTailMessages] = useState<KafkaMessageView[]>([])

  const [authSession, setAuthSession] = useState<AuthSessionPublic>({
    providerId: 'keycloak',
    loggedIn: false,
  })

  const hasRegistry = srStatus === 'ok'
  const authMethod = config ? authMethodFromConfig(config) : 'none'

  const filteredTopics = useMemo(() => {
    const q = topicQuery.trim().toLowerCase()
    if (!q) return topics
    return topics.filter((t) => t.name.toLowerCase().includes(q))
  }, [topics, topicQuery])

  const filteredGroups = useMemo(() => {
    const q = groupQuery.trim().toLowerCase()
    if (!q) return groups
    return groups.filter((g) => g.groupId.toLowerCase().includes(q))
  }, [groups, groupQuery])

  useEffect(() => {
    localStorage.setItem(SIDEBAR_KEY, sidebarOpen ? '1' : '0')
  }, [sidebarOpen])

  useEffect(() => {
    localStorage.setItem(CONNECTIONS_KEY, JSON.stringify({ connections, activeId }))
  }, [connections, activeId])

  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if (e.key === 'Escape') setSidebarOpen(false)
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [])

  useEffect(() => {
    if (!config) return
    setBrokersText(config.brokers.join(','))
  }, [activeId]) // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if (!window.kafdesk) {
      setError(t(localStorage.getItem(LOCALE_KEY) === 'en' ? 'en' : 'ru', 'preloadMissing'))
      return
    }
    window.kafdesk.status().then((s) => {
      setConnected(s.connected)
      setSrStatus(s.schemaRegistry)
    })
    void window.kafdesk.authSession().then(setAuthSession)
    void window.kafdesk.updateSettings?.().then((s) => setUpdateAuto(s.autoUpdate))
    const offTail = window.kafdesk.onTailMessage((msg) => {
      setTailMessages((prev) => [msg, ...prev].slice(0, 200))
    })
    const offUpdate = window.kafdesk.onUpdateStatus?.((s) => {
      setUpdateStatus(s)
      if (s.state === 'available') pushLog('info', `Update available: ${s.version ?? ''}`)
      if (s.state === 'downloaded') pushLog('info', `Update downloaded: ${s.version ?? ''} — restart to install`)
      if (s.state === 'error' && s.message) pushLog('warn', `Update check failed: ${s.message}`)
    })
    return () => {
      offTail()
      offUpdate?.()
    }
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if (tab !== 'groups' || !connected || !lagAutoRefresh) return
    const id = setInterval(() => {
      void loadLagSummary(true)
    }, 10_000)
    return () => clearInterval(id)
  }, [tab, connected, lagAutoRefresh, selectedTopic]) // eslint-disable-line react-hooks/exhaustive-deps

  function updateConfig(patch: Partial<ClusterConfig>) {
    setConnections((prev) => prev.map((c) => (c.id === activeId ? { ...c, ...patch } : c)))
  }

  function setAuthMethod(method: AuthMethodUi) {
    if (method === 'none') {
      updateConfig({ authMode: 'none' })
      return
    }
    if (method === 'sasl') {
      updateConfig({
        authMode: 'sasl',
        sasl: {
          mechanism: config?.sasl?.mechanism || 'plain',
          username: config?.sasl?.username || '',
          password: config?.sasl?.password || '',
        },
      })
      return
    }
    if (method === 'keycloak') {
      updateConfig({
        authMode: 'oauthbearer',
        sso: {
          providerId: 'keycloak',
          keycloak: config?.sso?.keycloak || { url: '', realm: '', clientId: '' },
        },
      })
      return
    }
    updateConfig({
      authMode: 'oauthbearer',
      sso: {
        providerId: 'oidc',
        oidc: config?.sso?.oidc || { issuer: '', clientId: '' },
        keycloak: config?.sso?.keycloak,
      },
    })
  }

  function selectConnection(id: string) {
    if (id === activeId) return
    if (connected) void disconnect()
    setActiveId(id)
    setPingInfo(null)
    setError(null)
  }

  const detailsRef = useRef<HTMLDivElement>(null)
  const selectedTopicRef = useRef<string | null>(null)
  selectedTopicRef.current = selectedTopic

  function addConnection() {
    const next = defaultConnection()
    next.name = `Connection ${connections.length + 1}`
    setConnections((prev) => [...prev, next])
    setActiveId(next.id)
    if (connected) void disconnect()
    setSidebarOpen(true)
    requestAnimationFrame(() => {
      detailsRef.current?.scrollIntoView({ behavior: 'smooth', block: 'nearest' })
    })
  }

  function editConnection(id: string) {
    if (id !== activeId) selectConnection(id)
    setSidebarOpen(true)
    requestAnimationFrame(() => {
      detailsRef.current?.scrollIntoView({ behavior: 'smooth', block: 'nearest' })
      const nameInput = detailsRef.current?.querySelector<HTMLInputElement>('input')
      nameInput?.focus()
      nameInput?.select()
    })
  }

  function duplicateConnection(id: string) {
    const src = connections.find((c) => c.id === id)
    if (!src) return
    const copy: ClusterConfig = {
      ...structuredClone(src),
      id: crypto.randomUUID(),
      name: `${src.name} copy`,
    }
    setConnections((prev) => [...prev, copy])
    setActiveId(copy.id)
    if (connected) void disconnect()
    setSidebarOpen(true)
    requestAnimationFrame(() => {
      detailsRef.current?.scrollIntoView({ behavior: 'smooth', block: 'nearest' })
    })
  }

  function requestDeleteConnection(id: string) {
    setPendingDeleteId(id)
  }

  function cancelDeleteConnection() {
    setPendingDeleteId(null)
  }

  function confirmDeleteConnection() {
    const id = pendingDeleteId
    setPendingDeleteId(null)
    if (!id) return
    const target = connections.find((c) => c.id === id)
    if (!target) return

    if (connections.length <= 1) {
      const fresh = defaultConnection()
      fresh.name = 'New connection'
      setConnections([fresh])
      setActiveId(fresh.id)
      setBrokersText(fresh.brokers.join(','))
      if (connected) void disconnect()
      pushLog('warn', `Удалён единственный коннект «${target.name}», создан пустой «New connection»`)
      setPingInfo(`«${target.name}» удалён`)
      return
    }

    const next = connections.filter((c) => c.id !== id)
    setConnections(next)
    if (activeId === id) {
      setActiveId(next[0].id)
      if (connected) void disconnect()
    }
    pushLog('info', `Удалён коннект «${target.name}»`)
    setPingInfo(`«${target.name}» удалён`)
  }

  function saveConnectionDetails() {
    if (!config) return
    updateConfig({ ...draftConfig() })
    setPingInfo(`saved «${config.name}»`)
  }

  async function ssoLogin() {
    if (!config?.sso) return
    setBusy(true)
    setError(null)
    try {
      const session = await window.kafdesk.authLogin({
        providerId: config.sso.providerId || 'keycloak',
        keycloak: config.sso.keycloak,
        oidc: config.sso.oidc,
      })
      setAuthSession(session)
      updateConfig({ authMode: 'oauthbearer' })
      setPingInfo(`SSO ok · ${session.username || session.email || 'user'}`)
      pushLog('info', `SSO login ok · ${session.username || session.email || 'user'}`)
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err)
      setError(msg)
      pushLog('error', `SSO login failed: ${msg}`)
    } finally {
      setBusy(false)
    }
  }

  async function ssoLogout() {
    setBusy(true)
    try {
      await window.kafdesk.authLogout()
      setAuthSession({ providerId: config?.sso?.providerId || 'keycloak', loggedIn: false })
      if (config?.authMode === 'oauthbearer') updateConfig({ authMode: 'none' })
      setPingInfo('SSO logged out')
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setBusy(false)
    }
  }

  function draftConfig(): ClusterConfig {
    return {
      ...config!,
      brokers: brokersText
        .split(',')
        .map((b) => b.trim())
        .filter(Boolean),
    }
  }

  async function connect() {
    if (!config) return
    setBusy(true)
    setError(null)
    setPingInfo(null)
    try {
      const next = draftConfig()
      updateConfig(next)
      const res = await window.kafdesk.connect(next)
      setConnected(true)
      setSrStatus(res.schemaRegistry)
      if (res.schemaRegistry === 'fail' && res.schemaRegistryError) {
        setPingInfo(`SR fail: ${res.schemaRegistryError}`)
      }
      const list = await window.kafdesk.listTopics()
      setTopics(list)
      if (res.schemaRegistry === 'ok') {
        try {
          setSubjects((await window.kafdesk.listSubjects()).sort())
        } catch {
          setSubjects([])
        }
      } else {
        setSubjects([])
      }
      setSidebarOpen(false)
      pushLog('info', `Connected to ${next.brokers.join(',')} (${next.authMode || 'none'})`)
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err)
      setError(msg)
      setConnected(false)
      setSrStatus('none')
      pushLog('error', `Connect failed: ${msg}`)
    } finally {
      setBusy(false)
    }
  }

  async function disconnect() {
    setBusy(true)
    try {
      if (tailing) await window.kafdesk.tailStop()
      await window.kafdesk.disconnect()
      setConnected(false)
      setSrStatus('none')
      setTopics([])
      setSelectedTopic(null)
      setTopicDetails(null)
      setMessages([])
      setPingInfo(null)
      setGroups([])
      setSelectedGroup(null)
      setGroupOffsets([])
      setTailing(false)
      setTailMessages([])
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setBusy(false)
    }
  }

  async function ping() {
    setBusy(true)
    setError(null)
    setPingInfo(null)
    try {
      if (!connected) {
        const next = draftConfig()
        updateConfig(next)
        const res = await window.kafdesk.connect(next)
        setConnected(true)
        setSrStatus(res.schemaRegistry)
        setTopics(await window.kafdesk.listTopics())
        if (res.schemaRegistry === 'ok') {
          try {
            setSubjects((await window.kafdesk.listSubjects()).sort())
          } catch {
            setSubjects([])
          }
        } else {
          setSubjects([])
        }
      }

      const res = await window.kafdesk.ping()
      setSrStatus(res.schemaRegistry.status)
      const kafkaPart = res.kafka.ok
        ? `Kafka ${res.kafka.ms ?? '?'}ms`
        : `Kafka fail: ${res.kafka.error ?? 'error'}`
      const srPart =
        res.schemaRegistry.status === 'none'
          ? 'no SR'
          : res.schemaRegistry.status === 'ok'
            ? `SR ${res.schemaRegistry.ms ?? '?'}ms`
            : `SR fail: ${res.schemaRegistry.error ?? 'error'}`
      setPingInfo(`${kafkaPart} · ${srPart}`)
      if (!res.kafka.ok) setConnected(false)
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
      setConnected(false)
      setSrStatus('none')
    } finally {
      setBusy(false)
    }
  }

  async function selectTopic(name: string) {
    setSelectedTopic(name)
    selectedTopicRef.current = name
    setMessages([])
    setTailMessages([])
    setTopicDetails(null)
    setError(null)
    setPartitionFilter('all')
    setBusy(true)
    try {
      if (tailing) {
        await window.kafdesk.tailStop()
        setTailing(false)
      }
      setTopicDetails(await window.kafdesk.getTopicOffsets(name))
      // Auto-load recent messages for the opened topic (all partitions)
      await loadMessages(name, { ...currentFilterState(), partitionFilter: 'all' })
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setBusy(false)
    }
  }

  function currentFilterState(): ConsumeFilterState {
    return {
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
  }

  function partitionsFor(filter: string): number[] | undefined {
    if (filter === 'all') return undefined
    const n = Number(filter)
    return Number.isFinite(n) ? [n] : undefined
  }

  function selectedPartitions(): number[] | undefined {
    return partitionsFor(partitionFilter)
  }

  function applyFilterState(f: ConsumeFilterState) {
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

  function saveCurrentQuery(name: string) {
    const trimmed = name.trim()
    if (!trimmed) return
    const q: SavedConsumeQuery = {
      id: crypto.randomUUID(),
      name: trimmed,
      createdAt: Date.now(),
      topic: selectedTopic,
      ...currentFilterState(),
    }
    const next = [q, ...savedQueries.filter((x) => x.name.toLowerCase() !== trimmed.toLowerCase())]
    setSavedQueries(next)
    persistSavedQueries(next)
    pushLog('info', `Saved query «${trimmed}»`)
  }

  function applySavedQuery(id: string) {
    const q = savedQueries.find((x) => x.id === id)
    if (!q) return
    const f: ConsumeFilterState = {
      search: q.search,
      searchKey: q.searchKey,
      headerKey: q.headerKey,
      headerValue: q.headerValue,
      partitionFilter: q.partitionFilter,
      startFrom: q.startFrom,
      startOffset: q.startOffset,
      limit: q.limit,
      timeFrom: q.timeFrom,
      timeTo: q.timeTo,
    }
    applyFilterState(f)
    const topic = q.topic && topics.some((t) => t.name === q.topic) ? q.topic : selectedTopic
    if (topic) {
      if (topic !== selectedTopic) {
        void selectTopic(topic).then(() => loadMessages(topic, f))
      } else {
        void loadMessages(topic, f)
      }
    }
  }

  function deleteSavedQuery(id: string) {
    const next = savedQueries.filter((x) => x.id !== id)
    setSavedQueries(next)
    persistSavedQueries(next)
  }

  async function loadMessages(topicName?: string, filterOverride?: ConsumeFilterState) {
    const topic = topicName || selectedTopic
    if (!topic) return
    const f = filterOverride ?? currentFilterState()
    setLoadingMessages(true)
    setError(null)
    try {
      if (tailing) {
        await window.kafdesk.tailStop()
        setTailing(false)
      }
      const rows = await window.kafdesk.consume({
        topic,
        startFrom: f.startFrom,
        startOffset: f.startFrom === 'offset' ? f.startOffset : undefined,
        limit: f.limit,
        search: f.search || undefined,
        searchKey: f.searchKey || undefined,
        searchHeaderKey: f.headerKey || undefined,
        searchHeaderValue: f.headerValue || undefined,
        timeFrom: timeInputToMs(f.timeFrom),
        timeTo: timeInputToMs(f.timeTo),
        partitions: partitionsFor(f.partitionFilter),
      })
      // Ignore stale responses if user already switched topic
      if (selectedTopicRef.current === topic) {
        setMessages(rows)
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setLoadingMessages(false)
    }
  }

  async function loadGroups() {
    setBusy(true)
    setError(null)
    try {
      setGroups(await window.kafdesk.listConsumerGroups())
      await loadLagSummary(true)
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setBusy(false)
    }
  }

  async function loadLagSummary(silent = false) {
    if (!window.kafdesk?.getLagSummary) return
    if (!silent) {
      setBusy(true)
      setError(null)
    }
    try {
      const res = await window.kafdesk.getLagSummary(selectedTopic || undefined)
      setLagSummary(res)
      // merge lag into group list rows
      setGroups((prev) =>
        prev.map((g) => {
          const s = res.groups.find((x) => x.groupId === g.groupId)
          return s
            ? {
                ...g,
                totalLag: s.totalLag,
                maxPartitionLag: s.maxPartitionLag,
                lagTopics: s.topics,
                lagPartitions: s.partitions,
              }
            : g
        }),
      )
    } catch (err) {
      if (!silent) setError(err instanceof Error ? err.message : String(err))
    } finally {
      if (!silent) setBusy(false)
    }
  }

  async function checkForUpdates() {
    try {
      if (window.kafdesk.checkForUpdates) {
        setUpdateStatus(await window.kafdesk.checkForUpdates())
      }
    } catch (err) {
      setUpdateStatus({
        state: 'error',
        message: err instanceof Error ? err.message : String(err),
      })
    }
  }

  async function setAutoUpdateEnabled(enabled: boolean) {
    setUpdateAuto(enabled)
    try {
      const s = await window.kafdesk.setAutoUpdate?.(enabled)
      if (s) setUpdateAuto(s.autoUpdate)
      if (!enabled) setUpdateStatus({ state: 'disabled' })
    } catch {
      /* keep local toggle */
    }
  }

  async function selectGroup(groupId: string) {
    setSelectedGroup(groupId)
    setBusy(true)
    setError(null)
    try {
      const rows = await window.kafdesk.getGroupOffsets(groupId, selectedTopic || undefined)
      setGroupOffsets(rows)
      if (rows[0]) {
        setMovePartition(String(rows[0].partition))
        setMoveOffset(rows[0].offset)
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setBusy(false)
    }
  }

  async function moveGroupOffset() {
    if (!selectedGroup || !selectedTopic) {
      setError('Select topic + consumer group to move offset')
      return
    }
    setBusy(true)
    setError(null)
    try {
      await window.kafdesk.setGroupOffsets({
        groupId: selectedGroup,
        topic: selectedTopic,
        partitions: [{ partition: Number(movePartition), offset: moveOffset }],
      })
      await selectGroup(selectedGroup)
      setPingInfo(`offset moved → ${selectedTopic} p${movePartition}=${moveOffset}`)
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setBusy(false)
    }
  }

  async function resetGroup(earliest: boolean) {
    if (!selectedGroup || !selectedTopic) {
      setError('Select topic + consumer group')
      return
    }
    setBusy(true)
    setError(null)
    try {
      await window.kafdesk.resetGroupOffsets(selectedGroup, selectedTopic, earliest)
      await selectGroup(selectedGroup)
      setPingInfo(`offsets reset → ${earliest ? 'earliest' : 'latest'}`)
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setBusy(false)
    }
  }

  async function startTail() {
    if (!selectedTopic) return
    setBusy(true)
    setError(null)
    try {
      setTailMessages([])
      await window.kafdesk.tailStart({
        topic: selectedTopic,
        partitions: selectedPartitions(),
        search: search || undefined,
        searchKey: searchKey || undefined,
        searchHeaderKey: headerKey || undefined,
        searchHeaderValue: headerValue || undefined,
      })
      setTailing(true)
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setBusy(false)
    }
  }

  async function stopTail() {
    setBusy(true)
    try {
      await window.kafdesk.tailStop()
      setTailing(false)
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setBusy(false)
    }
  }

  async function onSubjectChange(next: string) {
    setSubject(next)
    setSchemaInfo(null)
    if (!next || !hasRegistry) return
    try {
      const info = await window.kafdesk.getLatestSchema(next)
      setSchemaInfo(info)
      if (produceFormat === 'avro' && !produceValue.trim()) {
        setProduceValue('{\n  \n}')
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    }
  }

  async function sendMessage() {
    if (!selectedTopic) return
    setBusy(true)
    setError(null)
    setProduceResult(null)
    try {
      const res = await window.kafdesk.produce({
        topic: selectedTopic,
        key: produceKey || undefined,
        value: produceValue,
        format: produceFormat,
        subject: produceFormat === 'avro' ? subject || undefined : undefined,
      })
      const typeLabel =
        res.format === 'avro' && res.schemaId != null
          ? `Avro (schema id ${res.schemaId})`
          : String(res.format).toUpperCase()
      setProduceResult(
        `sent as ${typeLabel} → partition ${res.partition}, offset ${res.offset ?? 'n/a'}`,
      )
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setBusy(false)
    }
  }

  return {
    config,
    sidebarOpen,
    setSidebarOpen,
    connections,
    activeId,
    pendingDeleteId,
    authMethod,
    authSession,
    brokersText,
    setBrokersText,
    connected,
    srStatus,
    busy,
    pingInfo,
    error,
    detailsRef,
    tab,
    setTab,
    selectedTopic,
    topicDetails,
    filteredTopics,
    topicQuery,
    setTopicQuery,
    messages,
    loadingMessages,
    search,
    setSearch,
    searchKey,
    setSearchKey,
    headerKey,
    setHeaderKey,
    headerValue,
    setHeaderValue,
    timeFrom,
    setTimeFrom,
    timeTo,
    setTimeTo,
    savedQueries,
    saveCurrentQuery,
    applySavedQuery,
    deleteSavedQuery,
    limit,
    setLimit,
    startFrom,
    setStartFrom,
    startOffset,
    setStartOffset,
    partitionFilter,
    setPartitionFilter,
    tailing,
    tailMessages,
    hasRegistry,
    produceFormat,
    setProduceFormat,
    subjects,
    subject,
    schemaInfo,
    produceKey,
    setProduceKey,
    produceValue,
    setProduceValue,
    produceResult,
    groups,
    filteredGroups,
    groupQuery,
    setGroupQuery,
    selectedGroup,
    movePartition,
    setMovePartition,
    moveOffset,
    setMoveOffset,
    groupOffsets,
    lagSummary,
    lagAutoRefresh,
    setLagAutoRefresh,
    loadLagSummary,
    updateStatus,
    checkForUpdates,
    updateAuto,
    setAutoUpdateEnabled,
    addConnection,
    selectConnection,
    editConnection,
    duplicateConnection,
    requestDeleteConnection,
    cancelDeleteConnection,
    confirmDeleteConnection,
    updateConfig,
    setAuthMethod,
    ssoLogin,
    ssoLogout,
    saveConnectionDetails,
    connect,
    disconnect,
    ping,
    selectTopic,
    loadMessages,
    loadGroups,
    selectGroup,
    moveGroupOffset,
    resetGroup,
    startTail,
    stopTail,
    onSubjectChange,
    sendMessage,
  }
}
