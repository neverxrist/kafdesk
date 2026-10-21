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
import {
  loadPinnedTopics,
  togglePinnedTopic,
} from '../lib/pinnedTopics'
import {
  loadProduceTemplates,
  persistProduceTemplates,
  type ProduceTemplate,
} from '../lib/produceTemplates'
import { mockFromAvroSchemaPretty } from '../lib/avroMock'
import { expandProduceTemplate } from '../lib/produceVars'
import { guessAvroSubject, messageBodyText, messageId } from '../lib/messageText'
import type { AppLogEntry } from './useAppLogs'

export type { SavedConsumeQuery }

function stripIpcError(raw: string): string {
  return raw.replace(/^Error invoking remote method '[^']+':\s*/i, '').trim() || raw
}

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
  /** Per consumer-group lag on the selected topic (committed offsets only). */
  const [topicGroupLags, setTopicGroupLags] = useState<
    Array<{ groupId: string; totalLag: number }> | null
  >(null)

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
  const [produceFormat, setProduceFormat] = useState<
    'avro' | 'avro-local' | 'json' | 'string'
  >('json')
  const [produceKey, setProduceKey] = useState('')
  /** null = broker auto (key hash / round-robin) */
  const [producePartition, setProducePartition] = useState<number | null>(null)
  const [produceValue, setProduceValue] = useState('')
  const [subjects, setSubjects] = useState<string[]>([])
  const [subject, setSubject] = useState('')
  const [schemaInfo, setSchemaInfo] = useState<SchemaSubjectInfo | null>(null)
  const [localSchema, setLocalSchema] = useState<string | null>(null)
  const [localSchemaName, setLocalSchemaName] = useState<string | null>(null)
  const [produceResult, setProduceResult] = useState<string | null>(null)
  const [produceTemplates, setProduceTemplates] = useState<ProduceTemplate[]>(() =>
    loadProduceTemplates(),
  )
  const [pinnedTopics, setPinnedTopics] = useState<string[]>([])
  const [diffMessages, setDiffMessages] = useState<KafkaMessageView[]>([])

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
  const [updateAuto, setUpdateAuto] = useState(false)

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
    return () => {
      if (filterReloadTimer.current) clearTimeout(filterReloadTimer.current)
    }
  }, [])

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
    void refreshStoredCreds()
    const offTail = window.kafdesk.onTailMessage((msg) => {
      setTailMessages((prev) => [msg, ...prev].slice(0, 200))
    })
    const offTailErr = window.kafdesk.onTailError?.((message) => {
      setTailing(false)
      setError(message)
      pushLog('error', `Tail failed: ${message}`)
    })
    const offUpdate = window.kafdesk.onUpdateStatus?.((s) => {
      setUpdateStatus(s)
      if (s.state === 'available') pushLog('info', `Update available: ${s.version ?? ''}`)
      if (s.state === 'downloaded')
        pushLog('info', `Update downloaded: ${s.version ?? ''} — open DMG, replace in Applications`)
      if (s.state === 'error' && s.message) pushLog('warn', `Update check failed: ${s.message}`)
    })
    return () => {
      offTail()
      offTailErr?.()
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
    updateConfig({
      authMode: 'sasl',
      sasl: {
        mechanism: config?.sasl?.mechanism || 'plain',
        username: config?.sasl?.username || '',
        password: config?.sasl?.password || '',
      },
    })
  }

  useEffect(() => {
    if (!config?.id) {
      setPinnedTopics([])
      return
    }
    setPinnedTopics(loadPinnedTopics(config.id))
  }, [config?.id])

  function togglePinTopic(name: string) {
    if (!config?.id) return
    setPinnedTopics(togglePinnedTopic(config.id, name))
  }

  function toggleDiffMessage(m: KafkaMessageView) {
    setDiffMessages((prev) => {
      const id = messageId(m)
      const exists = prev.find((x) => messageId(x) === id)
      if (exists) return prev.filter((x) => messageId(x) !== id)
      if (prev.length >= 2) return [prev[1], m]
      return [...prev, m]
    })
  }

  function clearDiff() {
    setDiffMessages([])
  }

  async function republishMessage(m: KafkaMessageView) {
    const format =
      m.format === 'avro' ? 'avro' : m.format === 'json' ? 'json' : 'string'
    setProduceFormat(format)
    setProduceKey(m.key ?? '')
    setProduceValue(messageBodyText(m))
    setProduceResult(null)
    setTab('produce')
    if (format === 'avro') {
      const guessed = guessAvroSubject(m.topic || selectedTopic || '', subjects)
      if (guessed) await onSubjectChange(guessed)
      else setSubject('')
    } else {
      setSubject('')
      setSchemaInfo(null)
    }
    pushLog(
      'info',
      `Republish prepared · p${m.partition} offset ${m.offset} as ${format}`,
    )
  }

  function saveProduceTemplate(name: string) {
    const trimmed = name.trim()
    if (!trimmed) return
    const next: ProduceTemplate[] = [
      {
        id: crypto.randomUUID(),
        name: trimmed,
        createdAt: Date.now(),
        topic: selectedTopic,
        format: produceFormat,
        subject,
        key: produceKey,
        value: produceValue,
      },
      ...produceTemplates,
    ]
    setProduceTemplates(next)
    persistProduceTemplates(next)
  }

  async function applyProduceTemplate(id: string) {
    const tpl = produceTemplates.find((t) => t.id === id)
    if (!tpl) return
    setProduceFormat(tpl.format)
    setProduceKey(tpl.key)
    setProduceValue(tpl.value)
    if (tpl.topic && tpl.topic !== selectedTopic) {
      selectTopic(tpl.topic)
    }
    if (tpl.format === 'avro' && tpl.subject) {
      await onSubjectChange(tpl.subject)
    } else {
      setSubject(tpl.subject)
      setSchemaInfo(null)
    }
  }

  function deleteProduceTemplate(id: string) {
    const next = produceTemplates.filter((t) => t.id !== id)
    setProduceTemplates(next)
    persistProduceTemplates(next)
  }

  async function selectConnection(id: string) {
    if (id === activeId) return
    const next = connections.find((c) => c.id === id)
    if (!next) return
    const wasConnected = connected
    setPendingDeleteId(null)
    setPingInfo(null)
    setError(null)
    if (wasConnected) {
      await disconnect()
    }
    setActiveId(id)
    setBrokersText(next.brokers.join(','))
    // One live Kafka session only — drop old, reconnect to the chosen profile
    if (wasConnected) {
      await connectWith(next)
    }
  }

  const detailsRef = useRef<HTMLDivElement>(null)
  const selectedTopicRef = useRef<string | null>(null)
  selectedTopicRef.current = selectedTopic
  const tabRef = useRef(tab)
  tabRef.current = tab
  const consumeGenRef = useRef(0)
  const enrichGenRef = useRef(0)
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

  function addConnection() {
    const next = defaultConnection()
    next.name = `Connection ${connections.length + 1}`
    setPendingDeleteId(null)
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

  const [storedCreds, setStoredCreds] = useState<{ username?: string; hasPassword: boolean }>({
    hasPassword: false,
  })

  async function refreshStoredCreds() {
    try {
      if (window.kafdesk.storedCredentials) {
        setStoredCreds(await window.kafdesk.storedCredentials())
      }
    } catch {
      /* ignore */
    }
  }

  async function runSsoLogin(request: Parameters<typeof window.kafdesk.authLogin>[0]) {
    setBusy(true)
    setError(null)
    try {
      const session = await window.kafdesk.authLogin(request)
      setAuthSession(session)
      updateConfig({ authMode: 'oauthbearer' })
      setPingInfo(`SSO ok · ${session.username || session.email || 'user'}`)
      pushLog('info', `SSO login ok · ${session.username || session.email || 'user'}`)
      void refreshStoredCreds()
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err)
      setError(msg)
      pushLog('error', `SSO login failed: ${msg}`)
    } finally {
      setBusy(false)
    }
  }

  async function ssoLogin() {
    if (!config?.sso) return
    await runSsoLogin({
      providerId: config.sso.providerId || 'keycloak',
      method: 'browser',
      keycloak: config.sso.keycloak,
      oidc: config.sso.oidc,
    })
  }

  async function ssoPasswordLogin(username: string, password: string, remember: boolean) {
    if (!config?.sso) return
    await runSsoLogin({
      providerId: config.sso.providerId || 'keycloak',
      method: 'password',
      keycloak: config.sso.keycloak,
      oidc: config.sso.oidc,
      username,
      password: password || undefined,
      rememberPassword: remember,
    })
  }

  async function ssoForgetCredentials() {
    try {
      await window.kafdesk.forgetCredentials?.()
      await refreshStoredCreds()
      setPingInfo('Stored password forgotten')
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
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

  async function connectWith(target: ClusterConfig) {
    setBusy(true)
    setError(null)
    setPingInfo(null)
    try {
      setConnections((prev) => prev.map((c) => (c.id === target.id ? target : c)))
      const res = await window.kafdesk.connect(target)
      setConnected(true)
      setSrStatus(res.schemaRegistry)
      if (res.schemaRegistry === 'fail' && res.schemaRegistryError) {
        setPingInfo(`SR fail: ${res.schemaRegistryError}`)
      }
      const list = await window.kafdesk.listTopics()
      setTopics(list)
      void enrichTopicApprox(list)
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
      pushLog('info', `Connected to ${target.brokers.join(',')} (${target.authMode || 'none'})`)
    } catch (err) {
      const msg = stripIpcError(err instanceof Error ? err.message : String(err))
      setError(msg)
      pushLog('error', `Connect failed: ${msg}`)
      await abandonConnection()
    } finally {
      setBusy(false)
    }
  }

  /** UI says offline → always tear down main-process Kafka session (no ghosts). */
  async function abandonConnection() {
    enrichGenRef.current += 1
    try {
      if (tailing) {
        try {
          await window.kafdesk.tailStop()
        } catch {
          /* ignore */
        }
      }
      try {
        await window.kafdesk.disconnect()
      } catch {
        /* ignore */
      }
    } finally {
      setConnected(false)
      setSrStatus('none')
      setTopics([])
      setSubjects([])
      setTailing(false)
      setTailMessages([])
      setSelectedTopic(null)
      setTopicDetails(null)
      setTopicGroupLags(null)
      setMessages([])
      setGroups([])
      setSelectedGroup(null)
      setGroupOffsets([])
    }
  }

  async function connect() {
    if (!config) return
    await connectWith(draftConfig())
  }

  async function reconnect() {
    if (!config) return
    const keepTopic = selectedTopic
    setBusy(true)
    setError(null)
    setPingInfo(null)
    enrichGenRef.current += 1
    try {
      if (tailing) {
        try {
          await window.kafdesk.tailStop()
        } catch {
          /* ignore */
        }
        setTailing(false)
        setTailMessages([])
      }
      try {
        await window.kafdesk.disconnect()
      } catch {
        /* ignore stale session */
      }
      setConnected(false)
      setSrStatus('none')

      const target = draftConfig()
      setConnections((prev) => prev.map((c) => (c.id === target.id ? target : c)))
      const res = await window.kafdesk.connect(target)
      setConnected(true)
      setSrStatus(res.schemaRegistry)
      if (res.schemaRegistry === 'fail' && res.schemaRegistryError) {
        setPingInfo(`SR fail: ${res.schemaRegistryError}`)
      }
      const list = await window.kafdesk.listTopics()
      setTopics(list)
      void enrichTopicApprox(list)
      if (res.schemaRegistry === 'ok') {
        try {
          setSubjects((await window.kafdesk.listSubjects()).sort())
        } catch {
          setSubjects([])
        }
      } else {
        setSubjects([])
      }
      if (keepTopic && list.some((t) => t.name === keepTopic)) {
        setSelectedTopic(keepTopic)
      } else {
        setSelectedTopic(null)
        setTopicDetails(null)
        setTopicGroupLags(null)
        setMessages([])
      }
      pushLog('info', `Reconnected to ${target.brokers.join(',')} (${target.authMode || 'none'})`)
    } catch (err) {
      const msg = stripIpcError(err instanceof Error ? err.message : String(err))
      setError(msg)
      pushLog('error', `Reconnect failed: ${msg}`)
      await abandonConnection()
    } finally {
      setBusy(false)
    }
  }

  async function disconnect() {
    setBusy(true)
    enrichGenRef.current += 1
    try {
      if (tailing) await window.kafdesk.tailStop()
      await window.kafdesk.disconnect()
      setConnected(false)
      setSrStatus('none')
      setTopics([])
      setSelectedTopic(null)
      setTopicDetails(null)
      setTopicGroupLags(null)
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
    const wasConnected = connected
    try {
      const next = draftConfig()
      updateConfig(next)
      // Offline: one-shot probe (connect → check → disconnect). Online: live ping.
      const res = wasConnected
        ? await window.kafdesk.ping()
        : await window.kafdesk.probe(next)

      const kafkaPart = res.kafka.ok
        ? `Kafka ${res.kafka.ms ?? '?'}ms`
        : `Kafka fail: ${res.kafka.error ?? 'error'}`
      const srPart =
        res.schemaRegistry.status === 'none'
          ? 'no SR'
          : res.schemaRegistry.status === 'ok'
            ? `SR ${res.schemaRegistry.ms ?? '?'}ms`
            : `SR fail: ${res.schemaRegistry.error ?? 'error'}`

      if (wasConnected) {
        setPingInfo(`${kafkaPart} · ${srPart}`)
        setSrStatus(res.schemaRegistry.status)
        if (!res.kafka.ok) {
          await abandonConnection()
        }
      } else if (res.kafka.ok) {
        // Stay offline — probe only. Make that explicit so "offline" ≠ "unreachable".
        setPingInfo(`OK (not connected) · ${kafkaPart} · ${srPart}`)
      } else {
        setPingInfo(`unreachable · ${kafkaPart} · ${srPart}`)
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err)
      setError(msg)
      setPingInfo(`unreachable · ${msg}`)
      if (wasConnected) await abandonConnection()
    } finally {
      setBusy(false)
    }
  }

  async function enrichTopicApprox(list: TopicInfo[]) {
    if (!window.kafdesk?.getTopicOffsets || list.length === 0) return
    const gen = ++enrichGenRef.current
    const concurrency = 6
    let cursor = 0
    const merged = new Map<string, TopicInfo>()

    function flush() {
      if (enrichGenRef.current !== gen) return
      setTopics((prev) =>
        prev.map((p) => {
          const next = merged.get(p.name)
          return next
            ? { ...p, approxMessages: next.approxMessages, partitions: next.partitions }
            : p
        }),
      )
    }

    async function worker() {
      while (cursor < list.length) {
        if (enrichGenRef.current !== gen) return
        const idx = cursor++
        const t = list[idx]
        try {
          const full = await window.kafdesk.getTopicOffsets(t.name)
          if (enrichGenRef.current !== gen) return
          merged.set(t.name, full)
          if (merged.size % 10 === 0) flush()
        } catch {
          /* skip topic */
        }
      }
    }

    await Promise.all(
      Array.from({ length: Math.min(concurrency, list.length) }, () => worker()),
    )
    if (merged.size > 0) flush()
  }

  async function selectTopic(name: string) {
    setSelectedTopic(name)
    setProducePartition(null)
    selectedTopicRef.current = name
    setMessages([])
    setTailMessages([])
    setDiffMessages([])
    setTopicDetails(null)
    setTopicGroupLags(null)
    setError(null)
    filterStateRef.current = { ...filterStateRef.current, partitionFilter: 'all' }
    setPartitionFilter('all')
    setBusy(true)
    try {
      if (tailing) {
        await window.kafdesk.tailStop()
        setTailing(false)
      }
      const details = await window.kafdesk.getTopicOffsets(name)
      if (selectedTopicRef.current !== name) return
      setTopicDetails(details)
      setTopics((prev) =>
        prev.map((t) =>
          t.name === name
            ? { ...t, approxMessages: details.approxMessages, partitions: details.partitions }
            : t,
        ),
      )
      // Auto-load recent messages for the opened topic (all partitions)
      await loadMessages(name, { ...filterStateRef.current, partitionFilter: 'all' })
      void window.kafdesk
        .getLagSummary?.(name)
        .then((res) => {
          if (selectedTopicRef.current !== name) return
          setTopicGroupLags(
            res.groups.map((g) => ({ groupId: g.groupId, totalLag: g.totalLag })),
          )
        })
        .catch(() => {
          if (selectedTopicRef.current === name) setTopicGroupLags(null)
        })
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setBusy(false)
    }
  }

  function currentFilterState(): ConsumeFilterState {
    return { ...filterStateRef.current }
  }

  function partitionsFor(filter: string): number[] | undefined {
    if (filter === 'all') return undefined
    const n = Number(filter)
    return Number.isFinite(n) ? [n] : undefined
  }

  function selectedPartitions(): number[] | undefined {
    return partitionsFor(partitionFilter)
  }

  function cancelScheduledConsumeReload() {
    if (filterReloadTimer.current) {
      clearTimeout(filterReloadTimer.current)
      filterReloadTimer.current = null
    }
  }

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
      const topic = selectedTopicRef.current
      if (!topic || tabRef.current !== 'consume') return
      void loadMessages(topic, { ...filterStateRef.current })
    }
    if (delayMs <= 0) run()
    else filterReloadTimer.current = setTimeout(run, delayMs)
  }

  /** UI setters: update state and re-fetch (selects immediately, text debounced). */
  function setSearchFilter(v: string) {
    setSearch(v)
    scheduleConsumeReload({ search: v }, 350)
  }
  function setSearchKeyFilter(v: string) {
    setSearchKey(v)
    scheduleConsumeReload({ searchKey: v }, 350)
  }
  function setHeaderKeyFilter(v: string) {
    setHeaderKey(v)
    scheduleConsumeReload({ headerKey: v }, 350)
  }
  function setHeaderValueFilter(v: string) {
    setHeaderValue(v)
    scheduleConsumeReload({ headerValue: v }, 350)
  }
  function setTimeFromFilter(v: string) {
    setTimeFrom(v)
    scheduleConsumeReload({ timeFrom: v })
  }
  function setTimeToFilter(v: string) {
    setTimeTo(v)
    scheduleConsumeReload({ timeTo: v })
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
    // Guard: never treat a click event (or other non-string) as a topic name
    const topic = (typeof topicName === 'string' && topicName) || selectedTopic
    if (!topic) return
    cancelScheduledConsumeReload()
    const f = filterOverride ?? { ...filterStateRef.current }
    const gen = ++consumeGenRef.current
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
      // Ignore stale responses if a newer filter/topic request already started
      if (selectedTopicRef.current === topic && consumeGenRef.current === gen) {
        setMessages(rows)
      }
    } catch (err) {
      if (consumeGenRef.current === gen) {
        setError(err instanceof Error ? err.message : String(err))
      }
    } finally {
      if (consumeGenRef.current === gen) {
        setLoadingMessages(false)
      }
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
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    }
  }

  function schemaForMock(): string | null {
    if (produceFormat === 'avro-local' && localSchema) return localSchema
    if (schemaInfo?.schema) return schemaInfo.schema
    if (localSchema) return localSchema
    return null
  }

  function generateFromSchema() {
    const schema = schemaForMock()
    if (!schema) {
      setError('Load an Avro subject or .avsc first')
      return
    }
    try {
      setProduceValue(mockFromAvroSchemaPretty(schema))
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    }
  }

  async function loadAvscFile() {
    try {
      const file = await window.kafdesk.pickAvscFile?.()
      if (!file) return
      JSON.parse(file.text)
      const name = file.path.split(/[/\\]/).pop() || 'local.avsc'
      setLocalSchema(file.text)
      setLocalSchemaName(name)
      setProduceFormat('avro-local')
      setProduceValue(mockFromAvroSchemaPretty(file.text))
      pushLog('info', `Loaded local schema ${file.path}`)
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    }
  }

  async function sendMessage() {
    if (!selectedTopic) return
    if (produceFormat === 'avro-local' && !localSchema) {
      setError('Load a .avsc file first')
      return
    }
    setBusy(true)
    setError(null)
    setProduceResult(null)
    try {
      const res = await window.kafdesk.produce({
        topic: selectedTopic,
        key: produceKey ? expandProduceTemplate(produceKey, 0) : undefined,
        value: expandProduceTemplate(produceValue, 0),
        format: produceFormat,
        subject: produceFormat === 'avro' ? subject || undefined : undefined,
        localSchema: produceFormat === 'avro-local' ? localSchema || undefined : undefined,
        partition: producePartition ?? undefined,
      })
      const typeLabel =
        (res.format === 'avro' || res.format === 'avro-local') && res.schemaId != null
          ? `Avro (schema id ${res.schemaId})`
          : String(res.format).toUpperCase()
      setProduceResult(
        `sent as ${typeLabel} → partition ${res.partition}, offset ${res.offset ?? 'n/a'}`,
      )
      pushLog('info', `Produce → ${selectedTopic} as ${typeLabel}`)
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err)
      setError(msg)
      pushLog('error', `Produce failed: ${msg}`)
    } finally {
      setBusy(false)
    }
  }

  async function sendBulk(count: number) {
    if (!selectedTopic) return
    if (produceFormat === 'avro-local' && !localSchema) {
      setError('Load a .avsc file first')
      return
    }
    const n = Math.min(2000, Math.max(1, Math.trunc(count)))
    setBusy(true)
    setError(null)
    setProduceResult(null)
    try {
      const messages = Array.from({ length: n }, (_, i) => ({
        key: produceKey ? expandProduceTemplate(produceKey, i) : undefined,
        value: expandProduceTemplate(produceValue, i),
      }))
      const res = await window.kafdesk.produceBatch({
        topic: selectedTopic,
        format: produceFormat,
        subject: produceFormat === 'avro' ? subject || undefined : undefined,
        localSchema: produceFormat === 'avro-local' ? localSchema || undefined : undefined,
        partition: producePartition ?? undefined,
        messages,
      })
      const typeLabel =
        (res.format === 'avro' || res.format === 'avro-local') && res.schemaId != null
          ? `Avro (schema id ${res.schemaId})`
          : String(res.format).toUpperCase()
      setProduceResult(
        `sent ${res.sent} as ${typeLabel}` +
          (res.lastPartition != null
            ? ` → last p${res.lastPartition} offset ${res.lastOffset ?? 'n/a'}`
            : ''),
      )
      pushLog('info', `Bulk produce ${res.sent} → ${selectedTopic}`)
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err)
      setError(msg)
      pushLog('error', `Bulk produce failed: ${msg}`)
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
    topicGroupLags,
    filteredTopics,
    topicQuery,
    setTopicQuery,
    pinnedTopics,
    togglePinTopic,
    messages,
    diffMessages,
    toggleDiffMessage,
    clearDiff,
    republishMessage,
    loadingMessages,
    search,
    setSearch: setSearchFilter,
    searchKey,
    setSearchKey: setSearchKeyFilter,
    headerKey,
    setHeaderKey: setHeaderKeyFilter,
    headerValue,
    setHeaderValue: setHeaderValueFilter,
    timeFrom,
    setTimeFrom: setTimeFromFilter,
    timeTo,
    setTimeTo: setTimeToFilter,
    savedQueries,
    saveCurrentQuery,
    applySavedQuery,
    deleteSavedQuery,
    limit,
    setLimit: setLimitFilter,
    startFrom,
    setStartFrom: setStartFromFilter,
    startOffset,
    setStartOffset: setStartOffsetFilter,
    partitionFilter,
    setPartitionFilter: setPartitionFilterUI,
    tailing,
    tailMessages,
    hasRegistry,
    produceFormat,
    setProduceFormat,
    localSchemaName,
    subjects,
    subject,
    schemaInfo,
    produceKey,
    setProduceKey,
    producePartition,
    setProducePartition,
    produceValue,
    setProduceValue,
    produceResult,
    produceTemplates,
    saveProduceTemplate,
    applyProduceTemplate,
    deleteProduceTemplate,
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
    ssoPasswordLogin,
    ssoForgetCredentials,
    storedCreds,
    ssoLogout,
    saveConnectionDetails,
    connect,
    reconnect,
    disconnect,
    clearError: () => setError(null),
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
    sendBulk,
    generateFromSchema,
    loadAvscFile,
  }
}
