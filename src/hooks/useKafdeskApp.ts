import { useEffect, useMemo, useRef, useState } from 'react'
import type {
  AuthSessionPublic,
  ClusterConfig,
  ConsumerGroupInfo,
  GroupOffsetEntry,
  KafkaMessageView,
  SchemaSubjectInfo,
  SchemaRegistryStatus,
  TopicInfo,
} from '../types/kafdesk'
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
import {
  emptyHeaderRow,
  headersFromRows,
  rowsFromHeaders,
  type ProduceHeaderRow,
} from '../lib/produceHeaders'
import { guessAvroSubject, messageBodyText, messageId, slimMessageForUi } from '../lib/messageText'
import { filterMessages } from '../lib/messageFilter'
import { wireSearch } from '../lib/searchScope'
import { stripIpcError } from '../lib/ipcError'
import {
  brokersFromHostPort,
  DEFAULT_BROKER_PORT,
  hostPortFromBrokers,
} from '../lib/brokerEndpoint'
import type { AppLogEntry } from './useAppLogs'
import { useConsumeFilters, type ConsumeFilterState } from './useConsumeFilters'

export type { SavedConsumeQuery }
export type { ConsumeFilterState }

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
  const initialBrokers = hostPortFromBrokers(config?.brokers ?? [])
  const [brokerHost, setBrokerHost] = useState(initialBrokers.hosts)
  const [brokerPort, setBrokerPort] = useState(initialBrokers.port || DEFAULT_BROKER_PORT)

  function syncBrokerFields(brokers: string[]) {
    const next = hostPortFromBrokers(brokers)
    setBrokerHost(next.hosts)
    setBrokerPort(next.port || DEFAULT_BROKER_PORT)
  }

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
  const [savedQueries, setSavedQueries] = useState<SavedConsumeQuery[]>(() => loadSavedQueries())
  const consumeFilters = useConsumeFilters(messages)
  const {
    search,
    searchKey,
    searchScope,
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
    partitionsFor: partitionsForFilter,
    selectedPartitions,
    setSearchFilter,
    setSearchScopeFilter,
    setHeaderKeyFilter,
    setHeaderValueFilter,
    setTimeFromFilter,
    setTimeToFilter,
    setPartitionFilterUI,
    setStartFromFilter,
    setStartOffsetFilter,
    setLimitFilter,
    resetPartitionToAll,
  } = consumeFilters

  const [tab, setTab] = useState<
    'consume' | 'produce' | 'groups' | 'schemas' | 'tail' | 'logs' | 'settings'
  >('consume')
  /** One-shot subject focus when opening Schemas from topic schema popover. */
  const [schemasFocusSubject, setSchemasFocusSubject] = useState<string | null>(null)
  const [pendingDeleteId, setPendingDeleteId] = useState<string | null>(null)
  const [produceFormat, setProduceFormat] = useState<
    'avro' | 'avro-local' | 'json' | 'string'
  >('json')
  const [produceKey, setProduceKey] = useState('')
  const [produceHeaderRows, setProduceHeaderRows] = useState<ProduceHeaderRow[]>([])
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
  const [lagSummary, setLagSummary] = useState<import('../types/kafdesk').LagSummaryResult | null>(null)
  const [lagAutoRefresh, setLagAutoRefresh] = useState(false)
  const [updateStatus, setUpdateStatus] = useState<import('../types/kafdesk').UpdateStatus>({
    state: 'idle',
  })
  const [updateAuto, setUpdateAuto] = useState(false)

  const [tailing, setTailing] = useState(false)
  const tailingRef = useRef(false)
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
    return () => cancelScheduledConsumeReload()
  }, [cancelScheduledConsumeReload])

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
    syncBrokerFields(config.brokers)
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
      // Ignore late emits after Stop (in-flight decode / IPC race).
      if (!tailingRef.current) return
      const slim = slimMessageForUi(msg)
      const id = messageId(slim)
      setTailMessages((prev) => {
        if (prev.some((m) => messageId(m) === id)) return prev
        return [slim, ...prev].slice(0, 100)
      })
    })
    const offTailErr = window.kafdesk.onTailError?.((message) => {
      tailingRef.current = false
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

  useEffect(() => {
    // Only leave Schemas after a real disconnect of SR — not during initial 'none'.
    if (tab === 'schemas' && connected && srStatus !== 'ok') {
      setTab('consume')
    }
  }, [tab, connected, srStatus])

  // Mac DMG auto-update: check on enable + every 6h while on
  useEffect(() => {
    if (!updateAuto || !window.kafdesk?.checkForUpdates) return
    void checkForUpdates()
    const id = setInterval(() => {
      void checkForUpdates()
    }, 6 * 60 * 60 * 1000)
    return () => clearInterval(id)
  }, [updateAuto]) // eslint-disable-line react-hooks/exhaustive-deps

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

  function reorderConnections(fromId: string, toId: string) {
    if (fromId === toId) return
    setConnections((prev) => {
      const from = prev.findIndex((c) => c.id === fromId)
      const to = prev.findIndex((c) => c.id === toId)
      if (from < 0 || to < 0 || from === to) return prev
      const next = [...prev]
      const [item] = next.splice(from, 1)
      next.splice(to, 0, item)
      return next
    })
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
    setProduceHeaderRows(rowsFromHeaders(m.headers))
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
    syncBrokerFields(next.brokers)
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
      syncBrokerFields(fresh.brokers)
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
      // Kafka OAUTHBEARER removed — SSO is password-grant session only (not broker auth).
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
    // Browser PKCE removed — password grant only.
    setError('Browser SSO removed — use password login')
    pushLog('error', 'Browser SSO / PKCE removed')
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
      brokers: brokersFromHostPort(brokerHost, brokerPort),
    }
  }

  async function connectWith(target: ClusterConfig) {
    setBusy(true)
    setError(null)
    setPingInfo(null)
    let connectTimeout: ReturnType<typeof setTimeout> | undefined
    try {
      setConnections((prev) => prev.map((c) => (c.id === target.id ? target : c)))
      const res = await Promise.race([
        window.kafdesk.connect(target),
        new Promise<never>((_, reject) => {
          connectTimeout = setTimeout(() => {
            reject(new Error('Connect timed out (20s). Check brokers / VPN / DNS, then retry.'))
          }, 20_000)
        }),
      ])
      if (connectTimeout) clearTimeout(connectTimeout)
      setConnected(true)
      setSrStatus(res.schemaRegistry)
      if (res.schemaRegistry === 'fail' && res.schemaRegistryError) {
        setPingInfo(`SR fail: ${res.schemaRegistryError}`)
      }
      if (res.brokerDnsWarning) {
        // Soft note only — do not open Connection error modal with DNS howto.
        setPingInfo(res.brokerDnsWarning)
        pushLog('warn', res.brokerDnsWarning)
      }
      // Unlock Disconnect immediately — topic/SR catalog must not pin the UI busy.
      setBusy(false)
      pushLog('info', `Connected to ${target.brokers.join(',')} (${target.authMode || 'none'})`)
      try {
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
      } catch (err) {
        const msg = stripIpcError(err instanceof Error ? err.message : String(err))
        setError(msg)
        pushLog('error', `Connected, but catalog failed: ${msg}`)
      }
    } catch (err) {
      if (connectTimeout) clearTimeout(connectTimeout)
      const msg = stripIpcError(err instanceof Error ? err.message : String(err))
      setError(msg)
      pushLog('error', `Connect failed: ${msg}`)
      await abandonConnection()
    } finally {
      if (connectTimeout) clearTimeout(connectTimeout)
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
      tailingRef.current = false
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
        tailingRef.current = false
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
      let connectTimeout: ReturnType<typeof setTimeout> | undefined
      const res = await Promise.race([
        window.kafdesk.connect(target),
        new Promise<never>((_, reject) => {
          connectTimeout = setTimeout(() => {
            reject(new Error('Reconnect timed out (20s). Check brokers / VPN / DNS, then retry.'))
          }, 20_000)
        }),
      ]).finally(() => {
        if (connectTimeout) clearTimeout(connectTimeout)
      })
      setConnected(true)
      setSrStatus(res.schemaRegistry)
      if (res.schemaRegistry === 'fail' && res.schemaRegistryError) {
        setPingInfo(`SR fail: ${res.schemaRegistryError}`)
      }
      if (res.brokerDnsWarning) {
        setPingInfo(res.brokerDnsWarning)
        pushLog('warn', res.brokerDnsWarning)
      }
      setBusy(false)
      pushLog('info', `Reconnected to ${target.brokers.join(',')} (${target.authMode || 'none'})`)
      try {
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
      } catch (err) {
        const msg = stripIpcError(err instanceof Error ? err.message : String(err))
        setError(msg)
        pushLog('error', `Reconnected, but catalog failed: ${msg}`)
        setSelectedTopic(null)
        setTopicDetails(null)
        setTopicGroupLags(null)
        setMessages([])
      }
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
    enrichGenRef.current += 1
    setBusy(true)
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
        /* force UI offline even if IPC is wedged */
      }
      setConnected(false)
      setSrStatus('none')
      setTopics([])
      setSubjects([])
      setSelectedTopic(null)
      setTopicDetails(null)
      setTopicGroupLags(null)
      setMessages([])
      setPingInfo(null)
      setGroups([])
      setSelectedGroup(null)
      setGroupOffsets([])
      tailingRef.current = false
      setTailing(false)
      setTailMessages([])
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
      setConnected(false)
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
        ? `Kafka OK ${res.kafka.ms ?? '?'}ms`
        : `Kafka fail: ${res.kafka.error ?? 'error'}`
      const srPart =
        res.schemaRegistry.status === 'none'
          ? 'SR —'
          : res.schemaRegistry.status === 'ok'
            ? `SR OK ${res.schemaRegistry.ms ?? '?'}ms`
            : `SR fail: ${res.schemaRegistry.error ?? 'error'}`
      const dnsPart = res.dnsWarning ? `DNS: ${res.dnsWarning}` : 'DNS OK'
      const summary = `${kafkaPart} · ${srPart} · ${dnsPart}`

      if (wasConnected) {
        setPingInfo(summary)
        setSrStatus(res.schemaRegistry.status)
        if (!res.kafka.ok) {
          await abandonConnection()
        }
      } else if (res.kafka.ok) {
        // Probe only — does not leave a live session. Point at Connect explicitly.
        setPingInfo(`${summary} — press Connect`)
      } else {
        setPingInfo(summary)
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
    // One at a time: each offsets call briefly owns the Kafka session.
    const concurrency = 1
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
    // Pause enrich; cancel in-flight consume. Only auto-read on Consume tab —
    // Produce-only ACL connections must not hit GroupAuthorizationFailed on click.
    enrichGenRef.current += 1
    consumeGenRef.current += 1
    setSelectedTopic(name)
    setProducePartition(null)
    selectedTopicRef.current = name
    setMessages([])
    setTailMessages([])
    setDiffMessages([])
    setTopicDetails(null)
    setTopicGroupLags(null)
    setError(null)
    resetPartitionToAll()
    const wantConsume = tabRef.current === 'consume'
    try {
      if (tailing) {
        await window.kafdesk.tailStop()
        tailingRef.current = false
        setTailing(false)
      }
      try {
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
        if (!wantConsume) return
        await loadMessages(name, { ...filterStateRef.current, partitionFilter: 'all' }, details)
      } catch (err) {
        if (!wantConsume) {
          // Offsets/watermarks optional for produce — keep topic selected.
          return
        }
        throw err
      }
      if (selectedTopicRef.current !== name) return
      // After messages — never race GroupCoordinator against consume.
      // Keep [] (not null) on failure so the «Читают» row stays visible.
      void window.kafdesk
        .getLagSummary?.(name)
        .then((res) => {
          if (selectedTopicRef.current !== name) return
          setTopicGroupLags(
            res.groups.map((g) => ({ groupId: g.groupId, totalLag: g.totalLag })),
          )
        })
        .catch(() => {
          if (selectedTopicRef.current === name) setTopicGroupLags([])
        })
    } catch (err) {
      const msg = stripIpcError(err instanceof Error ? err.message : String(err))
      setError(msg)
      setLoadingMessages(false)
    }
  }

  async function refreshSubjects() {
    if (!hasRegistry || !window.kafdesk?.listSubjects) return
    setBusy(true)
    setError(null)
    try {
      setSubjects((await window.kafdesk.listSubjects()).sort())
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setBusy(false)
    }
  }

  function changeTab(next: typeof tab) {
    const prev = tabRef.current
    setTab(next)
    // Drop heavy message window when leaving Consume — WebView keeps DOM/JS otherwise.
    if (prev === 'consume' && next !== 'consume') {
      setMessages([])
      clearDiff()
    }
    if (prev === 'tail' && next !== 'tail') {
      setTailMessages([])
      if (tailing) {
        tailingRef.current = false
        setTailing(false)
        void window.kafdesk.tailStop().catch(() => {})
      }
    }
    if (
      next === 'consume' &&
      prev !== 'consume' &&
      selectedTopicRef.current &&
      connected
    ) {
      void loadMessages(selectedTopicRef.current)
    }
    if (next === 'schemas' && connected && hasRegistry && subjects.length === 0) {
      void refreshSubjects()
    }
  }

  function useSubjectForProduce(subjectName: string) {
    setProduceFormat('avro')
    void onSubjectChange(subjectName)
    changeTab('produce')
  }

  function openSubjectInSchemas(subjectName: string) {
    setSchemasFocusSubject(subjectName)
    changeTab('schemas')
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
      searchScope: q.searchScope ?? 'both',
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

  async function loadMessages(
    topicName?: string,
    filterOverride?: ConsumeFilterState,
    offsetsFrom?: TopicInfo | null,
  ) {
    // Guard: never treat a click event (or other non-string) as a topic name
    const topic = (typeof topicName === 'string' && topicName) || selectedTopic
    if (!topic) return
    // Prefer consume over background topic-size enrich (they share one Kafka session).
    enrichGenRef.current += 1
    cancelScheduledConsumeReload()
    const f = filterOverride ?? { ...filterStateRef.current }
    const gen = ++consumeGenRef.current
    setLoadingMessages(true)
    setError(null)
    // Clear immediately so switching beginning/end doesn't keep stale rows
    setMessages([])
    let uiTimeout: ReturnType<typeof setTimeout> | undefined
    try {
      if (tailing) {
        await window.kafdesk.tailStop()
        tailingRef.current = false
        setTailing(false)
      }
      const details = offsetsFrom ?? topicDetails
      const partitionWatermarks =
        details?.name === topic
          ? details.partitions
              .map((p) => {
                const low = p.offsetLow != null ? Number(p.offsetLow) : NaN
                const high = p.offsetHigh != null ? Number(p.offsetHigh) : NaN
                if (!Number.isFinite(low) || !Number.isFinite(high)) return null
                return { partition: p.partitionId, low, high }
              })
              .filter((x): x is { partition: number; low: number; high: number } => x != null)
          : undefined

      // Window only — search/key/headers/time filter client-side on `messages`.
      // Hard UI cap: never leave the empty-state spinner forever if IPC wedges.
      const rows = await Promise.race([
        window.kafdesk.consume({
          topic,
          startFrom: f.startFrom,
          startOffset: f.startFrom === 'offset' ? f.startOffset : undefined,
          limit: f.limit,
          partitions: partitionsForFilter(f.partitionFilter),
          partitionWatermarks:
            partitionWatermarks && partitionWatermarks.length > 0
              ? partitionWatermarks
              : undefined,
        }),
        new Promise<never>((_, reject) => {
          uiTimeout = setTimeout(() => {
            reject(
              new Error(
                'Consume timed out (20s). Broker slow or previous request still finishing — try again.',
              ),
            )
          }, 20_000)
        }),
      ])
      // Ignore stale responses if a newer filter/topic request already started
      if (selectedTopicRef.current === topic && consumeGenRef.current === gen) {
        setMessages(rows.map(slimMessageForUi))
      }
    } catch (err) {
      if (consumeGenRef.current === gen) {
        const msg = stripIpcError(err instanceof Error ? err.message : String(err))
        // Ignore stale UI-timeout / errors after the user already switched topic.
        if (selectedTopicRef.current === topic) {
          setError(msg)
        }
      }
    } finally {
      if (uiTimeout) clearTimeout(uiTimeout)
      if (consumeGenRef.current === gen) {
        setLoadingMessages(false)
      }
    }
  }

  brokerReloadRef.current = (filter) => {
    const topic = selectedTopicRef.current
    if (!topic || tabRef.current !== 'consume') return
    void loadMessages(topic, filter)
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

  async function openDownloadedUpdate() {
    try {
      await window.kafdesk.openDownloadedUpdate?.()
    } catch (err) {
      setUpdateStatus({
        state: 'error',
        message: err instanceof Error ? err.message : String(err),
      })
    }
  }

  async function openReleases() {
    try {
      await window.kafdesk.openReleases?.()
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

  function buildTailRequest(topic: string) {
    const wired = wireSearch(search, searchScope)
    return {
      topic,
      partitions: selectedPartitions(),
      search: wired.search,
      searchKey: wired.searchKey,
      searchScope: wired.searchScope,
      searchHeaderKey: headerKey || undefined,
      searchHeaderValue: headerValue || undefined,
    }
  }

  async function startTail() {
    if (!selectedTopic) return
    setBusy(true)
    setError(null)
    try {
      // Keep UI buffer; always attach at latest (no catch-up dump of the Stop gap).
      await window.kafdesk.tailStart(buildTailRequest(selectedTopic))
      tailingRef.current = true
      setTailing(true)
    } catch (err) {
      tailingRef.current = false
      setTailing(false)
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setBusy(false)
    }
  }

  // Hot-swap partition filter while running. Search/header stay UI-only (no buffer prune).
  useEffect(() => {
    if (!tailing || !selectedTopic || !window.kafdesk?.tailSetFilter) return
    const t = window.setTimeout(() => {
      void window.kafdesk.tailSetFilter(buildTailRequest(selectedTopic)).catch(() => {})
    }, 250)
    return () => window.clearTimeout(t)
    // eslint-disable-next-line react-hooks/exhaustive-deps -- partition filter while tailing
  }, [tailing, selectedTopic, partitionFilter])

  async function stopTail() {
    // Flip gate first so late kafka://tail-message events are dropped.
    tailingRef.current = false
    setTailing(false)
    setBusy(true)
    try {
      await window.kafdesk.tailStop()
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
        headers: headersFromRows(produceHeaderRows),
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
        headers: headersFromRows(produceHeaderRows),
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
    brokerHost,
    setBrokerHost,
    brokerPort,
    setBrokerPort,
    connected,
    srStatus,
    busy,
    pingInfo,
    error,
    detailsRef,
    tab,
    setTab: changeTab,
    refreshSubjects,
    useSubjectForProduce,
    openSubjectInSchemas,
    schemasFocusSubject,
    clearSchemasFocusSubject: () => setSchemasFocusSubject(null),
    selectedTopic,
    topicDetails,
    topicGroupLags,
    topics,
    filteredTopics,
    topicQuery,
    setTopicQuery,
    pinnedTopics,
    togglePinTopic,
    messages,
    filteredMessages,
    windowMessageCount: messages.length,
    diffMessages,
    toggleDiffMessage,
    clearDiff,
    republishMessage,
    loadingMessages,
    search,
    setSearch: setSearchFilter,
    searchScope,
    setSearchScope: setSearchScopeFilter,
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
    tailBufferCount: tailMessages.length,
    tailMessages: filterMessages(tailMessages, {
      search,
      searchKey,
      searchScope,
      headerKey,
      headerValue,
    }),
    hasRegistry,
    produceFormat,
    setProduceFormat,
    localSchemaName,
    subjects,
    subject,
    schemaInfo,
    produceKey,
    setProduceKey,
    produceHeaderRows,
    setProduceHeaderRows,
    addProduceHeaderRow: () => setProduceHeaderRows((prev) => [...prev, emptyHeaderRow()]),
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
    openDownloadedUpdate,
    openReleases,
    updateAuto,
    setAutoUpdateEnabled,
    addConnection,
    reorderConnections,
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
