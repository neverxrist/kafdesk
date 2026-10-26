import {
  useEffect,
  useRef,
  useState,
  type PointerEvent as ReactPointerEvent,
  type RefObject,
} from 'react'
import type {
  ClusterConfig,
  ConnectionEnvTag,
  SchemaRegistryStatus,
  TlsPemConfig,
} from '../types/kafdesk'
import {
  securityProtocolFromConfig,
  type AuthMethodUi,
  type SecurityProtocolUi,
} from '../lib/connections'
import type { useLocale } from '../hooks/useLocale'
import PasswordInput from './PasswordInput'
import PathBrowseInput from './PathBrowseInput'
import {
  ConnectIcon,
  DelIcon,
  DupIcon,
  EditIcon,
  PingIcon,
  ReconnectIcon,
} from './ConnActionIcons'

type Tr = ReturnType<typeof useLocale>['tr']

/** Name / host / user / password / SR URL — keep localStorage + IPC lean. */
const CONN_FIELD_MAX = 255

function clampConnField(v: string, max = CONN_FIELD_MAX): string {
  return v.length <= max ? v : v.slice(0, max)
}

function tlsPemNonEmpty(t: TlsPemConfig): boolean {
  return Boolean(
    t.caPath?.trim() ||
      t.certPath?.trim() ||
      t.keyPath?.trim() ||
      t.rejectUnauthorized === false,
  )
}

function tlsPemSame(a: TlsPemConfig, b: TlsPemConfig): boolean {
  return (
    (a.caPath?.trim() || '') === (b.caPath?.trim() || '') &&
    (a.certPath?.trim() || '') === (b.certPath?.trim() || '') &&
    (a.keyPath?.trim() || '') === (b.keyPath?.trim() || '') &&
    a.rejectUnauthorized === b.rejectUnauthorized
  )
}

function envTagLabel(tr: Tr, tag: ConnectionEnvTag): string {
  switch (tag) {
    case 'prod':
      return tr('envTag_prod')
    case 'stage':
      return tr('envTag_stage')
    case 'dev':
      return tr('envTag_dev')
    default:
      return tr('envTag_other')
  }
}

export default function ConnectionDrawer({
  tr,
  sidebarOpen,
  setSidebarOpen,
  connections,
  activeId,
  pendingDeleteId,
  config,
  authMethod,
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
  onAddConnection,
  onSelectConnection,
  onEditConnection,
  onReorderConnections,
  onDuplicateConnection,
  onRequestDeleteConnection,
  onCancelDeleteConnection,
  onConfirmDeleteConnection,
  onUpdateConfig,
  onSaveConnectionDetails,
  onConnect,
  onReconnect,
  onDisconnect,
  onPing,
  lockNote,
}: {
  tr: Tr
  sidebarOpen: boolean
  setSidebarOpen: (v: boolean) => void
  connections: ClusterConfig[]
  activeId: string
  pendingDeleteId: string | null
  config: ClusterConfig
  authMethod: AuthMethodUi
  brokerHost: string
  setBrokerHost: (v: string) => void
  brokerPort: string
  setBrokerPort: (v: string) => void
  connected: boolean
  srStatus: SchemaRegistryStatus
  busy: boolean
  pingInfo: string | null
  error: string | null
  detailsRef: RefObject<HTMLDivElement | null>
  onAddConnection: () => void
  onSelectConnection: (id: string) => void
  onEditConnection: (id: string) => void
  onReorderConnections: (fromId: string, toId: string) => void
  onDuplicateConnection: (id: string) => void
  onRequestDeleteConnection: (id: string) => void
  onCancelDeleteConnection: () => void
  onConfirmDeleteConnection: () => void
  onUpdateConfig: (patch: Partial<ClusterConfig>) => void
  onSaveConnectionDetails: () => void
  onConnect: () => void
  onReconnect: () => void
  onDisconnect: () => void
  onPing: () => void
  lockNote?: string
}) {
  const [dragId, setDragId] = useState<string | null>(null)
  const [overId, setOverId] = useState<string | null>(null)
  const dragIdRef = useRef<string | null>(null)
  const overIdRef = useRef<string | null>(null)
  const hasSr = Boolean(config.schemaRegistryUrl?.trim())
  const hasSrAuth = Boolean(
    config.schemaRegistryAuth?.username?.trim() || config.schemaRegistryAuth?.password,
  )
  /** Opt-in: only when explicitly true. */
  const srReuseKafkaTls = config.schemaRegistryReuseKafkaTls === true
  /** Dedicated SR PEMs (patchSrTls sets reuse=false). Legacy shared PEMs stay in Kafka fold. */
  const srTlsDedicated = config.schemaRegistryReuseKafkaTls === false
  /**
   * Kafka TLS UI: prefer kafkaTls; fall back to legacy shared schemaRegistryTls
   * only when kafkaTls is unset and SR is not using its own dedicated PEMs.
   */
  const kafkaTls: TlsPemConfig =
    config.kafkaTls ?? (srTlsDedicated ? {} : (config.schemaRegistryTls ?? {}))
  /**
   * SR-own PEMs: show dedicated SR paths; hide legacy-shared / Kafka copies so the
   * SR fold does not look "filled" when the user only configured Kafka TLS.
   */
  const srTlsOwn: TlsPemConfig = (() => {
    const sr = config.schemaRegistryTls
    if (!sr || !tlsPemNonEmpty(sr)) return {}
    if (!config.kafkaTls) {
      // Explicit SR PEMs (Browse in SR fold) — always show.
      if (srTlsDedicated) return sr
      return {} // legacy shared → Kafka fold owns it
    }
    if (tlsPemSame(sr, config.kafkaTls)) return {} // duplicate of Kafka
    return sr
  })()
  const hasKafkaTls = tlsPemNonEmpty(kafkaTls)
  const hasSrTlsOwn = tlsPemNonEmpty(srTlsOwn)
  const [tlsOpen, setTlsOpen] = useState(false)
  const [srOpen, setSrOpen] = useState(false)
  const [srTlsOpen, setSrTlsOpen] = useState(false)

  // One-shot: migrate legacy shared / seeded duplicate PEMs into kafkaTls only.
  // Skip when SR PEMs are dedicated (reuse explicitly false).
  useEffect(() => {
    if (
      !config.kafkaTls &&
      config.ssl &&
      config.schemaRegistryReuseKafkaTls !== false &&
      config.schemaRegistryTls &&
      tlsPemNonEmpty(config.schemaRegistryTls)
    ) {
      onUpdateConfig({
        kafkaTls: config.schemaRegistryTls,
        schemaRegistryTls: undefined,
      })
      return
    }
    if (
      config.kafkaTls &&
      config.schemaRegistryTls &&
      tlsPemSame(config.kafkaTls, config.schemaRegistryTls)
    ) {
      onUpdateConfig({ schemaRegistryTls: undefined })
    }
  }, [config.id]) // eslint-disable-line react-hooks/exhaustive-deps -- migrate once per connection

  const nameOk = Boolean(config.name?.trim())
  const hostOk = Boolean(brokerHost.trim())
  const portOk = Boolean(brokerPort.trim().replace(/\D/g, ''))
  const brokersOk = hostOk && portOk
  const saslUserOk = authMethod !== 'sasl' || Boolean(config.sasl?.username?.trim())
  const saslPassOk = authMethod !== 'sasl' || Boolean(config.sasl?.password)
  // SR is optional — opening the panel must not block Connect
  const canConnect = nameOk && brokersOk && saslUserOk && saslPassOk

  function patchKafkaTls(patch: Partial<TlsPemConfig>) {
    const legacyShared =
      !config.kafkaTls &&
      config.schemaRegistryTls &&
      config.schemaRegistryReuseKafkaTls !== false
    const base = config.kafkaTls ?? (legacyShared ? config.schemaRegistryTls! : {})
    onUpdateConfig({
      kafkaTls: { ...base, ...patch },
      // Leave dedicated SR PEMs alone; clear only legacy shared field
      ...(legacyShared ? { schemaRegistryTls: undefined } : {}),
    })
  }

  function patchSrTls(patch: Partial<TlsPemConfig>) {
    onUpdateConfig({
      schemaRegistryReuseKafkaTls: false,
      schemaRegistryTls: { ...(config.schemaRegistryTls ?? {}), ...patch },
    })
  }

  const securityProtocol = securityProtocolFromConfig(config)
  const brokerTlsOn = securityProtocol === 'ssl' || securityProtocol === 'sasl_ssl'

  useEffect(() => {
    if (!brokerTlsOn) setTlsOpen(false)
  }, [brokerTlsOn])

  function setSecurityProtocol(next: SecurityProtocolUi) {
    const ssl = next === 'ssl' || next === 'sasl_ssl'
    const sasl = next === 'sasl_plaintext' || next === 'sasl_ssl'
    if (sasl) {
      onUpdateConfig({
        ssl,
        authMode: 'sasl',
        sasl: {
          mechanism: config.sasl?.mechanism || 'plain',
          username: config.sasl?.username || '',
          password: config.sasl?.password || '',
        },
      })
      return
    }
    onUpdateConfig({ ssl, authMode: 'none' })
  }

  const tlsMeta = kafkaTls.caPath?.trim()
    ? tr('tlsToggleCa')
    : hasKafkaTls
      ? tr('tlsToggleCustom')
      : brokerTlsOn
        ? tr('tlsToggleSslHint')
        : tr('tlsToggleOff')

  useEffect(() => {
    if (!dragId) return
    function onMove(e: PointerEvent) {
      const el = document.elementFromPoint(e.clientX, e.clientY)
      const item = el?.closest('[data-conn-id]') as HTMLElement | null
      const id = item?.getAttribute('data-conn-id')
      const next = id && id !== dragIdRef.current ? id : null
      overIdRef.current = next
      setOverId(next)
    }
    function onUp() {
      const from = dragIdRef.current
      const to = overIdRef.current
      dragIdRef.current = null
      overIdRef.current = null
      setDragId(null)
      setOverId(null)
      document.body.classList.remove('conn-reordering')
      if (from && to) onReorderConnections(from, to)
    }
    window.addEventListener('pointermove', onMove)
    window.addEventListener('pointerup', onUp)
    window.addEventListener('pointercancel', onUp)
    return () => {
      window.removeEventListener('pointermove', onMove)
      window.removeEventListener('pointerup', onUp)
      window.removeEventListener('pointercancel', onUp)
      document.body.classList.remove('conn-reordering')
    }
  }, [dragId, onReorderConnections])

  function onReorderPointerDown(e: ReactPointerEvent<HTMLSpanElement>, id: string) {
    if (e.button !== 0) return
    e.preventDefault()
    e.stopPropagation()
    dragIdRef.current = id
    overIdRef.current = null
    setDragId(id)
    setOverId(null)
    document.body.classList.add('conn-reordering')
  }

  // Prefer form URL: empty field must not keep stale session "OK".
  const srMeta = !hasSr
    ? tr('srToggleOff')
    : srStatus === 'ok'
      ? tr('srToggleOn')
      : srStatus === 'fail'
        ? tr('srToggleFail')
        : hasSrAuth
          ? tr('srToggleUrlAuth')
          : tr('srToggleUrlSet')

  useEffect(() => {
    // Keep panels closed when switching connections; reopen only on demand
    setTlsOpen(false)
    setSrOpen(false)
  }, [config.id])

  function reqLabel(text: string, ok: boolean) {
    return (
      <label className={ok ? undefined : 'label-required'}>
        {text}
        <span className="req-star" title={tr('requiredMark')}>
          *
        </span>
      </label>
    )
  }

  return (
    <>
      <button
        type="button"
        className={`sidebar-backdrop ${sidebarOpen ? 'show' : ''}`}
        aria-label="Close connections"
        onClick={() => setSidebarOpen(false)}
      />

      <aside className={`sidebar ${sidebarOpen ? 'open' : ''}`} aria-hidden={!sidebarOpen}>
        <div className="brand-row">
          <div className="brand">
            <h1>
              Kaf<span className="brand-desk">Desk</span>
            </h1>
            <span>{tr('connections')}</span>
          </div>
          <button
            className="sidebar-collapse-btn"
            type="button"
            title={tr('collapse')}
            onClick={() => setSidebarOpen(false)}
          >
            ⟨
          </button>
        </div>

        {lockNote ? <p className="sidebar-lock-note">{lockNote}</p> : null}

        <div className="conn-list-head">
          <h2>{tr('saved')}</h2>
          <button type="button" className="icon-btn" onClick={onAddConnection}>
            {tr('new')}
          </button>
        </div>

        <div className="conn-list drag-scroll">
          {connections.map((c) => {
            const active = c.id === activeId
            const confirming = pendingDeleteId === c.id
            const dragging = dragId === c.id
            const dragOver = overId === c.id && dragId != null && dragId !== c.id
            return (
              <div
                key={c.id}
                data-conn-id={c.id}
                className={`conn-item${active ? ' active' : ''}${confirming ? ' confirming' : ''}${dragging ? ' is-dragging' : ''}${dragOver ? ' is-drag-over' : ''}`.trim()}
              >
                {confirming ? (
                  <>
                    <div className="conn-confirm">
                      <span>{tr('deleteConfirm', { name: c.name })}</span>
                    </div>
                    <div className="conn-actions conn-confirm-actions">
                      <button type="button" className="danger-btn" onClick={onConfirmDeleteConnection}>
                        {tr('delete')}
                      </button>
                      <button type="button" onClick={onCancelDeleteConnection}>
                        {tr('cancel')}
                      </button>
                    </div>
                  </>
                ) : (
                  <>
                    <span
                      className="conn-drag-handle no-drag-scroll"
                      role="button"
                      tabIndex={0}
                      title={tr('reorderConnections')}
                      aria-label={tr('reorderConnections')}
                      onPointerDown={(e) => onReorderPointerDown(e, c.id)}
                    >
                      ⋮⋮
                    </span>
                    <button
                      type="button"
                      className="conn-select"
                      onClick={() => onSelectConnection(c.id)}
                    >
                      <strong>
                        {active && <span className={`dot ${connected ? 'on' : ''}`} />}
                        {c.envTag ? (
                          <span
                            className={`conn-env-tag env-${c.envTag}`}
                            title={envTagLabel(tr, c.envTag)}
                          >
                            {envTagLabel(tr, c.envTag)}
                          </span>
                        ) : null}
                        {c.name}
                      </strong>
                      <small className="conn-meta">
                        <span className="conn-meta-host">
                          {active ? (connected ? tr('online') : tr('offline')) + ' · ' : ''}
                          {c.brokers.join(', ') || '—'}
                        </span>
                        {c.schemaRegistryUrl ? <span className="sr-hint">SR</span> : null}
                      </small>
                    </button>
                    <div className="conn-actions">
                      {active && (
                        <>
                          <button
                            type="button"
                            className="conn-icon-btn"
                            disabled={busy || connected || !canConnect}
                            onClick={onConnect}
                            title={tr('connect')}
                            aria-label={tr('connect')}
                          >
                            <ConnectIcon />
                          </button>
                          <button
                            type="button"
                            className="conn-icon-btn"
                            disabled={busy || !connected}
                            onClick={onReconnect}
                            title={tr('reconnect')}
                            aria-label={tr('reconnect')}
                          >
                            <ReconnectIcon />
                          </button>
                        </>
                      )}
                      <button
                        type="button"
                        className="conn-icon-btn"
                        onClick={() => onEditConnection(c.id)}
                        title={tr('edit')}
                        aria-label={tr('edit')}
                      >
                        <EditIcon />
                      </button>
                      <button
                        type="button"
                        className="conn-icon-btn"
                        onClick={() => onDuplicateConnection(c.id)}
                        title={tr('dup')}
                        aria-label={tr('dup')}
                      >
                        <DupIcon />
                      </button>
                      <button
                        type="button"
                        className="conn-icon-btn danger-text"
                        onClick={() => onRequestDeleteConnection(c.id)}
                        title={tr('del')}
                        aria-label={tr('del')}
                      >
                        <DelIcon />
                      </button>
                    </div>
                  </>
                )}
              </div>
            )
          })}
        </div>

        <div className="panel drag-scroll" ref={detailsRef}>
          <h2>{tr('editTitle', { name: config.name })}</h2>
          <p className="hint">{tr('editHint')}</p>
          <div className="conn-name-env-row">
            <div className={`field ${nameOk ? '' : 'field-missing'}`.trim()}>
              {reqLabel(tr('name'), nameOk)}
              <input
                value={config.name}
                maxLength={CONN_FIELD_MAX}
                onChange={(e) => onUpdateConfig({ name: clampConnField(e.target.value) })}
                aria-required
                autoCapitalize="none"
                autoCorrect="off"
                autoComplete="off"
                spellCheck={false}
                lang="en"
              />
            </div>
            <div className="field conn-env-field">
              <label>{tr('envTag')}</label>
              <select
                value={config.envTag || ''}
                onChange={(e) => {
                  const v = e.target.value as ConnectionEnvTag | ''
                  onUpdateConfig({ envTag: v ? v : undefined })
                }}
              >
                <option value="">{tr('envTagNone')}</option>
                <option value="prod">{tr('envTag_prod')}</option>
                <option value="stage">{tr('envTag_stage')}</option>
                <option value="dev">{tr('envTag_dev')}</option>
                <option value="other">{tr('envTag_other')}</option>
              </select>
            </div>
          </div>

          <div className="broker-host-port-row">
            <div className={`field broker-host-field ${hostOk ? '' : 'field-missing'}`.trim()}>
              {reqLabel(tr('brokerHost'), hostOk)}
              <input
                value={brokerHost}
                maxLength={CONN_FIELD_MAX}
                onChange={(e) => setBrokerHost(clampConnField(e.target.value))}
                placeholder={tr('brokerHostPh')}
                aria-required
                title={tr('brokerHostsHint')}
                autoCapitalize="none"
                autoCorrect="off"
                autoComplete="off"
                spellCheck={false}
                lang="en"
              />
            </div>
            <div className={`field broker-port-field ${portOk ? '' : 'field-missing'}`.trim()}>
              {reqLabel(tr('brokerPort'), portOk)}
              <input
                value={brokerPort}
                inputMode="numeric"
                pattern="[0-9]*"
                onChange={(e) => setBrokerPort(e.target.value.replace(/[^\d]/g, '').slice(0, 5))}
                aria-required
              />
            </div>
          </div>

          <div className="field auth-method-field">
            <label>{tr('securityProtocol')}</label>
            <div className="auth-method-row">
              <select
                value={securityProtocol}
                onChange={(e) => setSecurityProtocol(e.target.value as SecurityProtocolUi)}
              >
                <option value="plaintext">{tr('securityPlaintext')}</option>
                <option value="ssl">{tr('securitySsl')}</option>
                <option value="sasl_plaintext">{tr('securitySaslPlaintext')}</option>
                <option value="sasl_ssl">{tr('securitySaslSsl')}</option>
              </select>
            </div>
          </div>

          {authMethod === 'sasl' && (
            <>
              <div className="field">
                <label>{tr('saslMechanism')}</label>
                <select
                  value={config.sasl?.mechanism || 'plain'}
                  onChange={(e) =>
                    onUpdateConfig({
                      sasl: {
                        mechanism: e.target.value as 'plain' | 'scram-sha-256' | 'scram-sha-512',
                        username: config.sasl?.username || '',
                        password: config.sasl?.password || '',
                      },
                    })
                  }
                >
                  <option value="plain">PLAIN</option>
                  <option value="scram-sha-256">SCRAM-SHA-256</option>
                  <option value="scram-sha-512">SCRAM-SHA-512</option>
                </select>
              </div>
              <div className={`field ${saslUserOk ? '' : 'field-missing'}`.trim()}>
                {reqLabel(tr('username'), saslUserOk)}
                <input
                  value={config.sasl?.username || ''}
                  maxLength={CONN_FIELD_MAX}
                  onChange={(e) =>
                    onUpdateConfig({
                      sasl: {
                        mechanism: config.sasl?.mechanism || 'plain',
                        username: clampConnField(e.target.value),
                        password: config.sasl?.password || '',
                      },
                    })
                  }
                  autoComplete="username"
                  aria-required
                />
              </div>
              <div className={`field ${saslPassOk ? '' : 'field-missing'}`.trim()}>
                {reqLabel(tr('password'), saslPassOk)}
                <PasswordInput
                  value={config.sasl?.password || ''}
                  maxLength={CONN_FIELD_MAX}
                  onChange={(password) =>
                    onUpdateConfig({
                      sasl: {
                        mechanism: config.sasl?.mechanism || 'plain',
                        username: config.sasl?.username || '',
                        password: clampConnField(password),
                      },
                    })
                  }
                  autoComplete="current-password"
                  showLabel={tr('showPassword')}
                  hideLabel={tr('hidePassword')}
                />
              </div>
            </>
          )}

          {brokerTlsOn && (
          <div className="sr-fold">
            <button
              type="button"
              className={`sr-fold-toggle ${hasKafkaTls ? 'has-sr' : ''}`}
              onClick={() => setTlsOpen((v) => !v)}
              aria-expanded={tlsOpen}
            >
              <span>{tr('tlsToggle')}</span>
              <span className="sr-fold-meta">
                {tlsMeta}
                <span className="sr-fold-chevron">{tlsOpen ? '▴' : '▾'}</span>
              </span>
            </button>
            {hasKafkaTls && !tlsOpen && kafkaTls.caPath && (
              <div className="sr-fold-summary" title={kafkaTls.caPath}>
                {kafkaTls.caPath}
              </div>
            )}
            {tlsOpen && (
              <div className="sr-fold-body">
                <div className="field">
                  <label>{tr('tlsCa')}</label>
                  <PathBrowseInput
                    value={kafkaTls.caPath || ''}
                    onChange={(caPath) => patchKafkaTls({ caPath })}
                    onBrowse={async () => {
                      const p = await window.kafdesk.pickPemFile?.()
                      if (p) patchKafkaTls({ caPath: p })
                    }}
                    placeholder={tr('tlsPathPh')}
                    browseLabel={tr('browse')}
                  />
                </div>
                <p className="tls-mtls-label">{tr('tlsClientCertSection')}</p>
                <div className="field">
                  <label>{tr('tlsCert')}</label>
                  <PathBrowseInput
                    value={kafkaTls.certPath || ''}
                    onChange={(certPath) => patchKafkaTls({ certPath })}
                    onBrowse={async () => {
                      const p = await window.kafdesk.pickPemFile?.()
                      if (p) patchKafkaTls({ certPath: p })
                    }}
                    placeholder={tr('tlsPathPh')}
                    browseLabel={tr('browse')}
                  />
                </div>
                <div className="field">
                  <label>{tr('tlsKey')}</label>
                  <PathBrowseInput
                    value={kafkaTls.keyPath || ''}
                    onChange={(keyPath) => patchKafkaTls({ keyPath })}
                    onBrowse={async () => {
                      const p = await window.kafdesk.pickPemFile?.()
                      if (p) patchKafkaTls({ keyPath: p })
                    }}
                    placeholder={tr('tlsPathPh')}
                    browseLabel={tr('browse')}
                  />
                </div>
                <div className="row">
                  <label className="status">
                    <input
                      type="checkbox"
                      checked={kafkaTls.rejectUnauthorized === false}
                      onChange={(e) => patchKafkaTls({ rejectUnauthorized: !e.target.checked })}
                    />
                    {tr('tlsInsecure')}
                  </label>
                </div>
                {hasKafkaTls && (
                  <button
                    type="button"
                    className="sr-clear"
                    onClick={() =>
                      onUpdateConfig({
                        kafkaTls: undefined,
                        // Keep legacy field clear if it was the only store
                        ...(config.kafkaTls ? {} : { schemaRegistryTls: undefined }),
                      })
                    }
                  >
                    {tr('tlsClear')}
                  </button>
                )}
              </div>
            )}
          </div>
          )}

          <div className="sr-fold">
            <button
              type="button"
              className={`sr-fold-toggle ${hasSr && srStatus === 'ok' ? 'has-sr' : ''} ${hasSr && srStatus === 'fail' ? 'sr-fail' : ''}`}
              onClick={() => setSrOpen((v) => !v)}
              aria-expanded={srOpen}
            >
              <span>{tr('srToggle')}</span>
              <span className="sr-fold-meta">
                {srMeta}
                <span className="sr-fold-chevron">{srOpen ? '▴' : '▾'}</span>
              </span>
            </button>
            {hasSr && !srOpen && (
              <div className="sr-fold-summary" title={config.schemaRegistryUrl}>
                {config.schemaRegistryUrl}
              </div>
            )}
            {srOpen && (
              <div className="sr-fold-body">
                <div className="field">
                  <label>{tr('srUrl')}</label>
                  <input
                    value={config.schemaRegistryUrl || ''}
                    maxLength={CONN_FIELD_MAX}
                    onChange={(e) =>
                      onUpdateConfig({ schemaRegistryUrl: clampConnField(e.target.value) })
                    }
                  />
                </div>
                <div className="field">
                  <label>{tr('srUser')}</label>
                  <input
                    value={config.schemaRegistryAuth?.username || ''}
                    maxLength={CONN_FIELD_MAX}
                    onChange={(e) =>
                      onUpdateConfig({
                        schemaRegistryAuth: {
                          username: clampConnField(e.target.value),
                          password: config.schemaRegistryAuth?.password || '',
                        },
                      })
                    }
                  />
                </div>
                <div className="field">
                  <label>{tr('srPass')}</label>
                  <PasswordInput
                    value={config.schemaRegistryAuth?.password || ''}
                    maxLength={CONN_FIELD_MAX}
                    onChange={(password) =>
                      onUpdateConfig({
                        schemaRegistryAuth: {
                          username: config.schemaRegistryAuth?.username || '',
                          password: clampConnField(password),
                        },
                      })
                    }
                    showLabel={tr('showPassword')}
                    hideLabel={tr('hidePassword')}
                  />
                </div>

                <div className="sr-fold sr-fold-nested">
                  <button
                    type="button"
                    className={`sr-fold-toggle ${srReuseKafkaTls || hasSrTlsOwn ? 'has-sr' : ''}`}
                    onClick={() => setSrTlsOpen((v) => !v)}
                    aria-expanded={srTlsOpen}
                  >
                    <span>{tr('srTlsToggle')}</span>
                    <span className="sr-fold-meta">
                      {srReuseKafkaTls
                        ? tr('srTlsToggleReuse')
                        : srTlsOwn.caPath?.trim()
                          ? tr('tlsToggleCa')
                          : hasSrTlsOwn
                            ? tr('tlsToggleCustom')
                            : tr('tlsToggleOff')}
                      <span className="sr-fold-chevron">{srTlsOpen ? '▴' : '▾'}</span>
                    </span>
                  </button>
                  {!srTlsOpen && !srReuseKafkaTls && hasSrTlsOwn && srTlsOwn.caPath && (
                    <div className="sr-fold-summary" title={srTlsOwn.caPath}>
                      {srTlsOwn.caPath}
                    </div>
                  )}
                  {srTlsOpen && (
                    <div className="sr-fold-body">
                      <label className="status sr-reuse-tls" title={tr('srReuseKafkaTlsTitle')}>
                        <input
                          type="checkbox"
                          checked={srReuseKafkaTls}
                          onChange={(e) =>
                            onUpdateConfig({ schemaRegistryReuseKafkaTls: e.target.checked })
                          }
                        />
                        {tr('srReuseKafkaTls')}
                      </label>
                      {!srReuseKafkaTls && (
                        <>
                          <div className="field">
                            <label>{tr('srTlsCa')}</label>
                            <PathBrowseInput
                              value={srTlsOwn.caPath || ''}
                              onChange={(caPath) => patchSrTls({ caPath })}
                              onBrowse={async () => {
                                const p = await window.kafdesk.pickPemFile?.()
                                if (p) patchSrTls({ caPath: p })
                              }}
                              placeholder={tr('srTlsPathPh')}
                              browseLabel={tr('browse')}
                            />
                          </div>
                          <p className="tls-mtls-label">{tr('tlsClientCertSection')}</p>
                          <div className="field">
                            <label>{tr('srTlsCert')}</label>
                            <PathBrowseInput
                              value={srTlsOwn.certPath || ''}
                              onChange={(certPath) => patchSrTls({ certPath })}
                              onBrowse={async () => {
                                const p = await window.kafdesk.pickPemFile?.()
                                if (p) patchSrTls({ certPath: p })
                              }}
                              placeholder={tr('srTlsPathPh')}
                              browseLabel={tr('browse')}
                            />
                          </div>
                          <div className="field">
                            <label>{tr('srTlsKey')}</label>
                            <PathBrowseInput
                              value={srTlsOwn.keyPath || ''}
                              onChange={(keyPath) => patchSrTls({ keyPath })}
                              onBrowse={async () => {
                                const p = await window.kafdesk.pickPemFile?.()
                                if (p) patchSrTls({ keyPath: p })
                              }}
                              placeholder={tr('srTlsPathPh')}
                              browseLabel={tr('browse')}
                            />
                          </div>
                          <div className="row">
                            <label className="status">
                              <input
                                type="checkbox"
                                checked={srTlsOwn.rejectUnauthorized === false}
                                onChange={(e) =>
                                  patchSrTls({ rejectUnauthorized: !e.target.checked })
                                }
                              />
                              {tr('srTlsInsecure')}
                            </label>
                          </div>
                          {hasSrTlsOwn && (
                            <button
                              type="button"
                              className="sr-clear"
                              onClick={() => onUpdateConfig({ schemaRegistryTls: undefined })}
                            >
                              {tr('srTlsClear')}
                            </button>
                          )}
                        </>
                      )}
                    </div>
                  )}
                </div>

                {(hasSr || hasSrAuth) && (
                  <button
                    type="button"
                    className="sr-clear"
                    onClick={() =>
                      onUpdateConfig({
                        schemaRegistryUrl: '',
                        schemaRegistryAuth: undefined,
                      })
                    }
                  >
                    {tr('srClear')}
                  </button>
                )}
              </div>
            )}
          </div>

          <div className="conn-form-actions">
            <div className="conn-form-actions-row session" role="group" aria-label={tr('connect')}>
              <button
                type="button"
                className="primary icon-action"
                disabled={busy || connected || !canConnect}
                onClick={onConnect}
                title={tr('connect')}
                aria-label={tr('connect')}
              >
                <ConnectIcon />
                <span>{tr('connect')}</span>
              </button>
              <button
                type="button"
                className="icon-action"
                disabled={busy || !connected}
                onClick={onReconnect}
                title={tr('reconnect')}
                aria-label={tr('reconnect')}
              >
                <ReconnectIcon />
                <span>{tr('reconnect')}</span>
              </button>
              <button
                type="button"
                disabled={!connected}
                onClick={onDisconnect}
                title={busy ? tr('disconnectBusy') : undefined}
              >
                {tr('disconnect')}
              </button>
              <button
                type="button"
                className="icon-action conn-ping-btn"
                disabled={busy || !canConnect}
                onClick={onPing}
                title={tr('testConnection')}
                aria-label={tr('testConnection')}
              >
                <PingIcon />
              </button>
            </div>
            <div className="conn-form-actions-row profile" role="group" aria-label={tr('edit')}>
              <button type="button" disabled={busy} onClick={onSaveConnectionDetails}>
                {tr('save')}
              </button>
              <button
                type="button"
                disabled={busy}
                onClick={() => onDuplicateConnection(activeId)}
              >
                {tr('duplicate')}
              </button>
              <button
                type="button"
                className="danger-btn"
                disabled={busy}
                onClick={() => onRequestDeleteConnection(activeId)}
              >
                {tr('delete')}
              </button>
            </div>
          </div>

          <div className="status status-block">
            <span className={`dot ${connected ? 'on' : ''}`} />
            {connected ? tr('online') : tr('offline')}
            {connected &&
              ` · ${srStatus === 'ok' ? tr('srOk') : srStatus === 'fail' ? tr('srFail') : tr('srNone')}`}
          </div>
          {pingInfo && <div className="ping-info">{pingInfo}</div>}
        </div>

        {error && <div className="error">{error}</div>}
      </aside>
    </>
  )
}
