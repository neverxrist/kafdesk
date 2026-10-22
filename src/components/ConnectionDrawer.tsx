import {
  useEffect,
  useRef,
  useState,
  type PointerEvent as ReactPointerEvent,
  type RefObject,
} from 'react'
import type { ClusterConfig, SchemaRegistryStatus } from '../types/kafdesk'
import type { AuthMethodUi } from '../lib/connections'
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
  onSetAuthMethod,
  onSaveConnectionDetails,
  onConnect,
  onReconnect,
  onDisconnect,
  onPing,
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
  onSetAuthMethod: (method: AuthMethodUi) => void
  onSaveConnectionDetails: () => void
  onConnect: () => void
  onReconnect: () => void
  onDisconnect: () => void
  onPing: () => void
}) {
  const [dragId, setDragId] = useState<string | null>(null)
  const [overId, setOverId] = useState<string | null>(null)
  const dragIdRef = useRef<string | null>(null)
  const overIdRef = useRef<string | null>(null)
  const hasSr = Boolean(config.schemaRegistryUrl?.trim())
  const hasSrAuth = Boolean(
    config.schemaRegistryAuth?.username?.trim() || config.schemaRegistryAuth?.password,
  )
  const tls = config.schemaRegistryTls
  const hasTls = Boolean(
    tls?.caPath?.trim() ||
      tls?.certPath?.trim() ||
      tls?.keyPath?.trim() ||
      tls?.rejectUnauthorized === false,
  )
  const [tlsOpen, setTlsOpen] = useState(false)
  const [srOpen, setSrOpen] = useState(false)

  const nameOk = Boolean(config.name?.trim())
  const hostOk = Boolean(brokerHost.trim())
  const portOk = Boolean(brokerPort.trim().replace(/\D/g, ''))
  const brokersOk = hostOk && portOk
  const saslUserOk = authMethod !== 'sasl' || Boolean(config.sasl?.username?.trim())
  const saslPassOk = authMethod !== 'sasl' || Boolean(config.sasl?.password)
  // SR is optional — opening the panel must not block Connect
  const canConnect = nameOk && brokersOk && saslUserOk && saslPassOk

  const tlsMeta = tls?.caPath?.trim()
    ? tr('tlsToggleCa')
    : hasTls
      ? tr('tlsToggleCustom')
      : config.ssl
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

  const srMeta =
    srStatus === 'ok'
      ? tr('srToggleOn')
      : srStatus === 'fail' && hasSr
        ? tr('srToggleFail')
        : hasSr && hasSrAuth
          ? tr('srToggleUrlAuth')
          : hasSr
            ? tr('srToggleUrlSet')
            : tr('srToggleOff')

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
          <div className={`field ${nameOk ? '' : 'field-missing'}`.trim()}>
            {reqLabel(tr('name'), nameOk)}
            <input
              value={config.name}
              onChange={(e) => onUpdateConfig({ name: e.target.value })}
              aria-required
              autoCapitalize="none"
              autoCorrect="off"
              autoComplete="off"
              spellCheck={false}
              lang="en"
            />
          </div>

          <div className="field auth-method-field">
            <label>{tr('kafkaAuth')}</label>
            <div className="auth-method-row">
              <select
                value={authMethod}
                onChange={(e) => onSetAuthMethod(e.target.value as AuthMethodUi)}
              >
                <option value="none">{tr('authNone')}</option>
                <option value="sasl">{tr('authSasl')}</option>
              </select>
            </div>
          </div>

          <div className="broker-host-port-row">
            <div className={`field broker-host-field ${hostOk ? '' : 'field-missing'}`.trim()}>
              <div className="field-label-row">
                {reqLabel(tr('brokerHost'), hostOk)}
                <label className={`ssl-toggle${config.ssl ? ' on' : ''}`}>
                  <input
                    type="checkbox"
                    checked={Boolean(config.ssl)}
                    onChange={(e) => onUpdateConfig({ ssl: e.target.checked })}
                  />
                  {tr('ssl')}
                </label>
              </div>
              <input
                value={brokerHost}
                onChange={(e) => setBrokerHost(e.target.value)}
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
                  onChange={(e) =>
                    onUpdateConfig({
                      sasl: {
                        mechanism: config.sasl?.mechanism || 'plain',
                        username: e.target.value,
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
                  onChange={(password) =>
                    onUpdateConfig({
                      sasl: {
                        mechanism: config.sasl?.mechanism || 'plain',
                        username: config.sasl?.username || '',
                        password,
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

          <div className="sr-fold">
            <button
              type="button"
              className={`sr-fold-toggle ${hasTls ? 'has-sr' : ''}`}
              onClick={() => setTlsOpen((v) => !v)}
              aria-expanded={tlsOpen}
            >
              <span>{tr('tlsToggle')}</span>
              <span className="sr-fold-meta">
                {tlsMeta}
                <span className="sr-fold-chevron">{tlsOpen ? '▴' : '▾'}</span>
              </span>
            </button>
            {hasTls && !tlsOpen && tls?.caPath && (
              <div className="sr-fold-summary" title={tls.caPath}>
                {tls.caPath}
              </div>
            )}
            {tlsOpen && (
              <div className="sr-fold-body">
                <p className="tls-fold-hint">{tr('tlsHint')}</p>
                <div className="field">
                  <label>{tr('tlsCa')}</label>
                  <PathBrowseInput
                    value={tls?.caPath || ''}
                    onChange={(caPath) =>
                      onUpdateConfig({
                        schemaRegistryTls: { ...config.schemaRegistryTls, caPath },
                      })
                    }
                    onBrowse={async () => {
                      const p = await window.kafdesk.pickPemFile?.()
                      if (p)
                        onUpdateConfig({
                          schemaRegistryTls: { ...config.schemaRegistryTls, caPath: p },
                        })
                    }}
                    placeholder={tr('tlsPathPh')}
                    browseLabel={tr('browse')}
                  />
                </div>
                <div className="field">
                  <label>{tr('tlsCert')}</label>
                  <PathBrowseInput
                    value={tls?.certPath || ''}
                    onChange={(certPath) =>
                      onUpdateConfig({
                        schemaRegistryTls: { ...config.schemaRegistryTls, certPath },
                      })
                    }
                    onBrowse={async () => {
                      const p = await window.kafdesk.pickPemFile?.()
                      if (p)
                        onUpdateConfig({
                          schemaRegistryTls: { ...config.schemaRegistryTls, certPath: p },
                        })
                    }}
                    placeholder={tr('tlsPathPh')}
                    browseLabel={tr('browse')}
                  />
                </div>
                <div className="field">
                  <label>{tr('tlsKey')}</label>
                  <PathBrowseInput
                    value={tls?.keyPath || ''}
                    onChange={(keyPath) =>
                      onUpdateConfig({
                        schemaRegistryTls: { ...config.schemaRegistryTls, keyPath },
                      })
                    }
                    onBrowse={async () => {
                      const p = await window.kafdesk.pickPemFile?.()
                      if (p)
                        onUpdateConfig({
                          schemaRegistryTls: { ...config.schemaRegistryTls, keyPath: p },
                        })
                    }}
                    placeholder={tr('tlsPathPh')}
                    browseLabel={tr('browse')}
                  />
                </div>
                <div className="row">
                  <label className="status">
                    <input
                      type="checkbox"
                      checked={tls?.rejectUnauthorized === false}
                      onChange={(e) =>
                        onUpdateConfig({
                          schemaRegistryTls: {
                            ...config.schemaRegistryTls,
                            rejectUnauthorized: !e.target.checked,
                          },
                        })
                      }
                    />
                    {tr('tlsInsecure')}
                  </label>
                </div>
                {hasTls && (
                  <button
                    type="button"
                    className="sr-clear"
                    onClick={() => onUpdateConfig({ schemaRegistryTls: undefined })}
                  >
                    {tr('tlsClear')}
                  </button>
                )}
              </div>
            )}
          </div>

          <div className="sr-fold">
            <button
              type="button"
              className={`sr-fold-toggle ${srStatus === 'ok' ? 'has-sr' : ''} ${srStatus === 'fail' ? 'sr-fail' : ''}`}
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
                    onChange={(e) => onUpdateConfig({ schemaRegistryUrl: e.target.value })}
                  />
                </div>
                <div className="field">
                  <label>{tr('srUser')}</label>
                  <input
                    value={config.schemaRegistryAuth?.username || ''}
                    onChange={(e) =>
                      onUpdateConfig({
                        schemaRegistryAuth: {
                          username: e.target.value,
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
                    onChange={(password) =>
                      onUpdateConfig({
                        schemaRegistryAuth: {
                          username: config.schemaRegistryAuth?.username || '',
                          password,
                        },
                      })
                    }
                    showLabel={tr('showPassword')}
                    hideLabel={tr('hidePassword')}
                  />
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
                title={tr('ping')}
                aria-label={tr('ping')}
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
