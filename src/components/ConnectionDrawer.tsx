import { useEffect, useState, type RefObject } from 'react'
import type { ClusterConfig, SchemaRegistryStatus } from '../../electron/types'
import type { AuthMethodUi } from '../lib/connections'
import type { useLocale } from '../hooks/useLocale'
import PasswordInput from './PasswordInput'
import { ConnectIcon, DelIcon, DupIcon, EditIcon, ReconnectIcon } from './ConnActionIcons'

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
  brokersText,
  setBrokersText,
  connected,
  srStatus,
  busy,
  pingInfo,
  error,
  detailsRef,
  onAddConnection,
  onSelectConnection,
  onEditConnection,
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
  brokersText: string
  setBrokersText: (v: string) => void
  connected: boolean
  srStatus: SchemaRegistryStatus
  busy: boolean
  pingInfo: string | null
  error: string | null
  detailsRef: RefObject<HTMLDivElement | null>
  onAddConnection: () => void
  onSelectConnection: (id: string) => void
  onEditConnection: (id: string) => void
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
  const hasSr = Boolean(config.schemaRegistryUrl?.trim())
  const [srOpen, setSrOpen] = useState(false)

  const nameOk = Boolean(config.name?.trim())
  const brokersOk = Boolean(brokersText.trim())
  const saslUserOk = authMethod !== 'sasl' || Boolean(config.sasl?.username?.trim())
  const saslPassOk = authMethod !== 'sasl' || Boolean(config.sasl?.password)
  // URL is required once the SR panel is open or a URL was already saved
  const srUrlNeeded = srOpen || hasSr
  const srUrlOk = !srUrlNeeded || Boolean(config.schemaRegistryUrl?.trim())

  const srMeta =
    srStatus === 'ok'
      ? tr('srToggleOn')
      : srStatus === 'fail' && hasSr
        ? tr('srToggleFail')
        : hasSr
          ? tr('srToggleUrlSet')
          : tr('srToggleOff')

  useEffect(() => {
    // Keep panel closed when switching connections; reopen only on demand
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
            <h1>KafDesk</h1>
            <span>{tr('connections')}</span>
          </div>
          <button
            className="icon-btn"
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

        {pendingDeleteId && (
          <div className="panel confirm-panel">
            <p className="hint" style={{ margin: 0 }}>
              {tr('deleteConfirm', {
                name: connections.find((c) => c.id === pendingDeleteId)?.name || '',
              })}
            </p>
            <div className="actions" style={{ marginTop: 8 }}>
              <button type="button" className="danger-btn" onClick={onConfirmDeleteConnection}>
                {tr('delete')}
              </button>
              <button type="button" onClick={onCancelDeleteConnection}>
                {tr('cancel')}
              </button>
            </div>
          </div>
        )}

        <div className="conn-list">
          {connections.map((c) => {
            const active = c.id === activeId
            return (
              <div key={c.id} className={`conn-item ${active ? 'active' : ''}`}>
                <button type="button" className="conn-select" onClick={() => onSelectConnection(c.id)}>
                  <strong>
                    {active && <span className={`dot ${connected ? 'on' : ''}`} />}
                    {c.name}
                  </strong>
                  <small>
                    {active ? (connected ? tr('online') : tr('offline')) + ' · ' : ''}
                    {c.brokers.join(', ') || '—'}
                  </small>
                  {c.schemaRegistryUrl ? <small className="sr-hint">SR</small> : null}
                </button>
                <div className="conn-actions">
                  {active && (
                    <>
                      <button
                        type="button"
                        className="conn-icon-btn"
                        disabled={busy || connected}
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
              </div>
            )
          })}
        </div>

        <div className="panel" ref={detailsRef}>
          <h2>{tr('editTitle', { name: config.name })}</h2>
          <p className="hint">{tr('editHint')}</p>
          <div className={`field ${nameOk ? '' : 'field-missing'}`.trim()}>
            {reqLabel(tr('name'), nameOk)}
            <input
              value={config.name}
              onChange={(e) => onUpdateConfig({ name: e.target.value })}
              aria-required
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

          <div className={`field ${brokersOk ? '' : 'field-missing'}`.trim()}>
            <div className="field-label-row">
              {reqLabel(tr('brokers'), brokersOk)}
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
              value={brokersText}
              onChange={(e) => setBrokersText(e.target.value)}
              aria-required
            />
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
                <div className={`field ${srUrlOk ? '' : 'field-missing'}`.trim()}>
                  {reqLabel(tr('srUrl'), srUrlOk)}
                  <input
                    value={config.schemaRegistryUrl || ''}
                    onChange={(e) => onUpdateConfig({ schemaRegistryUrl: e.target.value })}
                    aria-required={srUrlNeeded}
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
                <div className="field">
                  <label>{tr('srTlsCa')}</label>
                  <div className="saved-queries-row">
                    <input
                      value={config.schemaRegistryTls?.caPath || ''}
                      onChange={(e) =>
                        onUpdateConfig({
                          schemaRegistryTls: {
                            ...config.schemaRegistryTls,
                            caPath: e.target.value,
                          },
                        })
                      }
                      placeholder={tr('srTlsPathPh')}
                    />
                    <button
                      type="button"
                      onClick={async () => {
                        const p = await window.kafdesk.pickPemFile?.()
                        if (p)
                          onUpdateConfig({
                            schemaRegistryTls: { ...config.schemaRegistryTls, caPath: p },
                          })
                      }}
                    >
                      {tr('browse')}
                    </button>
                  </div>
                </div>
                <div className="field">
                  <label>{tr('srTlsCert')}</label>
                  <div className="saved-queries-row">
                    <input
                      value={config.schemaRegistryTls?.certPath || ''}
                      onChange={(e) =>
                        onUpdateConfig({
                          schemaRegistryTls: {
                            ...config.schemaRegistryTls,
                            certPath: e.target.value,
                          },
                        })
                      }
                      placeholder={tr('srTlsPathPh')}
                    />
                    <button
                      type="button"
                      onClick={async () => {
                        const p = await window.kafdesk.pickPemFile?.()
                        if (p)
                          onUpdateConfig({
                            schemaRegistryTls: { ...config.schemaRegistryTls, certPath: p },
                          })
                      }}
                    >
                      {tr('browse')}
                    </button>
                  </div>
                </div>
                <div className="field">
                  <label>{tr('srTlsKey')}</label>
                  <div className="saved-queries-row">
                    <input
                      value={config.schemaRegistryTls?.keyPath || ''}
                      onChange={(e) =>
                        onUpdateConfig({
                          schemaRegistryTls: {
                            ...config.schemaRegistryTls,
                            keyPath: e.target.value,
                          },
                        })
                      }
                      placeholder={tr('srTlsPathPh')}
                    />
                    <button
                      type="button"
                      onClick={async () => {
                        const p = await window.kafdesk.pickPemFile?.()
                        if (p)
                          onUpdateConfig({
                            schemaRegistryTls: { ...config.schemaRegistryTls, keyPath: p },
                          })
                      }}
                    >
                      {tr('browse')}
                    </button>
                  </div>
                </div>
                <div className="row">
                  <label className="status">
                    <input
                      type="checkbox"
                      checked={config.schemaRegistryTls?.rejectUnauthorized === false}
                      onChange={(e) =>
                        onUpdateConfig({
                          schemaRegistryTls: {
                            ...config.schemaRegistryTls,
                            rejectUnauthorized: !e.target.checked,
                          },
                        })
                      }
                    />
                    {tr('srTlsInsecure')}
                  </label>
                </div>
                {hasSr && (
                  <button
                    type="button"
                    className="sr-clear"
                    onClick={() =>
                      onUpdateConfig({
                        schemaRegistryUrl: '',
                        schemaRegistryAuth: undefined,
                        schemaRegistryTls: undefined,
                      })
                    }
                  >
                    {tr('srClear')}
                  </button>
                )}
              </div>
            )}
          </div>

          <div className="actions">
            <button type="button" disabled={busy} onClick={onSaveConnectionDetails}>
              {tr('save')}
            </button>
            <button
              type="button"
              className="primary icon-action"
              disabled={busy || connected}
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
            <button disabled={busy || !connected} onClick={onDisconnect}>
              {tr('disconnect')}
            </button>
            <button disabled={busy} onClick={onPing}>
              {tr('ping')}
            </button>
            <button type="button" disabled={busy} onClick={() => onDuplicateConnection(activeId)}>
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
