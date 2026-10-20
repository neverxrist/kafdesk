import { useEffect, useState, type RefObject } from 'react'
import type { AuthSessionPublic, ClusterConfig, SchemaRegistryStatus } from '../../electron/types'
import type { AuthMethodUi } from '../lib/connections'
import type { useLocale } from '../hooks/useLocale'

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
  authSession,
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
  onSsoLogin,
  onSsoPasswordLogin,
  onSsoForgetCredentials,
  storedCreds,
  onSsoLogout,
  onSaveConnectionDetails,
  onConnect,
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
  authSession: AuthSessionPublic
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
  onSsoLogin: () => void
  onSsoPasswordLogin: (username: string, password: string, remember: boolean) => void
  onSsoForgetCredentials: () => void
  storedCreds: { username?: string; hasPassword: boolean }
  onSsoLogout: () => void
  onSaveConnectionDetails: () => void
  onConnect: () => void
  onDisconnect: () => void
  onPing: () => void
}) {
  const hasSr = Boolean(config.schemaRegistryUrl?.trim())
  const [srOpen, setSrOpen] = useState(false)
  const [ssoUsername, setSsoUsername] = useState('')
  const [ssoPassword, setSsoPassword] = useState('')
  const [rememberPw, setRememberPw] = useState(true)

  useEffect(() => {
    if (storedCreds.username && !ssoUsername) setSsoUsername(storedCreds.username)
  }, [storedCreds.username]) // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    // Keep panel closed when switching connections; reopen only on demand
    setSrOpen(false)
  }, [config.id])

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
          {connections.map((c) => (
            <div key={c.id} className={`conn-item ${c.id === activeId ? 'active' : ''}`}>
              <button type="button" className="conn-select" onClick={() => onSelectConnection(c.id)}>
                <strong>{c.name}</strong>
                <small>{c.brokers.join(', ') || '—'}</small>
                {c.schemaRegistryUrl ? <small className="sr-hint">SR</small> : null}
              </button>
              <div className="conn-actions">
                <button type="button" onClick={() => onEditConnection(c.id)} title={tr('edit')}>
                  {tr('edit')}
                </button>
                <button type="button" onClick={() => onDuplicateConnection(c.id)} title={tr('dup')}>
                  {tr('dup')}
                </button>
                <button
                  type="button"
                  className="danger-text"
                  onClick={() => onRequestDeleteConnection(c.id)}
                  title={tr('del')}
                >
                  {tr('del')}
                </button>
              </div>
            </div>
          ))}
        </div>

        <div className="panel" ref={detailsRef}>
          <h2>{tr('editTitle', { name: config.name })}</h2>
          <p className="hint">{tr('editHint')}</p>
          <div className="field">
            <label>{tr('name')}</label>
            <input value={config.name} onChange={(e) => onUpdateConfig({ name: e.target.value })} />
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
                <option value="keycloak">{tr('authKeycloak')}</option>
                <option value="oidc">{tr('authOidc')}</option>
              </select>
              {(authMethod === 'keycloak' || authMethod === 'oidc') && (
                <span
                  className="warn-tri"
                  title={authMethod === 'oidc' ? tr('authOidcWarn') : tr('authKeycloakWarn')}
                >
                  ⚠
                </span>
              )}
            </div>
          </div>

          <div className="field">
            <label>{tr('brokers')}</label>
            <input value={brokersText} onChange={(e) => setBrokersText(e.target.value)} />
          </div>
          <div className="row">
            <label className="status">
              <input
                type="checkbox"
                checked={Boolean(config.ssl)}
                onChange={(e) => onUpdateConfig({ ssl: e.target.checked })}
              />
              {tr('ssl')}
            </label>
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
              <div className="field">
                <label>{tr('username')}</label>
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
                />
              </div>
              <div className="field">
                <label>{tr('password')}</label>
                <input
                  type="password"
                  value={config.sasl?.password || ''}
                  onChange={(e) =>
                    onUpdateConfig({
                      sasl: {
                        mechanism: config.sasl?.mechanism || 'plain',
                        username: config.sasl?.username || '',
                        password: e.target.value,
                      },
                    })
                  }
                  autoComplete="current-password"
                />
              </div>
            </>
          )}

          {authMethod === 'keycloak' && (
            <>
              <p className="hint">{tr('clientIdHint')}</p>
              <div className="field">
                <label>{tr('keycloakUrl')}</label>
                <input
                  value={config.sso?.keycloak?.url || ''}
                  onChange={(e) =>
                    onUpdateConfig({
                      sso: {
                        providerId: 'keycloak',
                        keycloak: {
                          url: e.target.value,
                          realm: config.sso?.keycloak?.realm || '',
                          clientId: config.sso?.keycloak?.clientId || '',
                        },
                      },
                    })
                  }
                  placeholder="https://sso.example.com"
                />
              </div>
              <div className="field">
                <label>{tr('realm')}</label>
                <input
                  value={config.sso?.keycloak?.realm || ''}
                  onChange={(e) =>
                    onUpdateConfig({
                      sso: {
                        providerId: 'keycloak',
                        keycloak: {
                          url: config.sso?.keycloak?.url || '',
                          realm: e.target.value,
                          clientId: config.sso?.keycloak?.clientId || '',
                        },
                      },
                    })
                  }
                  placeholder="realm"
                />
              </div>
              <div className="field">
                <label>{tr('clientId')}</label>
                <input
                  value={config.sso?.keycloak?.clientId || ''}
                  onChange={(e) =>
                    onUpdateConfig({
                      sso: {
                        providerId: 'keycloak',
                        keycloak: {
                          url: config.sso?.keycloak?.url || '',
                          realm: config.sso?.keycloak?.realm || '',
                          clientId: e.target.value,
                        },
                      },
                    })
                  }
                  placeholder="my-app"
                />
              </div>
              <div className="field">
                <label>{tr('ssoUsername')}</label>
                <input
                  value={ssoUsername}
                  onChange={(e) => setSsoUsername(e.target.value)}
                  autoComplete="username"
                  placeholder="login"
                />
              </div>
              <div className="field">
                <label>{tr('ssoPassword')}</label>
                <input
                  type="password"
                  value={ssoPassword}
                  onChange={(e) => setSsoPassword(e.target.value)}
                  autoComplete="current-password"
                  placeholder={storedCreds.hasPassword ? tr('ssoPasswordSaved') : ''}
                />
              </div>
              <label className="settings-toggle" style={{ marginBottom: 8 }}>
                <input
                  type="checkbox"
                  checked={rememberPw}
                  onChange={(e) => setRememberPw(e.target.checked)}
                />
                <span>{tr('rememberPassword')}</span>
              </label>
              <div className="actions">
                {!authSession.loggedIn ? (
                  <>
                    <button
                      type="button"
                      className="primary"
                      disabled={busy || !ssoUsername.trim() || (!ssoPassword && !storedCreds.hasPassword)}
                      onClick={() => onSsoPasswordLogin(ssoUsername, ssoPassword, rememberPw)}
                    >
                      {tr('loginWithPassword')}
                    </button>
                    <button type="button" disabled={busy} onClick={onSsoLogin}>
                      {tr('loginBrowser')}
                    </button>
                  </>
                ) : (
                  <button type="button" disabled={busy} onClick={onSsoLogout}>
                    {tr('logoutSso')}
                  </button>
                )}
                {storedCreds.hasPassword && (
                  <button type="button" disabled={busy} onClick={onSsoForgetCredentials}>
                    {tr('forgetPassword')}
                  </button>
                )}
              </div>
              <div className="status status-block">
                <span className={`dot ${authSession.loggedIn ? 'on' : ''}`} />
                {authSession.loggedIn
                  ? tr('ssoOk', {
                      user: authSession.username || authSession.email || 'ok',
                    })
                  : tr('ssoOffline')}
              </div>
            </>
          )}

          {authMethod === 'oidc' && (
            <div className="hint">
              <span className="warn-tri" title={tr('authOidcWarn')}>
                ⚠
              </span>{' '}
              {tr('authOidcWarn')}
            </div>
          )}

          <div className="sr-fold">
            <button
              type="button"
              className={`sr-fold-toggle ${hasSr ? 'has-sr' : ''}`}
              onClick={() => setSrOpen((v) => !v)}
              aria-expanded={srOpen}
            >
              <span>{tr('srToggle')}</span>
              <span className="sr-fold-meta">
                {hasSr ? tr('srToggleOn') : tr('srToggleOff')}
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
                    placeholder="http://localhost:18081"
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
                  <input
                    type="password"
                    value={config.schemaRegistryAuth?.password || ''}
                    onChange={(e) =>
                      onUpdateConfig({
                        schemaRegistryAuth: {
                          username: config.schemaRegistryAuth?.username || '',
                          password: e.target.value,
                        },
                      })
                    }
                  />
                </div>
                {hasSr && (
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

          <div className="actions">
            <button type="button" disabled={busy} onClick={onSaveConnectionDetails}>
              {tr('save')}
            </button>
            {!connected ? (
              <button className="primary" disabled={busy} onClick={onConnect}>
                {tr('connect')}
              </button>
            ) : (
              <button disabled={busy} onClick={onDisconnect}>
                {tr('disconnect')}
              </button>
            )}
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
