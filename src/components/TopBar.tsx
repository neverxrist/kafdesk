import { useEffect, useRef, useState } from 'react'
import type { AuthSessionPublic, ClusterConfig, SchemaRegistryStatus } from '../../electron/types'
import type { Theme } from '../lib/connections'
import type { useLocale } from '../hooks/useLocale'
import { ConnectIcon, ReconnectIcon } from './ConnActionIcons'

type Tr = ReturnType<typeof useLocale>['tr']

export default function TopBar({
  tr,
  theme,
  toggleTheme,
  toggleLocale,
  setSidebarOpen,
  onOpenSettings,
  settingsActive,
  connections,
  activeId,
  onSelectConnection,
  config,
  connected,
  srStatus,
  authSession,
  busy,
  onConnect,
  onReconnect,
}: {
  tr: Tr
  theme: Theme
  toggleTheme: () => void
  toggleLocale: () => void
  setSidebarOpen: (v: boolean | ((prev: boolean) => boolean)) => void
  onOpenSettings: () => void
  settingsActive?: boolean
  connections: ClusterConfig[]
  activeId: string
  onSelectConnection: (id: string) => void
  config: ClusterConfig
  connected: boolean
  srStatus: SchemaRegistryStatus
  authSession: AuthSessionPublic
  busy?: boolean
  onConnect: () => void
  onReconnect: () => void
}) {
  const [open, setOpen] = useState(false)
  const wrapRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (!open) return
    function onDoc(e: MouseEvent) {
      if (!wrapRef.current?.contains(e.target as Node)) setOpen(false)
    }
    function onKey(e: KeyboardEvent) {
      if (e.key === 'Escape') setOpen(false)
    }
    document.addEventListener('mousedown', onDoc)
    window.addEventListener('keydown', onKey)
    return () => {
      document.removeEventListener('mousedown', onDoc)
      window.removeEventListener('keydown', onKey)
    }
  }, [open])

  return (
    <div className="topbar">
      <button
        type="button"
        className="conn-toggle"
        onClick={() => setSidebarOpen((v) => !v)}
        title={tr('connections')}
      >
        <span className="conn-toggle-bars" aria-hidden>
          ☰
        </span>
        {tr('connections')}
      </button>

      <div className={`topbar-conn-wrap ${open ? 'open' : ''}`} ref={wrapRef}>
        <button
          type="button"
          className="topbar-conn"
          aria-expanded={open}
          aria-haspopup="listbox"
          disabled={busy}
          onClick={() => setOpen((v) => !v)}
        >
          <span className={`dot ${connected ? 'on' : ''}`} />
          <div className="topbar-conn-text">
            <strong>{config.name}</strong>
            <small>
              {connected
                ? `${tr('online')} · ${srStatus === 'ok' ? tr('srOk') : srStatus === 'fail' ? tr('srFail') : tr('srNone')}`
                : tr('offline')}
              {authSession.loggedIn ? ` · SSO ${authSession.username || 'ok'}` : ''}
              {config.brokers[0] ? ` · ${config.brokers[0]}` : ''}
            </small>
          </div>
          <span className="topbar-conn-chevron" aria-hidden>
            {open ? '▴' : '▾'}
          </span>
        </button>

        {open && (
          <div className="topbar-conn-menu" role="listbox" aria-label={tr('connections')}>
            {connections.map((c) => {
              const active = c.id === activeId
              return (
                <button
                  key={c.id}
                  type="button"
                  role="option"
                  aria-selected={active}
                  className={`topbar-conn-option ${active ? 'active' : ''}`}
                  disabled={busy}
                  onClick={() => {
                    setOpen(false)
                    if (!active) void onSelectConnection(c.id)
                  }}
                >
                  <span className="topbar-conn-option-name">{c.name}</span>
                  <small>{c.brokers[0] || '—'}</small>
                </button>
              )
            })}
            <button
              type="button"
              className="topbar-conn-manage"
              onClick={() => {
                setOpen(false)
                setSidebarOpen(true)
              }}
            >
              {tr('manageConnections')}
            </button>
          </div>
        )}
      </div>

      <div className="topbar-conn-btns">
        <button
          type="button"
          className="topbar-conn-btn icon"
          disabled={busy || connected}
          onClick={onConnect}
          title={tr('connect')}
          aria-label={tr('connect')}
        >
          <ConnectIcon />
        </button>
        <button
          type="button"
          className="topbar-conn-btn icon"
          disabled={busy || !connected}
          onClick={onReconnect}
          title={tr('reconnect')}
          aria-label={tr('reconnect')}
        >
          <ReconnectIcon />
        </button>
      </div>

      <div className="topbar-controls">
        <button
          className={`icon-btn settings-btn ${settingsActive ? 'active' : ''}`}
          onClick={onOpenSettings}
          type="button"
          title={tr('settings')}
          aria-label={tr('settings')}
        >
          ⚙ {tr('settings')}
        </button>
        <button className="icon-btn" onClick={toggleLocale} type="button" title="Language">
          {tr('langSwitch')}
        </button>
        <button className="icon-btn" onClick={toggleTheme} type="button" title="Theme">
          {theme === 'dark' ? tr('themeLight') : tr('themeDark')}
        </button>
      </div>
    </div>
  )
}
