import { useEffect, useRef, useState } from 'react'
import type { AuthSessionPublic, ClusterConfig, SchemaRegistryStatus } from '../types/kafdesk'
import type { Theme } from '../lib/connections'
import type { useLocale } from '../hooks/useLocale'
import { ConnectIcon, ReconnectIcon } from './ConnActionIcons'

type Tr = ReturnType<typeof useLocale>['tr']

function SidebarPanelIcon() {
  return (
    <svg
      className="conn-toggle-icon"
      width="18"
      height="18"
      viewBox="0 0 18 18"
      fill="none"
      aria-hidden
    >
      <rect x="1.5" y="2.5" width="15" height="13" rx="2" stroke="currentColor" strokeWidth="1.4" />
      <path d="M6.5 2.5v13" stroke="currentColor" strokeWidth="1.4" />
      <rect x="3" y="4.5" width="2.2" height="9" rx="0.4" fill="currentColor" opacity="0.85" />
    </svg>
  )
}

export type AppTab = 'consume' | 'produce' | 'groups' | 'tail' | 'logs' | 'settings'

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
  tab,
  onTabChange,
  selectedTopic,
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
  tab: AppTab
  onTabChange: (tab: AppTab) => void
  selectedTopic: string | null
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
        title={tr('manageConnections')}
        aria-label={tr('manageConnections')}
      >
        <SidebarPanelIcon />
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
            <strong title={config.name}>{config.name}</strong>
            <small
              title={[
                connected
                  ? `${tr('online')} · ${srStatus === 'ok' ? tr('srOk') : srStatus === 'fail' ? tr('srFail') : tr('srNone')}`
                  : tr('offline'),
                authSession.loggedIn ? `SSO ${authSession.username || 'ok'}` : null,
                config.brokers[0] || null,
              ]
                .filter(Boolean)
                .join(' · ')}
            >
              {connected ? tr('online') : tr('offline')}
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

      <nav className="topbar-tabs" aria-label="Main">
        {(
          [
            ['consume', tr('consume')],
            ['produce', tr('produce')],
            ['tail', tr('tail')],
            ['groups', tr('groups')],
            ['logs', tr('logs')],
          ] as const
        ).map(([id, label]) => (
          <button
            key={id}
            type="button"
            className={`topbar-tab${tab === id ? ' active' : ''}`}
            onClick={() => onTabChange(id)}
          >
            {label}
          </button>
        ))}
        {selectedTopic && tab !== 'logs' && tab !== 'settings' && (
          <span className="topbar-tab-topic" title={selectedTopic}>
            {selectedTopic}
          </span>
        )}
      </nav>

      <div className="topbar-controls">
        <button
          className={`icon-btn settings-btn ${settingsActive ? 'active' : ''}`}
          onClick={onOpenSettings}
          type="button"
          title={tr('settings')}
          aria-label={tr('settings')}
        >
          ⚙
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
