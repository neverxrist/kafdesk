import type { AuthSessionPublic, ClusterConfig, SchemaRegistryStatus } from '../../electron/types'
import type { Theme } from '../lib/connections'
import type { useLocale } from '../hooks/useLocale'

type Tr = ReturnType<typeof useLocale>['tr']

export default function TopBar({
  tr,
  theme,
  toggleTheme,
  toggleLocale,
  setSidebarOpen,
  onOpenSettings,
  settingsActive,
  config,
  connected,
  srStatus,
  authSession,
}: {
  tr: Tr
  theme: Theme
  toggleTheme: () => void
  toggleLocale: () => void
  setSidebarOpen: (v: boolean | ((prev: boolean) => boolean)) => void
  onOpenSettings: () => void
  settingsActive?: boolean
  config: ClusterConfig
  connected: boolean
  srStatus: SchemaRegistryStatus
  authSession: AuthSessionPublic
}) {
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
      <div className="topbar-conn" onClick={() => setSidebarOpen(true)} role="button">
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
