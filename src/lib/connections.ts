import type { ClusterConfig } from '../../electron/types'

export const CONNECTIONS_KEY = 'kafdesk.connections.v1'
export const LEGACY_KEY = 'kafdesk.connection.v1'
export const THEME_KEY = 'kafdesk.theme'
export const SIDEBAR_KEY = 'kafdesk.sidebarOpen'

export type Theme = 'dark' | 'light'
export type AuthMethodUi = 'none' | 'sasl'

export function authMethodFromConfig(c: ClusterConfig): AuthMethodUi {
  if (c.authMode === 'sasl') return 'sasl'
  // oauthbearer / legacy SSO configs fall back to "none" in the UI
  return 'none'
}

export function defaultConnection(): ClusterConfig {
  return {
    id: crypto.randomUUID(),
    name: 'New connection',
    brokers: [],
    clientId: 'kafdesk',
    ssl: false,
    authMode: 'none',
  }
}

export function loadConnections(): { connections: ClusterConfig[]; activeId: string } {
  try {
    const raw = localStorage.getItem(CONNECTIONS_KEY)
    if (raw) {
      const parsed = JSON.parse(raw) as { connections: ClusterConfig[]; activeId: string }
      if (parsed.connections?.length) return parsed
    }
  } catch {
    /* ignore */
  }

  try {
    const legacy = localStorage.getItem(LEGACY_KEY)
    if (legacy) {
      const one = JSON.parse(legacy) as ClusterConfig
      return { connections: [one], activeId: one.id }
    }
  } catch {
    /* ignore */
  }

  const first = defaultConnection()
  return { connections: [first], activeId: first.id }
}

export function loadTheme(): Theme {
  const saved = localStorage.getItem(THEME_KEY)
  return saved === 'light' ? 'light' : 'dark'
}

export function applyTheme(theme: Theme) {
  document.documentElement.setAttribute('data-theme', theme)
  localStorage.setItem(THEME_KEY, theme)
}
