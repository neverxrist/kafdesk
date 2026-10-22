import type { ClusterConfig } from '../types/kafdesk'

export const CONNECTIONS_KEY = 'kafdesk.connections.v1'
export const LEGACY_KEY = 'kafdesk.connection.v1'
export const THEME_KEY = 'kafdesk.theme'
export const SIDEBAR_KEY = 'kafdesk.sidebarOpen'

export type Theme = 'dark' | 'light'
export type AuthMethodUi = 'none' | 'sasl'
/** librdkafka security.protocol — drives both TLS and SASL in the form. */
export type SecurityProtocolUi = 'plaintext' | 'ssl' | 'sasl_plaintext' | 'sasl_ssl'

export function authMethodFromConfig(c: ClusterConfig): AuthMethodUi {
  if (c.authMode === 'sasl') return 'sasl'
  if (c.authMode === 'none' || c.authMode === 'oauthbearer') return 'none'
  // Legacy saves without authMode — same inference as Rust build_client_config
  if (c.sasl?.username?.trim()) return 'sasl'
  return 'none'
}

export function securityProtocolFromConfig(c: ClusterConfig): SecurityProtocolUi {
  const sasl = authMethodFromConfig(c) === 'sasl'
  const ssl = Boolean(c.ssl)
  if (sasl && ssl) return 'sasl_ssl'
  if (sasl) return 'sasl_plaintext'
  if (ssl) return 'ssl'
  return 'plaintext'
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
