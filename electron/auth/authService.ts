import fs from 'node:fs'
import path from 'node:path'
import { app } from 'electron'
import type {
  AuthLoginRequest,
  AuthProviderInfo,
  AuthSessionPublic,
  KeycloakSsoConfig,
  OidcSsoConfig,
  StoredCredentialsInfo,
} from '../types'
import {
  clearCredentials,
  getStoredCredentialsInfo,
  getStoredPassword,
  normalizeLogin,
  saveCredentials,
} from './credentialStore'
import { keycloakProvider } from './keycloakProvider'
import type { AuthProvider, AuthSession } from './types'

/** Extension slot: register more providers here later (Azure AD, Okta, …) */
const providers: AuthProvider[] = [
  keycloakProvider,
  {
    id: 'oidc',
    label: 'Generic OIDC',
    implemented: false,
    description: 'Скоро — любой OIDC issuer',
    async login() {
      throw new Error('Generic OIDC ещё не реализован — используй Keycloak')
    },
  },
]

class AuthService {
  /** Session lives in memory only — tokens are never written to disk */
  private session: AuthSession | null = null
  private lastLogin?: Omit<AuthLoginRequest, 'password'>

  private storePath(): string {
    return path.join(app.getPath('userData'), 'auth-session.json')
  }

  listProviders(): AuthProviderInfo[] {
    return providers.map((p) => ({
      id: p.id,
      label: p.label,
      implemented: p.implemented,
      description: p.description,
    }))
  }

  getPublicSession(): AuthSessionPublic {
    if (!this.session) {
      return { providerId: 'keycloak', loggedIn: false }
    }
    return {
      providerId: this.session.providerId,
      username: this.session.username,
      email: this.session.email,
      expiresAt: this.session.expiresAt,
      loggedIn: true,
    }
  }

  storedCredentials(): StoredCredentialsInfo {
    return getStoredCredentialsInfo()
  }

  forgetCredentials(): void {
    clearCredentials()
  }

  /** Only non-secret login context survives restarts (never tokens/password) */
  loadFromDisk(): void {
    try {
      const raw = fs.readFileSync(this.storePath(), 'utf8')
      const parsed = JSON.parse(raw) as { lastLogin?: Omit<AuthLoginRequest, 'password'> }
      this.lastLogin = parsed.lastLogin
    } catch {
      /* ignore */
    }
    this.session = null
  }

  private persistContext(): void {
    try {
      fs.writeFileSync(
        this.storePath(),
        JSON.stringify({ lastLogin: this.lastLogin }, null, 2),
        'utf8',
      )
    } catch (err) {
      console.error('Failed to persist auth context', err)
    }
  }

  async login(request: AuthLoginRequest): Promise<AuthSessionPublic> {
    const provider = providers.find((p) => p.id === request.providerId)
    if (!provider) throw new Error(`Unknown auth provider: ${request.providerId}`)
    if (!provider.implemented) {
      throw new Error(`Provider «${provider.label}» ещё не подключён`)
    }

    let password = request.password
    const username = request.username ? normalizeLogin(request.username) : undefined
    if (request.method === 'password' && !password && username) {
      password = getStoredPassword(username) ?? undefined
    }

    this.session = await provider.login({
      keycloak: request.keycloak,
      oidc: request.oidc,
      method: request.method,
      username,
      password,
    })

    if (request.method === 'password' && username && password) {
      if (request.rememberPassword) {
        try {
          saveCredentials(username, password)
        } catch (err) {
          console.error('Failed to save credentials', err)
        }
      } else {
        clearCredentials()
      }
    }

    const { password: _pw, ...context } = request
    this.lastLogin = { ...context, username }
    this.persistContext()
    return this.getPublicSession()
  }

  async logout(): Promise<void> {
    this.session = null
    this.lastLogin = undefined
    try {
      fs.unlinkSync(this.storePath())
    } catch {
      /* ignore */
    }
  }

  /** Silent re-login with the stored password when refresh is impossible */
  private async relogin(): Promise<boolean> {
    const cfg = this.lastLogin
    if (!cfg?.keycloak) return false
    const info = getStoredCredentialsInfo()
    if (!info.username) return false
    const password = getStoredPassword(info.username)
    if (!password) return false
    try {
      await this.login({
        providerId: 'keycloak',
        method: 'password',
        keycloak: cfg.keycloak,
        username: info.username,
        password,
        rememberPassword: true,
      })
      return true
    } catch (err) {
      console.error('Silent re-login failed', err)
      return false
    }
  }

  /** Access token for Kafka OAUTHBEARER / SR Bearer, refreshes if needed */
  async getAccessToken(sso?: {
    keycloak?: KeycloakSsoConfig
    oidc?: OidcSsoConfig
  }): Promise<string | null> {
    if (!this.session?.accessToken) {
      // No live session (e.g. after restart) — try stored credentials once
      if (await this.relogin()) return this.session?.accessToken ?? null
      return null
    }

    const skew = 30_000
    const expired = this.session.expiresAt != null && Date.now() > this.session.expiresAt - skew

    if (expired) {
      const provider = providers.find((p) => p.id === this.session!.providerId)
      const cfg = sso || this.lastLogin
      if (this.session.refreshToken && provider?.refresh && cfg) {
        try {
          this.session = await provider.refresh(this.session, {
            keycloak: cfg.keycloak,
            oidc: cfg.oidc,
          })
          return this.session.accessToken
        } catch (err) {
          console.error('Token refresh failed', err)
        }
      }
      // Refresh impossible/failed — silent re-login from keychain password
      if (await this.relogin()) return this.session?.accessToken ?? null
      return null
    }

    return this.session.accessToken
  }
}

export const authService = new AuthService()
