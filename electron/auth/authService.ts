import fs from 'node:fs'
import path from 'node:path'
import { app } from 'electron'
import type {
  AuthLoginRequest,
  AuthProviderInfo,
  AuthSessionPublic,
  KeycloakSsoConfig,
  OidcSsoConfig,
} from '../types'
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
  private session: AuthSession | null = null
  private lastLogin?: AuthLoginRequest

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

  loadFromDisk(): void {
    try {
      const raw = fs.readFileSync(this.storePath(), 'utf8')
      const parsed = JSON.parse(raw) as { session: AuthSession; lastLogin?: AuthLoginRequest }
      this.session = parsed.session
      this.lastLogin = parsed.lastLogin
    } catch {
      this.session = null
    }
  }

  private persist(): void {
    try {
      fs.writeFileSync(
        this.storePath(),
        JSON.stringify({ session: this.session, lastLogin: this.lastLogin }, null, 2),
        'utf8',
      )
    } catch (err) {
      console.error('Failed to persist auth session', err)
    }
  }

  async login(request: AuthLoginRequest): Promise<AuthSessionPublic> {
    const provider = providers.find((p) => p.id === request.providerId)
    if (!provider) throw new Error(`Unknown auth provider: ${request.providerId}`)
    if (!provider.implemented) {
      throw new Error(`Provider «${provider.label}» ещё не подключён`)
    }
    this.session = await provider.login({
      keycloak: request.keycloak,
      oidc: request.oidc,
    })
    this.lastLogin = request
    this.persist()
    return this.getPublicSession()
  }

  async logout(): Promise<void> {
    this.session = null
    try {
      fs.unlinkSync(this.storePath())
    } catch {
      /* ignore */
    }
  }

  /** Access token for Kafka OAUTHBEARER / SR Bearer, refreshes if needed */
  async getAccessToken(sso?: {
    keycloak?: KeycloakSsoConfig
    oidc?: OidcSsoConfig
  }): Promise<string | null> {
    if (!this.session?.accessToken) return null

    const skew = 30_000
    const expired =
      this.session.expiresAt != null && Date.now() > this.session.expiresAt - skew

    if (expired && this.session.refreshToken) {
      const provider = providers.find((p) => p.id === this.session!.providerId)
      const cfg = sso || this.lastLogin
      if (provider?.refresh && cfg) {
        try {
          this.session = await provider.refresh(this.session, {
            keycloak: cfg.keycloak,
            oidc: cfg.oidc,
          })
          this.persist()
        } catch (err) {
          console.error('Token refresh failed', err)
          return null
        }
      }
    }

    return this.session.accessToken
  }
}

export const authService = new AuthService()
