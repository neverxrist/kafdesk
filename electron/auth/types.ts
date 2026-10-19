import type { AuthProviderId, KeycloakSsoConfig, OidcSsoConfig } from '../types'

export interface AuthTokens {
  accessToken: string
  refreshToken?: string
  idToken?: string
  expiresAt?: number
  tokenType?: string
}

export interface AuthSession extends AuthTokens {
  providerId: AuthProviderId
  username?: string
  email?: string
}

export interface AuthProvider {
  id: AuthProviderId
  label: string
  implemented: boolean
  description?: string
  login(input: {
    keycloak?: KeycloakSsoConfig
    oidc?: OidcSsoConfig
  }): Promise<AuthSession>
  refresh?(session: AuthSession, input: {
    keycloak?: KeycloakSsoConfig
    oidc?: OidcSsoConfig
  }): Promise<AuthSession>
}
