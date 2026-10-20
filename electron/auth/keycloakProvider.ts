import http from 'node:http'
import { shell } from 'electron'
import type { KeycloakSsoConfig } from '../types'
import { createPkcePair, randomState } from './pkce'
import type { AuthProvider, AuthSession } from './types'

function trimSlash(url: string): string {
  return url.replace(/\/$/, '')
}

function realmBase(cfg: KeycloakSsoConfig): string {
  return `${trimSlash(cfg.url)}/realms/${encodeURIComponent(cfg.realm)}`
}

function parseJwtPayload(token: string): Record<string, unknown> {
  try {
    const part = token.split('.')[1]
    if (!part) return {}
    const json = Buffer.from(part.replace(/-/g, '+').replace(/_/g, '/'), 'base64').toString('utf8')
    return JSON.parse(json) as Record<string, unknown>
  } catch {
    return {}
  }
}

function waitForCode(port: number, expectedState: string): Promise<string> {
  return new Promise((resolve, reject) => {
    const server = http.createServer((req, res) => {
      try {
        const url = new URL(req.url || '/', `http://127.0.0.1:${port}`)
        if (url.pathname !== '/callback') {
          res.writeHead(404)
          res.end('Not found')
          return
        }
        const err = url.searchParams.get('error')
        const desc = url.searchParams.get('error_description')
        if (err) {
          res.writeHead(400, { 'Content-Type': 'text/html; charset=utf-8' })
          res.end(`<h3>Login failed</h3><p>${err}: ${desc || ''}</p>`)
          server.close()
          reject(new Error(desc || err))
          return
        }
        const state = url.searchParams.get('state')
        const code = url.searchParams.get('code')
        if (!code || state !== expectedState) {
          res.writeHead(400, { 'Content-Type': 'text/html; charset=utf-8' })
          res.end('<h3>Invalid callback</h3>')
          server.close()
          reject(new Error('Invalid OAuth callback (state/code)'))
          return
        }
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' })
        res.end(
          '<h3>KafDesk</h3><p>Login OK — можно закрыть вкладку и вернуться в приложение.</p>',
        )
        server.close()
        resolve(code)
      } catch (e) {
        server.close()
        reject(e)
      }
    })

    server.listen(port, '127.0.0.1')
    server.on('error', reject)

    setTimeout(() => {
      server.close()
      reject(new Error('Keycloak login timed out (2 min)'))
    }, 120_000)
  })
}

async function findFreePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const srv = http.createServer()
    srv.listen(0, '127.0.0.1', () => {
      const addr = srv.address()
      if (!addr || typeof addr === 'string') {
        srv.close()
        reject(new Error('Cannot allocate port'))
        return
      }
      const { port } = addr
      srv.close(() => resolve(port))
    })
    srv.on('error', reject)
  })
}

async function exchangeToken(
  cfg: KeycloakSsoConfig,
  body: Record<string, string>,
): Promise<AuthSession> {
  const tokenUrl = `${realmBase(cfg)}/protocol/openid-connect/token`
  const res = await fetch(tokenUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams(body),
  })
  if (!res.ok) {
    const text = await res.text()
    throw new Error(`Token exchange failed: HTTP ${res.status} ${text}`)
  }
  const data = (await res.json()) as {
    access_token: string
    refresh_token?: string
    id_token?: string
    expires_in?: number
    token_type?: string
  }
  const payload = parseJwtPayload(data.access_token)
  const username =
    (typeof payload.preferred_username === 'string' && payload.preferred_username) ||
    (typeof payload.sub === 'string' && payload.sub) ||
    undefined
  const email = typeof payload.email === 'string' ? payload.email : undefined

  return {
    providerId: 'keycloak',
    accessToken: data.access_token,
    refreshToken: data.refresh_token,
    idToken: data.id_token,
    expiresAt: data.expires_in ? Date.now() + data.expires_in * 1000 : undefined,
    tokenType: data.token_type || 'Bearer',
    username,
    email,
  }
}

async function passwordLogin(
  cfg: KeycloakSsoConfig,
  username: string,
  password: string,
): Promise<AuthSession> {
  const base = {
    grant_type: 'password',
    client_id: cfg.clientId,
    username,
    password,
  }
  try {
    return await exchangeToken(cfg, {
      ...base,
      scope: 'openid profile email offline_access',
    })
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err)
    // Some clients reject offline_access — retry with the plain scope set
    if (msg.includes('invalid_scope')) {
      return exchangeToken(cfg, { ...base, scope: 'openid profile email' })
    }
    throw err
  }
}

export const keycloakProvider: AuthProvider = {
  id: 'keycloak',
  label: 'Keycloak',
  implemented: true,
  description: 'OIDC login: password grant or browser PKCE',

  async login(input) {
    const cfg = input.keycloak
    if (!cfg?.url?.trim() || !cfg.realm?.trim() || !cfg.clientId?.trim()) {
      throw new Error('Keycloak: укажи URL, realm и clientId')
    }

    if (input.method === 'password') {
      const username = (input.username || '').trim()
      if (!username || !input.password) {
        throw new Error('Keycloak: укажи логин и пароль')
      }
      return passwordLogin(cfg, username, input.password)
    }

    const { verifier, challenge } = createPkcePair()
    const state = randomState()
    const port = await findFreePort()
    const redirectUri = `http://127.0.0.1:${port}/callback`

    const authUrl = new URL(`${realmBase(cfg)}/protocol/openid-connect/auth`)
    authUrl.searchParams.set('client_id', cfg.clientId)
    authUrl.searchParams.set('redirect_uri', redirectUri)
    authUrl.searchParams.set('response_type', 'code')
    authUrl.searchParams.set('scope', 'openid profile email')
    authUrl.searchParams.set('state', state)
    authUrl.searchParams.set('code_challenge', challenge)
    authUrl.searchParams.set('code_challenge_method', 'S256')

    const codePromise = waitForCode(port, state)
    await shell.openExternal(authUrl.toString())
    const code = await codePromise

    return exchangeToken(cfg, {
      grant_type: 'authorization_code',
      client_id: cfg.clientId,
      code,
      redirect_uri: redirectUri,
      code_verifier: verifier,
    })
  },

  async refresh(session, input) {
    const cfg = input.keycloak
    if (!cfg || !session.refreshToken) {
      throw new Error('No refresh token')
    }
    return exchangeToken(cfg, {
      grant_type: 'refresh_token',
      client_id: cfg.clientId,
      refresh_token: session.refreshToken,
    })
  },
}
