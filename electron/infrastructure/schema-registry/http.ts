import http from 'node:http'
import https from 'node:https'
import { URL } from 'node:url'
import { authService } from '../../auth/authService'
import { clusterAuthMode } from '../kafka/buildKafkaConfig'
import { buildSchemaRegistryAgent } from '../../services/srTls'
import type { ClusterConfig } from '../../types'

export async function schemaRegistryAuthHeaders(
  config: ClusterConfig | null | undefined,
): Promise<Record<string, string>> {
  const headers: Record<string, string> = {}
  if (!config) return headers

  const authMode = clusterAuthMode(config)
  if (authMode === 'oauthbearer') {
    const token = await authService.getAccessToken(config.sso)
    if (token) headers.Authorization = `Bearer ${token}`
    return headers
  }

  if (config.schemaRegistryAuth?.username) {
    const basic = Buffer.from(
      `${config.schemaRegistryAuth.username}:${config.schemaRegistryAuth.password}`,
    ).toString('base64')
    headers.Authorization = `Basic ${basic}`
  }
  return headers
}

export function schemaRegistryHttp(
  url: string,
  headers: Record<string, string>,
  config: ClusterConfig | null | undefined,
): Promise<{ status: number; body: string }> {
  const agent = buildSchemaRegistryAgent(config)
  return new Promise((resolve, reject) => {
    const u = new URL(url)
    const lib = u.protocol === 'https:' ? https : http
    const req = lib.request(
      u,
      {
        method: 'GET',
        headers,
        agent: u.protocol === 'https:' ? agent : undefined,
        timeout: 10_000,
      },
      (res) => {
        const chunks: Buffer[] = []
        res.on('data', (c) => chunks.push(c))
        res.on('end', () =>
          resolve({
            status: res.statusCode || 0,
            body: Buffer.concat(chunks).toString('utf8'),
          }),
        )
      },
    )
    req.on('error', reject)
    req.on('timeout', () => {
      req.destroy()
      reject(new Error('Schema Registry request timed out'))
    })
    req.end()
  })
}
