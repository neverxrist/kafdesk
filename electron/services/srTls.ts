import fs from 'node:fs'
import https from 'node:https'
import type { ClusterConfig } from '../types'

/** Build Node https.Agent for Schema Registry (corporate CA / mTLS / lab skip-verify). */
export function buildSchemaRegistryAgent(config: ClusterConfig | null | undefined): https.Agent | undefined {
  const tls = config?.schemaRegistryTls
  if (!tls) return undefined

  const read = (p?: string) => {
    const path = p?.trim()
    if (!path) return undefined
    try {
      return fs.readFileSync(path)
    } catch {
      throw new Error(`Cannot read TLS file: ${path}`)
    }
  }

  const ca = read(tls.caPath)
  const cert = read(tls.certPath)
  const key = read(tls.keyPath)
  const rejectUnauthorized = tls.rejectUnauthorized !== false

  if (!ca && !cert && !key && rejectUnauthorized) return undefined

  return new https.Agent({
    ca,
    cert,
    key,
    rejectUnauthorized,
  })
}
