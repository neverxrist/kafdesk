export const DEFAULT_BROKER_PORT = '9092'

/** Split `host:port` / `[ipv6]:port` / bare host. */
export function splitBrokerEndpoint(entry: string): { host: string; port: string | null } {
  const s = entry.trim()
  if (!s) return { host: '', port: null }

  if (s.startsWith('[')) {
    const m = /^\[([^\]]+)\](?::(\d+))?$/.exec(s)
    if (m) return { host: m[1], port: m[2] ?? null }
  }

  const idx = s.lastIndexOf(':')
  if (idx > 0 && /^\d+$/.test(s.slice(idx + 1))) {
    return { host: s.slice(0, idx), port: s.slice(idx + 1) }
  }
  return { host: s, port: null }
}

/** UI fields from stored `brokers: ["host:port", ...]`. */
export function hostPortFromBrokers(brokers: string[]): { hosts: string; port: string } {
  if (!brokers.length) return { hosts: '', port: DEFAULT_BROKER_PORT }

  const parsed = brokers.map(splitBrokerEndpoint).filter((p) => p.host)
  if (!parsed.length) return { hosts: '', port: DEFAULT_BROKER_PORT }

  const ports = parsed.map((p) => p.port).filter((p): p is string => Boolean(p))
  const unique = [...new Set(ports)]

  if (unique.length > 1) {
    // Rare mixed ports — keep full endpoints in the host field
    return { hosts: brokers.join(','), port: DEFAULT_BROKER_PORT }
  }

  return {
    hosts: parsed.map((p) => p.host).join(','),
    port: unique[0] ?? DEFAULT_BROKER_PORT,
  }
}

/** Build bootstrap list from separate host + port fields. */
export function brokersFromHostPort(hostsText: string, portText: string): string[] {
  const digits = portText.trim().replace(/\D/g, '')
  const fallback = digits || DEFAULT_BROKER_PORT

  return hostsText
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean)
    .map((entry) => {
      const { host, port } = splitBrokerEndpoint(entry)
      if (!host) return ''
      // Bare host → shared port field; host that already has :port keeps it
      return `${host}:${port ?? fallback}`
    })
    .filter(Boolean)
}
