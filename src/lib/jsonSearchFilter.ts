import type { KafkaMessageView } from '../types/kafdesk'
import { messageBodyText } from './messageText'

/** Strip one layer of wrapping quotes — users often paste `"value"` from JSON. */
function unwrapQuoted(q: string): string {
  const t = q.trim()
  if (t.length < 2) return t
  const a = t[0]
  const b = t[t.length - 1]
  if ((a === '"' && b === '"') || (a === "'" && b === "'")) {
    return t.slice(1, -1).trim()
  }
  return t
}

function needlesOf(query: string | string[]): string[] {
  const list = Array.isArray(query) ? query : [query]
  return list.map((q) => unwrapQuoted(q).toLowerCase()).filter(Boolean)
}

function matchesAny(text: string, needles: string[]): boolean {
  const t = text.toLowerCase()
  return needles.some((q) => t.includes(q))
}

/**
 * Keep only JSON fields where the key or a nested value matches any needle
 * (case-insensitive). When a key matches, its full value is kept.
 * Returns `undefined` if nothing matched.
 */
export function filterJsonByMatch(value: unknown, query: string | string[]): unknown | undefined {
  const needles = needlesOf(query)
  if (!needles.length) return value

  if (value === null || typeof value === 'boolean' || typeof value === 'number') {
    return matchesAny(String(value), needles) ? value : undefined
  }
  if (typeof value === 'string') {
    return matchesAny(value, needles) ? value : undefined
  }
  if (Array.isArray(value)) {
    const next = value
      .map((item) => filterJsonByMatch(item, needles))
      .filter((item): item is unknown => item !== undefined)
    return next.length > 0 ? next : undefined
  }
  if (typeof value === 'object') {
    const out: Record<string, unknown> = {}
    for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
      if (matchesAny(k, needles)) {
        out[k] = v
        continue
      }
      const nested = filterJsonByMatch(v, needles)
      if (nested !== undefined) out[k] = nested
    }
    return Object.keys(out).length > 0 ? out : undefined
  }
  return undefined
}

/** Parse the on-screen body so Hits trims what the user actually sees. */
function parseBodyData(full: string, m: KafkaMessageView): unknown | undefined {
  const trimmed = full.trim()
  if (
    (trimmed.startsWith('{') && trimmed.endsWith('}')) ||
    (trimmed.startsWith('[') && trimmed.endsWith(']'))
  ) {
    try {
      return JSON.parse(trimmed)
    } catch {
      /* fall through */
    }
  }
  if (m.value != null && typeof m.value === 'object') return m.value
  return undefined
}

/** Body text for display — optionally trimmed to search hits only. */
export function messageBodyTextFiltered(
  m: KafkaMessageView,
  query: string | string[],
  matchesOnly: boolean,
): string {
  const needles = needlesOf(query)
  const full = messageBodyText(m)
  if (!matchesOnly || !needles.length) return full

  const data = parseBodyData(full, m)
  if (data === undefined) {
    return matchesAny(full, needles) ? full : ''
  }

  const filtered = filterJsonByMatch(data, needles)
  if (filtered === undefined) {
    // Key-only list match (or needle only in raw) — don't blank to `{}`.
    return matchesAny(full, needles) ? full : '{\n}'
  }
  try {
    return JSON.stringify(filtered, null, 2)
  } catch {
    return full
  }
}
