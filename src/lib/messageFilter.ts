import type { KafkaMessageView } from '../types/kafdesk'
import { timeInputToMs } from './savedQueries'
import type { SearchScope } from './searchScope'
import { uiSearchFromStored, wireSearch } from './searchScope'

export interface ClientMessageFilter {
  search?: string
  searchKey?: string
  searchScope?: SearchScope
  headerKey?: string
  headerValue?: string
  /** datetime-local string — filtered on the loaded window only */
  timeFrom?: string
  timeTo?: string
}

function messageTsMs(raw: string): number | null {
  const n = Number(raw)
  if (!Number.isFinite(n)) return null
  return n < 1e12 ? n * 1000 : n
}

function valueHaystack(m: KafkaMessageView): string {
  return `${m.rawPreview} ${JSON.stringify(m.value)}`.toLowerCase()
}

/** Client-side filter over an already loaded consume window (not a broker re-fetch). */
export function messageMatchesClientFilter(
  m: KafkaMessageView,
  f: ClientMessageFilter,
): boolean {
  const { text, scope } = uiSearchFromStored(f.search ?? '', f.searchKey ?? '', f.searchScope)
  const needle = text.trim().toLowerCase()
  const headerKey = f.headerKey?.trim().toLowerCase()
  const headerValue = f.headerValue?.trim().toLowerCase()

  if (needle) {
    const keyText = (m.key || '').toLowerCase()
    const ok =
      scope === 'key'
        ? keyText.includes(needle)
        : scope === 'value'
          ? valueHaystack(m).includes(needle)
          : `${keyText} ${valueHaystack(m)}`.includes(needle)
    if (!ok) return false
  }

  if (headerKey || headerValue) {
    const entries = Object.entries(m.headers || {})
    const ok = entries.some(([k, v]) => {
      const lk = k.toLowerCase()
      const lv = String(v).toLowerCase()
      if (headerKey && !lk.includes(headerKey)) return false
      if (headerValue && !lv.includes(headerValue)) return false
      return true
    })
    if (!ok) return false
  }

  const fromMs = f.timeFrom ? timeInputToMs(f.timeFrom) : undefined
  const toMs = f.timeTo ? timeInputToMs(f.timeTo) : undefined
  if (fromMs != null || toMs != null) {
    const ts = messageTsMs(m.timestamp)
    if (ts == null) return false
    if (fromMs != null && ts < fromMs) return false
    if (toMs != null && ts > toMs) return false
  }

  return true
}

export function filterMessages(
  messages: KafkaMessageView[],
  f: ClientMessageFilter,
): KafkaMessageView[] {
  const { text } = uiSearchFromStored(f.search ?? '', f.searchKey ?? '', f.searchScope)
  const active =
    Boolean(text.trim()) ||
    Boolean(f.headerKey?.trim()) ||
    Boolean(f.headerValue?.trim()) ||
    Boolean(f.timeFrom?.trim()) ||
    Boolean(f.timeTo?.trim())
  if (!active) return messages
  return messages.filter((m) => messageMatchesClientFilter(m, f))
}

export function clientFilterActive(f: ClientMessageFilter): boolean {
  const { text } = uiSearchFromStored(f.search ?? '', f.searchKey ?? '', f.searchScope)
  return (
    Boolean(text.trim()) ||
    Boolean(f.headerKey?.trim()) ||
    Boolean(f.headerValue?.trim()) ||
    Boolean(f.timeFrom?.trim()) ||
    Boolean(f.timeTo?.trim())
  )
}

/** Expand filter state into IPC search fields. */
export function wireClientSearch(f: {
  search: string
  searchKey: string
  searchScope: SearchScope
}) {
  const { text, scope } = uiSearchFromStored(f.search, f.searchKey, f.searchScope)
  return wireSearch(text, scope)
}
