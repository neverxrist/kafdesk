import type { KafkaMessageView } from '../../electron/types'
import { timeInputToMs } from './savedQueries'

export interface ClientMessageFilter {
  search?: string
  searchKey?: string
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

/** Client-side filter over an already loaded consume window (not a broker re-fetch). */
export function messageMatchesClientFilter(
  m: KafkaMessageView,
  f: ClientMessageFilter,
): boolean {
  const search = f.search?.trim().toLowerCase()
  const searchKey = f.searchKey?.trim().toLowerCase()
  const headerKey = f.headerKey?.trim().toLowerCase()
  const headerValue = f.headerValue?.trim().toLowerCase()

  const keyText = (m.key || '').toLowerCase()
  if (searchKey && !keyText.includes(searchKey)) return false

  if (search) {
    const haystack = `${m.key ?? ''} ${m.rawPreview} ${JSON.stringify(m.value)}`.toLowerCase()
    if (!haystack.includes(search)) return false
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
  const active =
    Boolean(f.search?.trim()) ||
    Boolean(f.searchKey?.trim()) ||
    Boolean(f.headerKey?.trim()) ||
    Boolean(f.headerValue?.trim()) ||
    Boolean(f.timeFrom?.trim()) ||
    Boolean(f.timeTo?.trim())
  if (!active) return messages
  return messages.filter((m) => messageMatchesClientFilter(m, f))
}

export function clientFilterActive(f: ClientMessageFilter): boolean {
  return (
    Boolean(f.search?.trim()) ||
    Boolean(f.searchKey?.trim()) ||
    Boolean(f.headerKey?.trim()) ||
    Boolean(f.headerValue?.trim()) ||
    Boolean(f.timeFrom?.trim()) ||
    Boolean(f.timeTo?.trim())
  )
}
