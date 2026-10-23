import type { KafkaMessageView } from '../types/kafdesk'
import { timeInputToMs } from './savedQueries'
import type { SearchClause, SearchScope } from './searchScope'
import { searchClauseActive, uiSearchFromStored, wireSearch } from './searchScope'

export interface ClientMessageFilter {
  search?: string
  searchKey?: string
  searchScope?: SearchScope
  /** Optional second AND clause (UI-only; not saved in queries). */
  search2?: string
  searchScope2?: SearchScope
  dualSearch?: boolean
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

function matchesClause(m: KafkaMessageView, text: string, scope: SearchScope): boolean {
  const needle = text.trim().toLowerCase()
  if (!needle) return true
  const keyText = (m.key || '').toLowerCase()
  if (scope === 'key') return keyText.includes(needle)
  if (scope === 'value') return valueHaystack(m).includes(needle)
  return `${keyText} ${valueHaystack(m)}`.includes(needle)
}

/** Active AND clauses from filter state (empty text = no-op). */
export function clientSearchClauses(f: ClientMessageFilter): SearchClause[] {
  const primary = uiSearchFromStored(f.search ?? '', f.searchKey ?? '', f.searchScope)
  const clauses: SearchClause[] = [{ text: primary.text, scope: primary.scope }]
  if (f.dualSearch) {
    const scope2 =
      f.searchScope2 === 'key' || f.searchScope2 === 'value' || f.searchScope2 === 'both'
        ? f.searchScope2
        : 'both'
    clauses.push({ text: f.search2 ?? '', scope: scope2 })
  }
  return clauses
}

/** Client-side filter over an already loaded consume window (not a broker re-fetch). */
export function messageMatchesClientFilter(
  m: KafkaMessageView,
  f: ClientMessageFilter,
): boolean {
  for (const c of clientSearchClauses(f)) {
    if (!searchClauseActive(c)) continue
    if (!matchesClause(m, c.text, c.scope)) return false
  }

  const headerKey = f.headerKey?.trim().toLowerCase()
  const headerValue = f.headerValue?.trim().toLowerCase()

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

function clientContentFilterActive(f: ClientMessageFilter): boolean {
  return clientSearchClauses(f).some(searchClauseActive)
}

export function filterMessages(
  messages: KafkaMessageView[],
  f: ClientMessageFilter,
): KafkaMessageView[] {
  const active =
    clientContentFilterActive(f) ||
    Boolean(f.headerKey?.trim()) ||
    Boolean(f.headerValue?.trim()) ||
    Boolean(f.timeFrom?.trim()) ||
    Boolean(f.timeTo?.trim())
  if (!active) return messages
  return messages.filter((m) => messageMatchesClientFilter(m, f))
}

export function clientFilterActive(f: ClientMessageFilter): boolean {
  return (
    clientContentFilterActive(f) ||
    Boolean(f.headerKey?.trim()) ||
    Boolean(f.headerValue?.trim()) ||
    Boolean(f.timeFrom?.trim()) ||
    Boolean(f.timeTo?.trim())
  )
}

/** Expand filter state into IPC search fields (first clause only). */
export function wireClientSearch(f: {
  search: string
  searchKey: string
  searchScope: SearchScope
}) {
  const { text, scope } = uiSearchFromStored(f.search, f.searchKey, f.searchScope)
  return wireSearch(text, scope)
}
