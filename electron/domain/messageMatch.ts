import type { KafkaMessageView } from '../types'

export interface BrokerMessageFilter {
  search?: string
  searchKey?: string
  headerKey?: string
  headerValue?: string
  timeFrom?: number
  timeTo?: number
}

function normalizeTsMs(raw: number): number {
  return raw < 1e12 ? raw * 1000 : raw
}

function headerValueText(
  v: Buffer | string | undefined | (Buffer | string)[],
): string {
  if (v == null) return ''
  if (Array.isArray(v)) {
    return v.map((x) => (Buffer.isBuffer(x) ? x.toString('utf8') : String(x))).join(',')
  }
  return Buffer.isBuffer(v) ? v.toString('utf8') : String(v)
}

/** Cheap key/header/time check before Avro/JSON decode. */
export function matchesRawPreFilter(
  message: {
    key: Buffer | null
    timestamp: string
    headers?: Record<string, Buffer | string | undefined | (Buffer | string)[]>
  },
  f: BrokerMessageFilter,
): boolean {
  const searchKey = f.searchKey?.trim().toLowerCase()
  if (searchKey) {
    const keyText = (message.key ? message.key.toString('utf8') : '').toLowerCase()
    if (!keyText.includes(searchKey)) return false
  }

  const headerKey = f.headerKey?.trim().toLowerCase()
  const headerValue = f.headerValue?.trim().toLowerCase()
  if (headerKey || headerValue) {
    const entries = Object.entries(message.headers || {})
    const ok = entries.some(([k, v]) => {
      if (v == null) return false
      const lv = headerValueText(v).toLowerCase()
      if (headerKey && !k.toLowerCase().includes(headerKey)) return false
      if (headerValue && !lv.includes(headerValue)) return false
      return true
    })
    if (!ok) return false
  }

  if (f.timeFrom != null || f.timeTo != null) {
    const raw = Number(message.timestamp)
    if (!Number.isFinite(raw)) return false
    const ts = normalizeTsMs(raw)
    if (f.timeFrom != null && ts < f.timeFrom) return false
    if (f.timeTo != null && ts > f.timeTo) return false
  }

  return true
}

export function matchesDecodedMessage(
  view: KafkaMessageView,
  filter: string | BrokerMessageFilter,
  maybeKey?: string,
): boolean {
  const f: BrokerMessageFilter =
    typeof filter === 'string' ? { search: filter, searchKey: maybeKey } : filter
  const search = f.search?.trim().toLowerCase()
  const searchKey = f.searchKey?.trim().toLowerCase()
  const headerKey = f.headerKey?.trim().toLowerCase()
  const headerValue = f.headerValue?.trim().toLowerCase()

  const keyText = (view.key || '').toLowerCase()
  const haystack = `${view.key ?? ''} ${view.rawPreview} ${JSON.stringify(view.value)}`.toLowerCase()
  if (searchKey && !keyText.includes(searchKey)) return false
  if (search && !haystack.includes(search)) return false

  if (headerKey || headerValue) {
    const entries = Object.entries(view.headers || {})
    const ok = entries.some(([k, v]) => {
      const lk = k.toLowerCase()
      const lv = String(v).toLowerCase()
      if (headerKey && !lk.includes(headerKey)) return false
      if (headerValue && !lv.includes(headerValue)) return false
      return true
    })
    if (!ok) return false
  }

  if (f.timeFrom != null || f.timeTo != null) {
    const tsRaw = Number(view.timestamp)
    if (!Number.isFinite(tsRaw)) return false
    const ts = normalizeTsMs(tsRaw)
    if (f.timeFrom != null && ts < f.timeFrom) return false
    if (f.timeTo != null && ts > f.timeTo) return false
  }

  return true
}

export function sortMessagesByTime(
  messages: KafkaMessageView[],
  newestFirst: boolean,
): KafkaMessageView[] {
  return [...messages].sort((a, b) => {
    const ta = Number(a.timestamp)
    const tb = Number(b.timestamp)
    if (ta !== tb) return newestFirst ? tb - ta : ta - tb
    if (a.partition !== b.partition) {
      return newestFirst ? b.partition - a.partition : a.partition - b.partition
    }
    const oa = Number(a.offset)
    const ob = Number(b.offset)
    return newestFirst ? ob - oa : oa - ob
  })
}
