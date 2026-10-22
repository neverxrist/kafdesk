export type ProduceHeaderRow = { id: string; key: string; value: string }

export function emptyHeaderRow(): ProduceHeaderRow {
  return { id: crypto.randomUUID(), key: '', value: '' }
}

/** Rows with blank keys are skipped. Empty map → undefined (do not send headers). */
export function headersFromRows(
  rows: ProduceHeaderRow[],
): Record<string, string> | undefined {
  const out: Record<string, string> = {}
  for (const row of rows) {
    const k = row.key.trim()
    if (!k) continue
    out[k] = row.value
  }
  return Object.keys(out).length ? out : undefined
}

export function rowsFromHeaders(
  headers: Record<string, string> | null | undefined,
): ProduceHeaderRow[] {
  const entries = Object.entries(headers || {})
  if (!entries.length) return []
  return entries.map(([key, value]) => ({
    id: crypto.randomUUID(),
    key,
    value,
  }))
}
