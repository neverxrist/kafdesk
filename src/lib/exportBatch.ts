import type { KafkaMessageView } from '../types/kafdesk'

export type BatchExportFormat = 'json' | 'csv'

const CSV_HEADER = [
  'topic',
  'partition',
  'offset',
  'timestamp',
  'key',
  'value',
  'format',
  'schemaId',
  'headers',
] as const

function valueAsText(v: unknown): string {
  if (v == null) return ''
  if (typeof v === 'string') return v
  try {
    return JSON.stringify(v)
  } catch {
    return String(v)
  }
}

function csvEscape(cell: string): string {
  if (/[",\n\r]/.test(cell)) return `"${cell.replace(/"/g, '""')}"`
  return cell
}

function rowCsv(m: KafkaMessageView): string {
  return [
    csvEscape(m.topic),
    String(m.partition),
    csvEscape(m.offset),
    csvEscape(m.timestamp),
    csvEscape(m.key ?? ''),
    csvEscape(valueAsText(m.value)),
    csvEscape(m.format),
    m.schemaId != null ? String(m.schemaId) : '',
    csvEscape(JSON.stringify(m.headers ?? {})),
  ].join(',')
}

/** Serialize the visible (already filtered) message window. Full values — not Hits-trimmed. */
export function serializeBatch(messages: KafkaMessageView[], format: BatchExportFormat): string {
  if (format === 'json') {
    const rows = messages.map((m) => ({
      topic: m.topic,
      partition: m.partition,
      offset: m.offset,
      timestamp: m.timestamp,
      key: m.key,
      value: m.value,
      format: m.format,
      schemaId: m.schemaId ?? null,
      headers: m.headers ?? {},
    }))
    return `${JSON.stringify(rows, null, 2)}\n`
  }

  // BOM so Excel opens UTF-8 cleanly
  const lines = [`\uFEFF${CSV_HEADER.join(',')}`, ...messages.map(rowCsv)]
  return `${lines.join('\n')}\n`
}

export function defaultExportFilename(topic: string | null, format: BatchExportFormat): string {
  const safe = (topic || 'messages').replace(/[^\w.\-]+/g, '_').slice(0, 80)
  const stamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19)
  return `${safe}_${stamp}.${format}`
}

/** Save via Tauri dialog, or browser download fallback. */
export async function saveBatchExport(
  contents: string,
  filename: string,
  format: BatchExportFormat,
): Promise<'saved' | 'cancelled'> {
  if (window.kafdesk?.saveTextFile) {
    const path = await window.kafdesk.saveTextFile({
      defaultPath: filename,
      contents,
      filters:
        format === 'json'
          ? [{ name: 'JSON', extensions: ['json'] }]
          : [{ name: 'CSV', extensions: ['csv'] }],
    })
    return path ? 'saved' : 'cancelled'
  }

  const mime = format === 'json' ? 'application/json' : 'text/csv;charset=utf-8'
  const blob = new Blob([contents], { type: mime })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  a.click()
  URL.revokeObjectURL(url)
  return 'saved'
}
