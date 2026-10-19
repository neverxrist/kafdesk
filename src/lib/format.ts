/** Kafka message timestamp is usually epoch millis as string */
export function formatMessageDateTime(timestamp: string | undefined | null): string | null {
  if (timestamp == null || timestamp === '') return null
  const n = Number(timestamp)
  if (!Number.isFinite(n)) return String(timestamp)
  // Kafka sometimes sends seconds; treat small values as seconds
  const ms = n < 1e12 ? n * 1000 : n
  try {
    return new Date(ms).toLocaleString(undefined, {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
    })
  } catch {
    return String(timestamp)
  }
}

export function formatBadgeLabel(format: string, schemaId?: number): string {
  if (format === 'avro' && schemaId != null) return `avro · id ${schemaId}`
  return format
}
