import type { MessageFormat } from '../types'

const CONFLUENT_MAGIC = 0x00

export function detectFormat(buffer: Buffer | null): MessageFormat {
  if (!buffer || buffer.length === 0) return 'string'

  // Confluent wire format: magic byte 0 + 4-byte schema id
  if (buffer.length >= 5 && buffer[0] === CONFLUENT_MAGIC) {
    return 'avro'
  }

  const asText = buffer.toString('utf8')
  const trimmed = asText.trim()
  if (
    (trimmed.startsWith('{') && trimmed.endsWith('}')) ||
    (trimmed.startsWith('[') && trimmed.endsWith(']'))
  ) {
    try {
      JSON.parse(trimmed)
      return 'json'
    } catch {
      // fall through
    }
  }

  // printable utf-8?
  if (/^[\x09\x0a\x0d\x20-\x7e\u00a0-\uffff]*$/u.test(asText)) {
    return 'string'
  }

  return 'bytes'
}

export function extractConfluentSchemaId(buffer: Buffer): number | undefined {
  if (buffer.length < 5 || buffer[0] !== CONFLUENT_MAGIC) return undefined
  return buffer.readUInt32BE(1)
}

export function toRawPreview(buffer: Buffer | null, format: MessageFormat): string {
  if (!buffer) return ''
  if (format === 'bytes') {
    return `0x${buffer.subarray(0, 64).toString('hex')}${buffer.length > 64 ? '…' : ''}`
  }
  const text = buffer.toString('utf8')
  return text.length > 2000 ? `${text.slice(0, 2000)}…` : text
}

export function parseJsonLoose(value: string): unknown {
  try {
    return JSON.parse(value)
  } catch {
    return value
  }
}
