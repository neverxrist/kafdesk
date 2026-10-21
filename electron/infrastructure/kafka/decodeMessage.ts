import type { SchemaRegistry } from '@kafkajs/confluent-schema-registry'
import type { KafkaMessage } from 'kafkajs'
import type { KafkaMessageView } from '../../types'
import {
  detectFormat,
  extractConfluentSchemaId,
  parseJsonLoose,
  toRawPreview,
} from '../../services/messageFormat'

type DecodeRegistry = Pick<SchemaRegistry, 'decode'>

/** Decode a KafkaJS message into the UI/IPC view model. */
export async function decodeKafkaMessage(
  topic: string,
  partition: number,
  message: KafkaMessage,
  registry: DecodeRegistry | null,
): Promise<KafkaMessageView> {
  const valueBuf = message.value
  const format = detectFormat(valueBuf)
  const schemaId = valueBuf ? extractConfluentSchemaId(valueBuf) : undefined
  let value: unknown = null

  if (valueBuf) {
    if (format === 'avro' && registry) {
      try {
        value = await registry.decode(valueBuf)
      } catch (err) {
        value = {
          _decodeError: err instanceof Error ? err.message : String(err),
          raw: toRawPreview(valueBuf, 'bytes'),
        }
      }
    } else if (format === 'json') {
      value = parseJsonLoose(valueBuf.toString('utf8'))
    } else if (format === 'string') {
      value = valueBuf.toString('utf8')
    } else {
      value = toRawPreview(valueBuf, 'bytes')
    }
  }

  const headers: Record<string, string> = {}
  if (message.headers) {
    for (const [k, v] of Object.entries(message.headers)) {
      if (v == null) continue
      if (Array.isArray(v)) {
        headers[k] = v.map((x) => (Buffer.isBuffer(x) ? x.toString('utf8') : String(x))).join(',')
      } else {
        headers[k] = Buffer.isBuffer(v) ? v.toString('utf8') : String(v)
      }
    }
  }

  return {
    topic,
    partition,
    offset: message.offset,
    timestamp: message.timestamp,
    key: message.key ? message.key.toString('utf8') : null,
    value,
    rawPreview: toRawPreview(valueBuf, format),
    format,
    headers,
    schemaId,
  }
}
