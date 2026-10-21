import type { SchemaRegistry } from '@kafkajs/confluent-schema-registry'
import {
  avroSubjectRequired,
  localAvroSchemaRequired,
  schemaRegistryMissing,
  SR_ENCODE_TIMEOUT_MS,
} from '../../domain'
import { encodeLocalAvro } from '../../services/localAvro'
import { parseJsonLoose } from '../../services/messageFormat'
import { withTimeout } from '../../services/withTimeout'
import type { ProduceBatchRequest, ProduceRequest } from '../../types'

type EncodeRegistry = Pick<SchemaRegistry, 'encode' | 'getLatestSchemaId'>

export async function encodeProduceValue(
  request: ProduceRequest,
  registry: EncodeRegistry | null,
): Promise<{ value: Buffer | string; schemaId?: number }> {
  if (request.format === 'avro') {
    if (!registry) throw schemaRegistryMissing()
    if (!request.subject) throw avroSubjectRequired()
    const payload = parseJsonLoose(request.value)
    const schemaId = await withTimeout(
      registry.getLatestSchemaId(request.subject),
      SR_ENCODE_TIMEOUT_MS,
      'Schema Registry getLatestSchemaId',
    )
    const value = await withTimeout(
      registry.encode(schemaId, payload),
      SR_ENCODE_TIMEOUT_MS,
      'Schema Registry encode',
    )
    return { value, schemaId }
  }

  if (request.format === 'avro-local') {
    if (!request.localSchema?.trim()) throw localAvroSchemaRequired()
    const schemaId = request.localSchemaId ?? 0
    return {
      value: encodeLocalAvro(request.localSchema, request.value, schemaId),
      schemaId,
    }
  }

  if (request.format === 'json') {
    const parsed = parseJsonLoose(request.value)
    return {
      value: typeof parsed === 'string' ? parsed : JSON.stringify(parsed),
    }
  }

  return { value: request.value }
}

export async function encodeProduceBatch(
  request: ProduceBatchRequest,
  registry: EncodeRegistry | null,
): Promise<{
  encoded: Array<{ key?: string; value: Buffer | string }>
  schemaId?: number
}> {
  if (request.format === 'avro') {
    if (!registry) throw schemaRegistryMissing()
    if (!request.subject) throw avroSubjectRequired()
    const schemaId = await withTimeout(
      registry.getLatestSchemaId(request.subject),
      SR_ENCODE_TIMEOUT_MS,
      'Schema Registry getLatestSchemaId',
    )
    const encoded: Array<{ key?: string; value: Buffer | string }> = []
    for (const m of request.messages) {
      const payload = parseJsonLoose(m.value)
      encoded.push({
        key: m.key,
        value: await withTimeout(
          registry.encode(schemaId, payload),
          SR_ENCODE_TIMEOUT_MS,
          'Schema Registry encode',
        ),
      })
    }
    return { encoded, schemaId }
  }

  if (request.format === 'avro-local') {
    if (!request.localSchema?.trim()) throw localAvroSchemaRequired()
    const schemaId = request.localSchemaId ?? 0
    return {
      schemaId,
      encoded: request.messages.map((m) => ({
        key: m.key,
        value: encodeLocalAvro(request.localSchema!, m.value, schemaId),
      })),
    }
  }

  if (request.format === 'json') {
    return {
      encoded: request.messages.map((m) => {
        const parsed = parseJsonLoose(m.value)
        return {
          key: m.key,
          value: typeof parsed === 'string' ? parsed : JSON.stringify(parsed),
        }
      }),
    }
  }

  return {
    encoded: request.messages.map((m) => ({ key: m.key, value: m.value })),
  }
}
