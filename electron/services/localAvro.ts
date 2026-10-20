import avro from 'avsc'
import { parseJsonLoose } from './messageFormat'

/**
 * Encode JSON payload with a local .avsc (no Schema Registry).
 * Confluent wire: magic 0 + schemaId (default 0) + Avro binary.
 */
export function encodeLocalAvro(
  schemaJson: string,
  value: string,
  schemaId = 0,
): Buffer {
  const schema = JSON.parse(schemaJson)
  const type = avro.Type.forSchema(schema)
  const payload = parseJsonLoose(value)
  const body = type.toBuffer(payload)
  const out = Buffer.alloc(5 + body.length)
  out[0] = 0
  out.writeInt32BE(schemaId >>> 0, 1)
  body.copy(out, 5)
  return out
}
