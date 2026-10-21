/** Typed app errors — prefer these over bare `throw new Error(...)` in main. */
export class AppError extends Error {
  readonly code: string

  constructor(message: string, code: string, options?: { cause?: unknown }) {
    super(message, options?.cause !== undefined ? { cause: options.cause } : undefined)
    this.name = 'AppError'
    this.code = code
  }
}

export function notConnected(what = 'Kafka'): AppError {
  return new AppError(`Not connected to ${what}`, 'NOT_CONNECTED')
}

export function schemaRegistryMissing(): AppError {
  return new AppError('Schema Registry is not configured', 'SR_NOT_CONFIGURED')
}

export function produceBatchTooLarge(max = 2000): AppError {
  return new AppError(`Bulk limit is ${max} messages per run`, 'PRODUCE_BATCH_TOO_LARGE')
}

export function avroSubjectRequired(): AppError {
  return new AppError('Avro subject is required', 'AVRO_SUBJECT_REQUIRED')
}

export function localAvroSchemaRequired(): AppError {
  return new AppError('Local .avsc schema is required', 'LOCAL_AVRO_SCHEMA_REQUIRED')
}

export function toErrorMessage(err: unknown): string {
  if (err instanceof Error) return err.message
  return String(err)
}
