/** Hard caps — UI may clamp lower; service must never exceed these. */
export const CONSUME_LIMIT_MAX = 500
export const PRODUCE_BATCH_MAX = 2000
export const PRODUCE_TIMEOUT_MS = 45_000
export const SR_ENCODE_TIMEOUT_MS = 20_000

export function clampConsumeLimit(limit?: number): number {
  return Math.min(CONSUME_LIMIT_MAX, Math.max(1, Math.trunc(limit ?? 25) || 25))
}

/** Hard wait for one-shot consume; normal exit is caught-up / quota. */
export function consumeWaitMs(opts: {
  limit: number
  partitionCount: number
  hasFilter: boolean
  startFrom: 'beginning' | 'end' | 'offset'
}): number {
  return Math.min(
    30_000,
    4_000 +
      opts.limit * 25 +
      opts.partitionCount * 250 +
      (opts.hasFilter ? 4_000 : 0) +
      (opts.startFrom === 'beginning' ? 1_500 : 0),
  )
}

export function consumeIdleMs(hasFilter: boolean): number {
  return hasFilter ? 1_200 : 800
}
