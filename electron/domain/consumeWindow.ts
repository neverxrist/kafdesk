import type { ConsumeStartFrom } from '../types'
import { clampConsumeLimit, consumeIdleMs, consumeWaitMs } from './consumeLimits'

export interface PartitionWatermark {
  partition: number
  low: string
  high: string
}

/** Result of admin.fetchTopicOffsetsByTimestamp (offset = first ≥ time). */
export interface PartitionTimestampSeek {
  partition: number
  offset: string
}

export interface ConsumeWindowInput {
  startFrom: ConsumeStartFrom
  startOffset?: string
  limit?: number
  partitions?: number[]
  /** When set, seek uses these offsets (from admin.fetchTopicOffsetsByTimestamp). */
  offsetsByTimestamp?: PartitionTimestampSeek[] | null
  /** True when timeFrom was requested but timestamp fetch failed → fall back to low. */
  timeFromSeekFailed?: boolean
  hasContentFilter: boolean
  hasTimeFilter: boolean
  topicOffsets: PartitionWatermark[]
}

export interface ConsumeWindowPlan {
  limit: number
  startFrom: ConsumeStartFrom
  hasFilter: boolean
  waitMs: number
  idleMs: number
  targetPartitions: number[]
  approxAvailable: number
  seekPlan: Array<{ partition: number; offset: string }>
  plannedStart: Map<number, number>
  /** Every target partition has an empty window — caller should return []. */
  empty: boolean
  newestFirst: boolean
}

function approxFor(
  targetPartitions: number[],
  highByPart: Map<number, PartitionWatermark>,
): number {
  return targetPartitions.reduce((sum, p) => {
    const o = highByPart.get(p)
    if (!o) return sum
    return sum + Math.max(0, Number(o.high) - Number(o.low))
  }, 0)
}

/**
 * Pure seek/window plan for one-shot UI consume.
 * Does not touch Kafka — timestamps already resolved by caller when needed.
 */
export function planConsumeWindow(input: ConsumeWindowInput): ConsumeWindowPlan {
  const limit = clampConsumeLimit(input.limit)
  const startFrom = input.startFrom
  const hasFilter = input.hasContentFilter || input.hasTimeFilter
  const highByPart = new Map(input.topicOffsets.map((o) => [o.partition, o]))
  const targetPartitions =
    input.partitions?.length
      ? input.partitions
      : input.topicOffsets.map((o) => o.partition)

  const approxAvailable = approxFor(targetPartitions, highByPart)
  const waitMs = consumeWaitMs({
    limit,
    partitionCount: targetPartitions.length,
    hasFilter,
    startFrom,
  })
  const idleMs = consumeIdleMs(hasFilter)
  const newestFirst = startFrom === 'end'

  const seekPlan: Array<{ partition: number; offset: string }> = []
  const plannedStart = new Map<number, number>()

  if (approxAvailable === 0 && startFrom !== 'offset') {
    return {
      limit,
      startFrom,
      hasFilter,
      waitMs,
      idleMs,
      targetPartitions,
      approxAvailable,
      seekPlan,
      plannedStart,
      empty: true,
      newestFirst,
    }
  }

  const endLookback = hasFilter
    ? Math.min(Math.max(approxAvailable, limit), Math.max(limit * 50, 2_000))
    : limit

  if (startFrom === 'offset' && input.startOffset != null && input.startOffset !== '') {
    for (const partition of targetPartitions) {
      const o = highByPart.get(partition)
      const low = o ? Number(o.low) : 0
      const high = o ? Number(o.high) : 0
      const raw = Number(input.startOffset) || 0
      const start = Math.max(low, Math.min(raw, high))
      plannedStart.set(partition, start)
      if (start < high) seekPlan.push({ partition, offset: String(start) })
    }
  } else if (input.offsetsByTimestamp?.length) {
    for (const row of input.offsetsByTimestamp) {
      if (!targetPartitions.includes(row.partition)) continue
      const o = highByPart.get(row.partition)
      if (!o) continue
      const low = Number(o.low)
      const high = Number(o.high)
      const start = Math.max(low, Math.min(Number(row.offset), high))
      plannedStart.set(row.partition, start)
      if (start < high) seekPlan.push({ partition: row.partition, offset: String(start) })
    }
  } else if (input.timeFromSeekFailed) {
    for (const partition of targetPartitions) {
      const o = highByPart.get(partition)
      if (!o) continue
      plannedStart.set(partition, Number(o.low))
      seekPlan.push({ partition, offset: String(o.low) })
    }
  } else if (startFrom === 'end') {
    for (const partition of targetPartitions) {
      const o = highByPart.get(partition)
      if (!o) continue
      const high = Number(o.high)
      const low = Number(o.low)
      const start = Math.max(low, high - endLookback)
      plannedStart.set(partition, start)
      if (start < high) seekPlan.push({ partition, offset: String(start) })
    }
  } else {
    for (const partition of targetPartitions) {
      const o = highByPart.get(partition)
      if (!o) continue
      const low = Number(o.low)
      const high = Number(o.high)
      plannedStart.set(partition, low)
      if (low < high) seekPlan.push({ partition, offset: String(low) })
    }
  }

  let finishedEmpty = 0
  for (const partition of targetPartitions) {
    const o = highByPart.get(partition)
    if (!o) {
      finishedEmpty++
      continue
    }
    const high = Number(o.high)
    const start = plannedStart.get(partition)
    if (high <= Number(o.low) || (start != null && start >= high)) {
      finishedEmpty++
    }
  }

  return {
    limit,
    startFrom,
    hasFilter,
    waitMs,
    idleMs,
    targetPartitions,
    approxAvailable,
    seekPlan,
    plannedStart,
    empty: finishedEmpty >= targetPartitions.length,
    newestFirst,
  }
}

/** Mark which partitions start already finished (empty window). */
export function initialFinishedPartitions(
  targetPartitions: number[],
  topicOffsets: PartitionWatermark[],
  plannedStart: Map<number, number>,
): Set<number> {
  const highByPart = new Map(topicOffsets.map((o) => [o.partition, o]))
  const finished = new Set<number>()
  for (const partition of targetPartitions) {
    const o = highByPart.get(partition)
    if (!o) {
      finished.add(partition)
      continue
    }
    const high = Number(o.high)
    const start = plannedStart.get(partition)
    if (high <= Number(o.low) || (start != null && start >= high)) {
      finished.add(partition)
    }
  }
  return finished
}
