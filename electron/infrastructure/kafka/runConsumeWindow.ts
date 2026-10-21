import type { Consumer, KafkaMessage } from 'kafkajs'
import {
  matchesDecodedMessage,
  matchesRawPreFilter,
  sortMessagesByTime,
  type ConsumeWindowPlan,
  type PartitionWatermark,
} from '../../domain'
import type { KafkaMessageView } from '../../types'

export interface ConsumeWindowRequestSlice {
  topic: string
  partitions?: number[]
  search?: string
  searchKey?: string
  searchHeaderKey?: string
  searchHeaderValue?: string
  timeFrom?: number
  timeTo?: number
}

/**
 * Run one-shot UI consume against an already subscribed consumer.
 * Seek only after run() — seek-before-run crashes KafkaJS/main.
 */
export async function runConsumeWindow(opts: {
  consumer: Consumer
  /** True while this consumer is still the active session instance. */
  isActive: () => boolean
  request: ConsumeWindowRequestSlice
  plan: ConsumeWindowPlan
  topicOffsets: PartitionWatermark[]
  finishedPartitions: Set<number>
  decode: (
    topic: string,
    partition: number,
    message: KafkaMessage,
  ) => Promise<KafkaMessageView>
}): Promise<KafkaMessageView[]> {
  const {
    consumer,
    isActive,
    request,
    plan,
    topicOffsets,
    finishedPartitions,
    decode,
  } = opts
  const {
    limit,
    startFrom,
    hasFilter,
    waitMs,
    idleMs,
    targetPartitions,
    seekPlan,
    plannedStart,
    newestFirst,
  } = plan

  const highByPart = new Map(topicOffsets.map((o) => [o.partition, o]))
  const messages: KafkaMessageView[] = []
  const takenByPart = new Map<number, number>()
  const filter = {
    search: request.search,
    searchKey: request.searchKey,
    headerKey: request.searchHeaderKey,
    headerValue: request.searchHeaderValue,
    timeFrom: request.timeFrom,
    timeTo: request.timeTo,
  }

  await new Promise<void>((resolve, reject) => {
    let settled = false
    let runStarted = false
    let idleTimer: ReturnType<typeof setTimeout> | null = null
    let hardTimer: ReturnType<typeof setTimeout> | null = null
    let seekFallback1: ReturnType<typeof setTimeout> | null = null
    let seekFallback2: ReturnType<typeof setTimeout> | null = null

    const clearIdle = () => {
      if (idleTimer) {
        clearTimeout(idleTimer)
        idleTimer = null
      }
    }
    const teardown = () => {
      if (hardTimer) clearTimeout(hardTimer)
      clearIdle()
      if (seekFallback1) clearTimeout(seekFallback1)
      if (seekFallback2) clearTimeout(seekFallback2)
    }
    const done = () => {
      if (settled) return
      settled = true
      teardown()
      resolve()
    }
    const bumpIdle = () => {
      if (settled) return
      clearIdle()
      idleTimer = setTimeout(() => done(), idleMs)
    }
    const checkCaughtUp = () => {
      if (finishedPartitions.size >= targetPartitions.length) done()
    }

    const seekAll = () => {
      if (settled || !runStarted || !isActive()) return
      try {
        for (const s of seekPlan) {
          consumer.seek({
            topic: request.topic,
            partition: s.partition,
            offset: s.offset,
          })
        }
      } catch {
        /* seek before group ready / after disconnect — ignore */
      }
    }
    if (seekPlan.length) {
      consumer.on(consumer.events.GROUP_JOIN, seekAll)
    }

    hardTimer = setTimeout(() => done(), waitMs)

    runStarted = true
    consumer
      .run({
        partitionsConsumedConcurrently: Math.min(targetPartitions.length, 8),
        eachMessage: async ({ topic, partition, message }) => {
          if (settled) return
          if (request.partitions && !request.partitions.includes(partition)) return

          const o = highByPart.get(partition)
          const high = o ? Number(o.high) : 0
          const offset = Number(message.offset)
          const start = plannedStart.get(partition)

          if (start != null && offset < start) {
            bumpIdle()
            if (o && offset >= high - 1) {
              finishedPartitions.add(partition)
              checkCaughtUp()
            }
            return
          }

          bumpIdle()

          if (o && offset >= high - 1) {
            finishedPartitions.add(partition)
          }

          if (!matchesRawPreFilter(message, filter)) {
            checkCaughtUp()
            return
          }

          const view = await decode(topic, partition, message)
          if (settled) return
          if (!matchesDecodedMessage(view, filter)) {
            checkCaughtUp()
            return
          }

          messages.push(view)
          const taken = (takenByPart.get(partition) ?? 0) + 1
          takenByPart.set(partition, taken)

          if (!hasFilter && startFrom === 'beginning' && taken >= limit) {
            finishedPartitions.add(partition)
          }

          if (hasFilter && startFrom !== 'end' && messages.length >= limit) {
            done()
            return
          }

          if (
            hasFilter &&
            startFrom === 'end' &&
            messages.length >= limit &&
            finishedPartitions.size >= targetPartitions.length
          ) {
            done()
            return
          }

          checkCaughtUp()
        },
      })
      .catch((err) => {
        if (settled) return
        settled = true
        teardown()
        reject(err)
      })

    if (seekPlan.length) {
      seekFallback1 = setTimeout(() => seekAll(), 50)
      seekFallback2 = setTimeout(() => seekAll(), 200)
      setTimeout(() => bumpIdle(), 300)
    } else {
      bumpIdle()
    }
  })

  return sortMessagesByTime(messages, newestFirst).slice(0, limit)
}
