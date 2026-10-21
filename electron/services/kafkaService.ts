import { Kafka, type Admin, type Consumer, type Producer } from 'kafkajs'
import { SchemaRegistry, SchemaType } from '@kafkajs/confluent-schema-registry'
import { authService } from '../auth/authService'
import {
  initialFinishedPartitions,
  matchesDecodedMessage,
  notConnected,
  planConsumeWindow,
  PRODUCE_BATCH_MAX,
  PRODUCE_TIMEOUT_MS,
  produceBatchTooLarge,
  schemaRegistryMissing,
  type PartitionTimestampSeek,
  type PartitionWatermark,
} from '../domain'
import {
  buildKafkaConfig,
  clusterAuthMode,
} from '../infrastructure/kafka/buildKafkaConfig'
import { decodeKafkaMessage } from '../infrastructure/kafka/decodeMessage'
import {
  encodeProduceBatch,
  encodeProduceValue,
} from '../infrastructure/kafka/encodeProduce'
import '../infrastructure/kafka/registerSnappy'
import { runConsumeWindow } from '../infrastructure/kafka/runConsumeWindow'
import {
  schemaRegistryAuthHeaders,
  schemaRegistryHttp,
} from '../infrastructure/schema-registry/http'
import type {
  ClusterConfig,
  ConnectResult,
  ConsumeRequest,
  ConsumerGroupInfo,
  GroupOffsetEntry,
  KafkaMessageView,
  PingResult,
  ProduceBatchRequest,
  ProduceBatchResult,
  ProduceRequest,
  SchemaRegistryStatus,
  SchemaSubjectInfo,
  SetGroupOffsetsRequest,
  TailRequest,
  TopicInfo,
} from '../types'
import { buildSchemaRegistryAgent } from './srTls'
import { withTimeout } from './withTimeout'

function bearerMiddleware(getToken: () => Promise<string | null>) {
  return () => ({
    async prepareRequest(next: () => Promise<{ enhance: (x: unknown) => unknown }>) {
      const req = await next()
      const token = await getToken()
      if (!token) return req
      return req.enhance({
        headers: { Authorization: `Bearer ${token}` },
      })
    },
  })
}

export class KafkaService {
  private kafka: Kafka | null = null
  private admin: Admin | null = null
  private producer: Producer | null = null
  private consumer: Consumer | null = null
  private registry: SchemaRegistry | null = null
  private config: ClusterConfig | null = null

  private schemaRegistryStatus: SchemaRegistryStatus = 'none'
  private schemaRegistryError?: string
  private tailHandler: ((msg: KafkaMessageView) => void) | null = null
  private tailing = false

  async connect(config: ClusterConfig): Promise<ConnectResult> {
    await this.disconnect()
    this.config = config
    this.schemaRegistryStatus = 'none'
    this.schemaRegistryError = undefined

    const authMode = clusterAuthMode(config)
    this.kafka = new Kafka(buildKafkaConfig(config))
    this.admin = this.kafka.admin()
    this.producer = this.kafka.producer()
    await this.admin.connect()
    await this.producer.connect()

    if (config.schemaRegistryUrl?.trim()) {
      const useBearer = authMode === 'oauthbearer'
      const agent = buildSchemaRegistryAgent(config)
      this.registry = new SchemaRegistry({
        host: config.schemaRegistryUrl.replace(/\/$/, ''),
        auth:
          !useBearer && config.schemaRegistryAuth?.username
            ? {
                username: config.schemaRegistryAuth.username,
                password: config.schemaRegistryAuth.password,
              }
            : undefined,
        agent,
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        middlewares: useBearer ? ([bearerMiddleware(() => authService.getAccessToken(config.sso))] as any) : [],
      })
      const sr = await this.probeSchemaRegistry()
      this.schemaRegistryStatus = sr.status
      this.schemaRegistryError = sr.error
    }

    return {
      ok: true,
      schemaRegistry: this.schemaRegistryStatus,
      schemaRegistryError: this.schemaRegistryError,
    }
  }

  async disconnect(): Promise<void> {
    await this.stopConsumer()
    try {
      await this.producer?.disconnect()
    } catch {
      /* ignore */
    }
    try {
      await this.admin?.disconnect()
    } catch {
      /* ignore */
    }
    this.producer = null
    this.admin = null
    this.kafka = null
    this.registry = null
    this.config = null
    this.schemaRegistryStatus = 'none'
    this.schemaRegistryError = undefined
  }

  isConnected(): boolean {
    return Boolean(this.admin && this.producer)
  }

  getSchemaRegistryStatus(): SchemaRegistryStatus {
    return this.schemaRegistryStatus
  }

  hasSchemaRegistry(): boolean {
    return this.schemaRegistryStatus === 'ok'
  }

  async ping(): Promise<PingResult> {
    this.ensureAdmin()
    return this.runPingChecks()
  }

  /**
   * One-shot reachability check. If already connected, pings in place.
   * Otherwise connects with `config`, measures, and disconnects — session stays offline.
   */
  async probe(config: ClusterConfig): Promise<PingResult> {
    if (this.isConnected()) {
      return this.runPingChecks()
    }
    try {
      await this.connect(config)
      return await this.runPingChecks()
    } finally {
      await this.disconnect()
    }
  }

  private async runPingChecks(): Promise<PingResult> {
    const result: PingResult = {
      kafka: { ok: false },
      schemaRegistry: { status: 'none' },
    }

    const kafkaStarted = Date.now()
    try {
      await this.admin!.describeCluster()
      result.kafka = { ok: true, ms: Date.now() - kafkaStarted }
    } catch (err) {
      result.kafka = {
        ok: false,
        ms: Date.now() - kafkaStarted,
        error: err instanceof Error ? err.message : String(err),
      }
    }

    if (!this.config?.schemaRegistryUrl?.trim()) {
      result.schemaRegistry = { status: 'none' }
      return result
    }

    const sr = await this.probeSchemaRegistry()
    // Only persist SR status onto the live session when still connected after checks
    if (this.isConnected()) {
      this.schemaRegistryStatus = sr.status
      this.schemaRegistryError = sr.error
    }
    result.schemaRegistry = sr
    return result
  }

  private async probeSchemaRegistry(): Promise<{
    status: SchemaRegistryStatus
    ms?: number
    error?: string
  }> {
    const url = this.config?.schemaRegistryUrl?.replace(/\/$/, '')
    if (!url) return { status: 'none' }

    const headers = await schemaRegistryAuthHeaders(this.config)
    headers.Accept = 'application/vnd.schemaregistry.v1+json'

    const started = Date.now()
    try {
      const res = await schemaRegistryHttp(`${url}/subjects`, headers, this.config)
      const ms = Date.now() - started
      if (res.status < 200 || res.status >= 300) {
        return { status: 'fail', ms, error: `HTTP ${res.status}` }
      }
      return { status: 'ok', ms }
    } catch (err) {
      return {
        status: 'fail',
        ms: Date.now() - started,
        error: err instanceof Error ? err.message : String(err),
      }
    }
  }

  async listTopics(): Promise<TopicInfo[]> {
    this.ensureAdmin()
    const names = (await this.admin!.listTopics()).filter((t) => !t.startsWith('__')).sort()
    const described = await this.admin!.fetchTopicMetadata({ topics: names })

    return described.topics.map((topic) => ({
      name: topic.name,
      partitions: topic.partitions
        .map((p) => ({
          partitionId: p.partitionId,
          leader: p.leader,
          replicas: p.replicas,
          isr: p.isr,
        }))
        .sort((a, b) => a.partitionId - b.partitionId),
    }))
  }

  async getTopicOffsets(topic: string): Promise<TopicInfo> {
    this.ensureAdmin()
    const described = await this.admin!.fetchTopicMetadata({ topics: [topic] })
    const meta = described.topics.find((t) => t.name === topic)
    if (!meta) throw new Error(`Topic not found: ${topic}`)

    const offsets = await this.admin!.fetchTopicOffsets(topic)
    const byPartition = new Map(offsets.map((o) => [Number(o.partition), o]))

    const partitions = meta.partitions
      .map((p) => {
        const o = byPartition.get(p.partitionId)
        const offsetLow = o?.low
        const offsetHigh = o?.high
        const approxMessages =
          offsetLow != null && offsetHigh != null
            ? Math.max(0, Number(offsetHigh) - Number(offsetLow))
            : undefined
        return {
          partitionId: p.partitionId,
          leader: p.leader,
          replicas: p.replicas,
          isr: p.isr,
          offsetLow,
          offsetHigh,
          approxMessages,
        }
      })
      .sort((a, b) => a.partitionId - b.partitionId)

    const approxMessages = partitions.reduce((sum, p) => sum + (p.approxMessages ?? 0), 0)

    return { name: topic, partitions, approxMessages }
  }

  private async stopConsumer(): Promise<void> {
    this.tailing = false
    this.tailHandler = null
    if (!this.consumer) return
    try {
      await this.consumer.disconnect()
    } catch {
      /* ignore */
    }
    this.consumer = null
  }

  async consume(request: ConsumeRequest): Promise<KafkaMessageView[]> {
    this.ensureKafka()
    this.ensureAdmin()
    await this.stopConsumer()

    const startFrom =
      request.startFrom ??
      (request.fromBeginning === false ? 'end' : 'beginning')
    const hasContentFilter = Boolean(
      request.search?.trim() ||
        request.searchKey?.trim() ||
        request.searchHeaderKey?.trim() ||
        request.searchHeaderValue?.trim(),
    )
    const hasTimeFilter = request.timeFrom != null || request.timeTo != null

    const rawOffsets = await this.admin!.fetchTopicOffsets(request.topic)
    const topicOffsets: PartitionWatermark[] = rawOffsets.map((o) => ({
      partition: Number(o.partition),
      low: o.low,
      high: o.high,
    }))

    let offsetsByTimestamp: PartitionTimestampSeek[] | null = null
    let timeFromSeekFailed = false
    if (request.timeFrom != null && Number.isFinite(request.timeFrom)) {
      try {
        const byTs = await this.admin!.fetchTopicOffsetsByTimestamp(
          request.topic,
          request.timeFrom,
        )
        offsetsByTimestamp = byTs.map((row) => ({
          partition: Number(row.partition),
          offset: String(row.offset),
        }))
      } catch {
        timeFromSeekFailed = true
      }
    }

    const plan = planConsumeWindow({
      startFrom,
      startOffset: request.startOffset,
      limit: request.limit,
      partitions: request.partitions,
      offsetsByTimestamp,
      timeFromSeekFailed,
      hasContentFilter,
      hasTimeFilter,
      topicOffsets,
    })
    if (plan.empty) return []

    const finishedPartitions = initialFinishedPartitions(
      plan.targetPartitions,
      topicOffsets,
      plan.plannedStart,
    )
    if (finishedPartitions.size >= plan.targetPartitions.length) {
      return []
    }

    this.consumer = this.kafka!.consumer({
      groupId: `kafdesk-ui-${Date.now()}`,
      sessionTimeout: 15_000,
      heartbeatInterval: 3_000,
      maxWaitTimeInMs: 400,
    })
    await this.consumer.connect()
    // Always fromBeginning when we seek — starting at tip (fromBeginning:false)
    // races the seek and often returns [] within the wait window.
    await this.consumer.subscribe({
      topic: request.topic,
      fromBeginning: true,
    })

    const consumer = this.consumer
    try {
      return await runConsumeWindow({
        consumer,
        isActive: () => this.consumer === consumer,
        request,
        plan,
        topicOffsets,
        finishedPartitions,
        decode: (topic, partition, message) =>
          this.toMessageView(topic, partition, message),
      })
    } finally {
      await this.stopConsumer()
    }
  }

  async startTail(
    request: TailRequest,
    onMessage: (msg: KafkaMessageView) => void,
    onError?: (message: string) => void,
  ): Promise<void> {
    this.ensureKafka()
    await this.stopConsumer()

    this.tailing = true
    this.tailHandler = onMessage
    this.consumer = this.kafka!.consumer({ groupId: `kafdesk-tail-${Date.now()}` })
    await this.consumer.connect()
    await this.consumer.subscribe({ topic: request.topic, fromBeginning: false })

    const filter = {
      search: request.search,
      searchKey: request.searchKey,
      headerKey: request.searchHeaderKey,
      headerValue: request.searchHeaderValue,
    }

    void this.consumer
      .run({
        eachMessage: async ({ topic, partition, message }) => {
          if (!this.tailing) return
          if (request.partitions && !request.partitions.includes(partition)) return
          const view = await this.toMessageView(topic, partition, message)
          if (!matchesDecodedMessage(view, filter)) return
          this.tailHandler?.(view)
        },
      })
      .catch(async (err) => {
        const msg = err instanceof Error ? err.message : String(err)
        this.tailing = false
        onError?.(msg)
        try {
          await this.stopConsumer()
        } catch {
          /* ignore */
        }
      })
  }

  async stopTail(): Promise<void> {
    await this.stopConsumer()
  }

  async listConsumerGroups(): Promise<ConsumerGroupInfo[]> {
    this.ensureAdmin()
    const { groups } = await this.admin!.listGroups()
    const ids = groups.map((g) => g.groupId).filter((id) => !id.startsWith('kafdesk-'))
    if (!ids.length) return []

    const described = await this.admin!.describeGroups(ids)
    return ids
      .map((groupId) => {
        const overview = groups.find((g) => g.groupId === groupId)
        const desc = described.groups.find((g) => g.groupId === groupId)
        return {
          groupId,
          protocolType: overview?.protocolType || 'unknown',
          state: desc?.state,
          members: desc?.members?.length,
        }
      })
      .sort((a, b) => a.groupId.localeCompare(b.groupId))
  }

  async getGroupOffsets(groupId: string, topic?: string): Promise<GroupOffsetEntry[]> {
    this.ensureAdmin()
    const topics = topic ? [topic] : undefined
    // Raw commits: resolveOffsets:true turns -1 into high watermark and hides real lag.
    const offsets = await this.admin!.fetchOffsets({
      groupId,
      topics,
      resolveOffsets: false,
    })

    const result: GroupOffsetEntry[] = []
    for (const t of offsets) {
      let highByPart = new Map<number, { low: string; high: string }>()
      try {
        const hw = await this.admin!.fetchTopicOffsets(t.topic)
        highByPart = new Map(hw.map((o) => [Number(o.partition), { low: o.low, high: o.high }]))
      } catch {
        /* topic may be gone */
      }

      for (const p of t.partitions) {
        const hw = highByPart.get(p.partition)
        const current = Number(p.offset)
        const high = hw ? Number(hw.high) : undefined
        const committed = Number.isFinite(current) && current >= 0
        // When scoped to a topic, skip partitions with no commit — listing the topic
        // in OffsetFetch returns -1 for every group, which is not real consumption.
        if (topic && !committed) continue
        result.push({
          topic: t.topic,
          partition: p.partition,
          offset: p.offset,
          metadata: p.metadata ?? undefined,
          low: hw?.low,
          high: hw?.high,
          lag:
            committed && high != null && !Number.isNaN(high)
              ? Math.max(0, high - current)
              : undefined,
        })
      }
    }

    return result.sort((a, b) => a.topic.localeCompare(b.topic) || a.partition - b.partition)
  }

  async getLagSummary(topic?: string): Promise<import('../types').LagSummaryResult> {
    this.ensureAdmin()
    const groups = await this.listConsumerGroups()
    const summaries: import('../types').GroupLagSummary[] = []
    for (const g of groups) {
      let offsets: Awaited<ReturnType<KafkaService['getGroupOffsets']>> = []
      try {
        offsets = await this.getGroupOffsets(g.groupId, topic)
      } catch {
        offsets = []
      }
      if (topic && offsets.length === 0) continue

      let totalLag = 0
      let maxPartitionLag = 0
      let worstTopic: string | undefined
      let worstPartition: number | undefined
      const topicSet = new Set<string>()
      for (const o of offsets) {
        topicSet.add(o.topic)
        const lag = o.lag ?? 0
        totalLag += lag
        if (lag > maxPartitionLag) {
          maxPartitionLag = lag
          worstTopic = o.topic
          worstPartition = o.partition
        }
      }
      summaries.push({
        groupId: g.groupId,
        totalLag,
        maxPartitionLag,
        topics: topicSet.size,
        partitions: offsets.length,
        worstTopic,
        worstPartition,
      })
    }

    summaries.sort((a, b) => b.totalLag - a.totalLag || a.groupId.localeCompare(b.groupId))
    return {
      groups: summaries,
      totalLag: summaries.reduce((s, x) => s + x.totalLag, 0),
      maxLag: summaries.reduce((s, x) => Math.max(s, x.totalLag), 0),
      updatedAt: Date.now(),
    }
  }

  async setGroupOffsets(request: SetGroupOffsetsRequest): Promise<void> {
    this.ensureAdmin()
    await this.admin!.setOffsets({
      groupId: request.groupId,
      topic: request.topic,
      partitions: request.partitions.map((p) => ({
        partition: p.partition,
        offset: p.offset,
      })),
    })
  }

  async resetGroupOffsets(groupId: string, topic: string, earliest: boolean): Promise<void> {
    this.ensureAdmin()
    await this.admin!.resetOffsets({ groupId, topic, earliest })
  }

  async produce(
    request: ProduceRequest,
  ): Promise<{ partition: number; offset?: string; format: string; schemaId?: number }> {
    this.ensureProducer()
    const { value, schemaId } = await encodeProduceValue(request, this.registry)

    const result = await withTimeout(
      this.producer!.send({
        topic: request.topic,
        messages: [
          {
            key: request.key,
            value,
            partition: request.partition,
            headers: request.headers,
          },
        ],
      }),
      PRODUCE_TIMEOUT_MS,
      'Kafka produce',
    )

    const first = result[0]
    // Produce API v3+ returns baseOffset; older APIs return offset.
    return {
      partition: first.partition,
      offset: first.offset ?? first.baseOffset,
      format: request.format,
      schemaId,
    }
  }

  async produceBatch(request: ProduceBatchRequest): Promise<ProduceBatchResult> {
    this.ensureProducer()
    if (!request.messages.length) {
      return { sent: 0, format: request.format }
    }
    if (request.messages.length > PRODUCE_BATCH_MAX) {
      throw produceBatchTooLarge(PRODUCE_BATCH_MAX)
    }

    const { encoded, schemaId } = await encodeProduceBatch(request, this.registry)

    let lastPartition: number | undefined
    let lastOffset: string | undefined
    const chunkSize = 100
    for (let i = 0; i < encoded.length; i += chunkSize) {
      const chunk = encoded.slice(i, i + chunkSize)
      const result = await withTimeout(
        this.producer!.send({
          topic: request.topic,
          messages: chunk.map((m) => ({
            key: m.key,
            value: m.value,
            partition: request.partition,
          })),
        }),
        PRODUCE_TIMEOUT_MS,
        'Kafka produce batch',
      )
      const first = result[0]
      if (first) {
        lastPartition = first.partition
        lastOffset = first.offset ?? first.baseOffset
      }
    }

    return {
      sent: encoded.length,
      format: request.format,
      schemaId,
      lastPartition,
      lastOffset,
    }
  }

  async listSubjects(): Promise<string[]> {
    this.ensureRegistry()
    const host = this.config!.schemaRegistryUrl!.replace(/\/$/, '')
    const headers = await schemaRegistryAuthHeaders(this.config)
    const res = await schemaRegistryHttp(`${host}/subjects`, headers, this.config)
    if (res.status < 200 || res.status >= 300) {
      throw new Error(`Schema Registry error: ${res.status}`)
    }
    return JSON.parse(res.body) as string[]
  }

  async getLatestSchema(subject: string): Promise<SchemaSubjectInfo> {
    this.ensureRegistry()
    const host = this.config!.schemaRegistryUrl!.replace(/\/$/, '')
    const headers = await schemaRegistryAuthHeaders(this.config)
    headers.Accept = 'application/vnd.schemaregistry.v1+json'
    const res = await schemaRegistryHttp(
      `${host}/subjects/${encodeURIComponent(subject)}/versions/latest`,
      headers,
      this.config,
    )
    if (res.status < 200 || res.status >= 300) {
      throw new Error(`Schema Registry error: ${res.status}`)
    }
    const data = JSON.parse(res.body) as {
      subject: string
      version: number
      id: number
      schemaType?: string
      schema: string
    }
    return {
      subject: data.subject,
      version: data.version,
      id: data.id,
      schemaType: data.schemaType || SchemaType.AVRO,
      schema: data.schema,
    }
  }

  private toMessageView(
    topic: string,
    partition: number,
    message: Parameters<typeof decodeKafkaMessage>[2],
  ): Promise<KafkaMessageView> {
    return decodeKafkaMessage(topic, partition, message, this.registry)
  }

  private ensureAdmin() {
    if (!this.admin) throw notConnected('Kafka')
  }

  private ensureProducer() {
    if (!this.producer) throw notConnected('Kafka')
  }

  private ensureKafka() {
    if (!this.kafka) throw notConnected('Kafka')
  }

  private ensureRegistry() {
    if (!this.registry || !this.config?.schemaRegistryUrl) {
      throw schemaRegistryMissing()
    }
  }
}

export const kafkaService = new KafkaService()
