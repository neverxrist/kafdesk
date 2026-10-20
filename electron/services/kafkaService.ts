import { Kafka, logLevel, type Admin, type Consumer, type Producer, type KafkaConfig } from 'kafkajs'
import { SchemaRegistry, SchemaType } from '@kafkajs/confluent-schema-registry'
import http from 'node:http'
import https from 'node:https'
import { URL } from 'node:url'
import { authService } from '../auth/authService'
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
import {
  detectFormat,
  extractConfluentSchemaId,
  parseJsonLoose,
  toRawPreview,
} from './messageFormat'
import { encodeLocalAvro } from './localAvro'
import { buildSchemaRegistryAgent } from './srTls'
import { withTimeout } from './withTimeout'

const CONSUME_LIMIT_MAX = 500
const PRODUCE_TIMEOUT_MS = 45_000
const SR_ENCODE_TIMEOUT_MS = 20_000

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

    const kafkaConfig: KafkaConfig = {
      clientId: config.clientId || 'kafdesk',
      brokers: config.brokers,
      ssl: config.ssl || false,
      logLevel: logLevel.ERROR,
      connectionTimeout: 10_000,
      requestTimeout: 30_000,
    }

    const authMode =
      config.authMode ||
      (config.sasl?.username ? 'sasl' : 'none')

    if (authMode === 'oauthbearer') {
      kafkaConfig.sasl = {
        mechanism: 'oauthbearer',
        oauthBearerProvider: async () => {
          const token = await authService.getAccessToken(config.sso)
          if (!token) throw new Error('SSO token missing — login via Keycloak first')
          return { value: token }
        },
      }
    } else if (authMode === 'sasl' && config.sasl?.username) {
      const { mechanism, username, password } = config.sasl
      if (mechanism === 'scram-sha-256') {
        kafkaConfig.sasl = { mechanism: 'scram-sha-256', username, password }
      } else if (mechanism === 'scram-sha-512') {
        kafkaConfig.sasl = { mechanism: 'scram-sha-512', username, password }
      } else {
        kafkaConfig.sasl = { mechanism: 'plain', username, password }
      }
    }

    this.kafka = new Kafka(kafkaConfig)
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

    const headers = await this.schemaRegistryHeaders()
    headers.Accept = 'application/vnd.schemaregistry.v1+json'

    const started = Date.now()
    try {
      const res = await this.schemaRegistryHttp(`${url}/subjects`, headers)
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

  private schemaRegistryHttp(
    url: string,
    headers: Record<string, string>,
  ): Promise<{ status: number; body: string }> {
    const agent = buildSchemaRegistryAgent(this.config)
    return new Promise((resolve, reject) => {
      const u = new URL(url)
      const lib = u.protocol === 'https:' ? https : http
      const req = lib.request(
        u,
        {
          method: 'GET',
          headers,
          agent: u.protocol === 'https:' ? agent : undefined,
          timeout: 10_000,
        },
        (res) => {
          const chunks: Buffer[] = []
          res.on('data', (c) => chunks.push(c))
          res.on('end', () =>
            resolve({
              status: res.statusCode || 0,
              body: Buffer.concat(chunks).toString('utf8'),
            }),
          )
        },
      )
      req.on('error', reject)
      req.on('timeout', () => {
        req.destroy()
        reject(new Error('Schema Registry request timed out'))
      })
      req.end()
    })
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

    const limit = Math.min(
      CONSUME_LIMIT_MAX,
      Math.max(1, Math.trunc(request.limit ?? 25) || 25),
    )
    const startFrom =
      request.startFrom ??
      (request.fromBeginning === false ? 'end' : 'beginning')
    const hasSearch = Boolean(
      request.search?.trim() ||
        request.searchKey?.trim() ||
        request.searchHeaderKey?.trim() ||
        request.searchHeaderValue?.trim() ||
        request.timeFrom != null ||
        request.timeTo != null,
    )
    // Safety cap only: normal exit is "caught up to snapshot high" (below),
    // not this timeout. Filtered scans used to always burn the full cap.
    const waitMs = hasSearch ? 6_000 : 2_500

    const topicOffsets = await this.admin!.fetchTopicOffsets(request.topic)
    const targetPartitions =
      request.partitions?.length
        ? request.partitions
        : topicOffsets.map((o) => Number(o.partition))

    const highByPart = new Map(topicOffsets.map((o) => [Number(o.partition), o]))
    const approxAvailable = targetPartitions.reduce((sum, p) => {
      const o = highByPart.get(p)
      if (!o) return sum
      return sum + Math.max(0, Number(o.high) - Number(o.low))
    }, 0)
    if (approxAvailable === 0 && startFrom !== 'offset') {
      return []
    }

    const groupId = `kafdesk-ui-${Date.now()}`
    this.consumer = this.kafka!.consumer({
      groupId,
      // faster assign for one-shot UI reads
      sessionTimeout: 10_000,
      heartbeatInterval: 2_000,
      maxWaitTimeInMs: 500,
    })
    await this.consumer.connect()
    await this.consumer.subscribe({
      topic: request.topic,
      fromBeginning: startFrom === 'beginning',
    })

    const seekPlan: Array<{ partition: number; offset: string }> = []
    /** Planned first offset per partition — used for caught-up detection */
    const plannedStart = new Map<number, number>()
    if (startFrom === 'offset' && request.startOffset != null && request.startOffset !== '') {
      for (const partition of targetPartitions) {
        seekPlan.push({ partition, offset: String(request.startOffset) })
        plannedStart.set(partition, Number(request.startOffset) || 0)
      }
    } else if (request.timeFrom != null && Number.isFinite(request.timeFrom)) {
      // Seek each partition to first message at/after timeFrom
      try {
        const byTs = await this.admin!.fetchTopicOffsetsByTimestamp(
          request.topic,
          request.timeFrom,
        )
        for (const row of byTs) {
          const partition = Number(row.partition)
          if (!targetPartitions.includes(partition)) continue
          const o = highByPart.get(partition)
          if (!o) continue
          const low = Number(o.low)
          const high = Number(o.high)
          const start = Math.max(low, Math.min(Number(row.offset), high))
          plannedStart.set(partition, start)
          if (start < high) seekPlan.push({ partition, offset: String(start) })
        }
      } catch {
        // fall back to beginning scan + client-side time filter
        for (const partition of targetPartitions) {
          const o = highByPart.get(partition)
          if (o) {
            seekPlan.push({ partition, offset: String(o.low) })
            plannedStart.set(partition, Number(o.low))
          }
        }
      }
    } else if (startFrom === 'end') {
      // Latest window: from each partition tip take up to `limit` messages
      // (not limit/n — that made End ≈ Beginning on small multi-partition topics).
      // After consume we sort by timestamp and keep global `limit`.
      for (const partition of targetPartitions) {
        const o = highByPart.get(partition)
        if (!o) continue
        const high = Number(o.high)
        const low = Number(o.low)
        const start = Math.max(low, high - limit)
        plannedStart.set(partition, start)
        if (start < high) {
          seekPlan.push({ partition, offset: String(start) })
        }
      }
    } else {
      for (const partition of targetPartitions) {
        const o = highByPart.get(partition)
        if (o) plannedStart.set(partition, Number(o.low))
      }
    }

    // Partitions whose window is empty finish immediately
    const finishedPartitions = new Set<number>()
    for (const partition of targetPartitions) {
      const o = highByPart.get(partition)
      if (!o) {
        finishedPartitions.add(partition)
        continue
      }
      const high = Number(o.high)
      const start = plannedStart.get(partition)
      if (high <= Number(o.low) || (start != null && start >= high)) {
        finishedPartitions.add(partition)
      }
    }
    if (finishedPartitions.size >= targetPartitions.length) {
      await this.stopConsumer()
      return []
    }

    if (seekPlan.length) {
      let sought = false
      const seekAll = () => {
        if (sought) return
        sought = true
        for (const s of seekPlan) {
          this.consumer!.seek({
            topic: request.topic,
            partition: s.partition,
            offset: s.offset,
          })
        }
      }
      this.consumer.on(this.consumer.events.GROUP_JOIN, seekAll)
    }

    const messages: KafkaMessageView[] = []
    const filter = {
      search: request.search,
      searchKey: request.searchKey,
      headerKey: request.searchHeaderKey,
      headerValue: request.searchHeaderValue,
      timeFrom: request.timeFrom,
      timeTo: request.timeTo,
    }

    await new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => resolve(), waitMs)
      let settled = false
      const done = () => {
        if (settled) return
        settled = true
        clearTimeout(timeout)
        resolve()
      }
      const checkCaughtUp = () => {
        if (finishedPartitions.size >= targetPartitions.length) done()
      }

      this.consumer!
        .run({
          eachMessage: async ({ topic, partition, message }) => {
            if (request.partitions && !request.partitions.includes(partition)) return

            // Caught-up tracking against the snapshot high watermark,
            // independent of whether this message matches the filter
            const o = highByPart.get(partition)
            if (o && Number(message.offset) >= Number(o.high) - 1) {
              finishedPartitions.add(partition)
            }

            if (!matchesRawPreFilter(message, filter)) {
              checkCaughtUp()
              return
            }

            const view = await this.toMessageView(topic, partition, message)
            if (!matchesSearch(view, filter)) {
              checkCaughtUp()
              return
            }

            messages.push(view)
            // For "end" collect the full tip windows first, then trim by time.
            // Early stop would bias toward whichever partition delivers first.
            if (startFrom !== 'end' && messages.length >= limit) {
              done()
              return
            }
            checkCaughtUp()
          },
        })
        .catch(reject)
    })

    await this.stopConsumer()
    // beginning/offset → oldest first; end → newest first (so Start from is visible)
    const newestFirst = startFrom === 'end'
    return messages
      .sort((a, b) => {
        const ta = Number(a.timestamp)
        const tb = Number(b.timestamp)
        if (ta !== tb) return newestFirst ? tb - ta : ta - tb
        if (a.partition !== b.partition) {
          return newestFirst ? b.partition - a.partition : a.partition - b.partition
        }
        const oa = Number(a.offset)
        const ob = Number(b.offset)
        return newestFirst ? ob - oa : oa - ob
      })
      .slice(0, limit)
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
          if (!matchesSearch(view, filter)) return
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
    const offsets = await this.admin!.fetchOffsets({
      groupId,
      topics,
      resolveOffsets: true,
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
        result.push({
          topic: t.topic,
          partition: p.partition,
          offset: p.offset,
          metadata: p.metadata ?? undefined,
          low: hw?.low,
          high: hw?.high,
          lag: high != null && !Number.isNaN(current) ? Math.max(0, high - current) : undefined,
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
        // Reuse per-partition lag calc; restrict to selected topic when given
        offsets = await this.getGroupOffsets(g.groupId, topic)
      } catch {
        offsets = []
      }
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

    let value: Buffer | string
    let schemaId: number | undefined
    if (request.format === 'avro') {
      if (!this.registry) throw new Error('Schema Registry is not configured')
      if (!request.subject) throw new Error('Avro subject is required')
      const payload = parseJsonLoose(request.value)
      schemaId = await withTimeout(
        this.registry.getLatestSchemaId(request.subject),
        SR_ENCODE_TIMEOUT_MS,
        'Schema Registry getLatestSchemaId',
      )
      value = await withTimeout(
        this.registry.encode(schemaId, payload),
        SR_ENCODE_TIMEOUT_MS,
        'Schema Registry encode',
      )
    } else if (request.format === 'avro-local') {
      if (!request.localSchema?.trim()) throw new Error('Local .avsc schema is required')
      schemaId = request.localSchemaId ?? 0
      value = encodeLocalAvro(request.localSchema, request.value, schemaId)
    } else if (request.format === 'json') {
      // normalize pretty/minified
      const parsed = parseJsonLoose(request.value)
      value = typeof parsed === 'string' ? parsed : JSON.stringify(parsed)
    } else {
      value = request.value
    }

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
    return {
      partition: first.partition,
      offset: first.offset,
      format: request.format,
      schemaId,
    }
  }

  async produceBatch(request: ProduceBatchRequest): Promise<ProduceBatchResult> {
    this.ensureProducer()
    if (!request.messages.length) {
      return { sent: 0, format: request.format }
    }
    if (request.messages.length > 2000) {
      throw new Error('Bulk limit is 2000 messages per run')
    }

    let schemaId: number | undefined
    const encoded: Array<{ key?: string; value: Buffer | string }> = []

    if (request.format === 'avro') {
      if (!this.registry) throw new Error('Schema Registry is not configured')
      if (!request.subject) throw new Error('Avro subject is required')
      schemaId = await withTimeout(
        this.registry.getLatestSchemaId(request.subject),
        SR_ENCODE_TIMEOUT_MS,
        'Schema Registry getLatestSchemaId',
      )
      for (const m of request.messages) {
        const payload = parseJsonLoose(m.value)
        encoded.push({
          key: m.key,
          value: await withTimeout(
            this.registry.encode(schemaId, payload),
            SR_ENCODE_TIMEOUT_MS,
            'Schema Registry encode',
          ),
        })
      }
    } else if (request.format === 'avro-local') {
      if (!request.localSchema?.trim()) throw new Error('Local .avsc schema is required')
      schemaId = request.localSchemaId ?? 0
      for (const m of request.messages) {
        encoded.push({
          key: m.key,
          value: encodeLocalAvro(request.localSchema, m.value, schemaId),
        })
      }
    } else if (request.format === 'json') {
      for (const m of request.messages) {
        const parsed = parseJsonLoose(m.value)
        encoded.push({
          key: m.key,
          value: typeof parsed === 'string' ? parsed : JSON.stringify(parsed),
        })
      }
    } else {
      for (const m of request.messages) {
        encoded.push({ key: m.key, value: m.value })
      }
    }

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
        lastOffset = first.offset
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
    const headers = await this.schemaRegistryHeaders()
    const res = await this.schemaRegistryHttp(`${host}/subjects`, headers)
    if (res.status < 200 || res.status >= 300) {
      throw new Error(`Schema Registry error: ${res.status}`)
    }
    return JSON.parse(res.body) as string[]
  }

  async getLatestSchema(subject: string): Promise<SchemaSubjectInfo> {
    this.ensureRegistry()
    const host = this.config!.schemaRegistryUrl!.replace(/\/$/, '')
    const headers = await this.schemaRegistryHeaders()
    headers.Accept = 'application/vnd.schemaregistry.v1+json'
    const res = await this.schemaRegistryHttp(
      `${host}/subjects/${encodeURIComponent(subject)}/versions/latest`,
      headers,
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

  private async schemaRegistryHeaders(): Promise<Record<string, string>> {
    const headers: Record<string, string> = {}
    const authMode =
      this.config?.authMode || (this.config?.sasl?.username ? 'sasl' : 'none')

    if (authMode === 'oauthbearer') {
      const token = await authService.getAccessToken(this.config?.sso)
      if (token) headers.Authorization = `Bearer ${token}`
      return headers
    }

    if (this.config?.schemaRegistryAuth?.username) {
      const basic = Buffer.from(
        `${this.config.schemaRegistryAuth.username}:${this.config.schemaRegistryAuth.password}`,
      ).toString('base64')
      headers.Authorization = `Basic ${basic}`
    }
    return headers
  }

  private async toMessageView(
    topic: string,
    partition: number,
    message: {
      offset: string
      timestamp: string
      key: Buffer | null
      value: Buffer | null
      headers?: Record<string, Buffer | string | undefined | (Buffer | string)[]>
    },
  ): Promise<KafkaMessageView> {
    const valueBuf = message.value
    const format = detectFormat(valueBuf)
    const schemaId = valueBuf ? extractConfluentSchemaId(valueBuf) : undefined
    let value: unknown = null

    if (valueBuf) {
      if (format === 'avro' && this.registry) {
        try {
          value = await this.registry.decode(valueBuf)
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

  private ensureAdmin() {
    if (!this.admin) throw new Error('Not connected to Kafka')
  }

  private ensureProducer() {
    if (!this.producer) throw new Error('Not connected to Kafka')
  }

  private ensureKafka() {
    if (!this.kafka) throw new Error('Not connected to Kafka')
  }

  private ensureRegistry() {
    if (!this.registry || !this.config?.schemaRegistryUrl) {
      throw new Error('Schema Registry is not configured')
    }
  }
}

interface MessageFilter {
  search?: string
  searchKey?: string
  headerKey?: string
  headerValue?: string
  timeFrom?: number
  timeTo?: number
}

/** Cheap key/header/time check before Avro/JSON decode */
function matchesRawPreFilter(
  message: {
    key: Buffer | null
    timestamp: string
    headers?: Record<string, Buffer | string | undefined | (Buffer | string)[]>
  },
  f: MessageFilter,
): boolean {
  const searchKey = f.searchKey?.trim().toLowerCase()
  if (searchKey) {
    const keyText = (message.key ? message.key.toString('utf8') : '').toLowerCase()
    if (!keyText.includes(searchKey)) return false
  }

  const headerKey = f.headerKey?.trim().toLowerCase()
  const headerValue = f.headerValue?.trim().toLowerCase()
  if (headerKey || headerValue) {
    const entries = Object.entries(message.headers || {})
    const ok = entries.some(([k, v]) => {
      if (v == null) return false
      const lv = (Array.isArray(v)
        ? v.map((x) => (Buffer.isBuffer(x) ? x.toString('utf8') : String(x))).join(',')
        : Buffer.isBuffer(v)
          ? v.toString('utf8')
          : String(v)
      ).toLowerCase()
      if (headerKey && !k.toLowerCase().includes(headerKey)) return false
      if (headerValue && !lv.includes(headerValue)) return false
      return true
    })
    if (!ok) return false
  }

  if (f.timeFrom != null || f.timeTo != null) {
    const raw = Number(message.timestamp)
    if (!Number.isFinite(raw)) return false
    const ts = raw < 1e12 ? raw * 1000 : raw
    if (f.timeFrom != null && ts < f.timeFrom) return false
    if (f.timeTo != null && ts > f.timeTo) return false
  }

  return true
}

function matchesSearch(view: KafkaMessageView, filter: string | MessageFilter, maybeKey?: string): boolean {
  const f: MessageFilter =
    typeof filter === 'string' ? { search: filter, searchKey: maybeKey } : filter
  const search = f.search?.trim().toLowerCase()
  const searchKey = f.searchKey?.trim().toLowerCase()
  const headerKey = f.headerKey?.trim().toLowerCase()
  const headerValue = f.headerValue?.trim().toLowerCase()

  const keyText = (view.key || '').toLowerCase()
  const haystack = `${view.key ?? ''} ${view.rawPreview} ${JSON.stringify(view.value)}`.toLowerCase()
  if (searchKey && !keyText.includes(searchKey)) return false
  if (search && !haystack.includes(search)) return false

  if (headerKey || headerValue) {
    const entries = Object.entries(view.headers || {})
    const ok = entries.some(([k, v]) => {
      const lk = k.toLowerCase()
      const lv = String(v).toLowerCase()
      if (headerKey && !lk.includes(headerKey)) return false
      if (headerValue && !lv.includes(headerValue)) return false
      return true
    })
    if (!ok) return false
  }

  if (f.timeFrom != null || f.timeTo != null) {
    const tsRaw = Number(view.timestamp)
    if (!Number.isFinite(tsRaw)) return false
    const ts = tsRaw < 1e12 ? tsRaw * 1000 : tsRaw
    if (f.timeFrom != null && ts < f.timeFrom) return false
    if (f.timeTo != null && ts > f.timeTo) return false
  }

  return true
}

export const kafkaService = new KafkaService()
