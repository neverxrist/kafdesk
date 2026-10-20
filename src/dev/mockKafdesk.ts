import type { KafDeskApi } from '../../electron/preload'
import type {
  AuthLoginRequest,
  AuthSessionPublic,
  ClusterConfig,
  ConsumeRequest,
  KafkaMessageView,
  ProduceRequest,
  SchemaSubjectInfo,
  TopicInfo,
} from '../../electron/types'

/** Browser-only stub so Vite UI can be exercised without Electron IPC. */
export function installMockKafdesk(): void {
  if (typeof window === 'undefined' || window.kafdesk) return

  let connected = false
  let config: ClusterConfig | null = null
  const auth: AuthSessionPublic = { providerId: 'keycloak', loggedIn: false }
  const tailListeners = new Set<(m: KafkaMessageView) => void>()

  const topics: TopicInfo[] = [
    {
      name: 'demo-json',
      partitions: [
        { partitionId: 0, leader: 0, replicas: [0], isr: [0], offsetLow: '0', offsetHigh: '12', approxMessages: 12 },
        { partitionId: 1, leader: 0, replicas: [0], isr: [0], offsetLow: '0', offsetHigh: '8', approxMessages: 8 },
        { partitionId: 2, leader: 0, replicas: [0], isr: [0], offsetLow: '0', offsetHigh: '5', approxMessages: 5 },
      ],
      approxMessages: 25,
    },
    {
      name: 'demo-avro',
      partitions: [
        { partitionId: 0, leader: 0, replicas: [0], isr: [0], offsetLow: '0', offsetHigh: '3', approxMessages: 3 },
      ],
      approxMessages: 3,
    },
    {
      name: 'demo-string',
      partitions: [
        { partitionId: 0, leader: 0, replicas: [0], isr: [0], offsetLow: '0', offsetHigh: '2', approxMessages: 2 },
      ],
      approxMessages: 2,
    },
    {
      name: 'orders.dlq',
      partitions: [
        { partitionId: 0, leader: 0, replicas: [0], isr: [0], offsetLow: '0', offsetHigh: '4', approxMessages: 4 },
      ],
      approxMessages: 4,
    },
    {
      name: 'payments.dlt',
      partitions: [
        { partitionId: 0, leader: 0, replicas: [0], isr: [0], offsetLow: '0', offsetHigh: '1', approxMessages: 1 },
      ],
      approxMessages: 1,
    },
    {
      name: 'user-events',
      partitions: [
        { partitionId: 0, leader: 0, replicas: [0], isr: [0], offsetLow: '0', offsetHigh: '40', approxMessages: 40 },
      ],
      approxMessages: 40,
    },
  ]

  const subjects = ['demo-avro-value', 'user-events-value']
  const schema: SchemaSubjectInfo = {
    subject: 'demo-avro-value',
    version: 1,
    id: 1,
    schemaType: 'AVRO',
    schema: JSON.stringify({
      type: 'record',
      name: 'DemoEvent',
      fields: [
        { name: 'id', type: 'string' },
        { name: 'count', type: 'int' },
        { name: 'active', type: 'boolean' },
      ],
    }),
  }

  function sampleMessages(topic: string, limit: number): KafkaMessageView[] {
    const now = Date.now()
    const rows: KafkaMessageView[] = []
    for (let i = 0; i < Math.min(limit, 6); i++) {
      const partition = i % 2
      const offset = String(100 + i)
      if (topic.includes('avro')) {
        rows.push({
          topic,
          partition,
          offset,
          timestamp: String(now - i * 60_000),
          key: `k-${i}`,
          value: { id: `demo-${i}`, count: i, active: i % 2 === 0 },
          rawPreview: '',
          format: 'avro',
          schemaId: 1,
          headers: i === 0 ? { source: 'mock' } : {},
        })
      } else if (topic.includes('dlq') || topic.includes('dlt')) {
        rows.push({
          topic,
          partition: 0,
          offset,
          timestamp: String(now - i * 90_000),
          key: `fail-${i}`,
          value: { error: 'processing failed', payload: { id: i }, attempt: i + 1 },
          rawPreview: '',
          format: 'json',
          headers: { 'x-exception': 'TimeoutException' },
        })
      } else {
        rows.push({
          topic,
          partition,
          offset,
          timestamp: String(now - i * 45_000),
          key: `user-${i}`,
          value: { hello: 'world', n: i, changed: i === 1 ? 'A' : 'B' },
          rawPreview: '',
          format: 'json',
          headers: {},
        })
      }
    }
    return rows
  }

  const api: KafDeskApi = {
    status: async () => ({
      connected,
      schemaRegistry: connected && config?.schemaRegistryUrl ? 'ok' : 'none',
    }),
    connect: async (cfg) => {
      config = cfg
      connected = true
      return {
        ok: true,
        schemaRegistry: cfg.schemaRegistryUrl?.trim() ? 'ok' : 'none',
      }
    },
    disconnect: async () => {
      connected = false
      return { ok: true }
    },
    ping: async () => ({
      kafka: { ok: connected, ms: 12 },
      schemaRegistry: {
        status: connected && config?.schemaRegistryUrl ? 'ok' : 'none',
        ms: 8,
      },
    }),
    probe: async (cfg) => ({
      kafka: { ok: true, ms: 12 },
      schemaRegistry: {
        status: cfg.schemaRegistryUrl?.trim() ? 'ok' : 'none',
        ms: 8,
      },
    }),
    listTopics: async () => topics,
    getTopicOffsets: async (topic) => {
      const t = topics.find((x) => x.name === topic)
      if (!t) throw new Error(`Unknown topic ${topic}`)
      return t
    },
    consume: async (req: ConsumeRequest) => sampleMessages(req.topic, req.limit ?? 25),
    produce: async (req: ProduceRequest) => ({
      partition: 0,
      offset: String(Date.now() % 10_000),
      format: req.format,
      schemaId: req.format === 'avro' ? 1 : undefined,
    }),
    listSubjects: async () => subjects,
    getLatestSchema: async (subject) => ({ ...schema, subject }),
    listConsumerGroups: async () => [
      { groupId: 'demo-consumer', protocolType: 'consumer', state: 'Stable', members: 1, totalLag: 3 },
    ],
    getGroupOffsets: async () => [
      { topic: 'demo-json', partition: 0, offset: '10', high: '12', low: '0', lag: 2 },
    ],
    setGroupOffsets: async () => ({ ok: true }),
    resetGroupOffsets: async () => ({ ok: true }),
    getLagSummary: async () => ({
      groups: [
        {
          groupId: 'demo-consumer',
          totalLag: 3,
          maxPartitionLag: 2,
          topics: 1,
          partitions: 1,
          worstTopic: 'demo-json',
          worstPartition: 0,
        },
      ],
      totalLag: 3,
      maxLag: 2,
      updatedAt: Date.now(),
    }),
    tailStart: async () => ({ ok: true }),
    tailStop: async () => ({ ok: true }),
    onTailMessage: (cb) => {
      tailListeners.add(cb)
      return () => tailListeners.delete(cb)
    },
    listAuthProviders: async () => [],
    authSession: async () => auth,
    authLogin: async (_req: AuthLoginRequest) => auth,
    authLogout: async () => ({ ok: true }),
    storedCredentials: async () => ({ hasPassword: false }),
    forgetCredentials: async () => ({ ok: true }),
    checkForUpdates: async () => ({ state: 'dev' as const }),
    updateStatus: async () => ({ state: 'dev' as const }),
    updateSettings: async () => ({ autoUpdate: true }),
    setAutoUpdate: async () => ({ autoUpdate: true }),
    onUpdateStatus: () => () => undefined,
  }

  window.kafdesk = api
  console.info('[kafdesk] browser mock API installed')
}
