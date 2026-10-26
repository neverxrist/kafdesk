export type MessageFormat = 'avro' | 'json' | 'string' | 'bytes'

/** How KafDesk authenticates to Kafka / SR for this connection */
export type ConnectionAuthMode = 'none' | 'sasl' | 'oauthbearer'

/** Built-in + future SSO providers */
export type AuthProviderId = 'keycloak' | 'oidc' | string

export interface KeycloakSsoConfig {
  url: string
  realm: string
  clientId: string
}

/** Generic OIDC (extension slot — same PKCE flow later) */
export interface OidcSsoConfig {
  issuer: string
  clientId: string
  scopes?: string
}

export interface ConnectionSsoConfig {
  providerId: AuthProviderId
  keycloak?: KeycloakSsoConfig
  oidc?: OidcSsoConfig
}

/** PEM paths on disk for Kafka broker SSL and/or Schema Registry HTTPS. */
export interface TlsPemConfig {
  caPath?: string
  certPath?: string
  keyPath?: string
  /** default true; set false for lab / self-signed without CA */
  rejectUnauthorized?: boolean
}

/** Soft env badge on a saved connection (avoids mixing prod/stage). */
export type ConnectionEnvTag = 'prod' | 'stage' | 'dev' | 'other'

export interface ClusterConfig {
  id: string
  name: string
  brokers: string[]
  clientId?: string
  ssl?: boolean
  /** default: sasl if username set, else none */
  authMode?: ConnectionAuthMode
  sasl?: {
    mechanism: 'plain' | 'scram-sha-256' | 'scram-sha-512'
    username: string
    password: string
  }
  schemaRegistryUrl?: string
  schemaRegistryAuth?: {
    username: string
    password: string
  }
  /** Kafka broker SSL PEMs (when `ssl` is on). */
  kafkaTls?: TlsPemConfig
  /**
   * Schema Registry HTTPS PEMs when not reusing Kafka TLS.
   * Legacy: older saves stored shared PEMs only here — still read as Kafka fallback.
   */
  schemaRegistryTls?: TlsPemConfig
  /**
   * Opt-in: when true, SR HTTPS uses `kafkaTls` (or legacy `schemaRegistryTls`).
   * When false/omitted, SR uses its own `schemaRegistryTls` paths.
   */
  schemaRegistryReuseKafkaTls?: boolean
  /** Optional env badge: prod / stage / dev / other */
  envTag?: ConnectionEnvTag
  /** SSO settings for this connection (Keycloak now, more later) */
  sso?: ConnectionSsoConfig
}

export interface AuthProviderInfo {
  id: AuthProviderId
  label: string
  /** false = shown in UI but not implemented yet */
  implemented: boolean
  description?: string
}

export interface AuthSessionPublic {
  providerId: AuthProviderId
  username?: string
  email?: string
  expiresAt?: number
  loggedIn: boolean
}

export interface AuthLoginRequest {
  providerId: AuthProviderId
  keycloak?: KeycloakSsoConfig
  oidc?: OidcSsoConfig
  /** default: browser PKCE. 'password' = direct grant with login/password */
  method?: 'browser' | 'password'
  username?: string
  password?: string
  rememberPassword?: boolean
}

export interface StoredCredentialsInfo {
  username?: string
  hasPassword: boolean
}

export interface TopicInfo {
  name: string
  partitions: PartitionInfo[]
  /** approx messages = sum(high - low) */
  approxMessages?: number
}

export interface PartitionInfo {
  partitionId: number
  leader: number
  replicas: number[]
  isr: number[]
  offsetLow?: string
  offsetHigh?: string
  /** approx messages in partition = max(0, high - low) */
  approxMessages?: number
}

export interface KafkaMessageView {
  topic: string
  partition: number
  offset: string
  timestamp: string
  key: string | null
  value: unknown
  rawPreview: string
  format: MessageFormat
  headers: Record<string, string>
  schemaId?: number
  /** Original payload size when `value` is a capped preview */
  truncatedBytes?: number
}

export type ConsumeStartFrom = 'beginning' | 'end' | 'offset'

export interface ConsumeRequest {
  topic: string
  /** @deprecated use startFrom */
  fromBeginning?: boolean
  startFrom?: ConsumeStartFrom
  /** used when startFrom === 'offset' */
  startOffset?: string
  limit?: number
  search?: string
  searchKey?: string
  searchHeaderKey?: string
  searchHeaderValue?: string
  /** epoch ms, inclusive */
  timeFrom?: number
  /** epoch ms, inclusive */
  timeTo?: number
  partitions?: number[]
  /** From a prior getTopicOffsets — avoids a second watermark round-trip on refresh */
  partitionWatermarks?: Array<{ partition: number; low: number; high: number }>
}

export interface ConsumerGroupInfo {
  groupId: string
  protocolType: string
  state?: string
  members?: number
  totalLag?: number
  maxPartitionLag?: number
  lagTopics?: number
  lagPartitions?: number
  topicNames?: string[]
}

export interface GroupMemberAssignment {
  topic: string
  partition: number
}

export interface GroupMemberInfo {
  memberId: string
  clientId: string
  host: string
  assignment: GroupMemberAssignment[]
}

export interface ConsumerGroupDetail {
  groupId: string
  state?: string
  memberCount: number
  coordinatorId?: number
  members: GroupMemberInfo[]
  topics: string[]
}

export interface GroupLagSummary {
  groupId: string
  totalLag: number
  maxPartitionLag: number
  topics: number
  partitions: number
  worstTopic?: string
  worstPartition?: number
  topicNames?: string[]
}

export interface LagSummaryResult {
  groups: GroupLagSummary[]
  totalLag: number
  maxLag: number
  updatedAt: number
  /** list_consumer_groups size at start of this lag walk */
  listed?: number
  /** groups we queried offsets for before the time cap */
  scanned?: number
  /** walk stopped early (6s) — lag totals incomplete */
  partial?: boolean
}

export type LagTrend = 'up' | 'down' | 'flat'

export interface SnapshotGroupRow {
  groupId: string
  state?: string | null
  members: number
  /** null when committed offsets could not be read (see error) */
  totalLag: number | null
  maxPartitionLag: number
  topics: number
  topicNames: string[]
  trend?: LagTrend | null
  /** lag > 0, committed frozen, log end growing over recent snapshots */
  stalled: boolean
  /** total lag per snapshot, oldest first (≤ 20) */
  history: number[]
  error?: string | null
}

export interface TopicLagRow {
  topic: string
  groups: number
  totalLag: number
}

export interface GroupSnapshotView {
  updatedAt: number
  durationMs: number
  listed: number
  scanned: number
  failed: number
  partial: boolean
  groups: SnapshotGroupRow[]
  topics: TopicLagRow[]
}

export interface TopicConsumerRow {
  groupId: string
  state?: string | null
  members: number
  lag: number | null
  maxPartitionLag: number
  trend?: LagTrend | null
  stalled: boolean
  history: number[]
  partitions: GroupOffsetEntry[]
}

export interface TopicConsumersView {
  topic: string
  updatedAt: number
  partial: boolean
  groups: TopicConsumerRow[]
}

export interface GroupOffsetEntry {
  topic: string
  partition: number
  offset: string
  high?: string
  low?: string
  lag?: number
  metadata?: string
}

export interface SetGroupOffsetsRequest {
  groupId: string
  topic: string
  partitions: Array<{ partition: number; offset: string }>
}

export interface TailResumeOffset {
  partition: number
  offset: number
}

/** Live tail flush (oldest → newest); `dropped` = evicted because rate exceeded the cap */
export interface TailBatch {
  messages: KafkaMessageView[]
  dropped: number
}

export interface TraceRequest {
  needle: string
  topics: string[]
  fromMs: number
  scanValues: boolean
}

export interface TraceHit {
  topic: string
  partition: number
  offset: string
  timestamp: string
  key: string | null
  matched: 'key' | 'value' | string
  preview: string
  fields: string[]
}

export interface TraceTopicStat {
  topic: string
  scanned: number
  hits: number
  capped: boolean
  keyPartition: number
  partitions: number
  error?: string
}

export interface TraceResult {
  hits: TraceHit[]
  topics: TraceTopicStat[]
  partial: boolean
  cancelled: boolean
  durationMs: number
}

export interface TraceProgress {
  topic: string
  done: number
  total: number
  hits: number
}

export interface TailRequest {
  topic: string
  partitions?: number[]
  search?: string
  searchKey?: string
  /** both | key | value — how `search` / `searchKey` apply */
  searchScope?: 'both' | 'key' | 'value'
  searchHeaderKey?: string
  searchHeaderValue?: string
  /** Resume after Stop (offset to read next). Absent → latest. */
  resumeOffsets?: TailResumeOffset[]
}

export interface UpdateStatus {
  state:
    | 'idle'
    | 'checking'
    | 'available'
    | 'not-available'
    | 'downloading'
    | 'downloaded'
    /** Newer release exists; open GitHub Releases (Win/Linux — no in-app install) */
    | 'manual'
    | 'error'
    | 'dev'
    | 'disabled'
    /** Legacy: unsigned Mac now uses DMG updater; kept for UI compat */
    | 'unsigned'
  version?: string
  percent?: number
  message?: string
}

export interface UpdateSettings {
  autoUpdate: boolean
}

export type ProduceFormat = 'avro' | 'avro-local' | 'json' | 'string'

export interface ProduceRequest {
  topic: string
  key?: string
  value: string
  format: ProduceFormat
  subject?: string
  /** Required for format === 'avro-local' (.avsc JSON text) */
  localSchema?: string
  /** Optional Confluent schema id embedded in wire format (default 0) */
  localSchemaId?: number
  partition?: number
  headers?: Record<string, string>
}

export interface ProduceResult {
  partition: number
  offset?: string
  format: ProduceFormat
  schemaId?: number
}

export interface ProduceBatchRequest {
  topic: string
  format: ProduceFormat
  subject?: string
  localSchema?: string
  localSchemaId?: number
  /** Fixed partition for all messages; omit for broker default (key / round-robin) */
  partition?: number
  /** Same headers on every message; omit or {} → no headers */
  headers?: Record<string, string>
  messages: Array<{ key?: string; value: string }>
}

export interface ProduceBatchResult {
  sent: number
  format: ProduceFormat
  schemaId?: number
  lastPartition?: number
  lastOffset?: string
}

export interface SchemaSubjectInfo {
  subject: string
  version: number
  id: number
  schemaType?: string
  schema: string
}

export type SchemaRegistryStatus = 'ok' | 'fail' | 'none'

export interface ConnectResult {
  ok: boolean
  schemaRegistry: SchemaRegistryStatus
  schemaRegistryError?: string
  /** Advertised broker DNS problem (unresolved or collapsed). */
  brokerDnsWarning?: string
}

export interface PingResult {
  kafka: { ok: boolean; ms?: number; error?: string }
  schemaRegistry: { status: SchemaRegistryStatus; ms?: number; error?: string }
  /** Advertised-broker DNS note from the same probe, if any. */
  dnsWarning?: string
}
