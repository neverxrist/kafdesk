/** Shared desktop IPC surface (Tauri bridge). */
import type {
  AuthLoginRequest,
  AuthProviderInfo,
  AuthSessionPublic,
  ClusterConfig,
  ConnectResult,
  ConsumeRequest,
  ConsumerGroupDetail,
  ConsumerGroupInfo,
  GroupOffsetEntry,
  KafkaMessageView,
  GroupSnapshotView,
  LagSummaryResult,
  PingResult,
  TopicConsumersView,
  ProduceBatchRequest,
  ProduceBatchResult,
  ProduceRequest,
  ProduceResult,
  SchemaRegistryStatus,
  SchemaSubjectInfo,
  SetGroupOffsetsRequest,
  StoredCredentialsInfo,
  TailBatch,
  TailRequest,
  TopicInfo,
  UpdateSettings,
  UpdateStatus,
} from '../types/kafdesk'

export type KafDeskApi = {
  connect: (config: ClusterConfig) => Promise<ConnectResult>
  disconnect: () => Promise<{ ok: boolean }>
  status: () => Promise<{ connected: boolean; schemaRegistry: SchemaRegistryStatus }>
  ping: () => Promise<PingResult>
  probe: (config: ClusterConfig) => Promise<PingResult>
  listTopics: () => Promise<TopicInfo[]>
  getTopicOffsets: (topic: string) => Promise<TopicInfo>
  /** Batched watermarks; unknown topics are omitted */
  topicsOffsets?: (topics: string[]) => Promise<TopicInfo[]>
  /** One message with its full value (list rows are size-capped) */
  fetchMessage?: (topic: string, partition: number, offset: string) => Promise<KafkaMessageView>
  consume: (request: ConsumeRequest) => Promise<KafkaMessageView[]>
  produce: (request: ProduceRequest) => Promise<ProduceResult>
  produceBatch: (request: ProduceBatchRequest) => Promise<ProduceBatchResult>
  pickPemFile: () => Promise<string | null>
  pickAvscFile: () => Promise<{ path: string; text: string } | null>
  /** Native save dialog; returns path or null if cancelled. */
  saveTextFile: (request: {
    contents: string
    defaultPath?: string
    filters?: Array<{ name: string; extensions: string[] }>
  }) => Promise<string | null>
  listSubjects: () => Promise<string[]>
  getLatestSchema: (subject: string) => Promise<SchemaSubjectInfo>
  listConsumerGroups: () => Promise<ConsumerGroupInfo[]>
  getGroupOffsets: (groupId: string, topic?: string) => Promise<GroupOffsetEntry[]>
  describeConsumerGroup: (groupId: string) => Promise<ConsumerGroupDetail>
  getLagSummary: (topic?: string, groupIds?: string[]) => Promise<LagSummaryResult>
  /** null = background slot busy — keep the previous snapshot */
  refreshGroupSnapshot?: () => Promise<GroupSnapshotView | null>
  /** From the last snapshot only; null before the first refresh */
  topicConsumers?: (topic: string) => Promise<TopicConsumersView | null>
  setGroupOffsets: (request: SetGroupOffsetsRequest) => Promise<{ ok: boolean }>
  resetGroupOffsets: (groupId: string, topic: string, earliest: boolean) => Promise<{ ok: boolean }>
  tailStart: (request: TailRequest) => Promise<{ ok: boolean }>
  tailStop: () => Promise<{ ok: boolean }>
  /** Hot-swap filters while live tail is running (no consumer restart). */
  tailSetFilter: (request: TailRequest) => Promise<{ ok: boolean }>
  onTailBatch: (cb: (batch: TailBatch) => void) => () => void
  onTailError: (cb: (message: string) => void) => () => void
  /** Opt-in Rust breadcrumbs for the Logs tab (default off). */
  setBackendDebug: (enabled: boolean) => Promise<{ ok: boolean }>
  getBackendDebug: () => Promise<boolean>
  onBackendDebugLog: (cb: (level: 'info' | 'warn' | 'error', msg: string) => void) => () => void
  listAuthProviders: () => Promise<AuthProviderInfo[]>
  authSession: () => Promise<AuthSessionPublic>
  authLogin: (request: AuthLoginRequest) => Promise<AuthSessionPublic>
  authLogout: () => Promise<{ ok: boolean }>
  storedCredentials: () => Promise<StoredCredentialsInfo>
  forgetCredentials: () => Promise<{ ok: boolean }>
  checkForUpdates: () => Promise<UpdateStatus>
  updateStatus: () => Promise<UpdateStatus>
  updateSettings: () => Promise<UpdateSettings>
  setAutoUpdate: (enabled: boolean) => Promise<UpdateSettings>
  openDownloadedUpdate: () => Promise<void>
  openReleases: () => Promise<void>
  getVersion: () => Promise<string>
  onUpdateStatus: (cb: (s: UpdateStatus) => void) => () => void
}
