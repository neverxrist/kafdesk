import { invoke } from '@tauri-apps/api/core'
import { listen, type UnlistenFn } from '@tauri-apps/api/event'
import { commandAllowed } from './faceControl'
import type { KafDeskApi } from './kafdeskApi'
import type {
  AuthLoginRequest,
  ClusterConfig,
  ConsumeRequest,
  ProduceBatchRequest,
  ProduceRequest,
  SetGroupOffsetsRequest,
  TailBatch,
  TailRequest,
  TraceProgress,
  TraceRequest,
  UpdateStatus,
  KafkaMessageView,
} from '../types/kafdesk'

/**
 * React StrictMode mounts → cleans up → remounts before `listen()` resolves.
 * If cleanup runs while `un` is still undefined, the first listener leaks and
 * every event fires twice (classic live-tail duplicate). Cancel after resolve.
 */
function on<T>(event: string, cb: (payload: T) => void): () => void {
  let alive = true
  let un: UnlistenFn | undefined
  void listen<T>(event, (e) => {
    if (alive) cb(e.payload)
  }).then((u) => {
    if (!alive) {
      u()
      return
    }
    un = u
  })
  return () => {
    alive = false
    un?.()
  }
}

type Call = <T>(cmd: string, args?: Record<string, unknown>) => Promise<T>
type OnEvent = <T>(event: string, cb: (payload: T) => void) => () => void

/** Same command map for the desktop window and the localhost page. */
export function buildKafdeskApi(call: Call, on: OnEvent): KafDeskApi {
  return {
    connect: (config: ClusterConfig) => call('kafka_connect', { config }),
    disconnect: () => call('kafka_disconnect'),
    status: () => call('kafka_status'),
    ping: () => call('kafka_ping'),
    probe: (config: ClusterConfig) => call('kafka_probe', { config }),
    listTopics: () => call('kafka_list_topics'),
    getTopicOffsets: (topic: string) => call('kafka_get_topic_offsets', { topic }),
    topicsOffsets: (topics: string[]) => call('kafka_topics_offsets', { topics }),
    fetchMessage: (topic: string, partition: number, offset: string) =>
      call('kafka_fetch_message', { topic, partition, offset }),
    consume: (request: ConsumeRequest) => call('kafka_consume', { request }),
    produce: (request: ProduceRequest) => call('kafka_produce', { request }),
    produceBatch: (request: ProduceBatchRequest) => call('kafka_produce_batch', { request }),
    pickPemFile: () => call('dialog_pick_pem_file'),
    pickAvscFile: () => call('dialog_pick_avsc_file'),
    saveTextFile: (request) => call('dialog_save_text_file', { request }),
    listSubjects: () => call('kafka_list_subjects'),
    getLatestSchema: (subject: string) => call('kafka_get_latest_schema', { subject }),
    listConsumerGroups: () => call('kafka_list_consumer_groups'),
    getGroupOffsets: (groupId: string, topic?: string) =>
      call('kafka_get_group_offsets', { groupId, topic }),
    describeConsumerGroup: (groupId: string) =>
      call('kafka_describe_consumer_group', { groupId }),
    getLagSummary: (topic?: string, groupIds?: string[]) =>
      call('kafka_get_lag_summary', { topic, groupIds }),
    refreshGroupSnapshot: () => call('kafka_refresh_group_snapshot'),
    topicConsumers: (topic: string) => call('kafka_topic_consumers', { topic }),
    setGroupOffsets: (request: SetGroupOffsetsRequest) =>
      call('kafka_set_group_offsets', { request }),
    resetGroupOffsets: (groupId: string, topic: string, earliest: boolean) =>
      call('kafka_reset_group_offsets', { groupId, topic, earliest }),
    tailStart: (request: TailRequest) => call('kafka_tail_start', { request }),
    tailStop: () => call('kafka_tail_stop'),
    tailSetFilter: (request: TailRequest) => call('kafka_tail_set_filter', { request }),
    onTailBatch: (cb: (batch: TailBatch) => void) =>
      on<TailBatch>('kafka://tail-batch', cb),
    onTailError: (cb: (message: string) => void) =>
      on<string>('kafka://tail-error', cb),
    traceKey: (request: TraceRequest) => call('kafka_trace_key', { request }),
    traceCancel: () => call('kafka_trace_cancel'),
    onTraceProgress: (cb: (progress: TraceProgress) => void) =>
      on<TraceProgress>('kafka://trace-progress', cb),
    setBackendDebug: (enabled: boolean) => call('set_backend_debug', { enabled }),
    getBackendDebug: () => call('get_backend_debug'),
    onBackendDebugLog: (cb: (level: 'info' | 'warn' | 'error', msg: string) => void) =>
      on<{ level?: string; msg?: string }>('kafka://debug-log', (payload) => {
        const level =
          payload.level === 'warn' || payload.level === 'error' ? payload.level : 'info'
        cb(level, payload.msg ?? '')
      }),
    listAuthProviders: () => call('auth_list_providers'),
    authSession: () => call('auth_session'),
    authLogin: (request: AuthLoginRequest) => call('auth_login', { request }),
    authLogout: () => call('auth_logout'),
    storedCredentials: () => call('auth_stored_credentials'),
    forgetCredentials: () => call('auth_forget_credentials'),
    checkForUpdates: () => call('app_check_for_updates'),
    updateStatus: () => call('app_update_status'),
    updateSettings: () => call('app_update_settings'),
    setAutoUpdate: (enabled: boolean) => call('app_set_auto_update', { enabled }),
    openDownloadedUpdate: () => call('app_open_downloaded_update'),
    openReleases: () => call('app_open_releases'),
    getVersion: () => call('app_get_version'),
    webFaceStatus: () => call('web_face_status'),
    webFacePutClientState: (raw) => call('web_face_put_client_state', { raw }),
    webFaceSetOwner: (owner, openBrowser) => call('web_face_set_owner', { owner, openBrowser }),
    onUpdateStatus: (cb: (s: UpdateStatus) => void) =>
      on<UpdateStatus>('app://update-status', cb),
  }
}

/** Install window.kafdesk backed by Tauri invoke/listen. */
export function installTauriKafdesk(): void {
  const call = <T,>(cmd: string, args?: Record<string, unknown>) => {
    if (!commandAllowed(cmd)) return Promise.reject(new Error('FACE_LOCKED'))
    return invoke<T>(cmd, args)
  }
  window.kafdesk = buildKafdeskApi(call, on)
}
