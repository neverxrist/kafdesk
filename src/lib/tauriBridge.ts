import { invoke } from '@tauri-apps/api/core'
import { listen, type UnlistenFn } from '@tauri-apps/api/event'
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
  UpdateStatus,
  KafkaMessageView,
} from '../types/kafdesk'

/**
 * React StrictMode mounts → cleans up → remounts before `listen()` resolves.
 * If cleanup runs while `un` is still undefined, the first listener leaks and
 * every event fires twice (classic live-tail duplicate). Cancel after resolve.
 */
function listenSafe<T>(event: string, cb: (payload: T) => void): () => void {
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

/** Install window.kafdesk backed by Tauri invoke/listen (same surface as Electron preload). */
export function installTauriKafdesk(): void {
  const api: KafDeskApi = {
    connect: (config: ClusterConfig) => invoke('kafka_connect', { config }),
    disconnect: () => invoke('kafka_disconnect'),
    status: () => invoke('kafka_status'),
    ping: () => invoke('kafka_ping'),
    probe: (config: ClusterConfig) => invoke('kafka_probe', { config }),
    listTopics: () => invoke('kafka_list_topics'),
    getTopicOffsets: (topic: string) => invoke('kafka_get_topic_offsets', { topic }),
    topicsOffsets: (topics: string[]) => invoke('kafka_topics_offsets', { topics }),
    fetchMessage: (topic: string, partition: number, offset: string) =>
      invoke('kafka_fetch_message', { topic, partition, offset }),
    consume: (request: ConsumeRequest) => invoke('kafka_consume', { request }),
    produce: (request: ProduceRequest) => invoke('kafka_produce', { request }),
    produceBatch: (request: ProduceBatchRequest) => invoke('kafka_produce_batch', { request }),
    pickPemFile: () => invoke('dialog_pick_pem_file'),
    pickAvscFile: () => invoke('dialog_pick_avsc_file'),
    saveTextFile: (request) => invoke('dialog_save_text_file', { request }),
    listSubjects: () => invoke('kafka_list_subjects'),
    getLatestSchema: (subject: string) => invoke('kafka_get_latest_schema', { subject }),
    listConsumerGroups: () => invoke('kafka_list_consumer_groups'),
    getGroupOffsets: (groupId: string, topic?: string) =>
      invoke('kafka_get_group_offsets', { groupId, topic }),
    describeConsumerGroup: (groupId: string) =>
      invoke('kafka_describe_consumer_group', { groupId }),
    getLagSummary: (topic?: string, groupIds?: string[]) =>
      invoke('kafka_get_lag_summary', { topic, groupIds }),
    refreshGroupSnapshot: () => invoke('kafka_refresh_group_snapshot'),
    topicConsumers: (topic: string) => invoke('kafka_topic_consumers', { topic }),
    setGroupOffsets: (request: SetGroupOffsetsRequest) =>
      invoke('kafka_set_group_offsets', { request }),
    resetGroupOffsets: (groupId: string, topic: string, earliest: boolean) =>
      invoke('kafka_reset_group_offsets', { groupId, topic, earliest }),
    tailStart: (request: TailRequest) => invoke('kafka_tail_start', { request }),
    tailStop: () => invoke('kafka_tail_stop'),
    tailSetFilter: (request: TailRequest) => invoke('kafka_tail_set_filter', { request }),
    onTailBatch: (cb: (batch: TailBatch) => void) =>
      listenSafe<TailBatch>('kafka://tail-batch', cb),
    onTailError: (cb: (message: string) => void) =>
      listenSafe<string>('kafka://tail-error', cb),
    setBackendDebug: (enabled: boolean) => invoke('set_backend_debug', { enabled }),
    getBackendDebug: () => invoke('get_backend_debug'),
    onBackendDebugLog: (cb: (level: 'info' | 'warn' | 'error', msg: string) => void) =>
      listenSafe<{ level?: string; msg?: string }>('kafka://debug-log', (payload) => {
        const level =
          payload.level === 'warn' || payload.level === 'error' ? payload.level : 'info'
        cb(level, payload.msg ?? '')
      }),
    listAuthProviders: () => invoke('auth_list_providers'),
    authSession: () => invoke('auth_session'),
    authLogin: (request: AuthLoginRequest) => invoke('auth_login', { request }),
    authLogout: () => invoke('auth_logout'),
    storedCredentials: () => invoke('auth_stored_credentials'),
    forgetCredentials: () => invoke('auth_forget_credentials'),
    checkForUpdates: () => invoke('app_check_for_updates'),
    updateStatus: () => invoke('app_update_status'),
    updateSettings: () => invoke('app_update_settings'),
    setAutoUpdate: (enabled: boolean) => invoke('app_set_auto_update', { enabled }),
    openDownloadedUpdate: () => invoke('app_open_downloaded_update'),
    openReleases: () => invoke('app_open_releases'),
    getVersion: () => invoke('app_get_version'),
    onUpdateStatus: (cb: (s: UpdateStatus) => void) =>
      listenSafe<UpdateStatus>('app://update-status', cb),
  }

  ;(window as Window & { kafdesk: KafDeskApi }).kafdesk = api
}
