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
  TailRequest,
  UpdateStatus,
  KafkaMessageView,
} from '../types/kafdesk'

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
    consume: (request: ConsumeRequest) => invoke('kafka_consume', { request }),
    produce: (request: ProduceRequest) => invoke('kafka_produce', { request }),
    produceBatch: (request: ProduceBatchRequest) => invoke('kafka_produce_batch', { request }),
    pickPemFile: () => invoke('dialog_pick_pem_file'),
    pickAvscFile: () => invoke('dialog_pick_avsc_file'),
    listSubjects: () => invoke('kafka_list_subjects'),
    getLatestSchema: (subject: string) => invoke('kafka_get_latest_schema', { subject }),
    listConsumerGroups: () => invoke('kafka_list_consumer_groups'),
    getGroupOffsets: (groupId: string, topic?: string) =>
      invoke('kafka_get_group_offsets', { groupId, topic }),
    getLagSummary: (topic?: string) => invoke('kafka_get_lag_summary', { topic }),
    setGroupOffsets: (request: SetGroupOffsetsRequest) =>
      invoke('kafka_set_group_offsets', { request }),
    resetGroupOffsets: (groupId: string, topic: string, earliest: boolean) =>
      invoke('kafka_reset_group_offsets', { groupId, topic, earliest }),
    tailStart: (request: TailRequest) => invoke('kafka_tail_start', { request }),
    tailStop: () => invoke('kafka_tail_stop'),
    onTailMessage: (cb: (msg: KafkaMessageView) => void) => {
      let un: UnlistenFn | undefined
      void listen<KafkaMessageView>('kafka://tail-message', (e) => cb(e.payload)).then((u) => {
        un = u
      })
      return () => {
        un?.()
      }
    },
    onTailError: (cb: (message: string) => void) => {
      let un: UnlistenFn | undefined
      void listen<string>('kafka://tail-error', (e) => cb(e.payload)).then((u) => {
        un = u
      })
      return () => {
        un?.()
      }
    },
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
    getVersion: () => invoke('app_get_version'),
    onUpdateStatus: (cb: (s: UpdateStatus) => void) => {
      let un: UnlistenFn | undefined
      void listen<UpdateStatus>('app://update-status', (e) => cb(e.payload)).then((u) => {
        un = u
      })
      return () => {
        un?.()
      }
    },
  }

  ;(window as Window & { kafdesk: KafDeskApi }).kafdesk = api
}
