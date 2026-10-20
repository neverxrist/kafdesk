import { contextBridge, ipcRenderer, type IpcRendererEvent } from 'electron'
import type {
  AuthLoginRequest,
  AuthProviderInfo,
  AuthSessionPublic,
  ClusterConfig,
  ConnectResult,
  ConsumeRequest,
  ConsumerGroupInfo,
  GroupOffsetEntry,
  KafkaMessageView,
  LagSummaryResult,
  PingResult,
  ProduceRequest,
  ProduceResult,
  SchemaRegistryStatus,
  SchemaSubjectInfo,
  SetGroupOffsetsRequest,
  StoredCredentialsInfo,
  TailRequest,
  TopicInfo,
  UpdateSettings,
  UpdateStatus,
} from './types'

const api = {
  connect: (config: ClusterConfig) =>
    ipcRenderer.invoke('kafka:connect', config) as Promise<ConnectResult>,
  disconnect: () => ipcRenderer.invoke('kafka:disconnect') as Promise<{ ok: boolean }>,
  status: () =>
    ipcRenderer.invoke('kafka:status') as Promise<{
      connected: boolean
      schemaRegistry: SchemaRegistryStatus
    }>,
  ping: () => ipcRenderer.invoke('kafka:ping') as Promise<PingResult>,
  listTopics: () => ipcRenderer.invoke('kafka:listTopics') as Promise<TopicInfo[]>,
  getTopicOffsets: (topic: string) =>
    ipcRenderer.invoke('kafka:getTopicOffsets', topic) as Promise<TopicInfo>,
  consume: (request: ConsumeRequest) =>
    ipcRenderer.invoke('kafka:consume', request) as Promise<KafkaMessageView[]>,
  produce: (request: ProduceRequest) =>
    ipcRenderer.invoke('kafka:produce', request) as Promise<ProduceResult>,
  listSubjects: () => ipcRenderer.invoke('kafka:listSubjects') as Promise<string[]>,
  getLatestSchema: (subject: string) =>
    ipcRenderer.invoke('kafka:getLatestSchema', subject) as Promise<SchemaSubjectInfo>,
  listConsumerGroups: () =>
    ipcRenderer.invoke('kafka:listConsumerGroups') as Promise<ConsumerGroupInfo[]>,
  getGroupOffsets: (groupId: string, topic?: string) =>
    ipcRenderer.invoke('kafka:getGroupOffsets', groupId, topic) as Promise<GroupOffsetEntry[]>,
  getLagSummary: (topic?: string) =>
    ipcRenderer.invoke('kafka:getLagSummary', topic) as Promise<LagSummaryResult>,
  setGroupOffsets: (request: SetGroupOffsetsRequest) =>
    ipcRenderer.invoke('kafka:setGroupOffsets', request) as Promise<{ ok: boolean }>,
  resetGroupOffsets: (groupId: string, topic: string, earliest: boolean) =>
    ipcRenderer.invoke('kafka:resetGroupOffsets', groupId, topic, earliest) as Promise<{
      ok: boolean
    }>,
  tailStart: (request: TailRequest) =>
    ipcRenderer.invoke('kafka:tailStart', request) as Promise<{ ok: boolean }>,
  tailStop: () => ipcRenderer.invoke('kafka:tailStop') as Promise<{ ok: boolean }>,
  onTailMessage: (cb: (msg: KafkaMessageView) => void) => {
    const listener = (_event: IpcRendererEvent, msg: KafkaMessageView) => cb(msg)
    ipcRenderer.on('kafka:tailMessage', listener)
    return () => {
      ipcRenderer.removeListener('kafka:tailMessage', listener)
    }
  },
  listAuthProviders: () =>
    ipcRenderer.invoke('auth:listProviders') as Promise<AuthProviderInfo[]>,
  authSession: () => ipcRenderer.invoke('auth:session') as Promise<AuthSessionPublic>,
  authLogin: (request: AuthLoginRequest) =>
    ipcRenderer.invoke('auth:login', request) as Promise<AuthSessionPublic>,
  authLogout: () => ipcRenderer.invoke('auth:logout') as Promise<{ ok: boolean }>,
  storedCredentials: () =>
    ipcRenderer.invoke('auth:storedCredentials') as Promise<StoredCredentialsInfo>,
  forgetCredentials: () =>
    ipcRenderer.invoke('auth:forgetCredentials') as Promise<{ ok: boolean }>,
  checkForUpdates: () => ipcRenderer.invoke('app:checkForUpdates') as Promise<UpdateStatus>,
  updateStatus: () => ipcRenderer.invoke('app:updateStatus') as Promise<UpdateStatus>,
  updateSettings: () => ipcRenderer.invoke('app:updateSettings') as Promise<UpdateSettings>,
  setAutoUpdate: (enabled: boolean) =>
    ipcRenderer.invoke('app:setAutoUpdate', enabled) as Promise<UpdateSettings>,
  onUpdateStatus: (cb: (s: UpdateStatus) => void) => {
    const listener = (_event: IpcRendererEvent, s: UpdateStatus) => cb(s)
    ipcRenderer.on('app:updateStatus', listener)
    return () => {
      ipcRenderer.removeListener('app:updateStatus', listener)
    }
  },
}

contextBridge.exposeInMainWorld('kafdesk', api)

export type KafDeskApi = typeof api
