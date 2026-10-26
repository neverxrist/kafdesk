import { buildKafdeskApi } from './tauriBridge'
import type { WebFaceStatus } from './kafdeskApi'
import { applyKafdeskStorage, commandAllowed, setFaceBrowser, setFaceOwner } from './faceControl'

const EVENTS = [
  'kafka://tail-batch',
  'kafka://tail-error',
  'kafka://trace-progress',
  'kafka://debug-log',
  'app://update-status',
  'face://client-state',
] as const

type Handler = (payload: unknown) => void

function readToken(): string {
  const fromQuery = new URLSearchParams(location.search).get('k') || ''
  if (fromQuery) sessionStorage.setItem('kafdesk.faceToken', fromQuery)
  return fromQuery || sessionStorage.getItem('kafdesk.faceToken') || ''
}

export async function preloadWebFace(): Promise<void> {
  setFaceBrowser(true)
  const token = readToken()
  if (!token) {
    setFaceOwner('desktop')
    return
  }
  try {
    const res = await fetch('/api/face', { headers: { 'X-Kafdesk-Token': token } })
    if (!res.ok) {
      setFaceOwner('desktop')
      return
    }
    const status = (await res.json()) as WebFaceStatus
    setFaceOwner(status.owner === 'web' ? 'web' : 'desktop')
    const stateRes = await fetch('/api/client-state', { headers: { 'X-Kafdesk-Token': token } })
    if (stateRes.ok) applyKafdeskStorage(await stateRes.json())
  } catch {
    setFaceOwner('desktop')
  }
}

/** Same UI, commands go to the desktop process over localhost. */
export function installHttpKafdesk(): void {
  const token = readToken()
  const listeners = new Map<string, Set<Handler>>()

  const source = new EventSource(`/api/events?k=${encodeURIComponent(token)}`)
  for (const name of EVENTS) {
    source.addEventListener(name, (ev) => {
      if (name === 'face://client-state') {
        if ((window as Window & { __kafdeskFaceReady?: boolean }).__kafdeskFaceReady) {
          location.reload()
        }
        return
      }
      const raw = (ev as MessageEvent<string>).data
      let payload: unknown = raw
      try {
        payload = JSON.parse(raw)
      } catch {
        payload = raw
      }
      const set = listeners.get(name)
      if (!set) return
      for (const cb of set) cb(payload)
    })
  }

  window.setInterval(() => {
    if (!token) return
    void fetch('/api/heartbeat', {
      method: 'POST',
      headers: { 'X-Kafdesk-Token': token },
    }).catch(() => {})
  }, 2000)

  const call = async <T,>(cmd: string, args?: Record<string, unknown>): Promise<T> => {
    if (!commandAllowed(cmd)) throw new Error('FACE_LOCKED')
    const res = await fetch('/api/invoke', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Kafdesk-Token': token,
      },
      body: JSON.stringify({ cmd, args: args ?? {} }),
    })
    const text = await res.text()
    let parsed: unknown = null
    if (text) {
      try {
        parsed = JSON.parse(text)
      } catch {
        if (!res.ok) throw new Error(text)
      }
    }
    if (!res.ok) {
      const message =
        parsed && typeof parsed === 'object' && 'error' in parsed
          ? String((parsed as { error?: unknown }).error ?? '')
          : text
      throw new Error(message || `HTTP ${res.status}`)
    }
    return parsed as T
  }

  const on = <T,>(event: string, cb: (payload: T) => void) => {
    let set = listeners.get(event)
    if (!set) {
      set = new Set()
      listeners.set(event, set)
    }
    const handler = cb as Handler
    set.add(handler)
    return () => set!.delete(handler)
  }

  window.kafdesk = buildKafdeskApi(call, on)
  ;(window as Window & { __kafdeskFaceReady?: boolean }).__kafdeskFaceReady = true
}
