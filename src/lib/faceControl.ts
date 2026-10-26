/** Which surface may send Kafka commands. The other one only looks. */

export type FaceOwner = 'desktop' | 'web'

let browser = false
let owner: FaceOwner = 'desktop'

const PASS_WHEN_LOCKED = new Set([
  'web_face_status',
  'web_face_set_owner',
  'app_get_version',
  'app_update_status',
  'app_update_settings',
  'app_set_auto_update',
  'app_check_for_updates',
  'app_open_releases',
  'app_open_downloaded_update',
  'get_backend_debug',
  'set_backend_debug',
  'ping_shell',
])

export function setFaceBrowser(next: boolean) {
  browser = next
}

export function isFaceBrowser() {
  return browser
}

export function setFaceOwner(next: FaceOwner) {
  owner = next
}

export function getFaceOwner(): FaceOwner {
  return owner
}

/** This window is the one allowed to call consume / produce / the rest of Kafka. */
export function thisSideHasControl() {
  return browser ? owner === 'web' : owner === 'desktop'
}

export function commandAllowed(cmd: string) {
  return thisSideHasControl() || PASS_WHEN_LOCKED.has(cmd)
}

/** Everything the desktop kept under `kafdesk.*`, including saved connections. */
export function collectKafdeskStorage(): Record<string, string> {
  const out: Record<string, string> = {}
  for (let i = 0; i < localStorage.length; i++) {
    const key = localStorage.key(i)
    if (!key?.startsWith('kafdesk.')) continue
    const value = localStorage.getItem(key)
    if (value != null) out[key] = value
  }
  return out
}

export function applyKafdeskStorage(raw: unknown) {
  if (!raw || typeof raw !== 'object') return
  for (const [key, value] of Object.entries(raw as Record<string, unknown>)) {
    if (!key.startsWith('kafdesk.') || typeof value !== 'string') continue
    localStorage.setItem(key, value)
  }
}

export function isFaceLockedError(err: unknown) {
  const msg = err instanceof Error ? err.message : String(err)
  return msg.includes('FACE_LOCKED')
}
