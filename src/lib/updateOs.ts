/** Coarse OS for update dialog copy (Tauri webview UA is enough). */
export type UpdateOs = 'mac' | 'win' | 'linux'

export function detectUpdateOs(): UpdateOs {
  const ua = navigator.userAgent
  if (/Mac|iPhone|iPad/i.test(ua)) return 'mac'
  if (/Win/i.test(ua)) return 'win'
  return 'linux'
}
