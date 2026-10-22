export const MSG_FONT_KEY = 'kafdesk.msgFontPx'
export const MSG_FONT_MIN = 11
export const MSG_FONT_MAX = 20
export const MSG_FONT_DEFAULT = 13

export function loadMsgFontPx(): number {
  try {
    const n = Number(localStorage.getItem(MSG_FONT_KEY))
    if (Number.isFinite(n) && n >= MSG_FONT_MIN && n <= MSG_FONT_MAX) return Math.round(n)
  } catch {
    /* ignore */
  }
  return MSG_FONT_DEFAULT
}

export function applyMsgFontPx(px: number) {
  const v = Math.min(MSG_FONT_MAX, Math.max(MSG_FONT_MIN, Math.round(px)))
  document.documentElement.style.setProperty('--msg-font-size', `${v}px`)
  try {
    localStorage.setItem(MSG_FONT_KEY, String(v))
  } catch {
    /* ignore */
  }
  return v
}
