export const SCHEMAS_WIDTH_KEY = 'kafdesk.schemasWidth'

export const SCHEMAS_WIDTH_DEFAULT = 320
export const SCHEMAS_WIDTH_MIN = 200
export const SCHEMAS_WIDTH_MAX = 640

export function clampSchemasWidth(px: number): number {
  return Math.min(SCHEMAS_WIDTH_MAX, Math.max(SCHEMAS_WIDTH_MIN, Math.round(px)))
}

export function loadSchemasWidth(): number {
  try {
    const n = Number(localStorage.getItem(SCHEMAS_WIDTH_KEY))
    if (Number.isFinite(n) && n > 0) return clampSchemasWidth(n)
  } catch {
    /* ignore */
  }
  return SCHEMAS_WIDTH_DEFAULT
}

export function persistSchemasWidth(px: number): void {
  try {
    localStorage.setItem(SCHEMAS_WIDTH_KEY, String(clampSchemasWidth(px)))
  } catch {
    /* ignore */
  }
}
