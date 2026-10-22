export const GROUPS_WIDTH_KEY = 'kafdesk.groupsWidth'

export const GROUPS_WIDTH_DEFAULT = 320
export const GROUPS_WIDTH_MIN = 200
export const GROUPS_WIDTH_MAX = 640

export function clampGroupsWidth(px: number): number {
  return Math.min(GROUPS_WIDTH_MAX, Math.max(GROUPS_WIDTH_MIN, Math.round(px)))
}

export function loadGroupsWidth(): number {
  try {
    const n = Number(localStorage.getItem(GROUPS_WIDTH_KEY))
    if (Number.isFinite(n) && n > 0) return clampGroupsWidth(n)
  } catch {
    /* ignore */
  }
  return GROUPS_WIDTH_DEFAULT
}

export function persistGroupsWidth(px: number): void {
  try {
    localStorage.setItem(GROUPS_WIDTH_KEY, String(clampGroupsWidth(px)))
  } catch {
    /* ignore */
  }
}
