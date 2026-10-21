/** Strip Electron IPC invoke prefix from Error.message. */
export function stripIpcError(raw: string): string {
  return raw.replace(/^Error invoking remote method '[^']+':\s*/i, '').trim() || raw
}
