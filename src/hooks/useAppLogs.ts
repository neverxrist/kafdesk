import { useCallback, useState } from 'react'

export type AppLogEntry = { ts: number; level: 'info' | 'warn' | 'error'; msg: string }

/** Cap while Logs tab may be closed — still enough for debugging. */
const LOG_CAP = 150

export function useAppLogs() {
  const [logs, setLogs] = useState<AppLogEntry[]>([])

  const pushLog = useCallback((level: AppLogEntry['level'], msg: string) => {
    setLogs((prev) => [{ ts: Date.now(), level, msg }, ...prev].slice(0, LOG_CAP))
  }, [])

  return { logs, setLogs, pushLog }
}
