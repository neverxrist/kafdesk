import { useState } from 'react'

export type AppLogEntry = { ts: number; level: 'info' | 'warn' | 'error'; msg: string }

export function useAppLogs() {
  const [logs, setLogs] = useState<AppLogEntry[]>([])

  function pushLog(level: AppLogEntry['level'], msg: string) {
    setLogs((prev) => [{ ts: Date.now(), level, msg }, ...prev].slice(0, 500))
  }

  return { logs, setLogs, pushLog }
}
