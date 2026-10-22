import { useEffect, useState } from 'react'
import type { AppLogEntry } from '../../hooks/useAppLogs'
import type { useLocale } from '../../hooks/useLocale'

type Tr = ReturnType<typeof useLocale>['tr']

export const BACKEND_DEBUG_KEY = 'kafdesk.backendDebug'

export default function LogsPanel({
  tr,
  logs,
  onClear,
  backendDebug,
  onBackendDebugChange,
}: {
  tr: Tr
  logs: AppLogEntry[]
  onClear: () => void
  backendDebug: boolean
  onBackendDebugChange: (enabled: boolean) => void
}) {
  return (
    <div className="logs-pane">
      <div className="logs-toolbar">
        <div className="logs-toolbar-left">
          <span className="status">{tr('entries', { n: logs.length })}</span>
          <label className="logs-debug-toggle" title={tr('backendDebugTitle')}>
            <input
              type="checkbox"
              checked={backendDebug}
              onChange={(e) => onBackendDebugChange(e.target.checked)}
            />
            {tr('backendDebug')}
          </label>
        </div>
        <button type="button" onClick={onClear}>
          {tr('clear')}
        </button>
      </div>
      {logs.length === 0 && <div className="empty">{tr('logsEmpty')}</div>}
      <ul className="log-list drag-scroll">
        {logs.map((l, i) => (
          <li key={`${l.ts}-${i}`} className={`log-line log-${l.level}`}>
            <time>{new Date(l.ts).toLocaleTimeString()}</time>
            <span className="log-level">{l.level}</span>
            <span className="log-msg">{l.msg}</span>
          </li>
        ))}
      </ul>
    </div>
  )
}

/** Persist toggle + subscribe to Rust breadcrumbs (idle when debug off). */
export function useBackendDebugSync(
  pushLog: (level: AppLogEntry['level'], msg: string) => void,
): [boolean, (enabled: boolean) => void] {
  const [backendDebug, setBackendDebug] = useState(false)

  useEffect(() => {
    const saved = localStorage.getItem(BACKEND_DEBUG_KEY) === '1'
    setBackendDebug(saved)
    void window.kafdesk.setBackendDebug?.(saved).catch(() => {})
    const un = window.kafdesk.onBackendDebugLog?.((level, msg) => {
      pushLog(level, `[backend] ${msg}`)
    })
    return () => un?.()
  }, [pushLog])

  function onBackendDebugChange(enabled: boolean) {
    setBackendDebug(enabled)
    localStorage.setItem(BACKEND_DEBUG_KEY, enabled ? '1' : '0')
    void window.kafdesk.setBackendDebug?.(enabled).catch(() => {})
  }

  return [backendDebug, onBackendDebugChange]
}
