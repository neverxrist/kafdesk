import { useEffect, useRef, useState } from 'react'
import type { AppLogEntry } from '../../hooks/useAppLogs'
import type { useLocale } from '../../hooks/useLocale'

type Tr = ReturnType<typeof useLocale>['tr']

export const BACKEND_DEBUG_KEY = 'kafdesk.backendDebug'

function WarnTipIcon() {
  return (
    <svg width="12" height="12" viewBox="0 0 12 12" fill="none" aria-hidden>
      <path
        d="M6 1.2 11 10.2H1L6 1.2Z"
        stroke="currentColor"
        strokeWidth="1.2"
        strokeLinejoin="round"
      />
      <path d="M6 4.6v2.4" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round" />
      <circle cx="6" cy="8.55" r="0.65" fill="currentColor" />
    </svg>
  )
}

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
  const [hintOpen, setHintOpen] = useState(false)
  const hintRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (!hintOpen) return
    function onDoc(e: MouseEvent) {
      if (hintRef.current && !hintRef.current.contains(e.target as Node)) setHintOpen(false)
    }
    function onKey(e: KeyboardEvent) {
      if (e.key === 'Escape') setHintOpen(false)
    }
    document.addEventListener('mousedown', onDoc)
    document.addEventListener('keydown', onKey)
    return () => {
      document.removeEventListener('mousedown', onDoc)
      document.removeEventListener('keydown', onKey)
    }
  }, [hintOpen])

  return (
    <div className="logs-pane">
      <div className="logs-toolbar">
        <div className="logs-toolbar-left">
          <span className="status">{tr('entries', { n: logs.length })}</span>
          <div className="logs-debug-wrap">
            <label className="logs-debug-toggle">
              <input
                type="checkbox"
                checked={backendDebug}
                onChange={(e) => onBackendDebugChange(e.target.checked)}
              />
              {tr('backendDebug')}
            </label>
            <div className="logs-debug-tip" ref={hintRef}>
              <button
                type="button"
                className={`logs-debug-tip-btn${hintOpen ? ' open' : ''}`}
                aria-expanded={hintOpen}
                aria-label={tr('backendDebugHint')}
                onClick={() => setHintOpen((v) => !v)}
              >
                <WarnTipIcon />
              </button>
              {hintOpen && (
                <div className="logs-debug-popover" role="note">
                  {tr('backendDebugHint')}
                </div>
              )}
            </div>
          </div>
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
