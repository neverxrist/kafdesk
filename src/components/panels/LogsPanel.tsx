import type { AppLogEntry } from '../../hooks/useAppLogs'
import type { useLocale } from '../../hooks/useLocale'

type Tr = ReturnType<typeof useLocale>['tr']

export default function LogsPanel({
  tr,
  logs,
  onClear,
}: {
  tr: Tr
  logs: AppLogEntry[]
  onClear: () => void
}) {
  return (
    <div className="logs-pane">
      <div className="logs-toolbar">
        <span className="status">{tr('entries', { n: logs.length })}</span>
        <button type="button" onClick={onClear}>
          {tr('clear')}
        </button>
      </div>
      {logs.length === 0 && <div className="empty">{tr('logsEmpty')}</div>}
      <ul className="log-list">
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
