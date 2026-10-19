import type { KafkaMessageView } from '../../electron/types'
import { formatBadgeLabel, formatMessageDateTime } from '../lib/format'

export default function MessageCard({
  message: m,
  keyLabel = 'key',
}: {
  message: KafkaMessageView
  keyLabel?: string
}) {
  const headerEntries = Object.entries(m.headers || {})
  const when = formatMessageDateTime(m.timestamp)

  return (
    <article className="msg">
      <div className="msg-head">
        <span className="msg-meta">
          <span className="msg-chip">p{m.partition}</span>
          <span className="msg-chip msg-offset">
            offset <strong>{m.offset}</strong>
            {when ? <span className="msg-datetime"> · {when}</span> : null}
          </span>
          {m.key != null && (
            <span className="msg-key" title={String(m.key)}>
              <em>{keyLabel}</em>
              <strong>{m.key}</strong>
            </span>
          )}
        </span>
        <span className={`badge ${m.format}`} title={`Detected type: ${m.format}`}>
          {formatBadgeLabel(m.format, m.schemaId)}
        </span>
      </div>
      {headerEntries.length > 0 && (
        <div className="msg-headers">
          {headerEntries.map(([k, v]) => (
            <span key={k} className="chip">
              {k}={v}
            </span>
          ))}
        </div>
      )}
      <pre>{typeof m.value === 'string' ? m.value : JSON.stringify(m.value, null, 2)}</pre>
    </article>
  )
}
