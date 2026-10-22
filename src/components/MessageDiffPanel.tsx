import type { KafkaMessageView } from '../types/kafdesk'
import { formatBadgeLabel } from '../lib/format'
import { messageBodyText } from '../lib/messageText'
import type { useLocale } from '../hooks/useLocale'

type Tr = ReturnType<typeof useLocale>['tr']

function meta(m: KafkaMessageView): string {
  return `p${m.partition} · offset ${m.offset}`
}

function schemaLabel(m: KafkaMessageView): string {
  return formatBadgeLabel(m.format, m.schemaId)
}

function schemasDiffer(a: KafkaMessageView, b: KafkaMessageView): boolean {
  if (a.format !== b.format) return true
  if (a.format === 'avro' || b.format === 'avro') {
    return (a.schemaId ?? null) !== (b.schemaId ?? null)
  }
  return false
}

export default function MessageDiffPanel({
  tr,
  left,
  right,
  onClear,
}: {
  tr: Tr
  left: KafkaMessageView
  right: KafkaMessageView
  onClear: () => void
}) {
  const schemaMismatch = schemasDiffer(left, right)

  return (
    <div className={`msg-diff panel${schemaMismatch ? ' msg-diff-schema-warn' : ''}`}>
      <div className="msg-diff-head">
        <h3>{tr('diffTitle')}</h3>
        <button type="button" onClick={onClear}>
          {tr('diffClear')}
        </button>
      </div>
      {schemaMismatch && (
        <div className="msg-diff-schema-banner" role="status">
          {tr('diffSchemaMismatch', {
            left: schemaLabel(left),
            right: schemaLabel(right),
          })}
        </div>
      )}
      <div className="msg-diff-grid">
        <div className="msg-diff-col">
          <div className="msg-diff-meta">
            <span>{meta(left)}</span>
            <span className={`badge ${left.format}`}>{schemaLabel(left)}</span>
          </div>
          <pre>{messageBodyText(left)}</pre>
        </div>
        <div className="msg-diff-col">
          <div className="msg-diff-meta">
            <span>{meta(right)}</span>
            <span className={`badge ${right.format}`}>{schemaLabel(right)}</span>
          </div>
          <pre>{messageBodyText(right)}</pre>
        </div>
      </div>
    </div>
  )
}
