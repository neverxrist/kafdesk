import { useMemo } from 'react'
import type { KafkaMessageView } from '../types/kafdesk'
import { formatBadgeLabel } from '../lib/format'
import { highlightMessageBody } from '../lib/jsonHighlight'
import { messageBodyText } from '../lib/messageText'
import type { useLocale } from '../hooks/useLocale'

type Tr = ReturnType<typeof useLocale>['tr']

function meta(m: KafkaMessageView): string {
  return `p${m.partition} · offset ${m.offset}`
}

function keyLabel(m: KafkaMessageView): string | null {
  if (m.key == null || m.key === '') return null
  return String(m.key)
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
  onRemove,
}: {
  tr: Tr
  left: KafkaMessageView
  right: KafkaMessageView
  onClear: () => void
  onRemove: (m: KafkaMessageView) => void
}) {
  const schemaMismatch = schemasDiffer(left, right)
  const leftBody = useMemo(() => highlightMessageBody(messageBodyText(left)), [left])
  const rightBody = useMemo(() => highlightMessageBody(messageBodyText(right)), [right])
  const leftKey = keyLabel(left)
  const rightKey = keyLabel(right)

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
            <div className="msg-diff-meta-right">
              <span className={`badge ${left.format}`}>{schemaLabel(left)}</span>
              <button
                type="button"
                className="msg-diff-remove"
                title={tr('diffRemove')}
                aria-label={tr('diffRemove')}
                onClick={() => onRemove(left)}
              >
                ×
              </button>
            </div>
          </div>
          {leftKey && (
            <div className="msg-diff-key" title={leftKey}>
              {leftKey}
            </div>
          )}
          <pre className="msg-diff-json">{leftBody}</pre>
        </div>
        <div className="msg-diff-col">
          <div className="msg-diff-meta">
            <span>{meta(right)}</span>
            <div className="msg-diff-meta-right">
              <span className={`badge ${right.format}`}>{schemaLabel(right)}</span>
              <button
                type="button"
                className="msg-diff-remove"
                title={tr('diffRemove')}
                aria-label={tr('diffRemove')}
                onClick={() => onRemove(right)}
              >
                ×
              </button>
            </div>
          </div>
          {rightKey && (
            <div className="msg-diff-key" title={rightKey}>
              {rightKey}
            </div>
          )}
          <pre className="msg-diff-json">{rightBody}</pre>
        </div>
      </div>
    </div>
  )
}
