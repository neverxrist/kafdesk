import type { KafkaMessageView } from '../../electron/types'
import { messageBodyText } from '../lib/messageText'
import type { useLocale } from '../hooks/useLocale'

type Tr = ReturnType<typeof useLocale>['tr']

function meta(m: KafkaMessageView): string {
  return `p${m.partition} · offset ${m.offset}`
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
  return (
    <div className="msg-diff panel">
      <div className="msg-diff-head">
        <h3>{tr('diffTitle')}</h3>
        <button type="button" onClick={onClear}>
          {tr('diffClear')}
        </button>
      </div>
      <div className="msg-diff-grid">
        <div className="msg-diff-col">
          <div className="msg-diff-meta">{meta(left)}</div>
          <pre>{messageBodyText(left)}</pre>
        </div>
        <div className="msg-diff-col">
          <div className="msg-diff-meta">{meta(right)}</div>
          <pre>{messageBodyText(right)}</pre>
        </div>
      </div>
    </div>
  )
}
