import { useMemo, useRef, useState, type RefObject } from 'react'
import type { KafkaMessageView } from '../types/kafdesk'
import { formatBadgeLabel } from '../lib/format'
import { highlightJsonFragment } from '../lib/jsonHighlight'
import { alignLineDiff, type DiffSideLine } from '../lib/lineDiff'
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

function filterOnlyChanges(
  left: DiffSideLine[],
  right: DiffSideLine[],
): { left: DiffSideLine[]; right: DiffSideLine[] } {
  const outL: DiffSideLine[] = []
  const outR: DiffSideLine[] = []
  for (let i = 0; i < left.length; i++) {
    const L = left[i]!
    const R = right[i]!
    // Skip identical paired lines (braces / unchanged text).
    if (L.text === R.text && L.kind !== 'removed' && R.kind !== 'added') continue
    if (L.kind === 'same' && R.kind === 'same') continue
    outL.push(L)
    outR.push(R)
  }
  return { left: outL, right: outR }
}

function DiffBody({
  lines,
  scrollRef,
  onScroll,
}: {
  lines: DiffSideLine[]
  scrollRef: RefObject<HTMLDivElement | null>
  onScroll: () => void
}) {
  return (
    <div className="msg-diff-json" ref={scrollRef} onScroll={onScroll}>
      {lines.map((line, i) => (
        <div key={i} className={`msg-diff-line msg-diff-line-${line.kind}`}>
          {line.kind === 'empty' ? '\u00a0' : highlightJsonFragment(line.text)}
        </div>
      ))}
    </div>
  )
}

export default function MessageDiffPanel({
  tr,
  left,
  right,
  onClear,
  onRemove,
  embedded = false,
  onlyChanges: onlyChangesProp,
  onOnlyChangesChange,
}: {
  tr: Tr
  left: KafkaMessageView
  right: KafkaMessageView
  onClear: () => void
  onRemove: (m: KafkaMessageView) => void
  /** Hide own header — dock toolbar already has Clear / title. */
  embedded?: boolean
  /** Controlled from dock toolbar (next to Clear). */
  onlyChanges?: boolean
  onOnlyChangesChange?: (on: boolean) => void
}) {
  const [onlyLocal, setOnlyLocal] = useState(false)
  const onlyChanges = onlyChangesProp ?? onlyLocal
  function setOnlyChanges(on: boolean) {
    onOnlyChangesChange?.(on)
    if (onlyChangesProp === undefined) setOnlyLocal(on)
  }
  const schemaMismatch = schemasDiffer(left, right)
  const aligned = useMemo(
    () => alignLineDiff(messageBodyText(left), messageBodyText(right)),
    [left, right],
  )
  const visible = useMemo(
    () => (onlyChanges ? filterOnlyChanges(aligned.left, aligned.right) : aligned),
    [aligned, onlyChanges],
  )
  const leftKey = keyLabel(left)
  const rightKey = keyLabel(right)

  const leftScroll = useRef<HTMLDivElement>(null)
  const rightScroll = useRef<HTMLDivElement>(null)
  const syncing = useRef(false)

  function syncFrom(source: 'left' | 'right') {
    if (syncing.current) return
    const a = source === 'left' ? leftScroll.current : rightScroll.current
    const b = source === 'left' ? rightScroll.current : leftScroll.current
    if (!a || !b) return
    syncing.current = true
    b.scrollTop = a.scrollTop
    b.scrollLeft = a.scrollLeft
    requestAnimationFrame(() => {
      syncing.current = false
    })
  }

  const onlyBtn = (
    <button
      type="button"
      className={onlyChanges ? 'msg-diff-only active' : 'msg-diff-only'}
      aria-pressed={onlyChanges}
      onClick={() => setOnlyChanges(!onlyChanges)}
    >
      {tr('diffOnlyChanges')}
    </button>
  )

  return (
    <div
      className={`msg-diff${embedded ? ' msg-diff-embedded' : ' panel'}${schemaMismatch ? ' msg-diff-schema-warn' : ''}`}
    >
      {!embedded && (
        <div className="msg-diff-head">
          <h3>{tr('diffTitle')}</h3>
          <div className="msg-diff-head-actions">
            {onlyBtn}
            <button type="button" onClick={onClear}>
              {tr('diffClear')}
            </button>
          </div>
        </div>
      )}
      {schemaMismatch && (
        <div className="msg-diff-schema-banner" role="status">
          {tr('diffSchemaMismatch', {
            left: schemaLabel(left),
            right: schemaLabel(right),
          })}
        </div>
      )}
      {onlyChanges && visible.left.length === 0 ? (
        <div className="msg-diff-empty">{tr('diffNoChanges')}</div>
      ) : (
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
            <DiffBody
              lines={visible.left}
              scrollRef={leftScroll}
              onScroll={() => syncFrom('left')}
            />
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
            <DiffBody
              lines={visible.right}
              scrollRef={rightScroll}
              onScroll={() => syncFrom('right')}
            />
          </div>
        </div>
      )}
    </div>
  )
}
