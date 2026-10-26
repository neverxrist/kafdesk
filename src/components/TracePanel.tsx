import { useEffect, useRef, useState } from 'react'
import type { TraceHit, TraceProgress, TraceResult } from '../types/kafdesk'
import type { TopicSet } from '../lib/topicSets'
import { TOPIC_SET_MAX } from '../lib/topicSets'
import { formatMessageDateTime } from '../lib/format'
import { stripIpcError } from '../lib/ipcError'
import type { useLocale } from '../hooks/useLocale'

type Tr = ReturnType<typeof useLocale>['tr']

const WINDOWS = [
  { id: '1h', ms: 60 * 60 * 1000 },
  { id: '6h', ms: 6 * 60 * 60 * 1000 },
  { id: '24h', ms: 24 * 60 * 60 * 1000 },
  { id: '7d', ms: 7 * 24 * 60 * 60 * 1000 },
] as const

type WindowId = (typeof WINDOWS)[number]['id']

const WINDOW_KEY = 'kafdesk.traceWindow'
const BODIES_KEY = 'kafdesk.traceBodies'

function loadWindow(): WindowId {
  try {
    const v = localStorage.getItem(WINDOW_KEY)
    if (v === '1h' || v === '6h' || v === '24h' || v === '7d') return v
  } catch {
    /* ignore */
  }
  return '1h'
}

export default function TracePanel({
  tr,
  open,
  onClose,
  connected,
  initialNeedle,
  sets,
  activeSetId,
  onActiveSetId,
  clusterTopics,
  onAddTopic,
  onRemoveTopic,
  onDeleteSet,
  onOpenHit,
}: {
  tr: Tr
  open: boolean
  onClose: () => void
  connected: boolean
  initialNeedle: string
  sets: TopicSet[]
  activeSetId: string | null
  onActiveSetId: (id: string | null) => void
  clusterTopics: string[]
  onAddTopic: (setId: string, topic: string) => void
  onRemoveTopic: (setId: string, topic: string) => void
  onDeleteSet: (setId: string) => void
  onOpenHit: (hit: TraceHit) => void
}) {
  const [needle, setNeedle] = useState(initialNeedle)
  const [windowId, setWindowId] = useState<WindowId>(loadWindow)
  const [scanValues, setScanValues] = useState(() => {
    try {
      return localStorage.getItem(BODIES_KEY) !== '0'
    } catch {
      return true
    }
  })
  const [addName, setAddName] = useState('')
  const [running, setRunning] = useState(false)
  const [progress, setProgress] = useState<TraceProgress | null>(null)
  const [result, setResult] = useState<TraceResult | null>(null)
  const [error, setError] = useState<string | null>(null)
  const genRef = useRef(0)
  const active = sets.find((s) => s.id === activeSetId) ?? null

  useEffect(() => {
    if (open) setNeedle(initialNeedle)
  }, [open, initialNeedle])

  useEffect(() => {
    if (!open) return
    function onKey(e: KeyboardEvent) {
      if (e.key === 'Escape') onClose()
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [open, onClose])

  useEffect(() => {
    if (!open) return
    return () => {
      void window.kafdesk.traceCancel?.()
    }
  }, [open])

  if (!open) return null

  async function run(nextNeedle: string, bodies: boolean) {
    const id = nextNeedle.trim()
    if (!active || active.topics.length === 0 || !id || !window.kafdesk.traceKey) return
    const gen = ++genRef.current
    setNeedle(id)
    setRunning(true)
    setError(null)
    setResult(null)
    setProgress(null)
    const off = window.kafdesk.onTraceProgress?.((p) => {
      if (genRef.current === gen) setProgress(p)
    })
    const win = WINDOWS.find((w) => w.id === windowId) ?? WINDOWS[0]
    try {
      const res = await window.kafdesk.traceKey({
        needle: id,
        topics: active.topics,
        fromMs: Date.now() - win.ms,
        scanValues: bodies,
      })
      if (genRef.current !== gen) return
      setResult(res)
    } catch (err) {
      if (genRef.current !== gen) return
      const msg = stripIpcError(err instanceof Error ? err.message : String(err))
      setError(msg.includes('group snapshot') ? tr('traceBusy') : msg)
    } finally {
      off?.()
      if (genRef.current === gen) setRunning(false)
    }
  }

  function stop() {
    void window.kafdesk.traceCancel?.()
  }

  function addTopic() {
    if (!active) return
    const name = addName.trim()
    if (!name || !clusterTopics.includes(name)) return
    onAddTopic(active.id, name)
    setAddName('')
  }

  return (
    <aside className="trace-panel" role="dialog" aria-label={tr('traceTitle')}>
      <div className="trace-head">
        <strong>{tr('traceTitle')}</strong>
        <button type="button" className="trace-close" onClick={onClose} aria-label={tr('close')}>
          ×
        </button>
      </div>

      {!connected ? (
        <p className="trace-hint">{tr('traceDisconnected')}</p>
      ) : sets.length === 0 || !active ? (
        <p className="trace-hint">{tr('traceNeedSet')}</p>
      ) : (
        <>
          <label className="trace-field">
            <span>{tr('traceNeedle')}</span>
            <input
              value={needle}
              onChange={(e) => setNeedle(e.target.value)}
              placeholder={tr('traceNeedlePlaceholder')}
              spellCheck={false}
              autoCapitalize="none"
              onKeyDown={(e) => {
                if (e.key === 'Enter') void run(needle, scanValues)
              }}
            />
          </label>

          <div className="trace-row">
            <select
              value={active.id}
              aria-label={tr('topicSetIn')}
              onChange={(e) => onActiveSetId(e.target.value || null)}
            >
              {sets.map((s) => (
                <option key={s.id} value={s.id}>
                  {s.name}
                </option>
              ))}
            </select>
            <select
              value={windowId}
              aria-label={tr('traceWindow')}
              onChange={(e) => {
                const id = e.target.value as WindowId
                setWindowId(id)
                try {
                  localStorage.setItem(WINDOW_KEY, id)
                } catch {
                  /* ignore */
                }
              }}
            >
              <option value="1h">{tr('trace1h')}</option>
              <option value="6h">{tr('trace6h')}</option>
              <option value="24h">{tr('trace24h')}</option>
              <option value="7d">{tr('trace7d')}</option>
            </select>
          </div>

          <label className="trace-bodies" title={tr('traceBodiesTitle')}>
            <input
              type="checkbox"
              checked={scanValues}
              onChange={(e) => {
                setScanValues(e.target.checked)
                try {
                  localStorage.setItem(BODIES_KEY, e.target.checked ? '1' : '0')
                } catch {
                  /* ignore */
                }
              }}
            />
            {tr('traceBodies')}
          </label>

          <div className="trace-actions">
            {running ? (
              <button type="button" className="primary" onClick={stop}>
                {tr('traceStop')}
              </button>
            ) : (
              <button
                type="button"
                className="primary"
                disabled={!needle.trim() || active.topics.length === 0}
                onClick={() => void run(needle, scanValues)}
              >
                {tr('traceRun')}
              </button>
            )}
            <button type="button" className="trace-delete" onClick={() => onDeleteSet(active.id)}>
              {tr('topicSetDelete')}
            </button>
          </div>

          <div className="trace-chips">
            {active.topics.map((name) => {
              const known = clusterTopics.includes(name)
              return (
                <span key={name} className={`trace-chip${known ? '' : ' missing'}`} title={known ? name : tr('topicSetMissing')}>
                  <span>{name}</span>
                  <button type="button" aria-label={tr('topicSetRemove')} onClick={() => onRemoveTopic(active.id, name)}>
                    ×
                  </button>
                </span>
              )
            })}
          </div>
          {active.topics.length < TOPIC_SET_MAX && (
            <div className="trace-add">
              <input
                value={addName}
                list="trace-topic-names"
                placeholder={tr('topicSetAdd')}
                onChange={(e) => setAddName(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter') addTopic()
                }}
                spellCheck={false}
              />
              <datalist id="trace-topic-names">
                {clusterTopics.map((name) => (
                  <option key={name} value={name} />
                ))}
              </datalist>
              <button type="button" disabled={!clusterTopics.includes(addName.trim())} onClick={addTopic}>
                {tr('traceAddTopic')}
              </button>
            </div>
          )}

          {running && progress && (
            <p className="trace-hint">
              {tr('traceProgress', {
                topic: progress.topic || '…',
                done: progress.done,
                total: progress.total,
              })}
            </p>
          )}
          {error && <p className="trace-error">{error}</p>}
          {result && (
            <div className="trace-results">
              {result.partial && <p className="trace-hint">{tr('tracePartial')}</p>}
              {result.hits.length === 0 && <p className="trace-hint">{tr('traceEmpty')}</p>}
              <ul className="trace-topics">
                {result.topics.map((s) => (
                  <li key={s.topic}>
                    <span>{s.topic}</span>
                    <span>
                      {s.error
                        ? s.error
                        : `${tr('traceHits', { n: s.hits })} · ${tr('traceScanned', { n: s.scanned })}`}
                      {s.capped ? ` · ${tr('traceCapped')}` : ''}
                    </span>
                  </li>
                ))}
              </ul>
              <ul className="trace-hits">
                {result.hits.map((hit) => (
                  <li key={`${hit.topic}-${hit.partition}-${hit.offset}`}>
                    <button type="button" className="trace-hit" onClick={() => onOpenHit(hit)}>
                      <span className="trace-hit-top">
                        <strong>{hit.topic}</strong>
                        <em>{hit.matched === 'key' ? tr('traceMatchedKey') : tr('traceMatchedValue')}</em>
                        <time>{formatMessageDateTime(hit.timestamp) ?? ''}</time>
                      </span>
                      <span className="trace-hit-meta">
                        p{hit.partition} · {hit.offset}
                        {hit.key ? ` · ${hit.key}` : ''}
                      </span>
                      {hit.preview && <span className="trace-hit-preview">{hit.preview}</span>}
                    </button>
                    {hit.fields.length > 0 && (
                      <div className="trace-fields">
                        {hit.fields.map((field) => (
                          <button
                            key={field}
                            type="button"
                            title={tr('traceFollow')}
                            onClick={() => void run(field, false)}
                          >
                            {field}
                          </button>
                        ))}
                      </div>
                    )}
                  </li>
                ))}
              </ul>
            </div>
          )}
        </>
      )}
    </aside>
  )
}
