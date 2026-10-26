import { useEffect, useRef, useState, type FormEvent, type ReactNode } from 'react'
import type { TraceHit, TraceProgress, TraceResult } from '../types/kafdesk'
import { TOPIC_SET_COUNT_MAX, type TopicSet } from '../lib/topicSets'
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

export function useTraceRun({
  tr,
  open,
  initialNeedle,
  sets,
  activeSetId,
}: {
  tr: Tr
  open: boolean
  initialNeedle: string
  sets: TopicSet[]
  activeSetId: string | null
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
  const [running, setRunning] = useState(false)
  const [progress, setProgress] = useState<TraceProgress | null>(null)
  const [result, setResult] = useState<TraceResult | null>(null)
  const [error, setError] = useState<string | null>(null)
  const genRef = useRef(0)
  const active = sets.find((s) => s.id === activeSetId) ?? null

  useEffect(() => {
    if (initialNeedle) setNeedle(initialNeedle)
  }, [initialNeedle])

  useEffect(() => {
    if (!open) return
    return () => {
      void window.kafdesk.traceCancel?.()
    }
  }, [open])

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

  function chooseWindow(id: WindowId) {
    setWindowId(id)
    try {
      localStorage.setItem(WINDOW_KEY, id)
    } catch {
      /* ignore */
    }
  }

  function chooseBodies(on: boolean) {
    setScanValues(on)
    try {
      localStorage.setItem(BODIES_KEY, on ? '1' : '0')
    } catch {
      /* ignore */
    }
  }

  return {
    needle,
    setNeedle,
    windowId,
    chooseWindow,
    scanValues,
    chooseBodies,
    running,
    progress,
    result,
    error,
    active,
    run,
    stop,
  }
}

export function TraceToolbar({
  tr,
  connected,
  sets,
  active,
  needle,
  setNeedle,
  windowId,
  chooseWindow,
  scanValues,
  chooseBodies,
  running,
  onRun,
  onStop,
  onClose,
  onActiveSetId,
  onCreateSet,
  onDeleteSet,
}: {
  tr: Tr
  connected: boolean
  sets: TopicSet[]
  active: TopicSet | null
  needle: string
  setNeedle: (v: string) => void
  windowId: WindowId
  chooseWindow: (id: WindowId) => void
  scanValues: boolean
  chooseBodies: (on: boolean) => void
  running: boolean
  onRun: () => void
  onStop: () => void
  onClose: () => void
  onActiveSetId: (id: string | null) => void
  onCreateSet: (name: string, topic?: string) => void
  onDeleteSet: (setId: string) => void
}) {
  const [draft, setDraft] = useState('')
  const [creating, setCreating] = useState(false)
  const setsFull = sets.length >= TOPIC_SET_COUNT_MAX

  function submitSet(e: FormEvent) {
    e.preventDefault()
    const name = draft.trim()
    if (!name) return
    onCreateSet(name)
    setDraft('')
    setCreating(false)
  }

  return (
    <div className="toolbar-trace">
      <button type="button" className="toolbar-trace-back" onClick={onClose} title={tr('traceBack')}>
        ←
      </button>
      {!connected ? (
        <p className="trace-hint toolbar-trace-hint">{tr('traceDisconnected')}</p>
      ) : sets.length === 0 ? (
        <form className="toolbar-trace-create" onSubmit={submitSet}>
          <label className="field toolbar-trace-set">
            <span>{tr('topicSetName')}</span>
            <input
              value={draft}
              onChange={(e) => setDraft(e.target.value)}
              placeholder={tr('topicSetNew')}
              spellCheck={false}
            />
          </label>
          <button type="submit" className="primary" disabled={!draft.trim()}>
            {tr('traceCreate')}
          </button>
        </form>
      ) : (
        <>
          <div className="toolbar-trace-set-wrap">
            {creating ? (
              <form className="toolbar-trace-create" onSubmit={submitSet}>
                <label className="field toolbar-trace-set">
                  <span>{tr('topicSetName')}</span>
                  <input
                    value={draft}
                    autoFocus
                    onChange={(e) => setDraft(e.target.value)}
                    placeholder={tr('topicSetNew')}
                    spellCheck={false}
                  />
                </label>
                <button type="submit" className="primary" disabled={!draft.trim()}>
                  {tr('traceCreate')}
                </button>
                <button
                  type="button"
                  className="toolbar-trace-new"
                  onClick={() => {
                    setCreating(false)
                    setDraft('')
                  }}
                  aria-label={tr('close')}
                >
                  ×
                </button>
              </form>
            ) : (
              <>
                <label className="field toolbar-trace-set">
                  <span>{tr('topicSetIn')}</span>
                  <select
                    value={active?.id ?? ''}
                    aria-label={tr('topicSetIn')}
                    onChange={(e) => onActiveSetId(e.target.value || null)}
                  >
                    {sets.map((s) => (
                      <option key={s.id} value={s.id}>
                        {s.name} ({s.topics.length})
                      </option>
                    ))}
                  </select>
                </label>
                <button
                  type="button"
                  className="toolbar-trace-new"
                  disabled={setsFull}
                  title={setsFull ? tr('topicSetCountFull') : tr('topicSetNew')}
                  aria-label={tr('topicSetNew')}
                  onClick={() => setCreating(true)}
                >
                  +
                </button>
                {active && (
                  <button
                    type="button"
                    className="toolbar-trace-new toolbar-trace-delete"
                    title={tr('topicSetDelete')}
                    aria-label={tr('topicSetDelete')}
                    onClick={() => onDeleteSet(active.id)}
                  >
                    <DeleteSetIcon />
                  </button>
                )}
              </>
            )}
          </div>
          <label className="field toolbar-trace-needle">
            <span>{tr('traceNeedle')}</span>
            <input
              value={needle}
              onChange={(e) => setNeedle(e.target.value)}
              spellCheck={false}
              autoCapitalize="none"
              onKeyDown={(e) => {
                if (e.key === 'Enter') onRun()
              }}
            />
          </label>
          <label className="field toolbar-trace-window">
            <span>{tr('traceWindow')}</span>
            <select
              value={windowId}
              aria-label={tr('traceWindow')}
              onChange={(e) => chooseWindow(e.target.value as WindowId)}
            >
              <option value="1h">{tr('trace1h')}</option>
              <option value="6h">{tr('trace6h')}</option>
              <option value="24h">{tr('trace24h')}</option>
              <option value="7d">{tr('trace7d')}</option>
            </select>
          </label>
          <label className="toolbar-trace-bodies" title={tr('traceBodiesTitle')}>
            <input
              type="checkbox"
              checked={scanValues}
              onChange={(e) => chooseBodies(e.target.checked)}
            />
            {tr('traceBodies')}
          </label>
          {running ? (
            <button type="button" className="primary" onClick={onStop}>
              {tr('traceStop')}
            </button>
          ) : (
            <button
              type="button"
              className="primary"
              disabled={!needle.trim() || !active || active.topics.length === 0}
              onClick={onRun}
            >
              {tr('traceRun')}
            </button>
          )}
        </>
      )}
    </div>
  )
}

export function TraceResults({
  tr,
  connected,
  active,
  clusterTopics,
  onRemoveTopic,
  running,
  progress,
  result,
  error,
  onOpenHit,
}: {
  tr: Tr
  connected: boolean
  active: TopicSet | null
  clusterTopics: string[]
  onRemoveTopic: (setId: string, topic: string) => void
  running: boolean
  progress: TraceProgress | null
  result: TraceResult | null
  error: string | null
  onOpenHit: (hit: TraceHit) => void
}) {
  let body: ReactNode
  if (!connected) {
    body = <p className="trace-hint">{tr('traceDisconnected')}</p>
  } else if (!active) {
    body = <p className="trace-hint">{tr('traceNeedSet')}</p>
  } else {
    body = (
      <>
        <div className="trace-chips">
          {active.topics.map((name) => {
            const known = clusterTopics.includes(name)
            return (
              <span
                key={name}
                className={`trace-chip${known ? '' : ' missing'}`}
                title={known ? name : tr('topicSetMissing')}
              >
                <span>{name}</span>
                <button
                  type="button"
                  aria-label={tr('topicSetRemove')}
                  onClick={() => onRemoveTopic(active.id, name)}
                >
                  ×
                </button>
              </span>
            )
          })}
          {active.topics.length === 0 && <span className="trace-hint">{tr('traceNeedTopics')}</span>}
        </div>
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
                      {(hit.matched === 'key' || hit.matched === 'both') && (
                        <em className="trace-where key">{tr('traceMatchedKey')}</em>
                      )}
                      {(hit.matched === 'value' || hit.matched === 'both') && (
                        <em className="trace-where body">{tr('traceMatchedValue')}</em>
                      )}
                      <time>{formatMessageDateTime(hit.timestamp) ?? ''}</time>
                    </span>
                    <span className="trace-hit-meta">
                      p{hit.partition} · {hit.offset}
                      {hit.key ? ` · ${hit.key}` : ''}
                    </span>
                  </button>
                </li>
              ))}
            </ul>
          </div>
        )}
      </>
    )
  }

  return <div className="trace-workspace">{body}</div>
}

function DeleteSetIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 16 16" aria-hidden="true">
      <path
        fill="none"
        stroke="currentColor"
        strokeWidth="1.4"
        strokeLinecap="round"
        strokeLinejoin="round"
        d="M3.5 4.5h9M6.2 4.5V3.4h3.6v1.1M4.6 4.5l.5 8.2h5.8l.5-8.2"
      />
    </svg>
  )
}
