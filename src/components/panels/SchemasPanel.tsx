import { useEffect, useMemo, useRef, useState, type PointerEvent as ReactPointerEvent } from 'react'
import type { SchemaSubjectInfo } from '../../types/kafdesk'
import type { useLocale } from '../../hooks/useLocale'
import {
  clampSchemasWidth,
  loadSchemasWidth,
  persistSchemasWidth,
} from '../../lib/schemasLayout'
import { RefreshIcon } from '../ConnActionIcons'

type Tr = ReturnType<typeof useLocale>['tr']

function prettySchema(schema: string): string {
  try {
    return JSON.stringify(JSON.parse(schema), null, 2)
  } catch {
    return schema
  }
}

export default function SchemasPanel({
  tr,
  connected,
  hasRegistry,
  subjects,
  busy,
  onRefreshSubjects,
  onUseForProduce,
  focusSubject,
  onFocusSubjectConsumed,
}: {
  tr: Tr
  connected: boolean
  hasRegistry: boolean
  subjects: string[]
  busy: boolean
  onRefreshSubjects: () => void
  onUseForProduce?: (subject: string) => void
  /** One-shot select from topic schema popover. */
  focusSubject?: string | null
  onFocusSubjectConsumed?: () => void
}) {
  const [query, setQuery] = useState('')
  const [selected, setSelected] = useState<string | null>(null)
  const [info, setInfo] = useState<SchemaSubjectInfo | null>(null)
  const [loading, setLoading] = useState(false)
  const [err, setErr] = useState<string | null>(null)
  const [schemasWidth, setSchemasWidth] = useState(() => loadSchemasWidth())
  const [resizing, setResizing] = useState(false)
  const resizeStart = useRef<{ x: number; w: number } | null>(null)
  const loadGen = useRef(0)
  const clearFocusRef = useRef(onFocusSubjectConsumed)
  clearFocusRef.current = onFocusSubjectConsumed

  useEffect(() => {
    if (!resizing) return
    function onMove(e: PointerEvent) {
      const start = resizeStart.current
      if (!start) return
      setSchemasWidth(clampSchemasWidth(start.w + (e.clientX - start.x)))
    }
    function onUp() {
      resizeStart.current = null
      setResizing(false)
      document.body.classList.remove('schemas-resizing')
      setSchemasWidth((w) => {
        persistSchemasWidth(w)
        return w
      })
    }
    window.addEventListener('pointermove', onMove)
    window.addEventListener('pointerup', onUp)
    window.addEventListener('pointercancel', onUp)
    return () => {
      window.removeEventListener('pointermove', onMove)
      window.removeEventListener('pointerup', onUp)
      window.removeEventListener('pointercancel', onUp)
    }
  }, [resizing])

  function onResizePointerDown(e: ReactPointerEvent<HTMLDivElement>) {
    e.preventDefault()
    resizeStart.current = { x: e.clientX, w: schemasWidth }
    setResizing(true)
    document.body.classList.add('schemas-resizing')
  }

  useEffect(() => {
    if (!focusSubject) return
    setSelected(focusSubject)
    setQuery('')
    clearFocusRef.current?.()
  }, [focusSubject])

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase()
    const list = q ? subjects.filter((s) => s.toLowerCase().includes(q)) : subjects
    return [...list].sort((a, b) => a.localeCompare(b))
  }, [subjects, query])

  useEffect(() => {
    if (selected && !subjects.includes(selected)) {
      setSelected(null)
      setInfo(null)
    }
  }, [subjects, selected])

  useEffect(() => {
    if (!selected || !hasRegistry) {
      setInfo(null)
      setErr(null)
      setLoading(false)
      return
    }
    const gen = ++loadGen.current
    setLoading(true)
    setErr(null)
    void window.kafdesk
      .getLatestSchema(selected)
      .then((res) => {
        if (loadGen.current !== gen) return
        setInfo(res)
      })
      .catch((e) => {
        if (loadGen.current !== gen) return
        setInfo(null)
        setErr(e instanceof Error ? e.message : String(e))
      })
      .finally(() => {
        if (loadGen.current === gen) setLoading(false)
      })
  }, [selected, hasRegistry])

  if (!connected) {
    return (
      <section className="messages schemas-pane">
        <div className="empty">{tr('schemasConnectFirst')}</div>
      </section>
    )
  }

  if (!hasRegistry) {
    return (
      <section className="messages schemas-pane">
        <div className="empty">{tr('topicSchemaNoSr')}</div>
      </section>
    )
  }

  return (
    <section className="messages schemas-pane">
      <div className="schemas-layout">
        <div
          className={`schemas-list drag-scroll${resizing ? ' is-resizing' : ''}`}
          style={{ width: schemasWidth }}
        >
          <div
            className="schemas-resize"
            role="separator"
            aria-orientation="vertical"
            aria-valuenow={schemasWidth}
            title={tr('resizeSchemas')}
            onPointerDown={onResizePointerDown}
          />
          <div className="field schemas-filter">
            <label>{tr('schemasCount', { n: filtered.length })}</label>
            <div className="schemas-filter-row">
              <input
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder={tr('filterSubjects')}
                disabled={busy}
                autoCapitalize="none"
                autoCorrect="off"
                autoComplete="off"
                spellCheck={false}
                lang="en"
              />
              <button
                type="button"
                className={`refresh-icon-btn${busy ? ' busy' : ''}`}
                disabled={busy}
                onClick={onRefreshSubjects}
                title={tr('refresh')}
                aria-label={tr('refresh')}
              >
                <RefreshIcon />
              </button>
            </div>
          </div>
          {filtered.length === 0 && (
            <div className="empty" style={{ padding: '0.75rem 0' }}>
              {subjects.length === 0 ? tr('schemasEmpty') : tr('schemasNoMatch')}
            </div>
          )}
          {filtered.map((s) => (
            <button
              key={s}
              type="button"
              className={`topic-item ${selected === s ? 'active' : ''}`}
              onClick={() => setSelected(s)}
              title={s}
            >
              {s}
            </button>
          ))}
        </div>
        <div className="schemas-detail drag-scroll">
          {!selected && <div className="empty">{tr('selectSchemaSubject')}</div>}
          {selected && (
            <div className="panel schemas-detail-panel">
              <div className="schemas-detail-head">
                <h2 className="schemas-detail-title" title={selected}>
                  {selected}
                </h2>
                {onUseForProduce && (
                  <button
                    type="button"
                    className="primary schemas-use-produce"
                    disabled={busy || loading || !info}
                    onClick={() => onUseForProduce(selected)}
                    title={tr('topicSchemaUseTitle')}
                  >
                    {tr('topicSchemaUse')}
                  </button>
                )}
              </div>
              {loading && <p className="field-hint">{tr('topicSchemaLoading')}</p>}
              {err && <p className="field-hint warn">{err}</p>}
              {info && !loading && (
                <>
                  <p className="topic-schema-meta">
                    {tr('activeSchema', { version: info.version, id: info.id })}
                    {info.schemaType ? ` · ${info.schemaType}` : ' · AVRO'}
                  </p>
                  <textarea
                    className="schema-preview schemas-preview"
                    readOnly
                    value={prettySchema(info.schema)}
                    spellCheck={false}
                  />
                </>
              )}
            </div>
          )}
        </div>
      </div>
    </section>
  )
}
