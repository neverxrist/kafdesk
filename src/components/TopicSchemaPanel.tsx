import { useEffect, useMemo, useRef, useState } from 'react'
import type { SchemaSubjectInfo } from '../types/kafdesk'
import type { useLocale } from '../hooks/useLocale'
import { subjectsForTopic, topicSubjectCandidates } from '../lib/messageText'

type Tr = ReturnType<typeof useLocale>['tr']

function prettySchema(schema: string): string {
  try {
    return JSON.stringify(JSON.parse(schema), null, 2)
  } catch {
    return schema
  }
}

function subjectLabel(tr: Tr, s: string): string {
  if (s.endsWith('-value')) return `${tr('topicSchemaValue')} · ${s}`
  if (s.endsWith('-key')) return `${tr('topicSchemaKey')} · ${s}`
  return s
}

export default function TopicSchemaPanel({
  tr,
  topic,
  subjects,
  hasRegistry,
  onUseSubject,
  onAutoApplySubject,
  onOpenInSchemas,
  defaultOpen,
  autoApplySubject = false,
  showNoneHint = true,
}: {
  tr: Tr
  topic: string | null
  subjects: string[]
  hasRegistry: boolean
  /** Produce: set subject (+ usually switch to Avro SR). */
  onUseSubject?: (subject: string) => void
  /**
   * Produce auto-fill after probe. Prefer over onUseSubject so restored drafts
   * are not overwritten (caller can no-op when Subject is already set).
   */
  onAutoApplySubject?: (subject: string) => void
  /** Consume popover: jump to Schemas tab with this subject selected. */
  onOpenInSchemas?: (subject: string) => void
  /** When false, stay collapsed. When true/omitted with matches — open (consume popover). */
  defaultOpen?: boolean
  /** Produce: once per topic, push best match into Subject (and usually Avro SR). */
  autoApplySubject?: boolean
  /** Warn plaque when probe finds no topic subject (Produce: only for Avro SR). */
  showNoneHint?: boolean
}) {
  const fromList = useMemo(
    () => (topic ? subjectsForTopic(topic, subjects) : []),
    [topic, subjects],
  )
  /** Subjects confirmed via GET …/subjects/{name}/versions/latest (list can be ACL-filtered). */
  const [probed, setProbed] = useState<string[]>([])
  const [probeDone, setProbeDone] = useState(false)
  const [picked, setPicked] = useState('')
  const [info, setInfo] = useState<SchemaSubjectInfo | null>(null)
  const [loading, setLoading] = useState(false)
  const [err, setErr] = useState<string | null>(null)
  const probeGen = useRef(0)
  const loadGen = useRef(0)
  const autoAppliedTopic = useRef<string | null>(null)

  const matched = useMemo(() => {
    const seen = new Set<string>()
    const out: string[] = []
    for (const s of [...probed, ...fromList]) {
      if (seen.has(s)) continue
      seen.add(s)
      out.push(s)
    }
    return out
  }, [probed, fromList])

  // Probe TopicNameStrategy names even when /subjects omits them.
  useEffect(() => {
    autoAppliedTopic.current = null
    if (!topic || !hasRegistry) {
      setProbed([])
      setProbeDone(true)
      return
    }
    const gen = ++probeGen.current
    setProbeDone(false)
    setProbed([])
    const candidates = topicSubjectCandidates(topic)
    let cancelled = false
    void (async () => {
      const found: string[] = []
      for (const name of candidates) {
        if (cancelled || probeGen.current !== gen) return
        try {
          await window.kafdesk.getLatestSchema(name)
          found.push(name)
        } catch {
          /* 404 / no subject */
        }
      }
      if (cancelled || probeGen.current !== gen) return
      setProbed(found)
      setProbeDone(true)
    })()
    return () => {
      cancelled = true
    }
  }, [topic, hasRegistry])

  useEffect(() => {
    if (!matched.length) {
      setPicked('')
      return
    }
    setPicked((prev) => (prev && matched.includes(prev) ? prev : matched[0]))
  }, [matched])

  // Produce: fill Subject with best topic schema once discovery finishes (don't re-force).
  useEffect(() => {
    const apply = onAutoApplySubject ?? onUseSubject
    if (!autoApplySubject || !apply || !topic || !probeDone || !matched.length) return
    if (autoAppliedTopic.current === topic) return
    // Prefer names for THIS topic — never pick a leftover -value from a prior probe.
    const preferred =
      matched.find((s) => s === `${topic}-value`) ||
      matched.find((s) => s === topic) ||
      matched.find((s) => s === `${topic}-key`) ||
      matched.find((s) => s.startsWith(`${topic}-`) && s.endsWith('-value')) ||
      matched.find((s) => s.startsWith(`${topic}-`) || s.startsWith(`${topic}.`)) ||
      matched[0]
    autoAppliedTopic.current = topic
    apply(preferred)
  }, [autoApplySubject, onAutoApplySubject, onUseSubject, topic, probeDone, matched])

  useEffect(() => {
    if (!picked || !hasRegistry) {
      setInfo(null)
      setErr(null)
      setLoading(false)
      return
    }
    const gen = ++loadGen.current
    setLoading(true)
    setErr(null)
    void window.kafdesk
      .getLatestSchema(picked)
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
  }, [picked, hasRegistry])

  // Produce passes defaultOpen={false} (collapsed). Consume popover opens when matches exist.
  const openAttr =
    defaultOpen === false ? undefined : matched.length > 0 || defaultOpen === true ? true : undefined

  if (!topic) return null
  if (!hasRegistry) {
    return (
      <details className="topic-schema-panel" open={openAttr}>
        <summary>{tr('topicSchema')}</summary>
        <p className="field-hint warn">{tr('topicSchemaNoSr')}</p>
      </details>
    )
  }

  const discovering = !probeDone && matched.length === 0

  if (probeDone && matched.length === 0) {
    if (!showNoneHint) return null
    return (
      <div className="produce-type-hint is-warn topic-schema-none-hint">
        <span className="badge warn">SR</span>
        <span>{tr('topicSchemaNone')}</span>
      </div>
    )
  }

  return (
    <details className="topic-schema-panel" open={openAttr}>
      <summary>
        {tr('topicSchema')}
        {matched.length > 0 && (
          <span className="topic-schema-count">{matched.length}</span>
        )}
      </summary>
      <div className="topic-schema-body">
        {discovering && <p className="field-hint">{tr('topicSchemaLoading')}</p>}
        {matched.length > 0 && (
          <div className="topic-schema-toolbar">
            <select
              value={picked}
              onChange={(e) => setPicked(e.target.value)}
              aria-label={tr('subject')}
            >
              {matched.map((s) => (
                <option key={s} value={s}>
                  {subjectLabel(tr, s)}
                </option>
              ))}
            </select>
            {onUseSubject && (
              <button
                type="button"
                disabled={!picked || loading}
                onClick={() => onUseSubject(picked)}
                title={tr('topicSchemaUseTitle')}
              >
                {tr('topicSchemaUse')}
              </button>
            )}
            {onOpenInSchemas && (
              <button
                type="button"
                disabled={!picked || loading}
                onClick={() => onOpenInSchemas(picked)}
                title={tr('topicSchemaOpenTabTitle')}
              >
                {tr('topicSchemaOpenTab')}
              </button>
            )}
          </div>
        )}
        {loading && <p className="field-hint">{tr('topicSchemaLoading')}</p>}
        {err && <p className="field-hint warn">{err}</p>}
        {info && !loading && (
          <>
            <p className="topic-schema-meta">
              {tr('activeSchema', { version: info.version, id: info.id })}
              {info.schemaType ? ` · ${info.schemaType}` : ' · AVRO'}
            </p>
            <textarea
              className="schema-preview topic-schema-preview"
              readOnly
              value={prettySchema(info.schema)}
              spellCheck={false}
            />
          </>
        )}
      </div>
    </details>
  )
}
