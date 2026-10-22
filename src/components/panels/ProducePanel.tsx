import { useEffect, useMemo, useRef, useState } from 'react'
import type { SchemaSubjectInfo, TopicInfo } from '../../types/kafdesk'
import type { useLocale } from '../../hooks/useLocale'
import type { ProduceTemplate } from '../../lib/produceTemplates'
import type { ProduceHeaderRow } from '../../lib/produceHeaders'
import { normalizeStraightQuotes } from '../../lib/produceVars'
import { sortSubjectsForTopic } from '../../lib/messageText'

type Tr = ReturnType<typeof useLocale>['tr']

function prettySchema(schema: string): string {
  try {
    return JSON.stringify(JSON.parse(schema), null, 2)
  } catch {
    return schema
  }
}

function ClearValueIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 14 14" fill="none" aria-hidden>
      <path
        d="M3.2 3.2 10.8 10.8M10.8 3.2 3.2 10.8"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinecap="round"
      />
    </svg>
  )
}

export default function ProducePanel({
  tr,
  selectedTopic,
  produceFormat,
  setProduceFormat,
  subjects,
  subject,
  onSubjectChange,
  hasRegistry,
  schemaInfo,
  localSchemaName,
  topicDetails,
  produceKey,
  setProduceKey,
  produceHeaderRows,
  setProduceHeaderRows,
  onAddProduceHeaderRow,
  producePartition,
  setProducePartition,
  produceValue,
  setProduceValue,
  busy,
  onSendMessage,
  onBulkSend,
  onGenerateFromSchema,
  onLoadAvsc,
  produceResult,
  error,
  templates,
  onSaveTemplate,
  onApplyTemplate,
  onDeleteTemplate,
}: {
  tr: Tr
  selectedTopic: string | null
  produceFormat: 'avro' | 'avro-local' | 'json' | 'string'
  setProduceFormat: (v: 'avro' | 'avro-local' | 'json' | 'string') => void
  subjects: string[]
  subject: string
  onSubjectChange: (next: string) => void
  hasRegistry: boolean
  schemaInfo: SchemaSubjectInfo | null
  localSchemaName: string | null
  topicDetails: TopicInfo | null
  produceKey: string
  setProduceKey: (v: string) => void
  produceHeaderRows: ProduceHeaderRow[]
  setProduceHeaderRows: (rows: ProduceHeaderRow[] | ((prev: ProduceHeaderRow[]) => ProduceHeaderRow[])) => void
  onAddProduceHeaderRow: () => void
  producePartition: number | null
  setProducePartition: (v: number | null) => void
  produceValue: string
  setProduceValue: (v: string) => void
  busy: boolean
  onSendMessage: () => void
  onBulkSend: (count: number) => void
  onGenerateFromSchema: () => void
  onLoadAvsc: () => void
  produceResult: string | null
  error: string | null
  templates: ProduceTemplate[]
  onSaveTemplate: (name: string) => void
  onApplyTemplate: (id: string) => void
  onDeleteTemplate: (id: string) => void
}) {
  const formatLabel =
    produceFormat === 'avro' || produceFormat === 'avro-local'
      ? 'Avro'
      : produceFormat === 'json'
        ? 'JSON'
        : 'String'
  const needsSubject = produceFormat === 'avro'
  const needsLocalSchema = produceFormat === 'avro-local'
  const canGenerate = Boolean(schemaInfo?.schema) || Boolean(localSchemaName)
  const canSend =
    Boolean(selectedTopic) &&
    !busy &&
    Boolean(produceValue.trim()) &&
    !(needsSubject && !subject) &&
    !(needsLocalSchema && !localSchemaName)
  const [templateId, setTemplateId] = useState('')
  const [templateName, setTemplateName] = useState('')
  const [bulkCount, setBulkCount] = useState(10)
  const [templatesOpen, setTemplatesOpen] = useState(false)
  const templatesRef = useRef<HTMLDivElement>(null)
  const orderedSubjects = useMemo(
    () => sortSubjectsForTopic(selectedTopic, subjects),
    [selectedTopic, subjects],
  )

  useEffect(() => {
    if (!templatesOpen) return
    function onDoc(e: MouseEvent) {
      if (!templatesRef.current?.contains(e.target as Node)) setTemplatesOpen(false)
    }
    function onKey(e: KeyboardEvent) {
      if (e.key === 'Escape') setTemplatesOpen(false)
    }
    document.addEventListener('mousedown', onDoc)
    window.addEventListener('keydown', onKey)
    return () => {
      document.removeEventListener('mousedown', onDoc)
      window.removeEventListener('keydown', onKey)
    }
  }, [templatesOpen])

  return (
    <section className="produce drag-scroll">
      <div className="panel produce-panel">
        <div className="produce-head">
          <h2>{tr('produce')}</h2>
          <div className={`produce-templates-menu ${templatesOpen ? 'open' : ''}`} ref={templatesRef}>
            <button
              type="button"
              className="produce-templates-btn"
              aria-expanded={templatesOpen}
              aria-haspopup="dialog"
              onClick={() => setTemplatesOpen((v) => !v)}
            >
              {tr('produceTemplates')}
              {templates.length ? ` · ${templates.length}` : ''}
              <span aria-hidden>{templatesOpen ? '▴' : '▾'}</span>
            </button>
            {templatesOpen && (
              <div className="produce-templates-popover" role="dialog" aria-label={tr('produceTemplates')}>
                <div className="field">
                  <label>{tr('selectTemplate')}</label>
                  <select value={templateId} onChange={(e) => setTemplateId(e.target.value)}>
                    <option value="">
                      {templates.length ? tr('selectTemplate') : tr('noTemplates')}
                    </option>
                    {templates.map((t) => (
                      <option key={t.id} value={t.id}>
                        {t.name}
                        {t.topic ? ` · ${t.topic}` : ''}
                      </option>
                    ))}
                  </select>
                </div>
                <div className="produce-templates-popover-actions">
                  <button
                    type="button"
                    disabled={!templateId || busy}
                    onClick={() => {
                      onApplyTemplate(templateId)
                      setTemplatesOpen(false)
                    }}
                  >
                    {tr('applyTemplate')}
                  </button>
                  <button
                    type="button"
                    disabled={!templateId}
                    onClick={() => {
                      onDeleteTemplate(templateId)
                      setTemplateId('')
                    }}
                  >
                    {tr('deleteTemplate')}
                  </button>
                </div>
                <div className="field">
                  <label>{tr('templateNamePh')}</label>
                  <input
                    value={templateName}
                    onChange={(e) => setTemplateName(e.target.value)}
                    placeholder={tr('templateNamePh')}
                  />
                </div>
                <button
                  type="button"
                  className="primary"
                  disabled={!templateName.trim()}
                  onClick={() => {
                    onSaveTemplate(templateName)
                    setTemplateName('')
                  }}
                >
                  {tr('saveTemplate')}
                </button>
              </div>
            )}
          </div>
        </div>

        {!selectedTopic && (
          <div className="empty" style={{ paddingTop: 0 }}>
            {tr('selectTopicLeft')}
          </div>
        )}

        <div className="field">
          <label>{tr('format')}</label>
          <select
            value={produceFormat}
            onChange={(e) =>
              setProduceFormat(e.target.value as 'avro' | 'avro-local' | 'json' | 'string')
            }
          >
            <option value="json">{tr('formatJson')}</option>
            <option value="avro">{tr('formatAvro')}</option>
            <option value="avro-local">{tr('formatAvroLocal')}</option>
            <option value="string">{tr('formatString')}</option>
          </select>
        </div>

        <div className={`produce-type-hint ${produceFormat === 'avro-local' ? 'avro' : produceFormat}`}>
          {produceFormat === 'avro' && (
            <>
              <span className="badge avro">AVRO · SR</span>
              <span>{tr('produceAvroHint')}</span>
            </>
          )}
          {produceFormat === 'avro-local' && (
            <>
              <span className="badge avro">AVRO · LOCAL</span>
              <span>{tr('produceAvroLocalHint')}</span>
            </>
          )}
          {produceFormat === 'json' && (
            <>
              <span className="badge json">JSON</span>
              <span>{tr('produceJsonHint')}</span>
            </>
          )}
          {produceFormat === 'string' && (
            <>
              <span className="badge string">STRING</span>
              <span>{tr('produceStringHint')}</span>
            </>
          )}
        </div>

        {produceFormat === 'avro' && (
          <div className="field">
            <label>{tr('subject')}</label>
            <select
              value={subject}
              onChange={(e) => onSubjectChange(e.target.value)}
              disabled={!hasRegistry}
            >
              <option value="">{tr('selectSubject')}</option>
              {orderedSubjects.map((s) => (
                <option key={s} value={s}>
                  {s}
                </option>
              ))}
            </select>
            {!hasRegistry && <p className="field-hint warn">{tr('srNotConnected')}</p>}
          </div>
        )}
        {produceFormat === 'avro-local' && (
          <div className="field">
            <label>{tr('loadAvsc')}</label>
            <div className="saved-queries-row">
              <button type="button" disabled={busy} onClick={onLoadAvsc}>
                {tr('browse')}
              </button>
              <span className="status">
                {localSchemaName
                  ? tr('localSchemaLoaded', { name: localSchemaName })
                  : tr('localSchemaNeeded')}
              </span>
            </div>
          </div>
        )}
        {schemaInfo && produceFormat === 'avro' && (
          <div className="field">
            <label>
              {tr('activeSchema', { version: schemaInfo.version, id: schemaInfo.id })}
              {schemaInfo.schemaType ? ` · ${schemaInfo.schemaType}` : ' · AVRO'}
            </label>
            <textarea className="schema-preview" readOnly value={prettySchema(schemaInfo.schema)} />
          </div>
        )}
        <div className="produce-key-part-row">
          <div className="field">
            <label>{tr('key')}</label>
            <div className="produce-key-row">
              <input value={produceKey} onChange={(e) => setProduceKey(e.target.value)} />
              <button
                type="button"
                className={produceKey.trim() === '{{uuid}}' ? 'active' : undefined}
                title={tr('autoKeyUuidTitle')}
                disabled={busy}
                onClick={() =>
                  setProduceKey(produceKey.trim() === '{{uuid}}' ? '' : '{{uuid}}')
                }
              >
                {tr('autoKeyUuid')}
              </button>
            </div>
          </div>
          <div className="field produce-partition-field">
            <label title={tr('producePartitionAutoHint')}>{tr('producePartition')}</label>
            <select
              value={producePartition == null ? 'auto' : String(producePartition)}
              onChange={(e) => {
                const v = e.target.value
                setProducePartition(v === 'auto' ? null : Number(v))
              }}
              disabled={!selectedTopic || !topicDetails}
              title={tr('producePartitionAutoHint')}
            >
              <option value="auto">{tr('producePartitionAuto')}</option>
              {topicDetails?.partitions.map((p) => (
                <option key={p.partitionId} value={String(p.partitionId)}>
                  p{p.partitionId}
                </option>
              ))}
            </select>
          </div>
        </div>

        <details className="produce-headers" open={produceHeaderRows.length > 0}>
          <summary>{tr('produceHeaders')}</summary>
          <div className="produce-headers-body">
            {produceHeaderRows.length === 0 ? (
              <p className="field-hint">{tr('produceHeadersEmpty')}</p>
            ) : (
              produceHeaderRows.map((row) => (
                <div key={row.id} className="produce-header-row">
                  <input
                    value={row.key}
                    placeholder={tr('headerKeyPh')}
                    disabled={busy}
                    onChange={(e) => {
                      const key = e.target.value
                      setProduceHeaderRows((prev) =>
                        prev.map((r) => (r.id === row.id ? { ...r, key } : r)),
                      )
                    }}
                  />
                  <input
                    value={row.value}
                    placeholder={tr('headerValuePh')}
                    disabled={busy}
                    onChange={(e) => {
                      const value = e.target.value
                      setProduceHeaderRows((prev) =>
                        prev.map((r) => (r.id === row.id ? { ...r, value } : r)),
                      )
                    }}
                  />
                  <button
                    type="button"
                    className="produce-header-remove"
                    title={tr('produceHeaderRemove')}
                    disabled={busy}
                    onClick={() =>
                      setProduceHeaderRows((prev) => prev.filter((r) => r.id !== row.id))
                    }
                  >
                    ×
                  </button>
                </div>
              ))
            )}
            <button type="button" disabled={busy} onClick={onAddProduceHeaderRow}>
              {tr('produceHeaderAdd')}
            </button>
          </div>
        </details>

        <div className="field">
          <label>
            {tr('value')}
            {produceFormat === 'avro' || produceFormat === 'avro-local'
              ? ` ${tr('valueMatchingSchema')}`
              : ''}
          </label>
          <div className="produce-value-wrap">
            <textarea
              className="produce-value"
              value={produceValue}
              spellCheck={false}
              autoCorrect="off"
              autoCapitalize="off"
              onChange={(e) => setProduceValue(normalizeStraightQuotes(e.target.value))}
            />
            <button
              type="button"
              className="produce-value-clear"
              disabled={busy || !produceValue}
              onClick={() => setProduceValue('')}
              title={tr('clear')}
              aria-label={tr('clear')}
            >
              <ClearValueIcon />
            </button>
          </div>
          {(produceFormat === 'avro' || produceFormat === 'avro-local') && (
            <div className="produce-schema-actions">
              <button type="button" disabled={busy || !canGenerate} onClick={onGenerateFromSchema}>
                {tr('generateFromSchema')}
              </button>
            </div>
          )}
        </div>
        <div className="produce-send-row">
          <button
            type="button"
            className="primary"
            disabled={!canSend}
            onClick={onSendMessage}
          >
            {tr('sendAs', { format: formatLabel })}
          </button>
          <label className="produce-bulk-count">
            <span>{tr('bulkCount')}</span>
            <input
              type="number"
              min={1}
              max={2000}
              value={bulkCount}
              onChange={(e) =>
                setBulkCount(Math.min(2000, Math.max(1, Number(e.target.value) || 1)))
              }
            />
          </label>
          <button
            type="button"
            disabled={!canSend}
            onClick={() => onBulkSend(bulkCount)}
          >
            {tr('bulkSend', { n: bulkCount })}
          </button>
        </div>
        {produceResult && (
          <div className="status" style={{ marginTop: 10 }}>
            {produceResult}
          </div>
        )}
        {error && (
          <div className="error" style={{ marginTop: 10 }}>
            {error}
          </div>
        )}
      </div>
    </section>
  )
}
