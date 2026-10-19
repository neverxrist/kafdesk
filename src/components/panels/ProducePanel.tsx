import type { SchemaSubjectInfo } from '../../../electron/types'
import type { useLocale } from '../../hooks/useLocale'

type Tr = ReturnType<typeof useLocale>['tr']

function prettySchema(schema: string): string {
  try {
    return JSON.stringify(JSON.parse(schema), null, 2)
  } catch {
    return schema
  }
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
  produceKey,
  setProduceKey,
  produceValue,
  setProduceValue,
  busy,
  onSendMessage,
  produceResult,
}: {
  tr: Tr
  selectedTopic: string | null
  produceFormat: 'avro' | 'json' | 'string'
  setProduceFormat: (v: 'avro' | 'json' | 'string') => void
  subjects: string[]
  subject: string
  onSubjectChange: (next: string) => void
  hasRegistry: boolean
  schemaInfo: SchemaSubjectInfo | null
  produceKey: string
  setProduceKey: (v: string) => void
  produceValue: string
  setProduceValue: (v: string) => void
  busy: boolean
  onSendMessage: () => void
  produceResult: string | null
}) {
  const formatLabel =
    produceFormat === 'avro' ? 'Avro' : produceFormat === 'json' ? 'JSON' : 'String'

  return (
    <section className="produce">
      <div className="panel produce-panel">
        <h2>{tr('produce')}</h2>
        {!selectedTopic && (
          <div className="empty" style={{ paddingTop: 0 }}>
            {tr('selectTopicLeft')}
          </div>
        )}
        <div className="field">
          <label>{tr('format')}</label>
          <select
            value={produceFormat}
            onChange={(e) => setProduceFormat(e.target.value as 'avro' | 'json' | 'string')}
          >
            <option value="json">{tr('formatJson')}</option>
            <option value="avro">{tr('formatAvro')}</option>
            <option value="string">{tr('formatString')}</option>
          </select>
        </div>

        <div className={`produce-type-hint ${produceFormat}`}>
          {produceFormat === 'avro' && (
            <>
              <span className="badge avro">AVRO</span>
              <span>{tr('produceAvroHint')}</span>
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
            <select value={subject} onChange={(e) => onSubjectChange(e.target.value)} disabled={!hasRegistry}>
              <option value="">{tr('selectSubject')}</option>
              {subjects.map((s) => (
                <option key={s} value={s}>
                  {s}
                </option>
              ))}
            </select>
            {!hasRegistry && <p className="field-hint warn">{tr('srNotConnected')}</p>}
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
        <div className="field">
          <label>{tr('key')}</label>
          <input value={produceKey} onChange={(e) => setProduceKey(e.target.value)} />
        </div>
        <div className="field">
          <label>
            {tr('value')}
            {produceFormat === 'avro' ? ` ${tr('valueMatchingSchema')}` : ''}
          </label>
          <textarea
            className="produce-value"
            value={produceValue}
            onChange={(e) => setProduceValue(e.target.value)}
          />
        </div>
        <button
          className="primary"
          disabled={!selectedTopic || busy || (produceFormat === 'avro' && !subject)}
          onClick={onSendMessage}
        >
          {tr('sendAs', { format: formatLabel })}
        </button>
        {produceResult && (
          <div className="status" style={{ marginTop: 10 }}>
            {produceResult}
          </div>
        )}
      </div>
    </section>
  )
}
