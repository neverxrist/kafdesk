/** Generate a sample / produce-template JSON matching an Avro schema. */

type AvroType = string | Record<string, unknown> | AvroType[]

const FIRST = ['Alex', 'Sam', 'Jordan', 'Casey', 'Riley', 'Morgan', 'Quinn', 'Avery']
const LAST = ['Ivanov', 'Petrov', 'Smith', 'Lee', 'Garcia', 'Kim', 'Novak', 'Rossi']

function pick<T>(arr: T[]): T {
  return arr[Math.floor(Math.random() * arr.length)]
}

function resolveNamed(
  t: Record<string, unknown>,
  named: Map<string, Record<string, unknown>>,
): Record<string, unknown> {
  if (typeof t.name === 'string') {
    const full =
      typeof t.namespace === 'string' && t.namespace
        ? `${t.namespace}.${t.name}`
        : String(t.name)
    named.set(full, t)
    named.set(String(t.name), t)
  }
  return t
}

/** Map Avro primitive + field name → {{placeholder}} (for bulk expand). */
function templateForPrimitive(fieldName: string, prim: string): unknown {
  const n = fieldName.toLowerCase()
  switch (prim) {
    case 'null':
      return null
    case 'boolean':
      return true
    case 'int':
    case 'long':
      if (/time|ts|date|stamp|at$/.test(n)) return '{{ts}}'
      if (/seq|index|offset|count|version|priority|retry|num|qty|quantity|rank/.test(n)) {
        return '{{i}}'
      }
      return '{{rand}}'
    case 'float':
    case 'double':
      return '{{rand}}'
    case 'bytes':
      return '{{uuid}}'
    case 'string':
      if (/email/.test(n)) return '{{email}}'
      if (/uuid/.test(n) || n === 'id' || n.endsWith('id')) return '{{uuid}}'
      if (/(^|_)first/.test(n)) return '{{first}}'
      if (/(^|_)last|surname/.test(n)) return '{{last}}'
      if (/name/.test(n)) return '{{name}}'
      if (/time|date|stamp/.test(n)) return '{{now}}'
      return 'val-{{i}}'
    default:
      return 'val-{{i}}'
  }
}

function concretePrimitive(prim: string): unknown {
  switch (prim) {
    case 'null':
      return null
    case 'boolean':
      return Math.random() > 0.5
    case 'int':
    case 'long':
      return Math.floor(Math.random() * 10_000)
    case 'float':
    case 'double':
      return Math.round(Math.random() * 1000) / 10
    case 'bytes':
      return btoa('kafdesk')
    case 'string':
      return `${pick(FIRST).toLowerCase()}.${pick(LAST).toLowerCase()}@example.com`
    default:
      return `value`
  }
}

function mockFromType(
  type: AvroType,
  named: Map<string, Record<string, unknown>>,
  depth: number,
  fieldName: string,
  template: boolean,
): unknown {
  if (depth > 12) return null

  if (typeof type === 'string') {
    if (named.has(type)) {
      return mockFromType(named.get(type)!, named, depth + 1, fieldName, template)
    }
    return template ? templateForPrimitive(fieldName, type) : concretePrimitive(type)
  }

  if (Array.isArray(type)) {
    const nonNull = type.find(
      (b) => b !== 'null' && !(typeof b === 'object' && b && (b as { type?: string }).type === 'null'),
    )
    const branch = nonNull ?? type[0]
    return mockFromType(branch as AvroType, named, depth + 1, fieldName, template)
  }

  if (type && typeof type === 'object') {
    const obj = resolveNamed(type as Record<string, unknown>, named)
    const t = String(obj.type || '')

    if (t === 'record') {
      const fields = Array.isArray(obj.fields) ? (obj.fields as Array<Record<string, unknown>>) : []
      const out: Record<string, unknown> = {}
      for (const f of fields) {
        const name = String(f.name || 'field')
        out[name] = mockFromType(f.type as AvroType, named, depth + 1, name, template)
      }
      return out
    }

    if (t === 'enum') {
      const symbols = Array.isArray(obj.symbols) ? (obj.symbols as string[]) : ['UNKNOWN']
      return pick(symbols)
    }

    if (t === 'array') {
      const n = template ? 1 : 1 + Math.floor(Math.random() * 2)
      return Array.from({ length: n }, () =>
        mockFromType(obj.items as AvroType, named, depth + 1, fieldName, template),
      )
    }

    if (t === 'map') {
      return {
        key1: mockFromType(obj.values as AvroType, named, depth + 1, fieldName, template),
      }
    }

    if (t === 'fixed') {
      const size = Number(obj.size) || 4
      return template
        ? '{{uuid}}'
        : btoa(String.fromCharCode(...Array.from({ length: size }, () => 1)))
    }

    if (obj.logicalType === 'uuid' || /uuid/i.test(String(obj.name || ''))) {
      return template ? '{{uuid}}' : crypto.randomUUID()
    }
    if (obj.logicalType === 'timestamp-millis' || obj.logicalType === 'timestamp-micros') {
      return template ? '{{ts}}' : Date.now()
    }
    if (obj.logicalType === 'date') {
      return template ? '{{i}}' : Math.floor(Date.now() / 86_400_000)
    }

    if (obj.type) {
      return mockFromType(obj.type as AvroType, named, depth + 1, fieldName, template)
    }
  }

  return null
}

/** Concrete sample values (no placeholders). */
export function mockFromAvroSchema(schemaJson: string): unknown {
  const schema = JSON.parse(schemaJson) as AvroType
  const named = new Map<string, Record<string, unknown>>()
  return mockFromType(schema, named, 0, '', false)
}

/**
 * Produce-template JSON: placeholders by field type/name so bulk Send ×N
 * expands differently each iteration (`{{uuid}}`, `{{i}}`, `{{ts}}`, …).
 * Numeric placeholders are left unquoted so expand yields JSON numbers.
 */
export function mockFromAvroSchemaPretty(schemaJson: string): string {
  const schema = JSON.parse(schemaJson) as AvroType
  const named = new Map<string, Record<string, unknown>>()
  const raw = mockFromType(schema, named, 0, '', true)
  let text = JSON.stringify(raw, null, 2)
  text = text.replace(/"\{\{(i|n|ts|rand)\}\}"/g, '{{$1}}')
  return text
}
