/** Expand {{placeholders}} in produce templates for bulk / single send. */

const FIRST = ['Alex', 'Sam', 'Jordan', 'Casey', 'Riley', 'Morgan', 'Quinn', 'Avery']
const LAST = ['Ivanov', 'Petrov', 'Smith', 'Lee', 'Garcia', 'Kim', 'Novak', 'Rossi']

function pick<T>(arr: T[]): T {
  return arr[Math.floor(Math.random() * arr.length)]
}

/** macOS smart quotes («» “”) break JSON — force ASCII quotes. */
export function normalizeStraightQuotes(s: string): string {
  return s
    .replace(/[\u00AB\u00BB\u201C\u201D\u201E\u201F]/g, '"')
    .replace(/[\u2018\u2019\u2039\u203A]/g, "'")
}

export function expandProduceTemplate(template: string, index: number): string {
  const now = new Date()
  const map: Record<string, string> = {
    i: String(index),
    n: String(index + 1),
    uuid: crypto.randomUUID(),
    now: now.toISOString(),
    ts: String(now.getTime()),
    /** Unix seconds — fits Avro `int` (millis do not). */
    ts_sec: String(Math.floor(now.getTime() / 1000)),
    date: now.toISOString().slice(0, 10),
    name: `${pick(FIRST)} ${pick(LAST)}`,
    first: pick(FIRST),
    last: pick(LAST),
    email: `${pick(FIRST).toLowerCase()}.${pick(LAST).toLowerCase()}@example.com`,
    rand: String(Math.floor(Math.random() * 1_000_000)),
  }

  const body = normalizeStraightQuotes(template)
  return body.replace(/\{\{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\}\}/g, (_, key: string) => {
    return map[key] ?? map[key.toLowerCase()] ?? `{{${key}}}`
  })
}
