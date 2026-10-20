import type { KafkaMessageView } from '../../electron/types'

export function messageBodyText(m: KafkaMessageView): string {
  if (typeof m.value === 'string') return m.value
  try {
    return JSON.stringify(m.value, null, 2)
  } catch {
    return String(m.value)
  }
}

export function messageId(m: KafkaMessageView): string {
  return `${m.topic}:${m.partition}:${m.offset}`
}

/** Best-effort subject guess for Avro republish */
export function guessAvroSubject(topic: string, subjects: string[]): string {
  const preferred = [`${topic}-value`, `${topic}-key`, topic]
  for (const p of preferred) {
    if (subjects.includes(p)) return p
  }
  const soft = subjects.find((s) => s.startsWith(topic) || s.includes(topic))
  return soft || ''
}
