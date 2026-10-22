import type { KafkaMessageView } from '../types/kafdesk'

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

/** Canonical TopicNameStrategy candidates (may exist even if /subjects list is filtered). */
export function topicSubjectCandidates(topic: string): string[] {
  if (!topic) return []
  return [`${topic}-value`, `${topic}-key`, topic]
}

/** Subjects likely tied to a topic (list match — soft includes topic name). */
export function subjectsForTopic(topic: string, subjects: string[]): string[] {
  if (!topic) return []
  const preferred = topicSubjectCandidates(topic)
  const exact = preferred.filter((p) => subjects.includes(p))
  const soft = subjects.filter((s) => {
    if (exact.includes(s)) return false
    if (s.startsWith(`${topic}-`) || s.startsWith(`${topic}.`) || s === topic) return true
    // Record/TopicRecord strategies often embed the topic mid-name.
    return s.includes(topic)
  })
  soft.sort((a, b) => {
    const pa = preferred.includes(a) ? 0 : a.includes('-value') ? 1 : 2
    const pb = preferred.includes(b) ? 0 : b.includes('-value') ? 1 : 2
    return pa - pb || a.localeCompare(b)
  })
  return [...exact, ...soft]
}

/** Topic-related subjects first, then alphabetical */
export function sortSubjectsForTopic(topic: string | null, subjects: string[]): string[] {
  if (!topic) return [...subjects].sort()
  const preferred = [`${topic}-value`, `${topic}-key`, topic]
  const score = (s: string) => {
    const i = preferred.indexOf(s)
    if (i >= 0) return i
    if (s.startsWith(`${topic}-`) || s.startsWith(topic)) return 10
    if (s.includes(topic)) return 20
    return 100
  }
  return [...subjects].sort((a, b) => score(a) - score(b) || a.localeCompare(b))
}
