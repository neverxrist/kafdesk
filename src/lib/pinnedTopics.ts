const KEY = 'kafdesk.pinnedTopics.v1'

type Store = Record<string, string[]>

function read(): Store {
  try {
    const raw = localStorage.getItem(KEY)
    if (!raw) return {}
    const parsed = JSON.parse(raw) as Store
    return parsed && typeof parsed === 'object' ? parsed : {}
  } catch {
    return {}
  }
}

function write(store: Store) {
  try {
    localStorage.setItem(KEY, JSON.stringify(store))
  } catch {
    /* ignore */
  }
}

export function loadPinnedTopics(connectionId: string): string[] {
  return read()[connectionId] || []
}

export function persistPinnedTopics(connectionId: string, names: string[]) {
  const store = read()
  store[connectionId] = names
  write(store)
}

export function togglePinnedTopic(connectionId: string, topic: string): string[] {
  const cur = loadPinnedTopics(connectionId)
  const next = cur.includes(topic) ? cur.filter((t) => t !== topic) : [...cur, topic]
  persistPinnedTopics(connectionId, next)
  return next
}

/** Heuristic for dead-letter / error topics */
export function isDlqTopic(name: string): boolean {
  return /(^|[._-])(dlq|dlt|dead[-_]?letter|error|errors|poison)([._-]|$)/i.test(name)
}

/** Internal / system topics (Schema Registry, etc.) — show at the bottom */
export function isInternalTopic(name: string): boolean {
  return name.startsWith('_')
}
