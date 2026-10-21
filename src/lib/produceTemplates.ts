export const PRODUCE_TEMPLATES_KEY = 'kafdesk.produceTemplates.v1'

export interface ProduceTemplate {
  id: string
  name: string
  createdAt: number
  topic: string | null
  format: 'avro' | 'avro-local' | 'json' | 'string'
  subject: string
  key: string
  value: string
}

export function loadProduceTemplates(): ProduceTemplate[] {
  try {
    const raw = localStorage.getItem(PRODUCE_TEMPLATES_KEY)
    if (!raw) return []
    const parsed = JSON.parse(raw) as ProduceTemplate[]
    return Array.isArray(parsed) ? parsed : []
  } catch {
    return []
  }
}

export function persistProduceTemplates(list: ProduceTemplate[]) {
  try {
    localStorage.setItem(PRODUCE_TEMPLATES_KEY, JSON.stringify(list))
  } catch {
    /* ignore */
  }
}
