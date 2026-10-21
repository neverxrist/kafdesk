import type { ReactNode } from 'react'

const TOKEN =
  /("(?:\\.|[^"\\])*")(\s*:)?|(\btrue\b|\bfalse\b|\bnull\b)|(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)|(\s+|[{}\[\],])/g

function looksLikeJson(text: string): boolean {
  const t = text.trim()
  return (t.startsWith('{') && t.endsWith('}')) || (t.startsWith('[') && t.endsWith(']'))
}

/** Lightweight JSON syntax highlight (no deps). Plain text returned as-is. */
export function highlightMessageBody(text: string): ReactNode {
  if (!looksLikeJson(text)) return text

  const nodes: ReactNode[] = []
  let last = 0
  let i = 0
  TOKEN.lastIndex = 0
  let m: RegExpExecArray | null
  while ((m = TOKEN.exec(text))) {
    if (m.index > last) {
      nodes.push(text.slice(last, m.index))
    }
    const [full, str, colon, lit, num, punct] = m
    if (str != null) {
      nodes.push(
        <span key={i++} className={colon ? 'jh-key' : 'jh-str'}>
          {str}
          {colon ?? ''}
        </span>,
      )
    } else if (lit != null) {
      nodes.push(
        <span key={i++} className={lit === 'null' ? 'jh-null' : 'jh-bool'}>
          {lit}
        </span>,
      )
    } else if (num != null) {
      nodes.push(
        <span key={i++} className="jh-num">
          {num}
        </span>,
      )
    } else {
      nodes.push(punct ?? full)
    }
    last = m.index + full.length
  }
  if (last < text.length) nodes.push(text.slice(last))
  return nodes
}
