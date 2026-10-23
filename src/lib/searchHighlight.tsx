import { cloneElement, Fragment, isValidElement, type ReactNode } from 'react'

/** Wrap case-insensitive substring matches in `<mark class="search-hit">`. */
export function highlightSearchMatches(text: string, query: string): ReactNode {
  const q = query.trim()
  if (!q || !text) return text
  const lower = text.toLowerCase()
  const needle = q.toLowerCase()
  if (!lower.includes(needle)) return text

  const parts: ReactNode[] = []
  let i = 0
  let k = 0
  while (i < text.length) {
    const idx = lower.indexOf(needle, i)
    if (idx < 0) {
      parts.push(text.slice(i))
      break
    }
    if (idx > i) parts.push(text.slice(i, idx))
    parts.push(
      <mark key={`h${k++}`} className="search-hit">
        {text.slice(idx, idx + needle.length)}
      </mark>,
    )
    i = idx + needle.length
  }
  return parts
}

/** Walk a React tree (e.g. JSON token spans) and mark matches in text leaves. */
export function applySearchHighlight(node: ReactNode, query: string): ReactNode {
  const q = query.trim()
  if (!q || node == null || node === false) return node
  if (typeof node === 'string' || typeof node === 'number') {
    return highlightSearchMatches(String(node), q)
  }
  if (Array.isArray(node)) {
    return node.map((child, i) => (
      <Fragment key={i}>{applySearchHighlight(child, q)}</Fragment>
    ))
  }
  if (isValidElement<{ children?: ReactNode }>(node)) {
    if (node.type === 'mark') return node
    const kids = node.props.children
    if (kids == null) return node
    return cloneElement(node, {
      ...node.props,
      children: applySearchHighlight(kids, q),
    })
  }
  return node
}
