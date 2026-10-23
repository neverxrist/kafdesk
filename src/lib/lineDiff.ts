export type DiffLineKind = 'same' | 'changed' | 'added' | 'removed' | 'empty'

export type DiffSideLine = {
  text: string
  kind: DiffLineKind
}

type AlignResult = { left: DiffSideLine[]; right: DiffSideLine[] }

/**
 * Prefer structural JSON align (same keys share a row — no LCS “float”).
 * Fall back to paired text lines for non-JSON.
 */
export function alignLineDiff(leftText: string, rightText: string): AlignResult {
  const leftVal = tryParseJson(leftText)
  const rightVal = tryParseJson(rightText)
  if (leftVal !== undefined && rightVal !== undefined) {
    const outL: DiffSideLine[] = []
    const outR: DiffSideLine[] = []
    diffValue(leftVal, rightVal, 0, outL, outR, false)
    if (outL.length === 0) {
      return emptyPair()
    }
    return { left: outL, right: outR }
  }
  return alignTextLines(leftText, rightText)
}

function emptyPair(): AlignResult {
  return {
    left: [{ text: '', kind: 'empty' }],
    right: [{ text: '', kind: 'empty' }],
  }
}

function tryParseJson(text: string): unknown | undefined {
  const t = text.trim()
  if (!t || (!(t.startsWith('{') || t.startsWith('[')))) return undefined
  try {
    return JSON.parse(t) as unknown
  } catch {
    return undefined
  }
}

function isPlainObject(v: unknown): v is Record<string, unknown> {
  return v != null && typeof v === 'object' && !Array.isArray(v)
}

function deepEqual(a: unknown, b: unknown): boolean {
  if (Object.is(a, b)) return true
  if (typeof a !== typeof b) return false
  if (a == null || b == null) return a === b
  if (Array.isArray(a) && Array.isArray(b)) {
    if (a.length !== b.length) return false
    return a.every((x, i) => deepEqual(x, b[i]))
  }
  if (isPlainObject(a) && isPlainObject(b)) {
    const ak = Object.keys(a)
    const bk = Object.keys(b)
    if (ak.length !== bk.length) return false
    return ak.every((k) => deepEqual(a[k], b[k]))
  }
  return false
}

function pad(depth: number): string {
  return '  '.repeat(depth)
}

function atom(v: unknown): string {
  return JSON.stringify(v)
}

function row(
  outL: DiffSideLine[],
  outR: DiffSideLine[],
  l: string,
  r: string,
  kind: DiffLineKind,
) {
  if (kind === 'removed') {
    outL.push({ text: l, kind: 'removed' })
    outR.push({ text: '', kind: 'empty' })
    return
  }
  if (kind === 'added') {
    outL.push({ text: '', kind: 'empty' })
    outR.push({ text: r, kind: 'added' })
    return
  }
  outL.push({ text: l, kind })
  outR.push({ text: r, kind })
}

/** Dump one side’s value as pretty lines (for keys only on left or right). */
function dumpSide(
  value: unknown,
  depth: number,
  key: string | undefined,
  comma: boolean,
  side: 'left' | 'right',
  outL: DiffSideLine[],
  outR: DiffSideLine[],
) {
  const lines = prettyLines(value, depth, key, comma)
  const kind = side === 'left' ? 'removed' : 'added'
  for (const line of lines) {
    if (side === 'left') {
      outL.push({ text: line, kind })
      outR.push({ text: '', kind: 'empty' })
    } else {
      outL.push({ text: '', kind: 'empty' })
      outR.push({ text: line, kind })
    }
  }
}

function prettyLines(
  value: unknown,
  depth: number,
  key: string | undefined,
  comma: boolean,
): string[] {
  const p = pad(depth)
  const head = key != null ? `${p}${atom(key)}: ` : p
  const c = comma ? ',' : ''

  if (value == null || typeof value !== 'object') {
    return [`${head}${atom(value)}${c}`]
  }

  if (Array.isArray(value)) {
    if (value.length === 0) return [`${head}[]${c}`]
    const lines = [`${head}[`]
    value.forEach((item, i) => {
      lines.push(...prettyLines(item, depth + 1, undefined, i < value.length - 1))
    })
    lines.push(`${p}]${c}`)
    return lines
  }

  const obj = value as Record<string, unknown>
  const keys = Object.keys(obj)
  if (keys.length === 0) return [`${head}{}${c}`]
  const lines = [`${head}{`]
  keys.forEach((k, i) => {
    lines.push(...prettyLines(obj[k], depth + 1, k, i < keys.length - 1))
  })
  lines.push(`${p}}${c}`)
  return lines
}

/**
 * @param comma — whether this value is followed by a sibling (needs trailing comma)
 */
function diffValue(
  left: unknown,
  right: unknown,
  depth: number,
  outL: DiffSideLine[],
  outR: DiffSideLine[],
  comma: boolean,
  key?: string,
) {
  const p = pad(depth)
  const head = key != null ? `${p}${atom(key)}: ` : p
  const c = comma ? ',' : ''

  if (deepEqual(left, right)) {
    for (const line of prettyLines(left, depth, key, comma)) {
      row(outL, outR, line, line, 'same')
    }
    return
  }

  if (isPlainObject(left) && isPlainObject(right)) {
    // Braces stay "same" — only leaves / missing keys carry color (MR-style).
    row(outL, outR, `${head}{`, `${head}{`, 'same')
    const ordered = mergeKeys(Object.keys(left), Object.keys(right))
    ordered.forEach((k, i) => {
      const last = i === ordered.length - 1
      const hasL = Object.prototype.hasOwnProperty.call(left, k)
      const hasR = Object.prototype.hasOwnProperty.call(right, k)
      if (hasL && hasR) {
        diffValue(left[k], right[k], depth + 1, outL, outR, !last, k)
      } else if (hasL) {
        dumpSide(left[k], depth + 1, k, !last, 'left', outL, outR)
      } else {
        dumpSide(right[k], depth + 1, k, !last, 'right', outL, outR)
      }
    })
    row(outL, outR, `${p}}${c}`, `${p}}${c}`, 'same')
    return
  }

  if (Array.isArray(left) && Array.isArray(right)) {
    row(outL, outR, `${head}[`, `${head}[`, 'same')
    const n = Math.max(left.length, right.length)
    for (let i = 0; i < n; i++) {
      const last = i === n - 1
      const hasL = i < left.length
      const hasR = i < right.length
      if (hasL && hasR) {
        diffValue(left[i], right[i], depth + 1, outL, outR, !last)
      } else if (hasL) {
        dumpSide(left[i], depth + 1, undefined, !last, 'left', outL, outR)
      } else {
        dumpSide(right[i], depth + 1, undefined, !last, 'right', outL, outR)
      }
    }
    row(outL, outR, `${p}]${c}`, `${p}]${c}`, 'same')
    return
  }

  // Different scalars or type mismatch — one aligned row.
  row(outL, outR, `${head}${atom(left)}${c}`, `${head}${atom(right)}${c}`, 'changed')
}

function mergeKeys(leftKeys: string[], rightKeys: string[]): string[] {
  const seen = new Set<string>()
  const out: string[] = []
  for (const k of leftKeys) {
    out.push(k)
    seen.add(k)
  }
  for (const k of rightKeys) {
    if (!seen.has(k)) out.push(k)
  }
  return out
}

function alignTextLines(leftText: string, rightText: string): AlignResult {
  const left = leftText === '' ? [] : leftText.split('\n')
  const right = rightText === '' ? [] : rightText.split('\n')
  const outL: DiffSideLine[] = []
  const outR: DiffSideLine[] = []
  const k = Math.min(left.length, right.length)
  for (let i = 0; i < k; i++) {
    if (left[i] === right[i]) {
      outL.push({ text: left[i]!, kind: 'same' })
      outR.push({ text: right[i]!, kind: 'same' })
    } else {
      outL.push({ text: left[i]!, kind: 'changed' })
      outR.push({ text: right[i]!, kind: 'changed' })
    }
  }
  for (let i = k; i < left.length; i++) {
    outL.push({ text: left[i]!, kind: 'removed' })
    outR.push({ text: '', kind: 'empty' })
  }
  for (let j = k; j < right.length; j++) {
    outL.push({ text: '', kind: 'empty' })
    outR.push({ text: right[j]!, kind: 'added' })
  }
  return outL.length ? { left: outL, right: outR } : emptyPair()
}
