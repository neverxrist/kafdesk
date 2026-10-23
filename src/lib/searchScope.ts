export type SearchScope = 'both' | 'key' | 'value'

export const SEARCH_SCOPES: SearchScope[] = ['both', 'key', 'value']

/** One search box + Key/Value scope (AND with a second clause if present). */
export interface SearchClause {
  text: string
  scope: SearchScope
}

/** Hits trims JSON value fields — only meaningful when Value is in scope. */
export function searchScopeIncludesValue(scope: SearchScope): boolean {
  return scope === 'both' || scope === 'value'
}

export function searchClauseActive(c: SearchClause): boolean {
  return Boolean(c.text.trim())
}

/** Needles for key / value highlight from active clauses. */
export function searchHighlightQueries(clauses: SearchClause[]): {
  keyQueries: string[]
  valueQueries: string[]
} {
  const keyQueries: string[] = []
  const valueQueries: string[] = []
  for (const c of clauses) {
    const q = c.text.trim()
    if (!q) continue
    if (c.scope !== 'value') keyQueries.push(q)
    if (c.scope !== 'key') valueQueries.push(q)
  }
  return { keyQueries, valueQueries }
}

/** Value-scope needles for Hits trim — one entry per Hits-on clause. */
export function searchHitsNeedles(
  clauses: SearchClause[],
  hitsOn: readonly [boolean, boolean] | boolean[],
): string[] {
  const out: string[] = []
  for (let i = 0; i < clauses.length; i++) {
    if (!hitsOn[i]) continue
    const c = clauses[i]
    const q = c?.text.trim()
    if (!q || !searchScopeIncludesValue(c.scope)) continue
    out.push(q)
  }
  return out
}

/** Map one search box + scope → wire fields for IPC / matchers. */
export function wireSearch(
  text: string,
  scope: SearchScope,
): { search?: string; searchKey?: string; searchScope: SearchScope } {
  const q = text.trim()
  if (!q) return { searchScope: scope }
  if (scope === 'key') return { searchKey: q, searchScope: 'key' }
  if (scope === 'value') return { search: q, searchScope: 'value' }
  return { search: q, searchScope: 'both' }
}

/** Restore UI text + scope from stored search / searchKey / optional scope. */
export function uiSearchFromStored(
  search: string,
  searchKey: string,
  scope?: SearchScope | string | null,
): { text: string; scope: SearchScope } {
  const s = (scope === 'both' || scope === 'key' || scope === 'value' ? scope : null) as
    | SearchScope
    | null
  if (s === 'key') return { text: searchKey || search, scope: 'key' }
  if (s === 'value') return { text: search || searchKey, scope: 'value' }
  if (s === 'both') return { text: search || searchKey, scope: 'both' }
  // Legacy saved queries (no scope)
  if (searchKey.trim() && !search.trim()) return { text: searchKey, scope: 'key' }
  if (search.trim() && searchKey.trim()) return { text: search, scope: 'both' }
  if (search.trim()) return { text: search, scope: 'both' }
  if (searchKey.trim()) return { text: searchKey, scope: 'key' }
  return { text: '', scope: 'both' }
}
