export type SearchScope = 'both' | 'key' | 'value'

export const SEARCH_SCOPES: SearchScope[] = ['both', 'key', 'value']

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
