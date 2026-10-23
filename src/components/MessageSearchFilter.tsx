import type { ReactNode } from 'react'
import type { SearchScope } from '../lib/searchScope'
import type { useLocale } from '../hooks/useLocale'

type Tr = ReturnType<typeof useLocale>['tr']

/** Per-row filter is Key or Value (legacy `both` shows as Value). */
function scopeTarget(scope: SearchScope): 'key' | 'value' {
  return scope === 'key' ? 'key' : 'value'
}

/** One search box + Key/Value scope toggle — used by Consume and Live Tail toolbars. */
export default function MessageSearchFilter({
  tr,
  id,
  value,
  scope,
  onChange,
  onScopeChange,
  matchesOnly = false,
  onMatchesOnlyChange,
  disabled,
  maxLength = 255,
  rowEnd,
}: {
  tr: Tr
  id: string
  value: string
  scope: SearchScope
  onChange: (v: string) => void
  onScopeChange: (s: SearchScope) => void
  /** Show only JSON value fields that match this row's query. */
  matchesOnly?: boolean
  onMatchesOnlyChange?: (v: boolean) => void
  disabled?: boolean
  maxLength?: number
  /** Extra control in the same row as scope + input (e.g. + / −). */
  rowEnd?: ReactNode
}) {
  const target = scopeTarget(scope)
  /** Hits trims JSON value fields — only when scope is Value. */
  const hitsAvailable = Boolean(onMatchesOnlyChange) && target === 'value'
  const hasQuery = Boolean(value.trim())
  /** Empty / Key → no Hits chrome (even if toggle was left on). */
  const hitsActive = hitsAvailable && matchesOnly && hasQuery
  const matchesDisabled = disabled || !hasQuery

  return (
    <div className="field toolbar-filter-field toolbar-search-combo toolbar-search-combo-nolabel">
      <div className="toolbar-search-row">
        <button
          type="button"
          className={`search-scope-toggle-btn scope-${target}`}
          disabled={disabled}
          title={tr('searchScopeToggleTitle')}
          aria-label={tr('searchScopeToggleTitle')}
          onClick={() => onScopeChange(target === 'key' ? 'value' : 'key')}
        >
          {target === 'key' ? tr('searchScopeKey') : tr('searchScopeValue')}
        </button>
        <div
          className={`toolbar-search-input-shell${hitsAvailable ? ' has-hits' : ''}${hitsActive ? ' hits-on' : ''}`}
        >
          <input
            id={id}
            value={value}
            maxLength={maxLength}
            onChange={(e) => onChange(e.target.value.slice(0, maxLength))}
            placeholder={tr('substring')}
            disabled={disabled}
            aria-label={tr('search')}
            autoCapitalize="off"
            autoCorrect="off"
            autoComplete="off"
            spellCheck={false}
            enterKeyHint="search"
          />
          {hitsAvailable && (
            <button
              type="button"
              className={`search-matches-only${hitsActive ? ' active' : ''}`}
              aria-pressed={hitsActive}
              disabled={matchesDisabled}
              title={tr('searchMatchesOnlyTitle')}
              onClick={() => onMatchesOnlyChange!(!matchesOnly)}
            >
              {tr('searchMatchesOnly')}
            </button>
          )}
        </div>
        {rowEnd}
      </div>
    </div>
  )
}

function DualToggleButton({
  kind,
  disabled,
  title,
  ariaLabel,
  onClick,
}: {
  kind: 'add' | 'remove'
  disabled?: boolean
  title: string
  ariaLabel: string
  onClick: () => void
}) {
  return (
    <button
      type="button"
      className={`dual-search-add${kind === 'remove' ? ' dual-search-remove' : ''}`}
      disabled={disabled}
      title={title}
      aria-label={ariaLabel}
      onClick={onClick}
    >
      {kind === 'add' ? '+' : '−'}
    </button>
  )
}

/** Up to two AND search rows (+ / −). Hits only on one row at a time. */
export function DualMessageSearchFilters({
  tr,
  idPrefix,
  search,
  searchScope,
  setSearch,
  setSearchScope,
  search2,
  searchScope2,
  setSearch2,
  setSearchScope2,
  dualSearch,
  onOpenDual,
  onCloseDual,
  hitsOn,
  onHitsOn,
  disabled,
  maxLength = 255,
}: {
  tr: Tr
  idPrefix: string
  search: string
  searchScope: SearchScope
  setSearch: (v: string) => void
  setSearchScope: (v: SearchScope) => void
  search2: string
  searchScope2: SearchScope
  setSearch2: (v: string) => void
  setSearchScope2: (v: SearchScope) => void
  dualSearch: boolean
  onOpenDual: () => void
  onCloseDual: () => void
  /** Independent Hits for each filter row. */
  hitsOn: [boolean, boolean]
  onHitsOn: (slot: 0 | 1, on: boolean) => void
  disabled?: boolean
  maxLength?: number
}) {
  return (
    <div className={`dual-search-filters${dualSearch ? ' dual' : ''}`}>
      <MessageSearchFilter
        tr={tr}
        id={`${idPrefix}-search`}
        value={search}
        scope={searchScope}
        onChange={setSearch}
        onScopeChange={setSearchScope}
        matchesOnly={hitsOn[0]}
        onMatchesOnlyChange={(on) => onHitsOn(0, on)}
        disabled={disabled}
        maxLength={maxLength}
        rowEnd={
          !dualSearch ? (
            <DualToggleButton
              kind="add"
              disabled={disabled}
              title={tr('searchAddFilter')}
              ariaLabel={tr('searchAddFilter')}
              onClick={onOpenDual}
            />
          ) : null
        }
      />
      {dualSearch ? (
        <>
          <span className="dual-search-and" aria-hidden>
            {tr('searchAnd')}
          </span>
          <MessageSearchFilter
            tr={tr}
            id={`${idPrefix}-search-2`}
            value={search2}
            scope={searchScope2}
            onChange={setSearch2}
            onScopeChange={setSearchScope2}
            matchesOnly={hitsOn[1]}
            onMatchesOnlyChange={(on) => onHitsOn(1, on)}
            disabled={disabled}
            maxLength={maxLength}
            rowEnd={
              <DualToggleButton
                kind="remove"
                disabled={disabled}
                title={tr('searchRemoveFilter')}
                ariaLabel={tr('searchRemoveFilter')}
                onClick={onCloseDual}
              />
            }
          />
        </>
      ) : null}
    </div>
  )
}
