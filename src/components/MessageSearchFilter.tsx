import type { SearchScope } from '../lib/searchScope'
import { SEARCH_SCOPES } from '../lib/searchScope'
import type { useLocale } from '../hooks/useLocale'

type Tr = ReturnType<typeof useLocale>['tr']

const SCOPE_LABEL: Record<SearchScope, 'searchScopeBoth' | 'searchScopeKey' | 'searchScopeValue'> = {
  both: 'searchScopeBoth',
  key: 'searchScopeKey',
  value: 'searchScopeValue',
}

/** One search box + Key / Value / Both — used by Consume and Live Tail toolbars. */
export default function MessageSearchFilter({
  tr,
  id,
  value,
  scope,
  onChange,
  onScopeChange,
  disabled,
  maxLength = 255,
}: {
  tr: Tr
  id: string
  value: string
  scope: SearchScope
  onChange: (v: string) => void
  onScopeChange: (s: SearchScope) => void
  disabled?: boolean
  maxLength?: number
}) {
  return (
    <div className="field toolbar-filter-field toolbar-search-combo">
      <label htmlFor={id}>{tr('search')}</label>
      <div className="toolbar-search-row">
        <div className="message-view-toggle search-scope-toggle" role="group" aria-label={tr('searchScope')}>
          {SEARCH_SCOPES.map((s) => (
            <button
              key={s}
              type="button"
              className={scope === s ? 'active' : undefined}
              disabled={disabled}
              onClick={() => onScopeChange(s)}
            >
              {tr(SCOPE_LABEL[s])}
            </button>
          ))}
        </div>
        <input
          id={id}
          value={value}
          maxLength={maxLength}
          onChange={(e) => onChange(e.target.value.slice(0, maxLength))}
          placeholder={tr('substring')}
          disabled={disabled}
          autoCapitalize="off"
          autoCorrect="off"
          autoComplete="off"
          spellCheck={false}
          enterKeyHint="search"
        />
      </div>
    </div>
  )
}
