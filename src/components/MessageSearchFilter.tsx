import type { SearchScope } from '../lib/searchScope'
import type { useLocale } from '../hooks/useLocale'

type Tr = ReturnType<typeof useLocale>['tr']

function keyOn(scope: SearchScope) {
  return scope === 'both' || scope === 'key'
}

function valueOn(scope: SearchScope) {
  return scope === 'both' || scope === 'value'
}

/** Toggle Key/Value — both may be on; at least one must stay on. */
function toggleKey(scope: SearchScope): SearchScope | null {
  if (scope === 'both') return 'value'
  if (scope === 'key') return null // can't turn both off
  return 'both'
}

function toggleValue(scope: SearchScope): SearchScope | null {
  if (scope === 'both') return 'key'
  if (scope === 'value') return null
  return 'both'
}

/** One search box + Key / Value toggles — used by Consume and Live Tail toolbars. */
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
  const kOn = keyOn(scope)
  const vOn = valueOn(scope)

  return (
    <div className="field toolbar-filter-field toolbar-search-combo">
      <label htmlFor={id}>{tr('search')}</label>
      <div className="toolbar-search-row">
        <div className="message-view-toggle search-scope-toggle" role="group" aria-label={tr('searchScope')}>
          <button
            type="button"
            className={kOn ? 'active' : undefined}
            aria-pressed={kOn}
            disabled={disabled}
            title={kOn && !vOn ? tr('searchScopeNeedOne') : undefined}
            onClick={() => {
              const next = toggleKey(scope)
              if (next) onScopeChange(next)
            }}
          >
            {tr('searchScopeKey')}
          </button>
          <button
            type="button"
            className={vOn ? 'active' : undefined}
            aria-pressed={vOn}
            disabled={disabled}
            title={vOn && !kOn ? tr('searchScopeNeedOne') : undefined}
            onClick={() => {
              const next = toggleValue(scope)
              if (next) onScopeChange(next)
            }}
          >
            {tr('searchScopeValue')}
          </button>
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
