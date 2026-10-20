import type { UpdateStatus } from '../../../electron/types'
import type { Theme } from '../../lib/connections'
import type { useLocale } from '../../hooks/useLocale'

type Tr = ReturnType<typeof useLocale>['tr']

function updateText(tr: Tr, s: UpdateStatus): string {
  switch (s.state) {
    case 'dev':
      return tr('updateDev')
    case 'unsigned':
      return tr('updateUnsigned')
    case 'disabled':
      return tr('updateDisabled')
    case 'checking':
      return tr('updateChecking')
    case 'available':
      return tr('updateAvailable', { version: s.version ?? '' })
    case 'downloaded':
      return tr('updateDownloaded', { version: s.version ?? '' })
    case 'not-available':
      return tr('updateNone')
    case 'downloading':
      return `${tr('updateChecking')} ${s.percent ?? 0}%`
    case 'error':
      return `${tr('updateError')}${s.message ? `: ${s.message}` : ''}`
    default:
      return tr('updateNone')
  }
}

export default function SettingsPanel({
  tr,
  theme,
  toggleTheme,
  toggleLocale,
  updateStatus,
  updateAuto,
  onSetAutoUpdate,
  onCheckUpdates,
}: {
  tr: Tr
  theme: Theme
  toggleTheme: () => void
  toggleLocale: () => void
  updateStatus: UpdateStatus
  updateAuto: boolean
  onSetAutoUpdate: (v: boolean) => void
  onCheckUpdates: () => void
}) {
  return (
    <div className="settings-pane">
      <section className="panel settings-block">
        <h2>{tr('updates')}</h2>
        <label className="settings-toggle">
          <input
            type="checkbox"
            checked={updateAuto}
            onChange={(e) => onSetAutoUpdate(e.target.checked)}
          />
          <span>{tr('autoUpdate')}</span>
        </label>
        <p className="settings-hint">{tr('autoUpdateHint')}</p>
        <div className="row" style={{ gap: 8, alignItems: 'center', flexWrap: 'wrap' }}>
          <button type="button" onClick={onCheckUpdates}>
            {tr('checkUpdates')}
          </button>
          <span className="status">{updateText(tr, updateStatus)}</span>
        </div>
      </section>

      <section className="panel settings-block">
        <h2>{tr('appearance')}</h2>
        <div className="row" style={{ gap: 8, flexWrap: 'wrap' }}>
          <button type="button" onClick={toggleTheme}>
            {theme === 'dark' ? tr('themeLight') : tr('themeDark')}
          </button>
          <span className="status">
            {tr('language')}:{' '}
            <button type="button" onClick={toggleLocale}>
              {tr('langSwitch')}
            </button>
          </span>
        </div>
      </section>
    </div>
  )
}
