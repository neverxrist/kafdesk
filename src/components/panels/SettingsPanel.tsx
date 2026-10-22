import { useEffect, useState } from 'react'
import type { UpdateStatus } from '../../types/kafdesk'
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
    case 'manual':
      return tr('updateManual', { version: s.version ?? '' })
    case 'not-available':
      return tr('updateNone')
    case 'downloading':
      return tr('updateDownloading', {
        percent: String(Math.round(s.percent ?? 0)),
      })
    case 'error':
      return `${tr('updateError')}${s.message ? `: ${s.message}` : ''}`
    case 'idle':
      return s.message ? s.message : ''
    default:
      return tr('updateNone')
  }
}

export default function SettingsPanel({
  tr,
  updateStatus,
  updateAuto,
  onSetAutoUpdate,
  onCheckUpdates,
  onOpenReleases,
}: {
  tr: Tr
  updateStatus: UpdateStatus
  updateAuto: boolean
  onSetAutoUpdate: (v: boolean) => void
  onCheckUpdates: () => void
  onOpenReleases: () => void
}) {
  const [appVersion, setAppVersion] = useState<string>('')

  useEffect(() => {
    void window.kafdesk.getVersion?.().then(setAppVersion).catch(() => setAppVersion(''))
  }, [])

  return (
    <div className="settings-pane">
      <section className="panel settings-block">
        <h2>{tr('updates')}</h2>
        {appVersion ? (
          <p className="settings-hint" style={{ marginTop: 0 }}>
            {tr('appVersion', { version: appVersion })}
          </p>
        ) : null}
        <label className="settings-toggle">
          <input
            type="checkbox"
            checked={updateAuto}
            onChange={(e) => onSetAutoUpdate(e.target.checked)}
          />
          <span>{tr('autoUpdate')}</span>
        </label>
        <p className="settings-hint">{tr('autoUpdateHint')}</p>
        <div className="settings-actions">
          <button type="button" onClick={onCheckUpdates}>
            {tr('checkUpdates')}
          </button>
          {updateStatus.state === 'error' || updateStatus.state === 'manual' ? (
            <button type="button" onClick={onOpenReleases}>
              {tr('openReleases')}
            </button>
          ) : null}
          <span className="status">{updateText(tr, updateStatus)}</span>
        </div>
        <p className="settings-hint settings-hint-foot">{tr('appearanceInTopbar')}</p>
      </section>
    </div>
  )
}
