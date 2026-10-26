import { useEffect, useState } from 'react'
import type { WebFaceStatus } from '../../lib/kafdeskApi'
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
  face,
  onSwitchFace,
}: {
  tr: Tr
  updateStatus: UpdateStatus
  updateAuto: boolean
  onSetAutoUpdate: (v: boolean) => void
  onCheckUpdates: () => void
  onOpenReleases: () => void
  face: WebFaceStatus
  onSwitchFace: (owner: 'desktop' | 'web') => Promise<void>
}) {
  const [appVersion, setAppVersion] = useState<string>('')
  const [switching, setSwitching] = useState(false)
  const [copied, setCopied] = useState(false)
  const [faceError, setFaceError] = useState<string | null>(null)

  useEffect(() => {
    void window.kafdesk.getVersion?.().then(setAppVersion).catch(() => setAppVersion(''))
  }, [])

  const toWeb = face.owner !== 'web'

  return (
    <div className="settings-pane">
      <section className="panel settings-block">
        <h2>{tr('faceTitle')}</h2>
        <p className="settings-hint" style={{ marginTop: 0 }}>
          {tr('faceHint')}
        </p>
        <div className="settings-actions">
          <button
            type="button"
            disabled={switching}
            onClick={() => {
              setSwitching(true)
              setFaceError(null)
              void onSwitchFace(toWeb ? 'web' : 'desktop')
                .catch((err: unknown) => {
                  setFaceError(err instanceof Error ? err.message : String(err))
                })
                .finally(() => setSwitching(false))
            }}
          >
            {toWeb ? tr('faceToWeb') : tr('faceToDesktop')}
          </button>
        </div>
        {face.url ? (
          <label className="settings-url">
            <span>{tr('faceUrl')}</span>
            <input readOnly value={face.url} onFocus={(e) => e.currentTarget.select()} />
            <button
              type="button"
              onClick={() => {
                const write = navigator.clipboard?.writeText(face.url || '')
                if (!write) return
                void write.then(() => {
                  setCopied(true)
                  window.setTimeout(() => setCopied(false), 1500)
                })
              }}
            >
              {copied ? tr('faceCopied') : tr('faceCopy')}
            </button>
          </label>
        ) : null}
        {faceError ? <p className="settings-hint">{faceError}</p> : null}
      </section>
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
      </section>
    </div>
  )
}
