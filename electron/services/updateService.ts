import { app, BrowserWindow, dialog } from 'electron'
import { autoUpdater } from 'electron-updater'
import { spawnSync } from 'node:child_process'
import fs from 'node:fs'
import path from 'node:path'
import type { UpdateSettings, UpdateStatus } from '../types'
import {
  checkAndDownloadMacDmg,
  getPendingMacDmg,
  installMacDmgAndRelaunch,
} from './macDmgUpdater'

const RELEASES_URL = 'https://github.com/neverxrist/kafdesk/releases'

let status: UpdateStatus = { state: 'idle' }
let listeners = new Set<(s: UpdateStatus) => void>()
let wired = false
let autoUpdateEnabled = true
let initialTimer: NodeJS.Timeout | null = null
let intervalTimer: NodeJS.Timeout | null = null

function settingsPath(): string {
  return path.join(app.getPath('userData'), 'update-settings.json')
}

function loadSettings(): UpdateSettings {
  try {
    const raw = fs.readFileSync(settingsPath(), 'utf8')
    const parsed = JSON.parse(raw) as Partial<UpdateSettings>
    autoUpdateEnabled = parsed.autoUpdate !== false
  } catch {
    autoUpdateEnabled = true
  }
  return { autoUpdate: autoUpdateEnabled }
}

function saveSettings() {
  try {
    fs.writeFileSync(settingsPath(), JSON.stringify({ autoUpdate: autoUpdateEnabled }, null, 2))
  } catch {
    /* ignore */
  }
}

function emit(next: UpdateStatus) {
  status = next
  for (const fn of listeners) {
    try {
      fn(status)
    } catch {
      /* ignore */
    }
  }
}

/**
 * Squirrel.Mac / ShipIt needs a real Developer ID. Unsigned CI builds use the
 * custom DMG updater instead (see macDmgUpdater.ts).
 */
function hasDeveloperIdSignature(): boolean {
  if (process.platform !== 'darwin') return true
  try {
    const r = spawnSync('codesign', ['-dv', '--verbose=4', process.execPath], {
      encoding: 'utf8',
    })
    const out = `${r.stdout ?? ''}\n${r.stderr ?? ''}`
    return /Authority=Developer ID Application/i.test(out)
  } catch {
    return false
  }
}

function useMacDmgUpdater(): boolean {
  return process.platform === 'darwin' && !hasDeveloperIdSignature()
}

function friendlyUpdateError(err: unknown): string {
  const raw = err instanceof Error ? err.message : String(err)
  if (
    /code signature/i.test(raw) ||
    /did not pass validation/i.test(raw) ||
    /ресурса?ы? кода отсутствуют/i.test(raw) ||
    /CodeResources/i.test(raw)
  ) {
    return `Mac ShipIt update rejected. Try Check for updates again (DMG installer) or download: ${RELEASES_URL}`
  }
  return raw
}

export function getUpdateStatus(): UpdateStatus {
  return status
}

export function getUpdateSettings(): UpdateSettings {
  return { autoUpdate: autoUpdateEnabled }
}

export function onUpdateStatus(fn: (s: UpdateStatus) => void): () => void {
  listeners.add(fn)
  fn(status)
  return () => listeners.delete(fn)
}

function wireElectronUpdater(win: BrowserWindow | null) {
  if (wired) return
  wired = true

  autoUpdater.autoDownload = true
  autoUpdater.autoInstallOnAppQuit = true

  autoUpdater.on('checking-for-update', () => emit({ state: 'checking' }))
  autoUpdater.on('update-available', (info) => emit({ state: 'available', version: info.version }))
  autoUpdater.on('update-not-available', () => emit({ state: 'not-available' }))
  autoUpdater.on('download-progress', (p) =>
    emit({ state: 'downloading', percent: Math.round(p.percent) }),
  )
  autoUpdater.on('update-downloaded', async (info) => {
    emit({ state: 'downloaded', version: info.version })
    if (win && !win.isDestroyed()) {
      const res = await dialog.showMessageBox(win, {
        type: 'info',
        title: 'KafDesk update',
        message: `KafDesk ${info.version} is ready to install.`,
        detail: 'Restart now to apply the update, or it will install on quit.',
        buttons: ['Restart now', 'Later'],
        defaultId: 0,
        cancelId: 1,
      })
      if (res.response === 0) autoUpdater.quitAndInstall()
    }
  })
  autoUpdater.on('error', (err) => emit({ state: 'error', message: friendlyUpdateError(err) }))
}

function clearTimers() {
  if (initialTimer) clearTimeout(initialTimer)
  if (intervalTimer) clearInterval(intervalTimer)
  initialTimer = null
  intervalTimer = null
}

function schedule(win: BrowserWindow | null) {
  clearTimers()
  if (!app.isPackaged || !autoUpdateEnabled) return
  initialTimer = setTimeout(() => void checkForUpdates(win), 5_000)
  intervalTimer = setInterval(() => void checkForUpdates(win), 6 * 60 * 60 * 1000)
}

async function offerPendingMacInstall(win: BrowserWindow | null): Promise<UpdateStatus> {
  const pending = getPendingMacDmg()
  if (!pending) {
    return status
  }
  emit({ state: 'downloaded', version: pending.version })
  if (win && !win.isDestroyed()) {
    const res = await dialog.showMessageBox(win, {
      type: 'info',
      title: 'KafDesk update',
      message: `KafDesk ${pending.version} is ready to install.`,
      detail: 'Restart now to replace the app from the downloaded DMG.',
      buttons: ['Restart now', 'Later'],
      defaultId: 0,
      cancelId: 1,
    })
    if (res.response === 0) installMacDmgAndRelaunch(pending.path)
  }
  return status
}

async function runMacDmgCheck(win: BrowserWindow | null): Promise<UpdateStatus> {
  const pending = getPendingMacDmg()
  if (pending) return offerPendingMacInstall(win)
  try {
    return await checkAndDownloadMacDmg(emit, win)
  } catch (err) {
    const s: UpdateStatus = {
      state: 'error',
      message: err instanceof Error ? err.message : String(err),
    }
    emit(s)
    return s
  }
}

export function setAutoUpdate(enabled: boolean, win: BrowserWindow | null): UpdateSettings {
  autoUpdateEnabled = enabled
  saveSettings()
  if (!enabled) {
    clearTimers()
    emit({ state: 'disabled' })
  } else if (!app.isPackaged) {
    emit({ state: 'dev' })
  } else {
    schedule(win)
    void checkForUpdates(win)
  }
  return getUpdateSettings()
}

export async function checkForUpdates(win: BrowserWindow | null): Promise<UpdateStatus> {
  if (!app.isPackaged) {
    emit({ state: 'dev', message: 'Updates are only available in packaged builds' })
    return status
  }

  if (useMacDmgUpdater()) {
    return runMacDmgCheck(win)
  }

  wireElectronUpdater(win)
  try {
    emit({ state: 'checking' })
    await autoUpdater.checkForUpdates()
  } catch (err) {
    // Fall back to DMG path if ShipIt/signing blows up on Mac
    if (process.platform === 'darwin') {
      return runMacDmgCheck(win)
    }
    emit({ state: 'error', message: friendlyUpdateError(err) })
  }
  return status
}

export function initAutoUpdate(win: BrowserWindow | null) {
  loadSettings()
  if (!app.isPackaged) {
    emit({ state: autoUpdateEnabled ? 'dev' : 'disabled' })
    return
  }
  if (!autoUpdateEnabled) {
    emit({ state: 'disabled' })
    return
  }
  if (useMacDmgUpdater()) {
    emit({
      state: 'idle',
      message: `Unsigned Mac build — updates via DMG from ${RELEASES_URL}`,
    })
  }
  schedule(win)
}
