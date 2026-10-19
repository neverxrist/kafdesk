import { app, BrowserWindow, dialog } from 'electron'
import { autoUpdater } from 'electron-updater'
import fs from 'node:fs'
import path from 'node:path'
import type { UpdateSettings, UpdateStatus } from '../types'

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

function wire(win: BrowserWindow | null) {
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
  autoUpdater.on('error', (err) =>
    emit({ state: 'error', message: err instanceof Error ? err.message : String(err) }),
  )
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
  wire(win)
  initialTimer = setTimeout(() => void checkForUpdates(win), 5_000)
  intervalTimer = setInterval(() => void checkForUpdates(win), 6 * 60 * 60 * 1000)
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
  wire(win)
  try {
    emit({ state: 'checking' })
    await autoUpdater.checkForUpdates()
  } catch (err) {
    emit({ state: 'error', message: err instanceof Error ? err.message : String(err) })
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
  schedule(win)
}
