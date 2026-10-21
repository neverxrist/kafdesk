import { app, BrowserWindow, dialog, ipcMain, nativeImage } from 'electron'
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { authService } from './auth/authService'
import { wrapIpcHandler } from './infrastructure/ipc/wrapHandler'
import { kafkaService } from './services/kafkaService'
import { isRunningUnderRosetta } from './services/macDmgUpdater'
import {
  checkForUpdates,
  getUpdateSettings,
  getUpdateStatus,
  initAutoUpdate,
  onUpdateStatus,
  setAutoUpdate,
} from './services/updateService'
import type {
  AuthLoginRequest,
  ClusterConfig,
  ConsumeRequest,
  ProduceBatchRequest,
  ProduceRequest,
  SetGroupOffsetsRequest,
  TailRequest,
} from './types'

const __dirname = path.dirname(fileURLToPath(import.meta.url))

process.env.DIST = path.join(__dirname, '../dist')
process.env.VITE_PUBLIC = app.isPackaged
  ? process.env.DIST
  : path.join(process.env.DIST, '../public')

app.setName('KafDesk')

let win: BrowserWindow | null = null
let splash: BrowserWindow | null = null
const VITE_DEV_SERVER_URL = process.env.VITE_DEV_SERVER_URL

function resolveAppIcon(): string | undefined {
  const candidates = app.isPackaged
    ? [path.join(process.resourcesPath, 'icon.png'), path.join(__dirname, '../dist/icon.png')]
    : [
        path.join(process.env.VITE_PUBLIC || '', 'icon.png'),
        path.join(__dirname, '../build/icon.png'),
        path.join(__dirname, '../public/icon.png'),
      ]
  return candidates.find((p) => Boolean(p) && fs.existsSync(p))
}

function resolveSplashHtml(): string | undefined {
  const candidates = app.isPackaged
    ? [path.join(process.env.DIST!, 'splash.html')]
    : [
        path.join(process.env.VITE_PUBLIC || '', 'splash.html'),
        path.join(__dirname, '../public/splash.html'),
      ]
  return candidates.find((p) => Boolean(p) && fs.existsSync(p))
}

/** Lightweight splash while Chromium/React boot — cannot cover Gatekeeper delay before process start. */
function createSplash(): BrowserWindow | null {
  const html = resolveSplashHtml()
  if (!html) return null
  const icon = resolveAppIcon()
  const w = new BrowserWindow({
    width: 280,
    height: 280,
    resizable: false,
    maximizable: false,
    minimizable: false,
    fullscreenable: false,
    frame: false,
    transparent: false,
    backgroundColor: '#0f1419',
    hasShadow: true,
    alwaysOnTop: true,
    skipTaskbar: true,
    show: true,
    center: true,
    ...(icon ? { icon } : {}),
    webPreferences: {
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: true,
    },
  })
  void w.loadFile(html)
  return w
}

function closeSplash(): void {
  if (splash && !splash.isDestroyed()) {
    splash.close()
  }
  splash = null
}

/** macOS copies quarantined apps to a random path — feels laggy vs a local build. */
function isMacAppTranslocated(): boolean {
  return process.platform === 'darwin' && /AppTranslocation/i.test(process.execPath)
}

async function warnIfMacPerfTrap(parent: BrowserWindow | null): Promise<void> {
  if (!app.isPackaged) return

  if (isRunningUnderRosetta()) {
    const opts = {
      type: 'warning' as const,
      title: 'KafDesk — wrong Mac build',
      message: 'Installed Intel (x64) build on Apple Silicon — running via Rosetta.',
      detail:
        'That makes the UI laggy (clicks ~0.5–1s).\n\n' +
        'Fix: install the arm64 DMG from GitHub Releases:\n' +
        'KafDesk-*-mac-arm64.dmg\n\n' +
        'Drag to Applications → Replace → Finish Install / xattr, then open.',
      buttons: ['OK'],
    }
    if (parent && !parent.isDestroyed()) await dialog.showMessageBox(parent, opts)
    else await dialog.showMessageBox(opts)
    return
  }

  if (!isMacAppTranslocated()) return
  const opts = {
    type: 'warning' as const,
    title: 'KafDesk — slow launch',
    message: 'macOS is running KafDesk from a temporary folder (App Translocation).',
    detail:
      'Fix:\n' +
      '1. Quit KafDesk\n' +
      '2. Drag KafDesk.app alone into /Applications (Replace)\n' +
      '3. Run Finish Install (or xattr -cr /Applications/KafDesk.app)\n' +
      '4. Open from Applications',
    buttons: ['OK'],
  }
  if (parent && !parent.isDestroyed()) await dialog.showMessageBox(parent, opts)
  else await dialog.showMessageBox(opts)
}

function createWindow() {
  const preloadPath = path.join(__dirname, 'preload.cjs')
  const icon = resolveAppIcon()

  win = new BrowserWindow({
    width: 1280,
    height: 840,
    minWidth: 1000,
    minHeight: 640,
    title: 'KafDesk',
    backgroundColor: '#0f1419',
    show: false,
    ...(icon ? { icon } : {}),
    webPreferences: {
      preload: preloadPath,
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: false,
    },
  })

  if (process.platform === 'darwin' && icon && app.dock) {
    app.dock.setIcon(nativeImage.createFromPath(icon))
  }

  win.once('ready-to-show', () => {
    win?.show()
    closeSplash()
  })

  win.webContents.on('did-fail-load', (_e, code, desc, url) => {
    console.error('did-fail-load', { code, desc, url })
    closeSplash()
    win?.show()
  })

  if (VITE_DEV_SERVER_URL) {
    win.loadURL(VITE_DEV_SERVER_URL)
  } else {
    win.loadFile(path.join(process.env.DIST!, 'index.html'))
  }
}

function registerIpc() {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const handle = (channel: string, fn: (event: Electron.IpcMainInvokeEvent, ...args: any[]) => Promise<any>) => {
    ipcMain.handle(channel, wrapIpcHandler(fn))
  }

  handle('kafka:connect', async (_e: Electron.IpcMainInvokeEvent, config: ClusterConfig) =>
    kafkaService.connect(config),
  )

  handle('kafka:disconnect', async () => {
    await kafkaService.disconnect()
    return { ok: true }
  })

  handle('kafka:status', async () => ({
    connected: kafkaService.isConnected(),
    schemaRegistry: kafkaService.getSchemaRegistryStatus(),
  }))

  handle('kafka:ping', async () => kafkaService.ping())

  handle('kafka:probe', async (_e: Electron.IpcMainInvokeEvent, config: ClusterConfig) =>
    kafkaService.probe(config),
  )

  handle('kafka:listTopics', async () => kafkaService.listTopics())

  handle('kafka:getTopicOffsets', async (_e: Electron.IpcMainInvokeEvent, topic: string) =>
    kafkaService.getTopicOffsets(topic),
  )

  handle('kafka:consume', async (_e: Electron.IpcMainInvokeEvent, request: ConsumeRequest) =>
    kafkaService.consume(request),
  )

  handle('kafka:produce', async (_e: Electron.IpcMainInvokeEvent, request: ProduceRequest) =>
    kafkaService.produce(request),
  )

  handle('kafka:produceBatch', async (_e: Electron.IpcMainInvokeEvent, request: ProduceBatchRequest) =>
    kafkaService.produceBatch(request),
  )

  ipcMain.handle('dialog:pickPemFile', async () => {
    const opts: Electron.OpenDialogOptions = {
      title: 'Select PEM / certificate file',
      properties: ['openFile'],
      filters: [
        { name: 'Certificates', extensions: ['pem', 'crt', 'cer', 'key', 'pub'] },
        { name: 'All files', extensions: ['*'] },
      ],
    }
    const res = win ? await dialog.showOpenDialog(win, opts) : await dialog.showOpenDialog(opts)
    if (res.canceled || !res.filePaths[0]) return null
    return res.filePaths[0]
  })

  ipcMain.handle('dialog:pickAvscFile', async () => {
    const opts: Electron.OpenDialogOptions = {
      title: 'Select Avro schema (.avsc)',
      properties: ['openFile'],
      filters: [
        { name: 'Avro schema', extensions: ['avsc', 'json'] },
        { name: 'All files', extensions: ['*'] },
      ],
    }
    const res = win ? await dialog.showOpenDialog(win, opts) : await dialog.showOpenDialog(opts)
    if (res.canceled || !res.filePaths[0]) return null
    const filePath = res.filePaths[0]
    const text = fs.readFileSync(filePath, 'utf8')
    return { path: filePath, text }
  })

  handle('kafka:listSubjects', async () => kafkaService.listSubjects())

  handle('kafka:getLatestSchema', async (_e: Electron.IpcMainInvokeEvent, subject: string) =>
    kafkaService.getLatestSchema(subject),
  )

  handle('kafka:listConsumerGroups', async () => kafkaService.listConsumerGroups())

  handle(
    'kafka:getGroupOffsets',
    async (_e: Electron.IpcMainInvokeEvent, groupId: string, topic?: string) =>
      kafkaService.getGroupOffsets(groupId, topic),
  )

  handle('kafka:getLagSummary', async (_e: Electron.IpcMainInvokeEvent, topic?: string) =>
    kafkaService.getLagSummary(topic),
  )

  handle(
    'kafka:setGroupOffsets',
    async (_e: Electron.IpcMainInvokeEvent, request: SetGroupOffsetsRequest) => {
      await kafkaService.setGroupOffsets(request)
      return { ok: true }
    },
  )

  handle(
    'kafka:resetGroupOffsets',
    async (
      _e: Electron.IpcMainInvokeEvent,
      groupId: string,
      topic: string,
      earliest: boolean,
    ) => {
      await kafkaService.resetGroupOffsets(groupId, topic, earliest)
      return { ok: true }
    },
  )

  handle('kafka:tailStart', async (e: Electron.IpcMainInvokeEvent, request: TailRequest) => {
    await kafkaService.startTail(
      request,
      (msg) => {
        e.sender.send('kafka:tailMessage', msg)
      },
      (errMsg) => {
        e.sender.send('kafka:tailError', errMsg)
      },
    )
    return { ok: true }
  })

  handle('kafka:tailStop', async () => {
    await kafkaService.stopTail()
    return { ok: true }
  })

  ipcMain.handle('auth:listProviders', async () => authService.listProviders())

  ipcMain.handle('auth:session', async () => authService.getPublicSession())

  ipcMain.handle('auth:login', async (_e, request: AuthLoginRequest) =>
    authService.login(request),
  )

  ipcMain.handle('auth:logout', async () => {
    await authService.logout()
    return { ok: true }
  })

  ipcMain.handle('auth:storedCredentials', async () => authService.storedCredentials())

  ipcMain.handle('auth:forgetCredentials', async () => {
    authService.forgetCredentials()
    return { ok: true }
  })

  ipcMain.handle('app:checkForUpdates', async (e) =>
    checkForUpdates(BrowserWindow.fromWebContents(e.sender)),
  )
  ipcMain.handle('app:updateStatus', async () => getUpdateStatus())
  ipcMain.handle('app:updateSettings', async () => getUpdateSettings())
  ipcMain.handle('app:setAutoUpdate', async (e, enabled: boolean) =>
    setAutoUpdate(Boolean(enabled), BrowserWindow.fromWebContents(e.sender)),
  )
  ipcMain.handle('app:getVersion', async () => app.getVersion())
}

app.whenReady().then(() => {
  splash = createSplash()
  // Safety: never leave splash stuck if ready-to-show is missed
  setTimeout(() => closeSplash(), 12_000)

  authService.loadFromDisk()
  registerIpc()
  createWindow()
  // Do NOT xattr -cr the running .app — recursive clear hammers disk and lags UI ~0.5–1s.
  // Quarantine is cleared by Finish Install / Terminal before first open.
  setTimeout(() => void warnIfMacPerfTrap(win), 1_500)
  // Forward update status to renderer
  onUpdateStatus((s) => {
    if (win && !win.isDestroyed()) win.webContents.send('app:updateStatus', s)
  })
  initAutoUpdate(win)
  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) {
      splash = createSplash()
      setTimeout(() => closeSplash(), 12_000)
      createWindow()
    }
  })
})

app.on('window-all-closed', async () => {
  closeSplash()
  await kafkaService.disconnect()
  if (process.platform !== 'darwin') app.quit()
})
