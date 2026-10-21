import { app, BrowserWindow, dialog, ipcMain, nativeImage } from 'electron'
import { spawn, spawnSync } from 'node:child_process'
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { authService } from './auth/authService'
import { kafkaService } from './services/kafkaService'
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

// Keep UI timers/compositor closer to `npm run dev` (Vite http) behaviour when packaged.
if (app.isPackaged) {
  app.commandLine.appendSwitch('disable-renderer-backgrounding')
  app.commandLine.appendSwitch('disable-background-timer-throttling')
}

let win: BrowserWindow | null = null
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

/** Packaged Mac: …/KafDesk.app */
function macAppBundlePath(): string {
  return path.resolve(process.execPath, '..', '..', '..')
}

/** Unsigned downloads get com.apple.quarantine → Gatekeeper scans ~100MB Electron on first open. */
function stripMacQuarantineBestEffort(): void {
  if (process.platform !== 'darwin' || !app.isPackaged) return
  const bundle = macAppBundlePath()
  if (!bundle.endsWith('.app') || !fs.existsSync(bundle)) return
  try {
    const check = spawnSync('xattr', ['-p', 'com.apple.quarantine', bundle], { encoding: 'utf8' })
    if (check.status !== 0) return
  } catch {
    return
  }
  const child = spawn('xattr', ['-cr', bundle], { detached: true, stdio: 'ignore' })
  child.unref()
}

/** macOS copies quarantined apps to a random path — feels laggy vs a local build. */
function isMacAppTranslocated(): boolean {
  return process.platform === 'darwin' && /AppTranslocation/i.test(process.execPath)
}

async function warnIfMacTranslocated(parent: BrowserWindow | null): Promise<void> {
  if (!app.isPackaged || !isMacAppTranslocated()) return
  const opts = {
    type: 'warning' as const,
    title: 'KafDesk — slow launch',
    message: 'macOS is running KafDesk from a temporary folder (App Translocation).',
    detail:
      'That is why it feels less snappy than a local build (no quarantine).\n\n' +
      'Fix:\n' +
      '1. Quit KafDesk\n' +
      '2. Drag KafDesk.app alone into /Applications (Replace)\n' +
      '3. Run “Finish Install” (or: xattr -cr /Applications/KafDesk.app)\n' +
      '4. Open from Applications\n\n' +
      'Also use the arm64 DMG on Apple Silicon — the x64 build runs under Rosetta and is slower.',
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
      backgroundThrottling: false,
    },
  })

  if (process.platform === 'darwin' && icon && app.dock) {
    app.dock.setIcon(nativeImage.createFromPath(icon))
  }

  win.once('ready-to-show', () => {
    win?.show()
  })

  win.webContents.on('did-fail-load', (_e, code, desc, url) => {
    console.error('did-fail-load', { code, desc, url })
  })

  if (VITE_DEV_SERVER_URL) {
    win.loadURL(VITE_DEV_SERVER_URL)
  } else {
    win.loadFile(path.join(process.env.DIST!, 'index.html'))
  }
}

function registerIpc() {
  ipcMain.handle('kafka:connect', async (_e, config: ClusterConfig) =>
    kafkaService.connect(config),
  )

  ipcMain.handle('kafka:disconnect', async () => {
    await kafkaService.disconnect()
    return { ok: true }
  })

  ipcMain.handle('kafka:status', async () => ({
    connected: kafkaService.isConnected(),
    schemaRegistry: kafkaService.getSchemaRegistryStatus(),
  }))

  ipcMain.handle('kafka:ping', async () => kafkaService.ping())

  ipcMain.handle('kafka:probe', async (_e, config: ClusterConfig) => kafkaService.probe(config))

  ipcMain.handle('kafka:listTopics', async () => kafkaService.listTopics())

  ipcMain.handle('kafka:getTopicOffsets', async (_e, topic: string) =>
    kafkaService.getTopicOffsets(topic),
  )

  ipcMain.handle('kafka:consume', async (_e, request: ConsumeRequest) =>
    kafkaService.consume(request),
  )

  ipcMain.handle('kafka:produce', async (_e, request: ProduceRequest) =>
    kafkaService.produce(request),
  )

  ipcMain.handle('kafka:produceBatch', async (_e, request: ProduceBatchRequest) =>
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

  ipcMain.handle('kafka:listSubjects', async () => kafkaService.listSubjects())

  ipcMain.handle('kafka:getLatestSchema', async (_e, subject: string) =>
    kafkaService.getLatestSchema(subject),
  )

  ipcMain.handle('kafka:listConsumerGroups', async () => kafkaService.listConsumerGroups())

  ipcMain.handle('kafka:getGroupOffsets', async (_e, groupId: string, topic?: string) =>
    kafkaService.getGroupOffsets(groupId, topic),
  )

  ipcMain.handle('kafka:getLagSummary', async (_e, topic?: string) =>
    kafkaService.getLagSummary(topic),
  )

  ipcMain.handle('kafka:setGroupOffsets', async (_e, request: SetGroupOffsetsRequest) => {
    await kafkaService.setGroupOffsets(request)
    return { ok: true }
  })

  ipcMain.handle(
    'kafka:resetGroupOffsets',
    async (_e, groupId: string, topic: string, earliest: boolean) => {
      await kafkaService.resetGroupOffsets(groupId, topic, earliest)
      return { ok: true }
    },
  )

  ipcMain.handle('kafka:tailStart', async (e, request: TailRequest) => {
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

  ipcMain.handle('kafka:tailStop', async () => {
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
}

app.whenReady().then(() => {
  authService.loadFromDisk()
  registerIpc()
  createWindow()
  // After UI is up: clear quarantine so next cold start skips Gatekeeper rescan
  setTimeout(() => stripMacQuarantineBestEffort(), 2_000)
  setTimeout(() => void warnIfMacTranslocated(win), 1_500)
  // Forward update status to renderer
  onUpdateStatus((s) => {
    if (win && !win.isDestroyed()) win.webContents.send('app:updateStatus', s)
  })
  initAutoUpdate(win)
  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) createWindow()
  })
})

app.on('window-all-closed', async () => {
  await kafkaService.disconnect()
  if (process.platform !== 'darwin') app.quit()
})
