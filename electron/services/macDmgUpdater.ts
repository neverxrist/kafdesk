import { app, BrowserWindow, clipboard, dialog, shell } from 'electron'
import { spawn, spawnSync } from 'node:child_process'
import fs from 'node:fs'
import https from 'node:https'
import http from 'node:http'
import path from 'node:path'
import { URL } from 'node:url'
import type { UpdateStatus } from '../types'

const OWNER = 'neverxrist'
const REPO = 'kafdesk'
const RELEASES_PAGE = `https://github.com/${OWNER}/${REPO}/releases`

export type MacUpdateProgress = (s: UpdateStatus) => void

function cmpSemver(a: string, b: string): number {
  const pa = a.replace(/^v/i, '').split(/[.+-]/).map((x) => parseInt(x, 10) || 0)
  const pb = b.replace(/^v/i, '').split(/[.+-]/).map((x) => parseInt(x, 10) || 0)
  const n = Math.max(pa.length, pb.length)
  for (let i = 0; i < n; i++) {
    const d = (pa[i] ?? 0) - (pb[i] ?? 0)
    if (d) return d
  }
  return 0
}

/** True Apple Silicon even when this process is the x64 binary under Rosetta. */
export function isAppleSilicon(): boolean {
  if (process.platform !== 'darwin') return false
  if (process.arch === 'arm64') return true
  try {
    const r = spawnSync('sysctl', ['-n', 'hw.optional.arm64'], { encoding: 'utf8' })
    return r.status === 0 && r.stdout.trim() === '1'
  } catch {
    return false
  }
}

/** Prefer native arm64 DMG on M-series — process.arch is x64 under Rosetta. */
export function macArch(): 'arm64' | 'x64' {
  if (process.arch === 'arm64' || isAppleSilicon()) return 'arm64'
  return 'x64'
}

export function isRunningUnderRosetta(): boolean {
  return process.platform === 'darwin' && process.arch === 'x64' && isAppleSilicon()
}

function httpGetJson<T>(url: string): Promise<T> {
  return new Promise((resolve, reject) => {
    const u = new URL(url)
    const lib = u.protocol === 'http:' ? http : https
    const req = lib.get(
      url,
      {
        headers: {
          Accept: 'application/vnd.github+json',
          'User-Agent': 'KafDesk-Updater',
          'X-GitHub-Api-Version': '2022-11-28',
        },
      },
      (res) => {
        if (res.statusCode && res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          httpGetJson<T>(res.headers.location).then(resolve, reject)
          return
        }
        if ((res.statusCode ?? 0) >= 400) {
          reject(new Error(`GitHub API ${res.statusCode}`))
          res.resume()
          return
        }
        const chunks: Buffer[] = []
        res.on('data', (c) => chunks.push(c))
        res.on('end', () => {
          try {
            resolve(JSON.parse(Buffer.concat(chunks).toString('utf8')) as T)
          } catch (e) {
            reject(e)
          }
        })
      },
    )
    req.on('error', reject)
  })
}

function downloadFile(
  url: string,
  dest: string,
  onProgress?: (percent: number) => void,
): Promise<void> {
  return new Promise((resolve, reject) => {
    const follow = (u: string) => {
      const parsed = new URL(u)
      const lib = parsed.protocol === 'http:' ? http : https
      const req = lib.get(
        u,
        { headers: { 'User-Agent': 'KafDesk-Updater' } },
        (res) => {
          if (res.statusCode && res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
            follow(res.headers.location)
            return
          }
          if ((res.statusCode ?? 0) >= 400) {
            reject(new Error(`Download failed: HTTP ${res.statusCode}`))
            res.resume()
            return
          }
          const total = Number(res.headers['content-length'] || 0)
          let got = 0
          const out = fs.createWriteStream(dest)
          res.on('data', (chunk: Buffer) => {
            got += chunk.length
            if (total > 0 && onProgress) onProgress(Math.min(99, Math.round((got / total) * 100)))
          })
          res.pipe(out)
          out.on('finish', () => {
            out.close()
            onProgress?.(100)
            resolve()
          })
          out.on('error', reject)
        },
      )
      req.on('error', reject)
    }
    follow(url)
  })
}

type GhAsset = { name: string; browser_download_url: string; size: number }
type GhRelease = { tag_name: string; assets: GhAsset[] }

type PendingMacUpdate = { path: string; version: string }

function pendingStorePath(): string {
  return path.join(app.getPath('userData'), 'pending-mac-update.json')
}

function writePending(p: PendingMacUpdate | null) {
  try {
    const f = pendingStorePath()
    if (!p) {
      if (fs.existsSync(f)) fs.unlinkSync(f)
      return
    }
    fs.writeFileSync(f, JSON.stringify(p), 'utf8')
  } catch {
    /* ignore */
  }
}

function readPending(): PendingMacUpdate | null {
  try {
    const raw = fs.readFileSync(pendingStorePath(), 'utf8')
    const parsed = JSON.parse(raw) as PendingMacUpdate
    if (!parsed?.path || !parsed?.version) return null
    if (!fs.existsSync(parsed.path)) {
      writePending(null)
      return null
    }
    return parsed
  } catch {
    return null
  }
}

export async function findLatestMacDmg(): Promise<{
  version: string
  url: string
  name: string
} | null> {
  const release = await httpGetJson<GhRelease>(
    `https://api.github.com/repos/${OWNER}/${REPO}/releases/latest`,
  )
  const version = release.tag_name.replace(/^v/i, '')
  const arch = macArch()
  const asset =
    release.assets.find((a) => /-mac-${arch}\.dmg$/i.test(a.name) && !a.name.endsWith('.blockmap')) ??
    release.assets.find(
      (a) => a.name.toLowerCase().includes(`mac-${arch}`) && a.name.endsWith('.dmg'),
    )
  if (!asset) return null
  return { version, url: asset.browser_download_url, name: asset.name }
}

export function isNewerVersion(remote: string, local: string): boolean {
  return cmpSemver(remote, local) > 0
}

export function getPendingMacDmg(): PendingMacUpdate | null {
  return readPending()
}

function cleanupInstallArtifacts(dmgPath?: string | null): void {
  const downloads = app.getPath('downloads')
  const victims = [
    dmgPath,
    path.join(downloads, 'KafDesk Finish Install.command'),
    path.join(downloads, 'KafDesk-after-install.txt'),
  ]
  for (const f of victims) {
    if (!f) continue
    try {
      if (fs.existsSync(f)) fs.unlinkSync(f)
    } catch {
      /* ignore */
    }
  }
  // Any leftover KafDesk-*.dmg in Downloads
  try {
    for (const name of fs.readdirSync(downloads)) {
      if (/^KafDesk.*\.dmg$/i.test(name)) {
        try {
          fs.unlinkSync(path.join(downloads, name))
        } catch {
          /* ignore */
        }
      }
    }
  } catch {
    /* ignore */
  }
}

export function clearPendingMacDmg(): void {
  const pending = readPending()
  cleanupInstallArtifacts(pending?.path)
  writePending(null)
}

/** Put DMG in Downloads so the user can find it (DBeaver-style). */
function ensureDmgInDownloads(dmgPath: string): string {
  const downloads = app.getPath('downloads')
  const dest = path.join(downloads, path.basename(dmgPath))
  if (path.resolve(dmgPath) === path.resolve(dest)) return dest
  try {
    fs.copyFileSync(dmgPath, dest)
    return dest
  } catch {
    return dmgPath
  }
}

const FINISH_INSTALL_SCRIPT = `#!/bin/bash
# Clear Gatekeeper quarantine; unmount & delete the DMG from Downloads.
# Run AFTER dragging KafDesk.app into Applications — do not open the app first.
set -euo pipefail
APP="/Applications/KafDesk.app"
DOWNLOADS="\${HOME}/Downloads"
if [[ ! -d "$APP" ]]; then
  osascript -e 'display dialog "Сначала перетащи KafDesk.app в Applications (Replace), потом снова запусти этот скрипт." buttons {"OK"} default button 1 with title "KafDesk"'
  exit 1
fi
xattr -cr "$APP" 2>/dev/null || true
for vol in /Volumes/KafDesk*; do
  if [[ -d "$vol" ]]; then
    hdiutil detach "$vol" -quiet 2>/dev/null || hdiutil detach "$vol" -force 2>/dev/null || true
  fi
done
sleep 0.4
rm -f "$DOWNLOADS"/KafDesk*.dmg 2>/dev/null || true
rm -f "$DOWNLOADS/KafDesk Finish Install.command" 2>/dev/null || true
rm -f "$DOWNLOADS/KafDesk-after-install.txt" 2>/dev/null || true
open "$APP"
SELF="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"
if [[ "$SELF" == "$DOWNLOADS/"* ]]; then
  rm -f "$SELF" 2>/dev/null || true
fi
`

const POST_INSTALL_CMDS = `xattr -cr /Applications/KafDesk.app
open /Applications/KafDesk.app`

/** Companion to the DMG: strip quarantine then open (avoids slow first Gatekeeper scan). */
function writeFinishInstallHelper(): string {
  const dest = path.join(app.getPath('downloads'), 'KafDesk Finish Install.command')
  fs.writeFileSync(dest, FINISH_INSTALL_SCRIPT, { mode: 0o755 })
  return dest
}

/**
 * Survives app.quit(): system dialog + clipboard with the Terminal one-liner.
 * (Electron dialogs disappear when we exit.)
 */
function showPostInstallHelp(): void {
  try {
    clipboard.writeText(POST_INSTALL_CMDS)
  } catch {
    /* ignore */
  }

  try {
    const notePath = path.join(app.getPath('downloads'), 'KafDesk-after-install.txt')
    fs.writeFileSync(
      notePath,
      `KafDesk — после установки\n\n` +
        `1. Перетащи KafDesk.app в Applications → Replace\n` +
        `2. Не открывай app сразу — сначала в Terminal:\n\n` +
        `${POST_INSTALL_CMDS}\n\n` +
        `Или «KafDesk Finish Install.command» (снимет quarantine и удалит DMG).\n` +
        `Без xattr первый старт очень долгий (Gatekeeper).\n`,
      'utf8',
    )
  } catch {
    /* ignore */
  }

  const scriptPath = path.join(app.getPath('temp'), 'kafdesk-post-install.applescript')
  const applescript = `
set msg to "1. Перетащи KafDesk.app в Applications → Replace" & return & return & ¬
  "2. НЕ открывай app двойным кликом — сначала Terminal (⌘V, уже в буфере):" & return & return & ¬
  "xattr -cr /Applications/KafDesk.app" & return & ¬
  "open /Applications/KafDesk.app" & return & return & ¬
  "Или «Finish Install» — он же удалит DMG из Downloads." & return & return & ¬
  "Если открыть без xattr — первый старт будет очень долгим (Gatekeeper)."
display dialog msg buttons {"OK"} default button 1 with title "KafDesk — после установки"
`
  try {
    fs.writeFileSync(scriptPath, applescript, 'utf8')
    spawn('osascript', [scriptPath], { detached: true, stdio: 'ignore' }).unref()
  } catch {
    /* ignore */
  }
}

/**
 * DBeaver-style Mac update: open the DMG in Finder, then quit so the user can
 * drag KafDesk.app → Applications → Replace, and launch the new app.
 * Silent in-place swap + relaunch was unreliable on unsigned builds.
 */
export function openMacDmgAndQuit(dmgPath: string, version?: string): void {
  const visible = ensureDmgInDownloads(dmgPath)
  const ver = version ?? readPending()?.version
  if (ver) writePending({ path: visible, version: ver })
  try {
    writeFinishInstallHelper()
  } catch {
    /* ignore */
  }

  showPostInstallHelp()

  void shell.openPath(visible).then((err) => {
    if (err) {
      // Fallback: reveal in Finder if openPath fails
      shell.showItemInFolder(visible)
    }
  })

  // Soft quit first so windows can close; force-exit shortly after.
  setTimeout(() => {
    app.quit()
    setTimeout(() => app.exit(0), 1500)
  }, 400)
}

export async function checkAndDownloadMacDmg(
  emit: MacUpdateProgress,
  win: BrowserWindow | null,
): Promise<UpdateStatus> {
  emit({ state: 'checking' })
  const latest = await findLatestMacDmg()
  if (!latest) {
    const s: UpdateStatus = {
      state: 'error',
      message: `No macOS DMG found for this arch (${macArch()}). See ${RELEASES_PAGE}`,
    }
    emit(s)
    return s
  }

  const current = app.getVersion()
  if (!isNewerVersion(latest.version, current)) {
    writePending(null)
    const s: UpdateStatus = { state: 'not-available', version: latest.version }
    emit(s)
    return s
  }

  emit({ state: 'available', version: latest.version })
  emit({ state: 'downloading', version: latest.version, percent: 0 })

  const dest = path.join(app.getPath('downloads'), latest.name)
  try {
    if (fs.existsSync(dest)) fs.unlinkSync(dest)
  } catch {
    /* ignore */
  }

  await downloadFile(latest.url, dest, (percent) =>
    emit({ state: 'downloading', version: latest.version, percent }),
  )

  writePending({ path: dest, version: latest.version })
  try {
    writeFinishInstallHelper()
  } catch {
    /* ignore */
  }
  const s: UpdateStatus = { state: 'downloaded', version: latest.version }
  emit(s)

  if (win && !win.isDestroyed()) {
    await offerOpenDmgDialog(win, latest.version, dest)
  }

  return s
}

export async function offerOpenDmgDialog(
  win: BrowserWindow,
  version: string,
  dmgPath: string,
): Promise<void> {
  const res = await dialog.showMessageBox(win, {
    type: 'info',
    title: 'KafDesk update',
    message: `KafDesk ${version} downloaded.`,
    detail:
      'Open DMG → KafDesk quits → drag into Applications (Replace).\n\n' +
      'Then in Terminal (we’ll copy this and show a reminder):\n\n' +
      `${POST_INSTALL_CMDS}\n\n` +
      'Or double-click “Finish Install” on the DMG / in Downloads.\n' +
      'Connections and settings are kept.',
    buttons: ['Open DMG & quit', 'Later'],
    defaultId: 0,
    cancelId: 1,
  })
  if (res.response === 0) {
    openMacDmgAndQuit(dmgPath, version)
  }
}
