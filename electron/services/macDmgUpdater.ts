import { app, BrowserWindow, dialog, shell } from 'electron'
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

function macArch(): 'arm64' | 'x64' {
  return process.arch === 'arm64' ? 'arm64' : 'x64'
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

export function clearPendingMacDmg(): void {
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
# Clear Gatekeeper quarantine so the first open is not a multi-minute scan.
set -euo pipefail
APP="/Applications/KafDesk.app"
if [[ ! -d "$APP" ]]; then
  osascript -e 'display dialog "Сначала перетащи KafDesk.app в Applications (Replace), потом снова запусти этот скрипт." buttons {"OK"} default button 1 with title "KafDesk"'
  exit 1
fi
xattr -cr "$APP" 2>/dev/null || true
open "$APP"
`

/** Companion to the DMG: strip quarantine then open (avoids slow first Gatekeeper scan). */
function writeFinishInstallHelper(): string {
  const dest = path.join(app.getPath('downloads'), 'KafDesk Finish Install.command')
  fs.writeFileSync(dest, FINISH_INSTALL_SCRIPT, { mode: 0o755 })
  return dest
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
      'Like DBeaver: Open DMG mounts the disk image, then KafDesk quits.\n\n' +
      '1. Drag KafDesk.app into Applications → Replace\n' +
      '2. Double-click “Finish Install” / “KafDesk Finish Install.command”\n' +
      '   (clears Gatekeeper quarantine — otherwise first open can take minutes)\n' +
      '3. App opens from Applications\n\n' +
      'Connections and settings are kept.',
    buttons: ['Open DMG & quit', 'Later'],
    defaultId: 0,
    cancelId: 1,
  })
  if (res.response === 0) {
    openMacDmgAndQuit(dmgPath, version)
  }
}
