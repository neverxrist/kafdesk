import { app, BrowserWindow, dialog } from 'electron'
import { spawn } from 'node:child_process'
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

/** Path to KafDesk.app bundle */
export function appBundlePath(): string {
  return path.resolve(path.dirname(process.execPath), '..', '..')
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

let pendingDmg: string | null = null
let pendingVersion: string | null = null

export function getPendingMacDmg(): { path: string; version: string } | null {
  if (!pendingDmg || !pendingVersion) return null
  if (!fs.existsSync(pendingDmg)) return null
  return { path: pendingDmg, version: pendingVersion }
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
    const s: UpdateStatus = { state: 'not-available', version: latest.version }
    emit(s)
    return s
  }

  emit({ state: 'available', version: latest.version })
  emit({ state: 'downloading', version: latest.version, percent: 0 })

  const dest = path.join(app.getPath('temp'), latest.name)
  try {
    if (fs.existsSync(dest)) fs.unlinkSync(dest)
  } catch {
    /* ignore */
  }

  await downloadFile(latest.url, dest, (percent) =>
    emit({ state: 'downloading', version: latest.version, percent }),
  )

  pendingDmg = dest
  pendingVersion = latest.version
  const s: UpdateStatus = { state: 'downloaded', version: latest.version }
  emit(s)

  if (win && !win.isDestroyed()) {
    const res = await dialog.showMessageBox(win, {
      type: 'info',
      title: 'KafDesk update',
      message: `KafDesk ${latest.version} downloaded.`,
      detail:
        'Restart now to replace the app from the DMG (no Apple signing required). Or choose Later and restart from Settings later.',
      buttons: ['Restart now', 'Later'],
      defaultId: 0,
      cancelId: 1,
    })
    if (res.response === 0) {
      installMacDmgAndRelaunch(dest)
    }
  }

  return s
}

/** Spawn a helper that waits for this process to exit, swaps the .app from the DMG, and relaunches. */
export function installMacDmgAndRelaunch(dmgPath: string): void {
  const appPath = appBundlePath()
  const scriptPath = path.join(app.getPath('temp'), 'kafdesk-apply-update.sh')
  const appName = path.basename(appPath)
  const parent = path.dirname(appPath)
  const pid = process.pid

  const script = `#!/bin/bash
set -euo pipefail
DMG=${JSON.stringify(dmgPath)}
APP=${JSON.stringify(appPath)}
APP_NAME=${JSON.stringify(appName)}
PARENT=${JSON.stringify(parent)}
PID=${pid}
LOG="$HOME/Library/Logs/KafDesk-update.log"
exec >>"$LOG" 2>&1
echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) apply update start"

while kill -0 "$PID" 2>/dev/null; do sleep 0.4; done
sleep 0.8

ATTACH_OUT=$(hdiutil attach -nobrowse -readonly "$DMG" || true)
MOUNT=$(printf '%s\\n' "$ATTACH_OUT" | awk '/\\/Volumes\\//{print $NF; exit}')
if [[ -z "\${MOUNT:-}" || ! -d "$MOUNT/$APP_NAME" ]]; then
  echo "Failed to mount DMG or find $APP_NAME"
  exit 1
fi

TMP="$PARENT/.\${APP_NAME}.updating"
BACKUP="$PARENT/.\${APP_NAME}.bak"
rm -rf "$TMP" "$BACKUP"
ditto "$MOUNT/$APP_NAME" "$TMP"
hdiutil detach "$MOUNT" -quiet || hdiutil detach "$MOUNT" -force || true

mv "$APP" "$BACKUP"
mv "$TMP" "$APP"
xattr -cr "$APP" || true
rm -rf "$BACKUP"
rm -f "$DMG" || true

echo "Relaunching $APP"
open "$APP"
echo "done"
`

  fs.writeFileSync(scriptPath, script, { mode: 0o755 })
  const child = spawn('/bin/bash', [scriptPath], {
    detached: true,
    stdio: 'ignore',
  })
  child.unref()
  app.quit()
}
