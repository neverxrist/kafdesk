#!/usr/bin/env node
/**
 * Dev-only (macOS): Dock tooltip uses the .app bundle filename, not productName.
 * Rename Electron.app → KafDesk.app and retarget path.txt + Info.plist.
 */
import { spawnSync } from 'node:child_process'
import fs from 'node:fs'
import path from 'node:path'
import { createRequire } from 'node:module'

if (process.platform !== 'darwin') process.exit(0)

const require = createRequire(import.meta.url)
const electronRoot = path.dirname(require.resolve('electron/package.json'))
const dist = path.join(electronRoot, 'dist')
const electronApp = path.join(dist, 'Electron.app')
const kafdeskApp = path.join(dist, 'KafDesk.app')
const pathTxt = path.join(electronRoot, 'path.txt')

const NAME = 'KafDesk'
const ID = 'app.kafdesk.desktop.dev'

function setPlist(plist, key, value) {
  const r = spawnSync('/usr/libexec/PlistBuddy', ['-c', `Set :${key} ${value}`, plist], {
    encoding: 'utf8',
  })
  if (r.status !== 0) {
    spawnSync('/usr/libexec/PlistBuddy', ['-c', `Add :${key} string ${value}`, plist], {
      encoding: 'utf8',
    })
  }
}

// Prefer renaming Electron.app → KafDesk.app (Dock reads the folder name)
let appPath = kafdeskApp
if (fs.existsSync(electronApp) && !fs.existsSync(kafdeskApp)) {
  fs.renameSync(electronApp, kafdeskApp)
  console.log('[patch-electron-name] renamed Electron.app → KafDesk.app')
} else if (fs.existsSync(kafdeskApp)) {
  // already renamed
} else if (fs.existsSync(electronApp)) {
  appPath = electronApp
} else {
  console.warn('[patch-electron-name] no Electron/KafDesk.app found, skip')
  process.exit(0)
}

if (!fs.existsSync(appPath)) appPath = fs.existsSync(kafdeskApp) ? kafdeskApp : electronApp

const plist = path.join(appPath, 'Contents', 'Info.plist')
setPlist(plist, 'CFBundleName', NAME)
setPlist(plist, 'CFBundleDisplayName', NAME)
setPlist(plist, 'CFBundleIdentifier', ID)

// Point the electron package at the renamed bundle
const relExec = `${path.basename(appPath)}/Contents/MacOS/Electron`
fs.writeFileSync(pathTxt, relExec)

const sign = spawnSync('codesign', ['--force', '--deep', '--sign', '-', appPath], {
  encoding: 'utf8',
})
if (sign.status !== 0) {
  console.warn('[patch-electron-name] codesign warning:', sign.stderr || sign.stdout)
}

console.log(`[patch-electron-name] Dock name → ${NAME} (${relExec})`)
