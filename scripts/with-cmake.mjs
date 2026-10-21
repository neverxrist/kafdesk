#!/usr/bin/env node
/**
 * Prepends a local/portable cmake to PATH, then runs the remaining args.
 * Used by tauri:dev / tauri:build so rdkafka-sys can compile without Homebrew.
 */
import { spawn, spawnSync } from 'node:child_process'
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const candidates = [
  path.join(root, 'src-tauri/.tools/cmake/bin/cmake'),
  path.join(root, 'src-tauri/.tools/cmake-3.31.6-macos-universal/CMake.app/Contents/bin/cmake'),
  '/tmp/cmake-3.31.6-macos-universal/CMake.app/Contents/bin/cmake',
  '/opt/homebrew/bin/cmake',
  '/usr/local/bin/cmake',
  '/usr/bin/cmake',
]

function findCmakeOnPath() {
  const cmd = process.platform === 'win32' ? 'where' : 'which'
  const r = spawnSync(cmd, ['cmake'], { encoding: 'utf8' })
  if (r.status !== 0) return null
  const line = (r.stdout || '').split(/\r?\n/).map((s) => s.trim()).find(Boolean)
  return line && fs.existsSync(line) ? line : null
}

function findCmake() {
  for (const p of candidates) {
    try {
      if (fs.existsSync(p) && fs.statSync(p).isFile()) return p
    } catch {
      /* ignore */
    }
  }
  return findCmakeOnPath()
}

const cmake = findCmake()
if (!cmake) {
  console.error(`
cmake not found — required to build librdkafka (rdkafka-sys).

Install one of:
  • brew install cmake          (macOS)
  • apt install cmake           (Linux)
  • choco install cmake         (Windows)
  • or download Kitware cmake and extract to:
      src-tauri/.tools/cmake-3.31.6-macos-universal/
    then:
      ln -sfn cmake-3.31.6-macos-universal/CMake.app/Contents src-tauri/.tools/cmake

Then re-run: npm run tauri:dev
`)
  process.exit(1)
}

const binDir = path.dirname(cmake)
const env = {
  ...process.env,
  PATH: `${binDir}${path.delimiter}${process.env.PATH || ''}`,
  CMAKE: cmake,
}

const args = process.argv.slice(2)
if (!args.length) {
  console.error('usage: node scripts/with-cmake.mjs <command> [args...]')
  process.exit(1)
}

const child = spawn(args[0], args.slice(1), {
  env,
  stdio: 'inherit',
  shell: process.platform === 'win32',
})
child.on('exit', (code, signal) => {
  if (signal) process.kill(process.pid, signal)
  process.exit(code ?? 1)
})
