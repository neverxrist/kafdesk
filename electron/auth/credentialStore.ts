import fs from 'node:fs'
import path from 'node:path'
import { app, safeStorage } from 'electron'

export interface StoredCredentialsInfo {
  username?: string
  hasPassword: boolean
}

interface StoredFile {
  username: string
  /** base64 of safeStorage-encrypted password (OS keychain backed) */
  passwordEncrypted: string
}

function storePath(): string {
  return path.join(app.getPath('userData'), 'auth-credentials.json')
}

function readFile(): StoredFile | null {
  try {
    const raw = fs.readFileSync(storePath(), 'utf8')
    const parsed = JSON.parse(raw) as StoredFile
    if (!parsed.username || !parsed.passwordEncrypted) return null
    return parsed
  } catch {
    return null
  }
}

export function normalizeLogin(login: string): string {
  const trimmed = login.trim()
  const at = trimmed.indexOf('@')
  return at > 0 ? trimmed.slice(0, at) : trimmed
}

export function getStoredCredentialsInfo(): StoredCredentialsInfo {
  const f = readFile()
  return { username: f?.username, hasPassword: Boolean(f) }
}

export function getStoredPassword(username?: string): string | null {
  const f = readFile()
  if (!f) return null
  if (username && normalizeLogin(username) !== f.username) return null
  if (!safeStorage.isEncryptionAvailable()) return null
  try {
    return safeStorage.decryptString(Buffer.from(f.passwordEncrypted, 'base64'))
  } catch {
    return null
  }
}

export function saveCredentials(username: string, password: string): void {
  if (!safeStorage.isEncryptionAvailable()) {
    throw new Error('OS secure storage is unavailable — password was not saved')
  }
  const data: StoredFile = {
    username: normalizeLogin(username),
    passwordEncrypted: safeStorage.encryptString(password).toString('base64'),
  }
  fs.writeFileSync(storePath(), JSON.stringify(data), { encoding: 'utf8', mode: 0o600 })
}

export function clearCredentials(): void {
  try {
    fs.unlinkSync(storePath())
  } catch {
    /* ignore */
  }
}
