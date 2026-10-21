import { AppError, toErrorMessage } from '../../domain'

/**
 * Preserve Error.message across Electron IPC (structured clone drops custom fields).
 * AppError.code is embedded so the renderer can parse it later if needed.
 */
export function toIpcError(err: unknown): Error {
  if (err instanceof AppError) {
    const e = new Error(err.message)
    e.name = 'AppError'
    ;(e as Error & { code?: string }).code = err.code
    return e
  }
  if (err instanceof Error) return err
  return new Error(toErrorMessage(err))
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type IpcFn = (event: Electron.IpcMainInvokeEvent, ...args: any[]) => Promise<any>

export function wrapIpcHandler(fn: IpcFn): IpcFn {
  return async (event, ...args) => {
    try {
      return await fn(event, ...args)
    } catch (err) {
      throw toIpcError(err)
    }
  }
}
