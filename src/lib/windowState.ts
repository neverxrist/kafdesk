const KEY = 'kafdesk.window.v1'

type WinState = { width: number; height: number; x: number; y: number }

function readSaved(): WinState | null {
  try {
    const raw = localStorage.getItem(KEY)
    if (!raw) return null
    const s = JSON.parse(raw) as WinState
    if (
      typeof s.width === 'number' &&
      typeof s.height === 'number' &&
      s.width >= 900 &&
      s.height >= 600
    ) {
      return s
    }
  } catch {
    /* ignore */
  }
  return null
}

/** Restore last window size/position (mac/win/linux Tauri). */
export async function restoreWindowState(): Promise<void> {
  const s = readSaved()
  if (!s) return
  try {
    const { getCurrentWindow } = await import('@tauri-apps/api/window')
    const { LogicalSize, LogicalPosition } = await import('@tauri-apps/api/dpi')
    const win = getCurrentWindow()
    await win.setSize(new LogicalSize(s.width, s.height))
    if (Number.isFinite(s.x) && Number.isFinite(s.y)) {
      await win.setPosition(new LogicalPosition(s.x, s.y))
    }
  } catch {
    /* web / unsupported */
  }
}

/** Persist size/position while the user resizes or moves the window. */
export async function installWindowStatePersistence(): Promise<void> {
  try {
    const { getCurrentWindow } = await import('@tauri-apps/api/window')
    const win = getCurrentWindow()
    let timer: ReturnType<typeof setTimeout> | undefined

    const save = async () => {
      try {
        const factor = await win.scaleFactor()
        const size = await win.outerSize()
        const pos = await win.outerPosition()
        const next: WinState = {
          width: Math.round(size.width / factor),
          height: Math.round(size.height / factor),
          x: Math.round(pos.x / factor),
          y: Math.round(pos.y / factor),
        }
        localStorage.setItem(KEY, JSON.stringify(next))
      } catch {
        /* ignore */
      }
    }

    const schedule = () => {
      if (timer) clearTimeout(timer)
      timer = setTimeout(() => void save(), 350)
    }

    await win.onResized(() => schedule())
    await win.onMoved(() => schedule())
  } catch {
    /* web / unsupported */
  }
}
