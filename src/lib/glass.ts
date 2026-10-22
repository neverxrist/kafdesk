import { detectUpdateOs } from './updateOs'

/** Glass is a macOS-only window effect (always on, applied after first paint). */
export function glassSupported(): boolean {
  return detectUpdateOs() === 'mac'
}

export function applyGlassDom(enabled: boolean) {
  document.documentElement.setAttribute('data-glass', enabled ? '1' : '0')
}

function afterFirstPaint(): Promise<void> {
  return new Promise((resolve) => {
    requestAnimationFrame(() => {
      requestAnimationFrame(() => {
        // Let React commit + layout finish before compositor glass work.
        setTimeout(resolve, 80)
      })
    })
  })
}

/**
 * Native macOS glass: Liquid Glass (26+) → vibrancy on older macOS.
 * Deferred so cold start isn't blocked by NSGlassEffectView setup.
 */
export async function applyMacGlass(): Promise<void> {
  if (!glassSupported()) {
    applyGlassDom(false)
    return
  }

  // Start solid; flip to glass only once the effect is ready.
  applyGlassDom(false)
  await afterFirstPaint()

  try {
    const { getCurrentWindow, Effect, EffectState } = await import('@tauri-apps/api/window')
    const win = getCurrentWindow()

    try {
      await win.setBackgroundColor([0, 0, 0, 0])
    } catch {
      /* optional */
    }
    try {
      const { getCurrentWebviewWindow } = await import('@tauri-apps/api/webviewWindow')
      await getCurrentWebviewWindow().setBackgroundColor([0, 0, 0, 0])
    } catch {
      /* optional */
    }

    await win.setEffects({
      effects: [Effect.LiquidGlassRegular, Effect.UnderWindowBackground],
      state: EffectState.Active,
      color: [12, 14, 20, 72],
    })
    applyGlassDom(true)
  } catch {
    applyGlassDom(false)
  }
}
