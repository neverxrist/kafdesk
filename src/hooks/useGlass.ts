import { useEffect } from 'react'
import { applyMacGlass, glassSupported } from '../lib/glass'

/** Enable macOS glass automatically (no settings). No-op elsewhere. */
export function useGlass() {
  useEffect(() => {
    void applyMacGlass()
  }, [])

  return { glassSupported: glassSupported() }
}
