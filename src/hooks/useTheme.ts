import { useEffect, useState } from 'react'
import { applyTheme, loadTheme, type Theme } from '../lib/connections'

export function useTheme() {
  const [theme, setTheme] = useState<Theme>(loadTheme)

  useEffect(() => {
    applyTheme(theme)
  }, [theme])

  function toggleTheme() {
    setTheme((v) => (v === 'dark' ? 'light' : 'dark'))
  }

  return { theme, toggleTheme }
}
