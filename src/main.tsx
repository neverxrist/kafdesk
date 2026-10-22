import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App'
// Bundle fonts into the app — Google Fonts CDN makes packaged builds janky/offline-fragile
import '@fontsource/ibm-plex-sans/400.css'
import '@fontsource/ibm-plex-sans/500.css'
import '@fontsource/ibm-plex-sans/600.css'
import '@fontsource/ibm-plex-sans/700.css'
import '@fontsource/ibm-plex-sans/cyrillic-400.css'
import '@fontsource/ibm-plex-sans/cyrillic-500.css'
import '@fontsource/ibm-plex-sans/cyrillic-600.css'
import '@fontsource/ibm-plex-sans/cyrillic-700.css'
import '@fontsource/ibm-plex-mono/400.css'
import '@fontsource/ibm-plex-mono/500.css'
import '@fontsource/ibm-plex-mono/600.css'
import '@fontsource/ibm-plex-mono/cyrillic-400.css'
import '@fontsource/ibm-plex-mono/cyrillic-500.css'
import '@fontsource/ibm-plex-mono/cyrillic-600.css'
import './styles.css'
import { installDragScroll } from './lib/dragScroll'
import { installTauriKafdesk } from './lib/tauriBridge'

installTauriKafdesk()
installDragScroll()

const savedTheme = localStorage.getItem('kafdesk.theme')
document.documentElement.setAttribute('data-theme', savedTheme === 'light' ? 'light' : 'dark')
/* Glass is applied after first paint (see applyMacGlass) — keep solid at boot. */
document.documentElement.setAttribute('data-glass', '0')

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
)
