import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App'
// Bundle fonts — keep weights lean (RAM + installer); 400/600 cover UI.
import '@fontsource/ibm-plex-sans/400.css'
import '@fontsource/ibm-plex-sans/600.css'
import '@fontsource/ibm-plex-sans/cyrillic-400.css'
import '@fontsource/ibm-plex-sans/cyrillic-600.css'
import '@fontsource/ibm-plex-mono/400.css'
import '@fontsource/ibm-plex-mono/cyrillic-400.css'
import './styles.css'
import { installDragScroll } from './lib/dragScroll'
import { applyMsgFontPx, loadMsgFontPx } from './lib/msgFontScale'
import { installHttpKafdesk, preloadWebFace } from './lib/httpBridge'
import { installTauriKafdesk } from './lib/tauriBridge'

async function installBackend() {
  if ('__TAURI_INTERNALS__' in window) {
    installTauriKafdesk()
    return
  }
  await preloadWebFace()
  installHttpKafdesk()
}

void installBackend().then(() => {
  installDragScroll()
  applyMsgFontPx(loadMsgFontPx())

  const savedTheme = localStorage.getItem('kafdesk.theme')
  document.documentElement.setAttribute('data-theme', savedTheme === 'light' ? 'light' : 'dark')
  /* Glass is applied after first paint (see applyMacGlass) — keep solid at boot. */
  document.documentElement.setAttribute('data-glass', '0')
  /* WKWebView with lang=ru capitalizes sentence starts in inputs otherwise. */
  document.documentElement.setAttribute('autocapitalize', 'off')
  document.documentElement.setAttribute('autocorrect', 'off')
  document.documentElement.setAttribute('spellcheck', 'false')
  document.body?.setAttribute('autocapitalize', 'off')
  document.body?.setAttribute('spellcheck', 'false')

  ReactDOM.createRoot(document.getElementById('root')!).render(
    <React.StrictMode>
      <App />
    </React.StrictMode>,
  )
})
