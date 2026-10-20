import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App'
import './styles.css'

// Browser exploratory testing: Electron preload is absent in plain Vite
if (import.meta.env.DEV && !(window as Window & { kafdesk?: unknown }).kafdesk) {
  const { installMockKafdesk } = await import('./dev/mockKafdesk')
  installMockKafdesk()
}

const savedTheme = localStorage.getItem('kafdesk.theme')
document.documentElement.setAttribute('data-theme', savedTheme === 'light' ? 'light' : 'dark')

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
)
