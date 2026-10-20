import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

/** Renderer-only Vite config for browser exploratory testing (no Electron). */
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5175,
    strictPort: true,
  },
})
