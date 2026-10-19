/// <reference types="vite/client" />

import type { KafDeskApi } from '../electron/preload'

declare global {
  interface Window {
    kafdesk: KafDeskApi
  }
}

export {}
