/// <reference types="vite/client" />

import type { KafDeskApi } from './lib/kafdeskApi'

declare global {
  interface Window {
    kafdesk: KafDeskApi
  }
}

export {}
