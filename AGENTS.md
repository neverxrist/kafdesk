# KafDesk — agent navigation

Electron + React (Vite) Kafka desktop client. UI lives under `src/`; IPC types in `electron/types`.

## Layout

| Path | Role |
|------|------|
| `src/main.tsx` | React entry |
| `src/App.tsx` | Layout composition only (thin) |
| `src/hooks/useKafdeskApp.ts` | App state + Kafka/IPC handlers |
| `src/i18n.ts` | RU/EN strings (`t(locale, key)`) |
| `src/styles.css` | Global styles (no CSS modules) |
| `src/lib/connections.ts` | localStorage keys, `loadConnections`, theme/SR helpers |
| `src/hooks/useLocale.ts` | Locale + `tr()` |
| `src/hooks/useTheme.ts` | Dark/light theme |
| `src/hooks/useAppLogs.ts` | In-app log buffer |
| `src/components/ConnectionDrawer.tsx` | Connections sidebar |
| `src/components/TopBar.tsx` | Header + conn summary |
| `src/components/TopicsSidebar.tsx` | Topic list (consume/tail/produce/groups) |
| `src/components/MessageCard.tsx` | Single Kafka message row |
| `src/components/panels/*.tsx` | Tab panels (+ consume/tail toolbars) |

## Backend

- `electron/main.ts` — main process
- `window.kafdesk` — preload API (required at runtime)

## Checks

```bash
npm run typecheck
npm run dev
```

When adding UI, prefer a panel or drawer component over growing `App.tsx`.

## Do not

- Add Docker / local Kafka packs to the repo (`share/`, compose, broker howto).
- Treat Electron ~100MB installers as a bug — Chromium dominates size; cut deps and buffers instead.

Persistent agent rules: `.cursor/rules/product-constraints.mdc`.
