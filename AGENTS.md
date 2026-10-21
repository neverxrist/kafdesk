# KafDesk — agent / architecture map

Desktop Kafka client: **Tauri 2** (Rust + librdkafka) + **React** (UI). No DB — persistence is `localStorage` / OS keychain / files.

See also: `.cursor/rules/product-constraints.mdc` (caps, consume contract, no README Kafka howto).

## Layout (Tauri-only)

```
src-tauri/src/
  domain/                 # caps, consume window plan, message match
  kafka/
    config.rs             # SSL/SASL + timeouts ClientConfig
    session.rs            # connect/disconnect/ping/topics facade
    consume.rs / produce.rs / tail.rs / groups.rs
    decode.rs             # Confluent wire + format detect
  avro_codec.rs           # schema-aware JSON↔Avro
  sr.rs                   # Schema Registry HTTP (Basic + mTLS)
  auth.rs                 # Keycloak password grant + keyring
  updates.rs              # Mac DMG download / progress
  commands.rs             # thin Tauri IPC (+ session detach for long ops)
  types.rs
src/
  App.tsx, hooks/, components/, panels/, lib/
  types/kafdesk.ts        # shared IPC DTOs
  lib/tauriBridge.ts      # window.kafdesk → invoke/listen
  lib/kafdeskApi.ts       # API surface type
scripts/with-cmake.mjs    # PATH for librdkafka cmake_build
share/                    # gitignored local Docker Kafka (dev only)
```

**Hotspots:** `kafka/session.rs` (facade state), `useKafdeskApp.ts`, `ConsumePanel.tsx`.

## Layers (Clean lite — no DDD ceremony)

| Layer | Status | Notes |
|-------|--------|-------|
| **Domain** | ✅ | `plan_consume_window`, `matches_*`, caps |
| **Infrastructure** | ✅ | kafka/* + sr + avro + auth + updates |
| **IPC** | ✅ | `commands.rs` → session; long consume/batch detach mutex |
| **Presentation** | ◐ | React; `useConsumeFilters` extracted |

## Consume contract (do not break)

- **Seek (broker):** startFrom / offset / limit / partition → new consume
- **Client filter:** search / key / headers / time → on loaded window only
- Caps: consume ≤500, bulk ≤2000; banner errors; disconnect on offline

## Auth / formats

- Kafka: PLAINTEXT, SSL, SASL PLAIN/SCRAM (no OAUTHBEARER on this stack)
- SR: Basic + PEM CA/mTLS; Avro encode/decode when SR probe `ok`
- Future: more SASL (GSSAPI, etc.), Protobuf via SR — add under `kafka/` + `avro_codec`-style modules

## Commands

- `npm run typecheck` — must stay green
- `npm run tauri:dev` / `tauri:build` — needs Rust + cmake (`scripts/with-cmake.mjs`)
- Do not push/release unless asked; do not delete `share/` or a running local stack
