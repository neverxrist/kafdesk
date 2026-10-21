# KafDesk

Desktop Kafka client (**Tauri 2** + Rust librdkafka) for Avro / JSON / String.

## Features

- Connections: SASL (PLAIN / SCRAM), SSL
- Topics, consume / live tail / produce (JSON · Avro · String)
- Filters, saved queries, message diff, republish, produce templates
- Consumer groups, lag, manual offsets
- Schema Registry subjects
- Theme, EN/RU, Mac DMG updates

## Install

Installers: [GitHub Releases](https://github.com/neverxrist/kafdesk/releases).

### macOS (unsigned builds)

CI/local builds may not be Apple Developer–signed. If Gatekeeper blocks:

```bash
xattr -cr /Applications/KafDesk.app
open /Applications/KafDesk.app
```

Or **System Settings → Privacy & Security → Open Anyway**.

**Updates:** download DMG → open & quit → drag to Applications → Replace (settings stay in user data).

## Dev

Needs Node 22+, Rust, and cmake (for librdkafka). Portable cmake can live under `src-tauri/.tools/cmake/` — `npm run tauri:*` injects PATH via `scripts/with-cmake.mjs`.

```bash
npm install
npm run tauri:dev
```

```bash
npm run build:mac     # .app + .dmg under src-tauri/target/release/bundle/
```
