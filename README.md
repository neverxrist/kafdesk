# KafDesk

Desktop Kafka client (**Tauri 2** + Rust librdkafka) for Avro / JSON / String.

## Features

- Connections: SASL (PLAIN / SCRAM), SSL
- Topics, consume / live tail / produce (JSON · Avro · String)
- Filters, saved queries, message diff, republish, produce templates
- Trace a value across a saved topic set: record key and message body, including Avro
- Consumer groups: lag by topic, trend, stalled groups, manual offsets
- Messages over 256 KB stay a preview until you load the full value
- Live tail flushes in batches and shows how many messages it skipped
- Schema Registry subjects
- Theme, EN/RU

## Install

[GitHub Releases](https://github.com/neverxrist/kafdesk/releases): macOS Apple Silicon (DMG), Windows (NSIS), Linux (AppImage and .deb).

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
