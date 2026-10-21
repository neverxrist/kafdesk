# KafDesk

Desktop Kafka client (Electron) for Avro / JSON / String.

## Features

- Connections: SASL (PLAIN / SCRAM), SSL
- Topics, consume / live tail / produce (JSON · Avro · String)
- Filters, saved queries, message diff, republish, produce templates
- Consumer groups, lag, manual offsets
- Schema Registry subjects
- Theme, EN/RU, auto-update

## Install

Installers: [GitHub Releases](https://github.com/neverxrist/kafdesk/releases).

### macOS (unsigned builds)

CI builds are not Apple Developer–signed / notarized yet. Gatekeeper quarantine on a ~100MB Electron app makes the **first** open very slow (or shows “Apple could not verify…”).

**Fast path (recommended):**

1. Drag `KafDesk.app` from the DMG into Applications → **Replace**.
2. Double-click **Finish Install** on the DMG (or run `xattr -cr /Applications/KafDesk.app` in Terminal).
3. That clears quarantine and opens the app.

**Manual:**

```bash
xattr -cr /Applications/KafDesk.app
open /Applications/KafDesk.app
```

If macOS still blocks: **System Settings → Privacy & Security → Open Anyway**.

**Updates:** like DBeaver — download DMG → open & quit → drag to Applications → run **Finish Install** (settings stay in user data).

## Dev

```bash
npm install
npm run dev
```

```bash
npm run build:mac     # macOS
npm run build:win     # Windows
npm run build:linux   # Linux (AppImage + deb)
```

