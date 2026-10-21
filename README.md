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

CI builds are not Apple Developer–signed / notarized yet, so Gatekeeper may block the first launch (“Apple could not verify…” / «не содержит вредоносного ПО»).

1. Do **not** double-click. Right-click `KafDesk.app` → **Open** → confirm **Open**.
2. Or: **System Settings → Privacy & Security** → find the blocked app notice → **Open Anyway**.
3. Or in Terminal (adjust the path if needed):

```bash
xattr -cr /Applications/KafDesk.app
open /Applications/KafDesk.app
```

Auto-update on Mac also needs a signed build — until then, install new versions from Releases manually.

## Dev

```bash
npm install
npm run dev
```

```bash
npm run build:mac   # macOS
npm run build:win   # Windows
```

