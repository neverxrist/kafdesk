# KafDesk

Desktop Kafka client (Electron) for Avro / JSON / String.

## Features

- Connections (drawer): SASL and optional Keycloak SSO
- Topics, partition offsets, consume / live tail / produce
- Consumer groups + manual offset move
- Schema Registry subjects
- Theme toggle, EN/RU, logs tab

## Dev

```bash
npm install
npm run dev
```

## Build (MVP)

```bash
npm run build:mac   # DMG + ZIP · arm64 (Apple Silicon) + x64 (Intel)
npm run build:win   # NSIS installer · x64
# or both from this machine:
npm run build
```

Artifacts land in `release/`:

- `KafDesk-0.1.0-mac-arm64.dmg`
- `KafDesk-0.1.0-mac-x64.dmg`
- `KafDesk-0.1.0-win-x64.exe`

Push tag `v0.1.0` to GitHub — Actions publishes a release (not draft) with `latest*.yml`, which the in-app updater (Logs → Check for updates) uses.

## Signing (optional)

Unsigned builds still work, macOS will warn via Gatekeeper. To sign:

- **macOS**: GitHub secrets `CSC_LINK` (base64 Developer ID `.p12`) + `CSC_KEY_PASSWORD`. For notarization also `APPLE_ID`, `APPLE_APP_SPECIFIC_PASSWORD`, `APPLE_TEAM_ID` and set `build.mac.notarize: true`.
- **Windows**: `CSC_LINK` (base64 `.pfx`) + `CSC_KEY_PASSWORD`.

Locally the same env vars are picked up by `electron-builder`.

## Notes

- Avro produce needs Schema Registry + subject.
- Keycloak SSO needs a public OIDC client + PKCE and redirect `http://127.0.0.1:*/callback` (ask IdP admins).
- After `npm install`, postinstall renames the macOS Electron bundle to **KafDesk.app** (Dock name). Fully quit the old process and restart `npm run dev`.
