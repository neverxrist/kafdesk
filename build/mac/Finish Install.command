#!/bin/bash
# Clear Gatekeeper quarantine so the first open is not a multi-minute scan.
# Double-click this AFTER dragging KafDesk.app into Applications (do not open the app first).
set -euo pipefail
APP="/Applications/KafDesk.app"
DOWNLOADS="${HOME}/Downloads"

if [[ ! -d "$APP" ]]; then
  osascript -e 'display dialog "Сначала перетащи KafDesk.app в Applications (Replace), потом снова запусти этот скрипт." buttons {"OK"} default button 1 with title "KafDesk"'
  exit 1
fi

xattr -cr "$APP" 2>/dev/null || true

# Unmount installer volume(s), then remove DMG + helpers from Downloads
for vol in /Volumes/KafDesk*; do
  if [[ -d "$vol" ]]; then
    hdiutil detach "$vol" -quiet 2>/dev/null || hdiutil detach "$vol" -force 2>/dev/null || true
  fi
done
sleep 0.4
rm -f "$DOWNLOADS"/KafDesk*.dmg 2>/dev/null || true
rm -f "$DOWNLOADS"/KafDesk\ Finish\ Install.command 2>/dev/null || true
rm -f "$DOWNLOADS"/KafDesk-after-install.txt 2>/dev/null || true

open "$APP"

# Self-delete if launched from Downloads copy
SELF="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"
if [[ "$SELF" == "$DOWNLOADS/"* ]]; then
  rm -f "$SELF" 2>/dev/null || true
fi
