#!/bin/bash
# Clear Gatekeeper quarantine so the first open is not a multi-minute scan.
# Double-click this after dragging KafDesk.app into Applications.
set -euo pipefail
APP="/Applications/KafDesk.app"
if [[ ! -d "$APP" ]]; then
  osascript -e 'display dialog "Сначала перетащи KafDesk.app в Applications (Replace), потом снова запусти этот скрипт." buttons {"OK"} default button 1 with title "KafDesk"'
  exit 1
fi
xattr -cr "$APP" 2>/dev/null || true
open "$APP"
