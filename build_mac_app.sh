#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAUNCHER="$ROOT_DIR/start_bot.command"
ICON_SOURCE="$ROOT_DIR/assets/app-icon.png"
APP_DIR="$HOME/Applications"
APP_PATH="$APP_DIR/Telegrambot Camoufox.app"

[[ "$(uname -s)" == "Darwin" ]] || { echo "Dieses Skript ist nur für macOS." >&2; exit 1; }
command -v osacompile >/dev/null 2>&1 || { echo "osacompile wurde nicht gefunden." >&2; exit 1; }
command -v sips >/dev/null 2>&1 || { echo "sips wurde nicht gefunden." >&2; exit 1; }
command -v iconutil >/dev/null 2>&1 || { echo "iconutil wurde nicht gefunden." >&2; exit 1; }
command -v codesign >/dev/null 2>&1 || { echo "codesign wurde nicht gefunden." >&2; exit 1; }
[[ -x "$LAUNCHER" ]] || { echo "start_bot.command fehlt oder ist nicht ausführbar." >&2; exit 1; }
[[ -f "$ICON_SOURCE" ]] || { echo "App-Logo fehlt: $ICON_SOURCE" >&2; exit 1; }

mkdir -p "$APP_DIR"
if [[ -e "$APP_PATH" ]]; then
  find "$APP_PATH" -depth -delete
fi

ESCAPED_LAUNCHER="${LAUNCHER//\\/\\\\}"
ESCAPED_LAUNCHER="${ESCAPED_LAUNCHER//\"/\\\"}"

osacompile -o "$APP_PATH" \
  -e 'on run' \
  -e 'tell application "Terminal"' \
  -e 'activate' \
  -e "do script quoted form of \"$ESCAPED_LAUNCHER\"" \
  -e 'end tell' \
  -e 'end run'

# macOS benötigt ein ICNS-Paket mit mehreren Auflösungen. Das Quell-PNG bleibt
# im Repository, damit jede Installation dasselbe App-Logo erzeugt.
ICON_WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$ICON_WORK_DIR"' EXIT
ICONSET="$ICON_WORK_DIR/AppIcon.iconset"
mkdir -p "$ICONSET"

for SIZE in 16 32 128 256 512; do
  sips -z "$SIZE" "$SIZE" "$ICON_SOURCE" --out "$ICONSET/icon_${SIZE}x${SIZE}.png" >/dev/null
  DOUBLE_SIZE=$((SIZE * 2))
  sips -z "$DOUBLE_SIZE" "$DOUBLE_SIZE" "$ICON_SOURCE" --out "$ICONSET/icon_${SIZE}x${SIZE}@2x.png" >/dev/null
done

iconutil -c icns "$ICONSET" -o "$APP_PATH/Contents/Resources/applet.icns"
touch "$APP_PATH"
codesign --force --deep --sign - "$APP_PATH" >/dev/null

printf '✅ App erstellt: %s\n' "$APP_PATH"
printf '✅ Eigenes App-Logo installiert.\n'
printf 'Du kannst sie aus ~/Applications in das Dock ziehen.\n'
