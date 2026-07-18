#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAUNCHER="$ROOT_DIR/start_bot.command"
APP_DIR="$HOME/Applications"
APP_PATH="$APP_DIR/Telegrambot Uploader.app"

[[ "$(uname -s)" == "Darwin" ]] || { echo "Dieses Skript ist nur für macOS." >&2; exit 1; }
command -v osacompile >/dev/null 2>&1 || { echo "osacompile wurde nicht gefunden." >&2; exit 1; }
[[ -x "$LAUNCHER" ]] || { echo "start_bot.command fehlt oder ist nicht ausführbar." >&2; exit 1; }

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

printf '✅ App erstellt: %s\n' "$APP_PATH"
printf 'Du kannst sie aus ~/Applications in das Dock ziehen.\n'
