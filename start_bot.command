#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$ROOT_DIR/bot/bot.pid"
cd "$ROOT_DIR"

show_error() {
  /usr/bin/osascript -e "display alert \"Telegrambot Uploader\" message \"$1\" as critical" >/dev/null 2>&1 || true
}

if [[ ! -x venv/bin/python ]]; then
  show_error "Die Python-Umgebung fehlt. Bitte zuerst setup_mac.sh ausführen."
  exit 1
fi

if [[ ! -f .env ]]; then
  show_error "Die .env-Datei fehlt. Bitte Zugangsdaten eintragen."
  exit 1
fi

mkdir -p bot
if [[ -f "$PID_FILE" ]]; then
  OLD_PID="$(tr -cd '0-9' < "$PID_FILE")"
  if [[ -n "$OLD_PID" ]] && kill -0 "$OLD_PID" 2>/dev/null; then
    /usr/bin/osascript -e 'display notification "Der Bot läuft bereits." with title "Telegrambot Uploader"' >/dev/null 2>&1 || true
    exit 0
  fi
fi

printf '%s\n' "$$" > "$PID_FILE"
cleanup() { find "$PID_FILE" -maxdepth 0 -type f -delete 2>/dev/null || true; }
trap cleanup EXIT INT TERM

printf '\nTelegrambot Uploader startet …\nProjekt: %s\n\n' "$ROOT_DIR"
venv/bin/python bot.py
